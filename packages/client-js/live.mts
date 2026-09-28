import WebSocket from "ws";
import {
  ADMISSION_HEADER,
  clientHeaders,
  httpTransport,
  responseError,
} from "./transport.mts";
import type { SocketEvents, Transport } from "./connection.mts";

export type ServerConnection = {
  /**
   * HTTP by route: `push` to `/sync/mutations`, `pull` to `/sync/pull`,
   * `action` to `/sync/actions`, `fetch` to `/sync/fetch` and `load` to
   * `/sync/loads`.
   */
  readonly push: Transport;
  /** Open `/sync/live`, send the subscribe frame once open, deliver frames until aborted or closed. */
  open(subscribe: string, signal: AbortSignal, on: SocketEvents): void;
};
export type ServerOptions = {
  url: string;
  token: string | (() => string | Promise<string>);
  /**
   * Sent with every request and the live upgrade, e.g. the application's
   * platform and build for the backend's `admit`. The headers AXTON sets
   * itself (`authorization`, `content-type`, the WebSocket handshake) are
   * refused.
   */
  headers?: Readonly<Record<string, string>>;
};
/** The most of a refused upgrade's body that is read. */
const REFUSAL_BYTES = 64 * 1024;
/** Frames waiting to be handed to Rust while the previous one is; beyond this the buffer is dropped and the worker recovers. */
const BUFFERED_FRAMES = 64;
/** Sockets and HTTP for one server; no sync decisions. */
export function createServerConnection(
  options: ServerOptions,
): ServerConnection {
  const base = new URL(options.url.replace(/\/$/, "") + "/sync/live");
  base.protocol =
    base.protocol === "https:" || base.protocol === "wss:" ? "wss:" : "ws:";
  const http = new URL(options.url);
  http.protocol =
    http.protocol === "wss:" || http.protocol === "https:" ? "https:" : "http:";
  const headers = clientHeaders(options.headers);
  return {
    push: httpTransport({
      ...options,
      url: http.toString().replace(/\/$/, ""),
    }),
    open(subscribe, signal, on) {
      let socket: WebSocket | undefined;
      let ended = false;
      const frames: string[] = [];
      let overflowed = false;
      let draining = false;
      const finish = (error?: unknown) => {
        if (ended) return;
        ended = true;
        signal.removeEventListener("abort", cancel);
        // terminate also cancels an upgrade still in progress. Keep the error listener
        // installed until close, since ws emits an error when terminating CONNECTING.
        socket?.terminate();
        if (error !== undefined) on.closed(error);
      };
      const cancel = () => finish();
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) return cancel();
      const drain = async () => {
        if (draining || ended) return;
        draining = true;
        socket?.pause();
        try {
          while (!ended && (overflowed || frames.length)) {
            if (overflowed) {
              overflowed = false;
              await on.overflow();
            } else {
              await on.message(frames.shift()!);
            }
          }
        } catch (error) {
          finish(error);
        } finally {
          draining = false;
          if (!ended) socket?.resume();
        }
      };
      void Promise.resolve()
        .then(() =>
          typeof options.token === "function" ? options.token() : options.token,
        )
        .then((token) => {
          if (ended) return;
          socket = new WebSocket(base, {
            headers: { ...headers, authorization: `Bearer ${token}` },
            maxPayload: 8 * 1024 * 1024,
          });
          const current = socket;
          current.on("error", (error) => finish(error));
          current.on("unexpected-response", (_request, response) => {
            // A refused upgrade: its body is read only when the server marked
            // it as an admission refusal, whose body the runtime reports.
            const status = response.statusCode ?? 0;
            if (response.headers[ADMISSION_HEADER] !== "refused") {
              response.resume();
              finish(responseError("live", status, "", false));
              return;
            }
            let text = "";
            response.setEncoding("utf8");
            response.on("data", (chunk: string) => {
              text += chunk;
              if (text.length > REFUSAL_BYTES) response.destroy();
            });
            response.on("end", () =>
              finish(responseError("live", status, text, true)),
            );
            response.on("error", (error) => finish(error));
            response.on("close", () =>
              finish(responseError("live", status, text, false)),
            );
          });
          current.on("close", (code, reason) =>
            finish(Error(`live disconnected: ${code} ${reason}`)),
          );
          current.on("open", () => {
            if (ended) return current.terminate();
            current.send(subscribe);
          });
          current.on("message", (data) => {
            if (ended) return;
            if (frames.length === BUFFERED_FRAMES) {
              // Keep the socket and the in-flight HTTP request alive: the worker
              // recovers from the durable cursor instead of starting over.
              frames.length = 0;
              overflowed = true;
            }
            frames.push(data.toString());
            void drain();
          });
        })
        .catch(finish);
    },
  };
}
