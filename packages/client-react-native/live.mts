import { httpTransport } from "../client-js/transport.mts";
import type { ServerOptions, ServerConnection } from "../client-js/live.mts";

/** The native WebSocket API has no pause/resume; the Rust controller handles lost-frame recovery. */
interface Socket {
  onopen: (() => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onerror: ((event: { message?: string }) => void) | null;
  onclose: ((event: { code: number; reason?: string }) => void) | null;
  send(data: string): void;
  close(): void;
}
type SocketConstructor = new (
  url: string,
  protocols: string[],
  options: { headers: Record<string, string> },
) => Socket;

export function createServerConnection(
  options: ServerOptions,
  SocketClass = globalThis.WebSocket as unknown as SocketConstructor,
): ServerConnection {
  const base = new URL(options.url.replace(/\/$/, "") + "/sync/live");
  base.protocol =
    base.protocol === "https:" || base.protocol === "wss:" ? "wss:" : "ws:";
  const http = new URL(options.url);
  http.protocol =
    http.protocol === "https:" || http.protocol === "wss:" ? "https:" : "http:";
  return {
    push: httpTransport({
      ...options,
      url: http.toString().replace(/\/$/, ""),
    }),
    open(subscribe, signal, on) {
      let socket: Socket | undefined;
      let ended = false;
      let draining = false;
      let overflowed = false;
      const frames: string[] = [];
      const finish = (error?: unknown) => {
        if (ended) return;
        ended = true;
        frames.length = 0;
        signal.removeEventListener("abort", cancel);
        if (socket) {
          socket.onopen =
            socket.onmessage =
            socket.onerror =
            socket.onclose =
              null;
          try {
            socket.close();
          } catch {
            /* The socket may already be closed. */
          }
        }
        if (error !== undefined) on.closed(error);
      };
      const cancel = () => finish();
      const drain = async () => {
        if (draining || ended) return;
        draining = true;
        try {
          while (!ended && (overflowed || frames.length)) {
            if (overflowed) {
              overflowed = false;
              await on.overflow();
            } else await on.message(frames.shift()!);
          }
        } catch (error) {
          finish(error);
        } finally {
          draining = false;
        }
      };
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) return cancel();
      void Promise.resolve()
        .then(() =>
          typeof options.token === "function" ? options.token() : options.token,
        )
        .then((token) => {
          if (ended) return;
          socket = new SocketClass(base.toString(), [], {
            headers: { authorization: `Bearer ${token}` },
          });
          socket.onopen = () => {
            if (ended) return;
            try {
              socket!.send(subscribe);
            } catch (error) {
              finish(error);
            }
          };
          socket.onerror = (event) =>
            finish(Error(event.message ?? "live connection failed"));
          socket.onclose = (event) =>
            finish(
              Error(`live disconnected: ${event.code} ${event.reason ?? ""}`),
            );
          socket.onmessage = (event) => {
            if (ended) return;
            // The frame bound Node's `ws` enforces with `maxPayload`.
            if (
              typeof event.data !== "string" ||
              event.data.length > 8 * 1024 * 1024
            ) {
              finish(Error("invalid live message"));
              return;
            }
            if (frames.length === 64) {
              frames.length = 0;
              overflowed = true;
            }
            frames.push(event.data);
            void drain();
          };
        })
        .catch(finish);
    },
  };
}
