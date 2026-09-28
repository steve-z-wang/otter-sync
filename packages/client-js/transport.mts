import type { Transport } from "./connection.mts";
/** The backend path of every runtime HTTP route. A route not listed here is refused, never posted to another path. */
const ROUTES: Readonly<Record<string, string>> = {
  push: "mutations",
  pull: "pull",
  action: "actions",
  fetch: "fetch",
  load: "loads",
};
/** The response header that marks an admission refusal (`refused`). */
export const ADMISSION_HEADER = "axton-admission";
/** Headers AXTON sets itself: the credential, the body and the WebSocket handshake. */
const RESERVED =
  /^(authorization|content-type|content-length|host|connection|upgrade|sec-websocket-.*)$/i;
/**
 * The application's headers for every request and the live upgrade, checked
 * once: string values, and none of the headers AXTON sets itself.
 */
export function clientHeaders(
  headers: Readonly<Record<string, string>> | undefined,
): Record<string, string> {
  const checked: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers ?? {})) {
    if (RESERVED.test(name)) throw Error(`reserved header ${name}`);
    if (typeof value !== "string")
      throw Error(`header ${name} must be a string`);
    checked[name] = value;
  }
  return checked;
}
/**
 * A non-2xx answer as an error carrying its `status`; an answer marked as an
 * admission refusal also carries its body as `refusal`.
 */
export function responseError(
  what: string,
  status: number,
  text: string,
  refused: boolean,
): Error {
  return Object.assign(
    Error(`${what} failed: ${status} ${text}`),
    refused ? { status, refusal: text } : { status },
  );
}
/** Transport for a backend started with `listen`. Errors carry `status` so `refreshAuth` can react to 401, and `refusal` when the server refused admission. */
export function httpTransport(options: {
  url: string;
  token: string | (() => string | Promise<string>);
  headers?: Readonly<Record<string, string>> | undefined;
}): Transport {
  const base = options.url.replace(/\/$/, "");
  const headers = clientHeaders(options.headers);
  return async (kind, body, signal) => {
    const route = Object.hasOwn(ROUTES, kind) ? ROUTES[kind] : undefined;
    if (route === undefined) throw Error(`unknown route ${kind}`);
    const token =
      typeof options.token === "function"
        ? await options.token()
        : options.token;
    if (signal?.aborted) throw Error("connection_closed");
    const response = await fetch(`${base}/sync/${route}`, {
      method: "POST",
      headers: {
        ...headers,
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body,
      signal: signal ?? null,
    });
    if (!response.ok)
      throw responseError(
        kind,
        response.status,
        await response.text(),
        response.headers.get(ADMISSION_HEADER) === "refused",
      );
    return response.text();
  };
}
