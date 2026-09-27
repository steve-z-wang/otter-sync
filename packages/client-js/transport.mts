import type { Transport } from "./connection.mts";
/** The backend path of every runtime HTTP route. A route not listed here is refused, never posted to another path. */
const ROUTES: Readonly<Record<string, string>> = {
  push: "mutations",
  pull: "pull",
  action: "actions",
  fetch: "fetch",
  load: "loads",
};
/** Transport for a backend started with `listen`. Errors carry `status` so `refreshAuth` can react to 401. */
export function httpTransport(options: {
  url: string;
  token: string | (() => string | Promise<string>);
}): Transport {
  const base = options.url.replace(/\/$/, "");
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
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
      },
      body,
      signal: signal ?? null,
    });
    if (!response.ok)
      throw Object.assign(
        Error(`${kind} failed: ${response.status} ${await response.text()}`),
        { status: response.status },
      );
    return response.text();
  };
}
