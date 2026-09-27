import type { Transport } from "./connection.mts";
/** Transport for a backend started with `listen`. Errors carry `status` so `refreshAuth` can react to 401. */
export function httpTransport(options: {
  url: string;
  token: string | (() => string | Promise<string>);
}): Transport {
  const base = options.url.replace(/\/$/, "");
  return async (kind, body, signal) => {
    const token =
      typeof options.token === "function"
        ? await options.token()
        : options.token;
    if (signal?.aborted) throw Error("connection_closed");
    const response = await fetch(
      `${base}/sync/${kind === "push" ? "mutations" : kind === "action" ? "actions" : kind === "fetch" ? "fetch" : "pull"}`,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
        },
        body,
        signal: signal ?? null,
      },
    );
    if (!response.ok)
      throw Object.assign(
        Error(`${kind} failed: ${response.status} ${await response.text()}`),
        { status: response.status },
      );
    return response.text();
  };
}
