// Transport-only harness: every state change enters the public native actor.
import { setImmediate } from "node:timers/promises";
import { createProxy } from "./proxy.mts";
export { createProxy };
export async function wait(predicate, label, timeout = 20000) {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await setImmediate();
  }
  throw Error(`Timed out waiting for ${label}`);
}
export const connection = (url, viewer = "demo-user") => ({
  url,
  token: viewer,
});
export const intent = (exchange) => JSON.parse(exchange.body);
export const mutation = (exchange) =>
  exchange.path === "/sync/mutations" &&
  intent(exchange).mutations.some((mutation) => mutation.name === "EditEntry");
