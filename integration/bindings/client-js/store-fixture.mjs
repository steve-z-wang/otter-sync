import { createHash } from "node:crypto";
// Open offline unless the test explicitly supplies a network connection.
export const offlineNetwork = () => ({
  open() {},
  async push() {
    throw Error("offline");
  },
});
export async function openStore(Client, options) {
  return Client.open({ ...options, stream: options.stream ?? "User:viewer" });
}

const canonical = (value) =>
  Array.isArray(value)
    ? `[${value.map(canonical).join(",")}]`
    : value && typeof value === "object"
      ? `{${Object.keys(value)
          .sort()
          .map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`)
          .join(",")}}`
      : JSON.stringify(value);
const context05 = (body) =>
  Object.fromEntries(
    ["protocol", "storeId", "stream", "materialization"].map((key) => [
      key,
      body[key],
    ]),
  );
const hash05 = (domain, value) =>
  createHash("sha256")
    .update(domain + "\0")
    .update(canonical(value))
    .digest("hex");
export function emptyHandshake(body) {
  return { protocol: 5, storeId: body.storeId, stream: body.stream, head: 0 };
}
const fixtureExpiry = Date.now() + 300000;
export function delivery05(body, changes = [], head = body.through ?? 0) {
  const through = body.through ?? null;
  const unit = { index: 0, through, changes };
  const part = {
    planId: `transport-${body.storeId}-${body.bootstrap ? "bootstrap" : "sync"}-${body.after}-${body.through}`,
    planDigest: "",
    unit: 0,
    part: 0,
    changes,
  };
  const manifest = {
    through,
    minimumCursor: changes.length
      ? Math.min(...changes.map((change) => change.cursor))
      : null,
    digest: hash05("axton:delivery-unit:5", unit),
    parts: [hash05("axton:delivery-part:5", { unit: 0, part: 0, changes })],
  };
  const header = {
    ...context05(body),
    planId: part.planId,
    digest: "",
    bootstrap: body.bootstrap ?? false,
    owner: body.owner ?? null,
    after: body.after ?? null,
    through,
    observedHead: head,
    expiresAt: fixtureExpiry,
    units: [manifest],
  };
  const { digest, ...unsigned } = header;
  header.digest = hash05("axton:delivery-plan:5", unsigned);
  part.planDigest = header.digest;
  return { header, parts: [part] };
}
export function read05(body, result = null, records = []) {
  return {
    ...context05(body),
    requestId: body.requestId,
    outcome: { kind: "succeeded", result },
    records,
  };
}
const request05 = (text) => {
  const request = typeof text === "string" ? JSON.parse(text) : text;
  if (request.protocol !== 5) throw Error("fixture requires protocol 5");
  return request;
};
export function emptyPull(text) {
  const request = request05(text);
  return JSON.stringify(request.materialization ? delivery05(request) : emptyHandshake(request));
}
export function emptyRead(text) {
  const request = request05(text);
  return JSON.stringify(read05(request, null,
    request.invocation.kind === "fetch" ? [{ key: request.invocation.key, cursor: null, state: null }] : []));
}
export function emptyMutation(text) {
  const request = request05(text);
  return JSON.stringify({
    ...context05(request), batchId: request.batchId, digest: request.digest,
    results: request.mutations.map(mutation => ({mutationId: mutation.id,
      outcome: {kind: "accepted", syncCursor: 0, result: null, targets: []}})),
  });
}
