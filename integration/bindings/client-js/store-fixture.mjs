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
export function delivery05(body, changes = [], head = body.through) {
  const unit = { index: 0, through: body.through, changes };
  const part = {
    planId: "transport-" + body.storeId,
    planDigest: "",
    unit: 0,
    part: 0,
    changes,
  };
  const manifest = {
    through: body.through,
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
    bootstrap: body.bootstrap,
    owner: null,
    after: body.after,
    through: body.through,
    observedHead: head,
    expiresAt: Date.now() + 300000,
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
export function emptyPull(text) {
  const request = typeof text === "string" ? JSON.parse(text) : text;
  if (request.protocol === 5)
    return JSON.stringify(
      request.materialization ? delivery05(request) : emptyHandshake(request),
    );
  const body = typeof text === "string" ? JSON.parse(text) : text;
  return JSON.stringify(
    body.kind === "start"
      ? { context: body.context, manifestId: "fixed", start: 0, total: 0 }
      : body.kind === "tail"
        ? { context: body.context, manifestId: body.manifestId, head: 0 }
        : {
            context: body.context,
            pageId: "empty",
            from: body.after,
            to: body.after,
            head: body.after,
            units: [],
          },
  );
}
export function emptyRead(text) {
  const request = typeof text === "string" ? JSON.parse(text) : text;
  if (request.protocol === 5)
    return JSON.stringify(
      read05(
        request,
        null,
        request.invocation.kind === "fetch"
          ? [{ key: request.invocation.key, cursor: null, state: null }]
          : [],
      ),
    );
  const body = typeof text === "string" ? JSON.parse(text) : text;
  return JSON.stringify({
    context: body.context,
    completion: {
      callId: body.callId,
      outcome: { status: "succeeded", result: null },
    },
    records: [],
  });
}
export function emptyMutation(text) {
  const request = typeof text === "string" ? JSON.parse(text) : text;
  if (request.protocol === 5)
    return JSON.stringify({
      ...context05(request),
      batchId: request.batchId,
      digest: request.digest,
      results: request.mutations.map((mutation) => ({
        mutationId: mutation.id,
        outcome: { kind: "accepted", syncCursor: 0, result: null, targets: [] },
      })),
    });
  const body = typeof text === "string" ? JSON.parse(text) : text;
  return JSON.stringify({
    context: body.context,
    intentDigest: createHash("sha256")
      .update("axton:protocol4:sha256:mutation-intent\0")
      .update(canonical(body))
      .digest("hex"),
    completion: {
      callId: body.callId,
      outcome: { status: "succeeded", result: null },
    },
    targets: [],
  });
}
