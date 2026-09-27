// The scripted backend and schema of the native Load binding tests (#173),
// shared by the Node and React Native suites. Not a test file itself.
import assert from "node:assert/strict";

const fields = [
  { name: "id", nullable: false, type: { kind: "scalar", name: "string" } },
  { name: "text", nullable: false, type: { kind: "scalar", name: "string" } },
  { name: "note", nullable: true, type: { kind: "scalar", name: "string" } },
];
const input = (name, scalar, nullable) => ({
  kind: "value",
  name,
  type: { kind: "scalar", name: scalar },
  nullable,
  list: false,
  required: true,
  cardinality: "single",
});
const load = (name, inputs) => ({
  name,
  version: 1,
  inputs,
  outputs: [
    {
      name: "entries",
      kind: "model",
      cardinality: "list",
      source: "handlerIdentity",
      model: "Entry",
      modelReadVersion: 1,
      handlerType: {
        kind: "identity",
        model: "Entry",
        fields: [{ name: "id", type: { kind: "scalar", name: "string" } }],
      },
    },
  ],
  input: { models: [], enums: [] },
  outputEnums: [],
});
export const schema = {
  enums: [],
  models: [{ name: "Entry", version: 1, identity: ["id"], fields }],
  resultModels: [
    { name: "Entry", version: 1, identity: ["id"], fields, enums: [] },
  ],
  loads: [
    load("Entries", [
      input("projectId", "uuid", false),
      input("since", "dateTime", true),
    ]),
    load("Recent", []),
  ],
};
export const PROJECT = "0190f0e0-1111-7222-8333-444455556666";
export const args = { projectId: PROJECT, since: null };

/** A succeeded page answering `intent` with Entry rows `[id, text]`. */
export function page(intent, rows, next = null) {
  return {
    loadId: intent.loadId,
    callId: intent.callId,
    outcome: {
      status: "succeeded",
      data: { entries: rows.map(([id]) => ({ id })) },
      next,
    },
    records: rows.map(([id, text], i) => ({
      model: "Entry",
      identity: { id },
      stamp: i + 1,
      state: { text, note: null },
    })),
  };
}
export function failed(intent, code) {
  return {
    loadId: intent.loadId,
    callId: intent.callId,
    outcome: { status: "failed", error: { code, message: "refused" } },
    records: [],
  };
}
export function deferred() {
  let resolve;
  const promise = new Promise((r) => (resolve = r));
  return { promise, resolve };
}
export async function until(predicate, what = "condition") {
  for (let i = 0; i < 500; i++) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  assert.fail(`timed out waiting for ${what}`);
}

/**
 * A backend that answers every `load` batch with `server.answer(intent)`, by
 * default one final page with one Entry per job. `server.gate` holds the
 * next batch; `server.error` fails it with that error.
 */
export function backend() {
  const server = {
    batches: [],
    gate: undefined,
    error: undefined,
    answer: (intent) => page(intent, [[intent.loadId.slice(0, 8), "loaded"]]),
    get intents() {
      return server.batches.flatMap((batch) => batch.loads);
    },
  };
  const connection = () => ({
    open() {},
    push: async (kind, text) => {
      assert.equal(kind, "load");
      const body = JSON.parse(text);
      server.batches.push(body);
      const gate = server.gate;
      if (gate) await gate.promise;
      if (server.error) throw server.error;
      return JSON.stringify({ loads: body.loads.map(server.answer) });
    },
  });
  return { server, connection };
}
