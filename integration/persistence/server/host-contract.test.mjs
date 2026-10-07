// Every retained host operation traverses the real TypeScript callback.
import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import {
  createBackend,
  MutationRejected,
} from "../../../packages/server/index.mts";
import { HOST_OPERATIONS } from "../../../packages/server/host-contract.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const fixture = JSON.parse(
  await readFile(
    new URL("../../../fixtures/protocol/host-operations.json", import.meta.url),
    "utf8",
  ),
);
const entry = (op) => fixture.operations.find((e) => e.op === op);
const first = (op) => entry(op).responses[0].value;
const config = {
  schema: {
    enums: [],
    models: [
      {
        name: "Task",
        version: 1,
        identity: ["id"],
        fields: [
          {
            name: "id",
            nullable: false,
            type: { kind: "scalar", name: "string" },
          },
          {
            name: "title",
            nullable: false,
            type: { kind: "scalar", name: "string" },
          },
        ],
      },
    ],
    actions: [
      {
        name: "Send",
        version: 1,
        kind: "mutation",
        inputs: [],
        outputs: [
          {
            name: "message",
            kind: "value",
            cardinality: "single",
            source: "handlerValue",
            type: { kind: "scalar", name: "string" },
          },
        ],
      },
    ],
  },
  mutations: [],
};
async function replay(
  requests,
  { reject = false, fail = false, loader, handler, onError, persisted } = {},
) {
  const answers = [],
    seen = [],
    loaded = [],
    calls = [];
  const backend = createBackend({
    config,
    protocol5: { authorizeStream: () => true },
    authenticate: () => "alice",
    onError,
    database: {
      transaction: (body) => body({}),
      persistence: () => ({
        call: async (r) => {
          seen.push(r);
          return persisted === undefined ? first(r.op) : persisted;
        },
      }),
    },
    native: {
      ...native,
      validateMutationBatch: (_c, r) => r,
      processBatchMember: async (_c, _p, _r, _o, callback) => {
        await callback(
          JSON.stringify({
            op: "protocol05",
            request: {
              op: "admit",
              owner: "alice",
              context: {
                protocol: 5,
                storeId: "s",
                stream: "User:alice",
                materialization: "m",
              },
            },
          }),
        );
        for (const r of requests)
          answers.push([r.op, JSON.parse(await callback(JSON.stringify(r)))]);
        return "{}";
      },
      encodeBatchAcknowledgement: () => "{}",
    },
    mutations: {
      send: async (call) => {
        calls.push(call);
        if (reject) throw new MutationRejected("send.denied");
        if (fail) throw new Error("handler broke");
        return handler ? handler(call) : { message: "sent" };
      },
    },
    loaders: {
      task: async (call) => {
        loaded.push(call);
        return loader ? loader(call) : first("load");
      },
    },
  });
  await backend.push("alice", JSON.stringify({ protocol: 5, mutations: [{}] }));
  return { answers, seen, loaded, calls };
}
test("fixture and TypeScript host enumerate each retained operation once", () => {
  assert.deepEqual(
    fixture.operations.map((e) => e.op).sort(),
    [...HOST_OPERATIONS].sort(),
  );
  assert.equal(
    new Set(fixture.operations.map((e) => e.op)).size,
    fixture.operations.length,
  );
});
test("every retained fixture operation traverses the host; every persisted response variant stays exact", async () => {
  const requests = fixture.operations.map((e) => e.request);
  const { answers, seen, loaded, calls } = await replay(requests);
  assert.deepEqual(
    answers.map(([op]) => op),
    HOST_OPERATIONS,
  );
  for (const [op, result] of answers) assert.deepEqual(result, first(op), op);
  assert.deepEqual(
    seen.map((r) => r.op),
    HOST_OPERATIONS.filter((op) => op !== "handleAction" && op !== "load"),
  );
  assert.equal(calls[0].ctx.userId, "alice");
  assert.equal(loaded[0].userId, "alice");
  assert.equal(loaded[0].stream, undefined);
  assert.equal(loaded[0].invalidate, undefined);
  assert.equal(typeof calls[0].ctx.stream.track.task, "function");
  assert.equal(typeof calls[0].ctx.streams, "function");
  for (const e of fixture.operations)
    if (e.op !== "handleAction" && e.op !== "load")
      for (const r of e.responses) {
        const replayed = await replay([e.request], { persisted: r.value });
        assert.deepEqual(replayed.answers, [[e.op, r.value]]);
      }
});
test("default authenticated Stream and explicit multi-Stream selectors capture exact declarations", async () => {
  const r = entry("handleAction").request;
  const { answers } = await replay([r], {
    handler: async ({ ctx }) => {
      ctx.stream.track.task({ id: "t" });
      ctx.streams(["A", "B"]).track.task({ id: "u" });
      return { message: "sent" };
    },
  });
  assert.deepEqual(answers[0][1].declarations, [
    {
      kind: "track",
      stream: r.context.stream,
      record: { model: "Task", identity: { id: "t" } },
    },
    {
      kind: "track",
      stream: "A",
      record: { model: "Task", identity: { id: "u" } },
    },
    {
      kind: "track",
      stream: "B",
      record: { model: "Task", identity: { id: "u" } },
    },
  ]);
});
test("handler refusal is data; infrastructure failure aborts the transaction", async () => {
  const r = entry("handleAction").request;
  assert.deepEqual((await replay([r], { reject: true })).answers, [
    ["handleAction", { rejection: "send.denied" }],
  ]);
  const errors = [];
  await assert.rejects(
    replay([r], { fail: true, onError: (e) => errors.push(e) }),
    /handler broke/,
  );
  assert.equal(errors[0].message, "handler broke");
});
test("Loaders preserve business refusal and report thrown or nonportable rows", async () => {
  const r = entry("load").request;
  const refused = await replay([r], {
    loader: async () => {
      throw new MutationRejected("task.denied");
    },
  });
  assert.deepEqual(refused.answers, [["load", { rejection: "task.denied" }]]);
  const errors = [];
  await assert.rejects(
    replay([r], {
      loader: async () => {
        throw new Error("loader broke");
      },
      onError: (e) => errors.push(e),
    }),
    /loader broke/,
  );
  assert.equal(errors[0].message, "loader broke");
  for (const loader of [
    async () => [undefined],
    async () => [{ title: NaN }],
  ]) {
    const errors = [];
    const result = await replay([r], {
      loader,
      onError: (e) => errors.push(e),
    });
    assert.equal(typeof result.answers[0][1].error, "string");
    assert.equal(errors.length, 1);
  }
});
