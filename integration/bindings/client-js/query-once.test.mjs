// Query once through the real native runtime (#158, #134): Rust decides
// Cached / Join / Fetch, runs the one request of a flight and completes every
// joined caller; this host executes the request effect and decodes an
// independent result for every caller.
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CallError } from "../../../packages/client-js/actions.mts";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-js/transaction.mts";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const fields = [
  { name: "id", type: { kind: "scalar", name: "string" }, nullable: false },
  { name: "title", type: { kind: "scalar", name: "string" }, nullable: false },
];
const handlerType = {
  kind: "identity",
  model: "Todo",
  fields: [{ name: "id", type: { kind: "scalar", name: "string" } }],
};
export const schema = {
  enums: [],
  models: [{ name: "Todo", version: 1, identity: ["id"], fields }],
  resultModels: [
    { name: "Todo", version: 1, identity: ["id"], fields, enums: [] },
  ],
  actions: [
    {
      name: "GetTodos",
      version: 1,
      kind: "query",
      inputs: [
        {
          kind: "value",
          name: "project",
          type: { kind: "scalar", name: "string" },
          nullable: false,
        },
      ],
      outputs: [
        {
          name: "todos",
          kind: "model",
          model: "Todo",
          modelReadVersion: 1,
          cardinality: "list",
          source: "handlerIdentity",
          handlerType,
        },
        {
          name: "tags",
          kind: "value",
          type: { kind: "scalar", name: "string" },
          cardinality: "list",
          source: "handlerValue",
        },
        {
          name: "asOf",
          kind: "value",
          type: { kind: "scalar", name: "dateTime" },
          cardinality: "single",
          source: "handlerValue",
        },
      ],
    },
    { name: "Ping", version: 1, inputs: [], outputs: [] },
  ],
};

/** Keeps the raw arrays: independence must come from the host, not the decoder. */
const decode = (value) => ({
  todos: value.todos,
  tags: value.tags,
  asOf: new Date(value.asOf),
});

function deferred() {
  let resolve, reject;
  const promise = new Promise((a, b) => {
    resolve = a;
    reject = b;
  });
  return { promise, resolve, reject };
}

/**
 * A client whose direct carrier counts requests. `respond(body, n)` builds
 * each response; `gates` (if set) holds the n-th request until released.
 */
export async function harness(body, options = {}) {
  const directory = await mkdtemp(join(tmpdir(), "axton-query-once-"));
  const path = join(directory, "db");
  const state = { requests: 0, gates: new Map(), fail: new Set() };
  const respond = (request, n) => {
    const title = `v${n}`;
    return JSON.stringify({
      completion: {
        callId: request.call.callId,
        outcome: {
          status: "succeeded",
          result: {
            todos: [{ id: "a", title }],
            tags: [title, "x"],
            asOf: "2026-01-02T03:04:05.000Z",
          },
        },
      },
      records:
        request.call.store === false
          ? []
          : [
              {
                model: "Todo",
                identity: { id: "a" },
                stamp: n,
                state: { title },
              },
            ],
    });
  };
  const wrapped = options.native ?? native;
  const DirectClient = createClient(wrapped, Transaction, () => ({
    open() {},
    push: async (kind, text) => {
      assert.equal(kind, "action");
      const n = ++state.requests;
      const gate = state.gates.get(n);
      if (gate) await gate.promise;
      if (state.fail.has(n)) throw Error(`network down ${n}`);
      return respond(JSON.parse(text), n);
    },
  }));
  const open = () => DirectClient.open({ path, schema });
  const client = await open();
  try {
    await body({ client, state, open, deferred });
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
}

const connect = (client) =>
  client.connect({ url: "http://unused", token: "token" });
const once = (client, project = "p", options = {}) =>
  client.invokeQuery("GetTodos", 1, { project }, decode, {
    once: true,
    ...options,
  });

test("concurrent once callers share one request and decode independent results", async () => {
  // The native batch that asks for the fetched flight's request also starts
  // another caller at that very moment: it must join the registered flight.
  let racing;
  let client;
  const wrapped = {
    runtimeOpen: (request, wake) => native.runtimeOpen(request, wake),
    runtimeSubmit: (runtimeId, message) =>
      native.runtimeSubmit(runtimeId, message),
    runtimeDrain(runtimeId) {
      const batch = native.runtimeDrain(runtimeId);
      const fetched = JSON.parse(batch).some(
        (event) =>
          event.type === "effect" &&
          event.operation.kind === "http" &&
          event.operation.route === "action",
      );
      if (fetched && !racing) racing = once(client);
      return batch;
    },
    runtimeDetach: (runtimeId) => native.runtimeDetach(runtimeId),
  };
  await harness(
    async ({ client: opened, state, deferred }) => {
      client = opened;
      const connection = await connect(client);
      const gate = deferred();
      state.gates.set(1, gate);
      const first = once(client);
      const second = once(client);
      const byKeyOrder = client.invokeQuery(
        "GetTodos",
        1,
        { project: "p" },
        decode,
        { once: true, store: true },
      );
      await new Promise((resolve) => setTimeout(resolve, 20));
      gate.resolve();
      const results = await Promise.all([first, second, byKeyOrder, racing]);
      assert.equal(state.requests, 1, "one network request");
      for (const result of results) {
        assert.deepEqual(result.todos, [{ id: "a", title: "v1" }]);
        assert.deepEqual(result.tags, ["v1", "x"]);
        assert.equal(result.asOf.toISOString(), "2026-01-02T03:04:05.000Z");
      }
      // Mutating one caller's arrays, objects or Date affects no one else.
      results[0].todos[0].title = "mutated";
      results[0].tags.push("mutated");
      results[0].asOf.setUTCFullYear(1999);
      assert.equal(results[1].todos[0].title, "v1");
      assert.deepEqual(results[1].tags, ["v1", "x"]);
      assert.equal(results[1].asOf.getUTCFullYear(), 2026);
      assert.notStrictEqual(results[0].todos, results[1].todos);
      const hit = await once(client);
      assert.equal(state.requests, 1, "a hit issues no request");
      assert.deepEqual(hit.todos, [{ id: "a", title: "v1" }]);
      assert.deepEqual(hit.tags, ["v1", "x"]);
      await connection.close();
    },
    { native: wrapped },
  );
});

test("a hit needs no carrier, even after reopen; a miss or refresh without one fails", async () => {
  await harness(async ({ client, state, open }) => {
    const connection = await connect(client);
    await once(client);
    await connection.close();
    assert.deepEqual((await once(client)).todos, [{ id: "a", title: "v1" }]);
    await assert.rejects(
      once(client, "other"),
      (error) =>
        error instanceof CallError && error.code === "action.unavailable",
    );
    await assert.rejects(
      once(client, "p", { refresh: true }),
      (error) =>
        error instanceof CallError && error.code === "action.unavailable",
    );
    // A released offline miss leaves nothing behind: it fetches when online.
    const reopened = await open();
    try {
      assert.deepEqual((await once(reopened)).tags, ["v1", "x"]);
      assert.equal(
        (await reopened.syncState()).pending,
        0,
        "never enqueued implicitly",
      );
    } finally {
      await reopened.close();
    }
    assert.equal(state.requests, 1);
  });
});

test("refresh requires once and bad options fail before any I/O", async () => {
  await harness(async ({ client, state }) => {
    const connection = await connect(client);
    const invalid = (error) =>
      error instanceof CallError &&
      error.code === "action.invalid_options" &&
      error.execution === "rejected";
    await assert.rejects(
      client.invokeQuery("GetTodos", 1, { project: "p" }, decode, {
        refresh: true,
      }),
      invalid,
    );
    await assert.rejects(
      client.invokeQuery("GetTodos", 1, { project: "p" }, decode, {
        once: "yes",
      }),
      invalid,
    );
    // Once controls are not accepted by the Mutation or enqueue routes.
    await assert.rejects(
      client.invokeDirectAction("Ping", 1, {}, () => undefined, {
        once: true,
      }),
      invalid,
    );
    await assert.rejects(
      client.invokeAction("GetTodos", 1, { project: "p" }, decode, {
        once: true,
      }),
      invalid,
    );
    await assert.rejects(
      client.invokeAction("Ping", 1, {}, () => undefined, { refresh: false }),
      invalid,
    );
    assert.equal(state.requests, 0);
    assert.equal((await client.syncState()).pending, 0);
    // A Mutation cannot be forged onto the once route.
    await assert.rejects(
      client.invokeQuery("Ping", 1, {}, () => undefined, { once: true }),
      (error) => error instanceof CallError,
    );
    assert.equal(state.requests, 0);
    await connection.close();
  });
});

test("default calls stay fresh and independent of the once snapshot", async () => {
  await harness(async ({ client, state }) => {
    const connection = await connect(client);
    const plain = await client.invokeQuery(
      "GetTodos",
      1,
      { project: "p" },
      decode,
    );
    assert.deepEqual(plain.tags, ["v1", "x"]);
    await client.invokeQuery("GetTodos", 1, { project: "p" }, decode, {});
    assert.equal(state.requests, 2, "every default call is a request");
    const first = await once(client);
    assert.equal(state.requests, 3, "default calls populated nothing");
    assert.deepEqual(first.tags, ["v3", "x"]);
    await client.invokeQuery("GetTodos", 1, { project: "p" }, decode);
    assert.equal(state.requests, 4);
    assert.deepEqual((await once(client)).tags, ["v3", "x"], "not replaced");
    const refreshed = await once(client, "p", { refresh: true });
    assert.equal(state.requests, 5);
    assert.deepEqual(refreshed.tags, ["v5", "x"]);
    assert.deepEqual((await once(client)).tags, ["v5", "x"]);
    await connection.close();
  });
});

test("a failed refresh settles every waiter, releases its flight and keeps the snapshot", async () => {
  await harness(async ({ client, state, deferred }) => {
    const connection = await connect(client);
    await once(client);
    const gate = deferred();
    state.gates.set(2, gate);
    state.fail.add(2);
    const refreshing = [
      once(client, "p", { refresh: true }),
      once(client, "p", { refresh: true }),
    ];
    // A plain once still hits the old snapshot while the refresh is out.
    assert.deepEqual((await once(client)).tags, ["v1", "x"]);
    gate.resolve();
    for (const waiter of refreshing)
      await assert.rejects(
        waiter,
        (error) =>
          error instanceof CallError &&
          error.code === "action.execution_unknown" &&
          error.cause?.message === "network down 2",
      );
    assert.equal(state.requests, 2);
    assert.deepEqual((await once(client)).tags, ["v1", "x"]);
    const retried = await once(client, "p", { refresh: true });
    assert.equal(state.requests, 3, "a retry is a new request");
    assert.deepEqual(retried.tags, ["v3", "x"]);
    await connection.close();
  });
});

test("invalidation forces a miss and fences an older in-flight result", async () => {
  await harness(async ({ client, state, deferred }) => {
    const connection = await connect(client);
    const gate = deferred();
    state.gates.set(1, gate);
    const older = once(client);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(
      await client.invalidateQuery("GetTodos", 1, { project: "p" }),
      undefined,
    );
    const newer = once(client);
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(state.requests, 2, "the new generation never joins");
    gate.resolve();
    assert.deepEqual((await older).tags, ["v1", "x"], "old callers resolve");
    assert.deepEqual((await newer).tags, ["v2", "x"]);
    assert.deepEqual((await once(client)).tags, ["v2", "x"]);
    await client.invalidateQuery("GetTodos", 1, { project: "p" });
    assert.deepEqual((await once(client)).tags, ["v3", "x"]);
    assert.equal(state.requests, 3);
    await connection.close();
  });
});

test("once and invalidate refuse an active transaction callback, even through a captured client", async () => {
  await harness(async ({ client, state }) => {
    const connection = await connect(client);
    await once(client);
    await client.transaction(async () => {
      await assert.rejects(
        once(client),
        (error) =>
          error instanceof CallError && error.code === "transaction_active",
      );
      await assert.rejects(
        client.invalidateQuery("GetTodos", 1, { project: "p" }),
        (error) =>
          error instanceof CallError && error.code === "transaction_active",
      );
    });
    assert.equal(state.requests, 1);
    await connection.close();
  });
});

test("closing settles a waiting once caller", async () => {
  await harness(async ({ client, state, deferred }) => {
    await connect(client);
    const gate = deferred();
    state.gates.set(1, gate);
    const waiting = once(client);
    await new Promise((resolve) => setTimeout(resolve, 20));
    const closing = client.close();
    await assert.rejects(
      waiting,
      (error) => error instanceof CallError && error.code === "client.closed",
    );
    gate.resolve();
    await closing;
  });
});
