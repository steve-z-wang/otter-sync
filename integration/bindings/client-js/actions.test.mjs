import {
  openStore,
  offlineNetwork,
  emptyPull,
  emptyRead,
  emptyMutation,
} from "./store-fixture.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import {
  ActionRegistry,
  CallError,
} from "../../../packages/client-js/actions.mts";
import { Client } from "../../../packages/client-js/index.mts";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRequire } from "node:module";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-js/transaction.mts";

test("observer retains a live wait, settles once, and caches the outcome", async () => {
  const registry = new ActionRegistry();
  const call = registry.register("one", (value) => ({ title: value.title }));
  assert.equal(call.status, "pending");
  const pending = call.wait();
  registry.complete({
    callId: "one",
    outcome: { status: "succeeded", result: { title: "A" } },
  });
  const first = await pending;
  assert.deepEqual(first, { result: { title: "A" }, error: null });
  assert.equal(call.status, "succeeded");
  assert.strictEqual(await call.wait(), first);
  assert.equal(registry.activeCount, 0);
});

test("observer converts backend failures and decoder failures into terminal outcomes", async () => {
  const registry = new ActionRegistry();
  const rejected = registry.register("rejected", (value) => value);
  const malformed = registry.register("malformed", () => {
    throw Error("bad result");
  });
  registry.complete({
    callId: "rejected",
    outcome: { status: "failed", code: "denied", execution: "rejected" },
  });
  registry.complete({
    callId: "malformed",
    outcome: { status: "succeeded", result: {} },
  });
  assert.equal((await rejected.wait()).error.code, "denied");
  assert.equal(
    (await malformed.wait()).error.code,
    "action.observation_failed",
  );
  assert.equal(malformed.status, "failed");
});

test("decoded results remain snapshots after the source object changes", async () => {
  const registry = new ActionRegistry();
  const call = registry.register("snapshot", (value) => ({
    title: value.title,
  }));
  const source = { title: "A" };
  registry.complete({
    callId: "snapshot",
    outcome: { status: "succeeded", result: source },
  });
  source.title = "B";
  assert.equal((await call.wait()).result.title, "A");
});

test("closing settles held handles even before wait is called", async () => {
  const registry = new ActionRegistry();
  const call = registry.register("one", (value) => value);
  registry.close();
  assert.equal(call.status, "failed");
  assert.equal((await call.wait()).error.code, "client.closed");
});

test("weak routing sweeps dead states but active waits remain live", async () => {
  const references = [];
  const registry = new ActionRegistry((state) => {
    const reference = {
      state,
      deref() {
        return this.state;
      },
    };
    references.push(reference);
    return reference;
  });
  registry.register("abandoned", (value) => value);
  references[0].state = undefined;
  registry.register("other", (value) => value);
  assert.equal(registry.routingCount, 1);
  let live = registry.register("live", (value) => value);
  const pending = live.wait();
  live = null;
  references[2].state = undefined;
  registry.complete({
    callId: "live",
    outcome: { status: "succeeded", result: 3 },
  });
  assert.equal((await pending).result, 3);
  assert.equal(registry.activeCount, 0);
});

const lifecycle = (code, execution) => (error) =>
  error instanceof CallError &&
  error.code === code &&
  error.execution === execution;

test("a provisional Call refuses an early wait, then observes normally once committed", async () => {
  const registry = new ActionRegistry();
  const call = registry.register("p", (value) => value, true);
  assert.equal(call.status, "pending");
  await assert.rejects(
    call.wait(),
    lifecycle("transaction_uncommitted", "unknown"),
  );
  // The refusal neither settles nor retains the Call.
  assert.equal(call.status, "pending");
  assert.equal(registry.activeCount, 0);
  assert.equal(registry.routingCount, 1);
  registry.transition("p", "committed");
  const waiting = call.wait();
  assert.equal(registry.activeCount, 1);
  registry.complete({
    callId: "p",
    outcome: { status: "succeeded", result: 1 },
  });
  assert.deepEqual(await waiting, { result: 1, error: null });
  assert.equal(call.status, "succeeded");
});

test("a rolled-back Call fails every wait and routes nothing more", async () => {
  const registry = new ActionRegistry();
  const rolled = registry.register("r", (value) => value, true);
  const kept = registry.register("k", (value) => value, true);
  registry.transition("r", "rolledBack");
  assert.equal(rolled.status, "failed");
  for (let i = 0; i < 2; i++)
    await assert.rejects(
      rolled.wait(),
      lifecycle("transaction_rolled_back", "rejected"),
    );
  assert.equal(registry.routingCount, 1);
  // A later transition or completion for it changes nothing.
  registry.transition("r", "committed");
  registry.complete({
    callId: "r",
    outcome: { status: "succeeded", result: 1 },
  });
  registry.transition("unknown", "rolledBack");
  await assert.rejects(
    rolled.wait(),
    lifecycle("transaction_rolled_back", "rejected"),
  );
  assert.equal(kept.status, "pending");
  await assert.rejects(
    kept.wait(),
    lifecycle("transaction_uncommitted", "unknown"),
  );
});

test("weak routing drops an unobserved provisional Call", () => {
  const references = [];
  const registry = new ActionRegistry((state) => {
    const reference = {
      state,
      deref() {
        return this.state;
      },
    };
    references.push(reference);
    return reference;
  });
  registry.register("abandoned", (value) => value, true);
  references[0].state = undefined;
  registry.register("other", (value) => value, true);
  assert.equal(registry.routingCount, 1);
  registry.transition("abandoned", "committed");
  registry.transition("other", "rolledBack");
  assert.equal(registry.routingCount, 0);
});

test("close leaves provisional Calls to their transition and the runtime's end rolls back the rest", async () => {
  const registry = new ActionRegistry();
  const durable = registry.register("d", (value) => value);
  const rolled = registry.register("r", (value) => value, true);
  const committed = registry.register("c", (value) => value, true);
  const orphan = registry.register("o", (value) => value, true);
  registry.close();
  assert.equal((await durable.wait()).error.code, "client.closed");
  assert.equal(rolled.status, "pending");
  registry.transition("r", "rolledBack");
  registry.transition("c", "committed");
  await assert.rejects(
    rolled.wait(),
    lifecycle("transaction_rolled_back", "rejected"),
  );
  assert.equal((await committed.wait()).error.code, "client.closed");
  assert.equal(orphan.status, "pending");
  registry.ended();
  await assert.rejects(
    orphan.wait(),
    lifecycle("transaction_rolled_back", "rejected"),
  );
  const late = registry.register("late", (value) => value, true);
  await assert.rejects(
    late.wait(),
    lifecycle("transaction_rolled_back", "rejected"),
  );
  assert.equal(registry.routingCount, 0);
});

test("missing WeakRef rejects before registration", () => {
  const registry = new ActionRegistry(null);
  assert.throws(
    () => registry.assertSupported(),
    (error) =>
      error instanceof CallError && error.code === "action.unsupported_runtime",
  );
  assert.equal(registry.routingCount, 0);
});

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const baseSchema = JSON.parse(
  await readFile(
    new URL("../../../fixtures/schemas/entry.json", import.meta.url),
    "utf8",
  ),
);
baseSchema.actions = [
  { name: "Ping", kind: "mutation", version: 1, inputs: [], outputs: [] },
  { name: "ReadPing", kind: "query", version: 1, inputs: [], outputs: [] },
  ...["Add", "Edit"].map((name) => ({
    name,
    kind: "mutation",
    version: 1,
    inputs: [
      {
        kind: "model",
        name: "entry",
        model: "Entry",
        operation: name === "Add" ? "create" : "update",
        cardinality: "single",
        ...(name === "Edit" ? { allowedFields: ["text", "note"] } : {}),
      },
    ],
    outputs: [],
  })),
];
async function withClient(
  body,
  { network = offlineNetwork, carrier = native, schema = baseSchema } = {},
) {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-observer-"));
  const TestClient = createClient(carrier, Transaction, network);
  const client = await openStore(TestClient, {
    path: join(directory, "db"),
    schema,
  });
  try {
    await body(client);
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
}
test("durable invocation registers before wake and drop settles its handle", async () => {
  await withClient(async (client) => {
    const call = await client.submitMutation("Ping", 1, {}, (x) => x);
    assert.equal(call.status, "pending");
    const waiting = call.wait();
    assert.equal((await client.syncState()).pending, 1);
    await client.drop(1);
    assert.equal((await waiting).error.code, "dropped");
    assert.equal(call.status, "failed");
    assert.equal((await client.syncState()).pending, 0);
  });
});
test("dropping a named create settles its live dependent update", async () => {
  await withClient(async (client) => {
    const created = await client.submitMutation(
      "Add",
      1,
      { entry: { id: "e", text: "A" } },
      (x) => x,
    );
    const dependent = await client.submitMutation(
      "Edit",
      1,
      { entry: { id: "e", text: "B" } },
      (x) => x,
    );
    const waiting = dependent.wait();
    await client.drop(1);
    assert.equal((await created.wait()).error.code, "dropped");
    assert.equal((await waiting).error.code, "dependency.rejected");
    assert.equal((await client.syncState()).pending, 0);
  });
});
test("reset discard settles every live handle and retires old optimism", async () => {
  await withClient(async (client) => {
    const first = await client.submitMutation("Ping", 1, {}, (x) => x);
    const second = await client.submitMutation(
      "Add",
      1,
      { entry: { id: "e", text: "A" } },
      (x) => x,
    );
    await assert.rejects(client.resetStore(), /pending/);
    await client.resetStore({ discardPending: true });
    for (const call of [first, second])
      assert.equal((await call.wait()).error.code, "abandoned");
    assert.equal(await client.read("Entry", { id: "e" }), null);
    assert.equal((await client.syncState()).pending, 0);
  });
});
test("close fails a durable handle before wait is called", async () => {
  await withClient(async (client) => {
    const call = await client.submitMutation("Ping", 1, {}, (x) => x);
    await client.close();
    assert.equal((await call.wait()).error.code, "client.closed");
  });
});
test("standalone writes stay out of the queue and reach reads and watch", async () => {
  await withClient(async (client) => {
    const observed = [];
    const stop = client.watch("Entry", {}, (rows) => observed.push(rows));
    try {
      await client.direct({
        model: "Entry",
        op: "create",
        identity: { id: "one" },
        values: { text: "A" },
      });
      assert.equal((await client.read("Entry", { id: "one" })).text, "A");
      assert.equal((await client.syncState()).pending, 0);
      await new Promise((resolve) => setImmediate(resolve));
      assert.ok(observed.some((rows) => rows.some((row) => row.text === "A")));
    } finally {
      stop();
    }
  });
});
test("Query decodes its committed response and maps unavailable transport", async () => {
  await withClient(
    async (client) => {
      await assert.rejects(
        client.invokeQuery("ReadPing", 1, {}, (x) => x),
        (error) =>
          error instanceof CallError && error.code === "action.unavailable",
      );
      const connection = await client.connect({
        url: "http://unused",
        token: "token",
      });
      assert.equal(
        await client.invokeQuery("ReadPing", 1, {}, () => "decoded"),
        "decoded",
      );
      assert.equal((await client.syncState()).pending, 0);
      await connection.close();
    },
    {
      network: () => ({
        open() {},
        push: async (kind, text) =>
          kind === "pull" || kind === "handshake"
            ? emptyPull(text)
            : emptyRead(text),
      }),
    },
  );
});
test("a throwing Query completion listener cannot replace a committed result", async () => {
  const previous = globalThis.reportError;
  const diagnostic = Error("diagnostic failed");
  const observed = [];
  globalThis.reportError = (error) => observed.push(error);
  try {
    await withClient(
      async (client) => {
        await client.connect({ url: "http://unused", token: "token" });
        client.onActionCompletion(() => {
          throw diagnostic;
        });
        assert.equal(
          await client.invokeQuery("ReadPing", 1, {}, () => "first"),
          "first",
        );
        assert.equal(
          await client.invokeQuery("ReadPing", 1, {}, () => "second"),
          "second",
        );
        assert.deepEqual(observed, [diagnostic, diagnostic]);
      },
      {
        network: () => ({
          open() {},
          push: async (kind, text) =>
            kind === "pull" || kind === "handshake"
              ? emptyPull(text)
              : emptyRead(text),
        }),
      },
    );
  } finally {
    globalThis.reportError = previous;
  }
});
test("real native receipt settles the observer before a throwing completion listener", async () => {
  const previous = globalThis.reportError;
  const diagnostic = Error("diagnostic failed");
  const observed = [];
  globalThis.reportError = (error) => observed.push(error);
  try {
    await withClient(
      async (client) => {
        client.onActionCompletion(() => {
          throw diagnostic;
        });
        const call = await client.submitMutation(
          "Ping",
          1,
          {},
          () => "accepted",
        );
        const waiting = call.wait();
        await client.connect({ url: "http://unused", token: "token" });
        let timer;
        try {
          assert.deepEqual(
            await Promise.race([
              waiting,
              new Promise(
                (_, reject) =>
                  (timer = setTimeout(
                    () => reject(Error("native receipt completion timed out")),
                    2000,
                  )),
              ),
            ]),
            { result: "accepted", error: null },
          );
        } finally {
          clearTimeout(timer);
        }
        assert.equal(call.status, "succeeded");
        assert.deepEqual(observed, [diagnostic]);
      },
      {
        network: () => ({
          open() {},
          push: async (kind, text) =>
            kind === "pull" || kind === "handshake"
              ? emptyPull(text)
              : emptyMutation(text),
        }),
      },
    );
  } finally {
    globalThis.reportError = previous;
  }
});
test("Query request store flag stays separate from a legal input named store", async () => {
  const schema = structuredClone(baseSchema);
  schema.actions = [
    {
      name: "ReadPing",
      kind: "query",
      version: 1,
      inputs: [
        {
          kind: "value",
          name: "store",
          type: { kind: "scalar", name: "string" },
          nullable: false,
        },
      ],
      outputs: [],
    },
  ];
  const bodies = [];
  await withClient(
    async (client) => {
      await client.connect({ url: "http://unused", token: "token" });
      await assert.rejects(
        client.invokeQuery("ReadPing", 1, { store: "biz" }, (x) => x, {
          store: { missing: false },
        }),
        (error) => error instanceof CallError,
      );
      await client.invokeQuery("ReadPing", 1, { store: "biz" }, (x) => x, {
        store: false,
      });
      await client.invokeQuery("ReadPing", 1, { store: "plain" }, (x) => x);
      assert.deepEqual(
        bodies.map((x) => [x.invocation.args, x.store]),
        [
          [{ store: "biz" }, false],
          [{ store: "plain" }, true],
        ],
      );
    },
    {
      schema,
      network: () => ({
        open() {},
        push: async (kind, text) => {
          if (kind === "pull" || kind === "handshake") return emptyPull(text);
          bodies.push(JSON.parse(text));
          return emptyRead(text);
        },
      }),
    },
  );
});
test("standalone named mutations and local writes reject inside an active callback", async () => {
  await withClient(async (client) => {
    await client.transaction(async () => {
      await assert.rejects(
        client.direct({
          model: "Entry",
          op: "create",
          identity: { id: "bad" },
          values: { text: "bad" },
        }),
        /transaction_active/,
      );
      await assert.rejects(
        client.submitMutation("Ping", 1, {}, (x) => x),
        /transaction_active/,
      );
    });
    assert.equal(await client.read("Entry", { id: "bad" }), null);
    assert.equal((await client.syncState()).pending, 0);
  });
});
test("retired anonymous and split action methods have no public entry point", async () => {
  await withClient(async (client) => {
    for (const name of [
      "mutate",
      "enqueue",
      "invokeAction",
      "submitAction",
      "invokeDirectAction",
      "callAction",
    ])
      assert.equal(client[name], undefined, name);
  });
});
// Callback-before-input, unawaited/foreign scopes, and same-batch durable lookup
// are exercised through mutation-native04 and call-durable04, not legacy batch carriers.
