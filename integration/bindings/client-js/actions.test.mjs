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

test("missing WeakRef rejects before registration", () => {
  const registry = new ActionRegistry(null);
  assert.throws(
    () => registry.assertSupported(),
    (error) =>
      error instanceof CallError &&
      error.code === "action.unsupported_runtime",
  );
  assert.equal(registry.routingCount, 0);
});

async function withClient(body) {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-observer-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  schema.actions = [{ name: "Ping", version: 1, inputs: [], outputs: [] }];
  const client = await Client.open({ path: join(directory, "db"), schema });
  try {
    await body(client);
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
}

test("durable invocation registers before wake and drop settles its handle", async () => {
  await withClient(async (client) => {
    const call = await client.invokeAction("Ping", 1, {}, () => undefined);
    assert.equal(call.status, "pending");
    const waiting = call.wait();
    const tasks = await client.syncState();
    assert.equal(tasks.pending, 1);
    await client.drop((await client.pendingTasks())[0]?.ordinal ?? 1);
    const outcome = await waiting;
    assert.equal(outcome.error.code, "dropped");
    assert.equal(call.status, "failed");
    assert.equal((await client.syncState()).pending, 0);
  });
});

test("dropping an unsent Action settles its live lifecycle dependent", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-dependent-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  schema.actions = [
    {
      name: "Add",
      version: 1,
      inputs: [
        {
          kind: "model",
          name: "entry",
          model: "Entry",
          operation: "create",
          cardinality: "single",
        },
      ],
      outputs: [],
    },
    {
      name: "Edit",
      version: 1,
      inputs: [
        {
          kind: "model",
          name: "entry",
          model: "Entry",
          operation: "update",
          cardinality: "single",
        },
      ],
      outputs: [],
    },
  ];
  const client = await Client.open({ path: join(directory, "db"), schema });
  try {
    const created = await client.invokeAction(
      "Add",
      1,
      { entry: { id: "e", text: "A" } },
      () => undefined,
    );
    const dependent = await client.invokeAction(
      "Edit",
      1,
      { entry: { id: "e", text: "B" } },
      () => undefined,
    );
    const waiting = dependent.wait();
    await client.drop(
      (await client.syncState("Entry", { id: "e" })).pending[0].ordinal,
    );
    assert.equal((await created.wait()).error.code, "dropped");
    assert.equal((await waiting).error.code, "dependency.rejected");
    assert.equal(dependent.status, "failed");
    assert.equal((await client.syncState()).pending, 0);
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("rebuild discard settles live unsent and frozen handles with distinct execution", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-rebuild-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  schema.actions = [{ name: "Ping", version: 1, inputs: [], outputs: [] }];
  const breaking = structuredClone(schema);
  breaking.models[0].fields.push({
    name: "due",
    nullable: false,
    type: { kind: "scalar", name: "string" },
  });
  try {
    for (const frozen of [false, true]) {
      const path = join(directory, frozen ? "frozen" : "unsent");
      const old = await Client.open({ path, schema });
      await old.invokeAction("Ping", 1, {}, () => undefined);
      await old.close();
      const client = await Client.open({ path, schema: breaking });
      try {
        const call = await client.invokeAction("Ping", 1, {}, () => undefined);
        const waiting = call.wait();
        if (frozen) await client.freeze();
        const report = await client.rebuild({ discardPending: true });
        assert.ok(
          report.abandonedCalls.some((entry) => entry.frozen === frozen),
        );
        const outcome = await waiting;
        assert.equal(outcome.error.code, "abandoned");
        assert.equal(outcome.error.execution, frozen ? "unknown" : "rejected");
        assert.equal(call.status, "failed");
      } finally {
        await client.close();
      }
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("close fails a durable handle before wait is called", async () => {
  await withClient(async (client) => {
    const call = await client.invokeAction("Ping", 1, {}, () => undefined);
    await client.close();
    assert.equal((await call.wait()).error.code, "client.closed");
  });
});

test("standalone local writes stay out of the durable queue and reach reads and watch", async () => {
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

test("the real native pump settles a waiting handle before a throwing diagnostic listener", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-action-pump-"));
  const schema = {
    enums: [],
    models: [],
    actions: [{ name: "Ping", version: 1, inputs: [], outputs: [] }],
  };
  let requests = 0;
  const PumpClient = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, bodyText) => {
      const body = JSON.parse(bodyText);
      if (kind !== "push") throw Error(`unexpected ${kind}`);
      requests++;
      return JSON.stringify({
        clientId: body.clientId,
        batchSequence: body.batchSequence,
        rejections: [],
        records: [],
        completions: body.mutations.map((mutation) => ({
          callId: mutation.callId,
          outcome: { status: "succeeded", result: null },
        })),
      });
    },
  }));
  const client = await PumpClient.open({ path: join(directory, "db"), schema });
  const previousReportError = globalThis.reportError;
  const diagnostic = Error("diagnostic failed");
  const observedErrors = [];
  globalThis.reportError = (error) => observedErrors.push(error);
  try {
    const connection = await client.connect({
      url: "http://unused",
      token: "token",
    });
    client.onActionCompletion(() => {
      throw diagnostic;
    });
    const call = await client.invokeAction("Ping", 1, {}, () => undefined);
    const deadline = Date.now() + 1000;
    while (call.status === "pending" && Date.now() < deadline)
      await new Promise((resolve) => setTimeout(resolve, 5));
    assert.equal(call.status, "succeeded", "the pump completes without wait()");
    const outcome = await Promise.race([
      call.wait(),
      new Promise((_, reject) =>
        setTimeout(() => reject(Error("wait timed out")), 1000),
      ),
    ]);
    assert.deepEqual(outcome, { result: undefined, error: null });
    assert.equal(call.status, "succeeded");
    assert.equal(requests, 1);
    assert.deepEqual(observedErrors, [diagnostic]);
    assert.equal((await client.syncState()).pending, 0);
    await connection.close();
  } finally {
    globalThis.reportError = previousReportError;
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable reports isolate throwing onError and keep the native pump alive", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-action-reports-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  schema.actions = [{ name: "Ping", version: 1, inputs: [], outputs: [] }];
  let requests = 0;
  const PumpClient = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, bodyText) => {
      assert.equal(kind, "push");
      const body = JSON.parse(bodyText);
      requests++;
      return JSON.stringify({
        clientId: body.clientId,
        batchSequence: body.batchSequence,
        rejections: [],
        completions: body.mutations.map((mutation) => ({
          callId: mutation.callId,
          outcome: { status: "succeeded", result: null },
        })),
        records:
          requests === 3
            ? []
            : ["a", "b"].map((id) => ({
                model: "Entry",
                identity: { id },
                stamp: 1,
                state: {
                  text: requests === 1 ? "first" : "second",
                  note: null,
                },
              })),
      });
    },
  }));
  const client = await PumpClient.open({ path: join(directory, "db"), schema });
  const previousReportError = globalThis.reportError;
  const diagnostic = Error("application diagnostic failed");
  const observed = [];
  const reports = [];
  globalThis.reportError = (error) => observed.push(error);
  try {
    const connection = await client.connect(
      { url: "http://unused", token: "token" },
      {
        onError: (report) => {
          reports.push(report);
          throw diagnostic;
        },
      },
    );
    const first = await client.invokeAction("Ping", 1, {}, () => undefined);
    assert.equal((await first.wait()).error, null);
    const second = await client.invokeAction("Ping", 1, {}, () => undefined);
    assert.equal((await second.wait()).error, null);
    const third = await client.invokeAction("Ping", 1, {}, () => undefined);
    assert.equal(
      (
        await Promise.race([
          third.wait(),
          new Promise((_, reject) =>
            setTimeout(() => reject(Error("pump stalled")), 1000),
          ),
        ])
      ).error,
      null,
    );
    assert.equal(requests, 3);
    assert.equal(reports.length, 2);
    assert.ok(reports.every((report) => report.message.includes("conflict")));
    assert.deepEqual(observed, [diagnostic, diagnostic]);
    assert.equal((await client.syncState()).pending, 0);
    await connection.close();
  } finally {
    globalThis.reportError = previousReportError;
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("direct invocation decodes a committed response and maps unavailable transport errors", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-action-direct-"));
  const schema = {
    enums: [],
    models: [],
    actions: [{ name: "Ping", version: 1, inputs: [], outputs: [] }],
  };
  const DirectClient = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, bodyText) => {
      if (kind !== "action") throw Error(`unexpected ${kind}`);
      const body = JSON.parse(bodyText);
      return JSON.stringify({
        completion: {
          callId: body.call.callId,
          outcome: { status: "succeeded", result: null },
        },
        records: [],
      });
    },
  }));
  const client = await DirectClient.open({
    path: join(directory, "db"),
    schema,
  });
  try {
    await assert.rejects(
      client.invokeDirectAction("Ping", 1, {}, () => undefined),
      (error) =>
        error instanceof CallError && error.code === "action.unavailable",
    );
    const connection = await client.connect({
      url: "http://unused",
      token: "token",
    });
    assert.equal(
      await client.invokeDirectAction("Ping", 1, {}, () => undefined),
      undefined,
    );
    assert.equal((await client.syncState()).pending, 0);
    await connection.close();
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("a throwing direct completion listener leaves the committed result intact", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(
    join(tmpdir(), "axton-action-direct-listener-"),
  );
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  schema.actions = [{ name: "Ping", version: 1, inputs: [], outputs: [] }];
  let requests = 0;
  const DirectClient = createClient(native, Transaction, () => ({
    open() {},
    push: async (_kind, bodyText) => {
      const body = JSON.parse(bodyText);
      requests++;
      return JSON.stringify({
        completion: {
          callId: body.call.callId,
          outcome:
            requests === 3
              ? {
                  status: "failed",
                  code: "handler.failed",
                  execution: "rejected",
                }
              : { status: "succeeded", result: null },
        },
        records:
          requests === 3
            ? []
            : [
                {
                  model: "Entry",
                  identity: { id: "e" },
                  stamp: 1,
                  state: {
                    text: requests === 1 ? "first" : "second",
                    note: null,
                  },
                },
              ],
      });
    },
  }));
  const client = await DirectClient.open({
    path: join(directory, "db"),
    schema,
  });
  const previous = globalThis.reportError;
  const diagnostic = Error("diagnostic failed");
  const reportDiagnostic = Error("report diagnostic failed");
  const observed = [];
  globalThis.reportError = (error) => observed.push(error);
  try {
    const connection = await client.connect(
      { url: "http://unused", token: "token" },
      {
        onError: () => {
          throw reportDiagnostic;
        },
      },
    );
    client.onActionCompletion(() => {
      throw diagnostic;
    });
    assert.equal(
      await client.invokeDirectAction("Ping", 1, {}, () => "seeded"),
      "seeded",
    );
    assert.equal(
      await client.invokeDirectAction("Ping", 1, {}, () => "decoded"),
      "decoded",
    );
    await assert.rejects(
      client.invokeDirectAction("Ping", 1, {}, () => undefined),
      (error) =>
        error instanceof CallError &&
        error.code === "handler.failed" &&
        error.execution === "rejected",
    );
    assert.deepEqual(observed, [
      diagnostic,
      diagnostic,
      reportDiagnostic,
      diagnostic,
    ]);
    await connection.close();
  } finally {
    globalThis.reportError = previous;
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("unsupported runtime fails before durable submission", async () => {
  await withClient(async (client) => {
    const original = globalThis.WeakRef;
    try {
      globalThis.WeakRef = undefined;
      await assert.rejects(
        client.invokeAction("Ping", 1, {}, () => undefined),
        (error) => error.code === "action.unsupported_runtime",
      );
    } finally {
      globalThis.WeakRef = original;
    }
    assert.equal((await client.syncState()).pending, 0);
  });
});

test("standalone direct writes and Actions reject from an active transaction callback", async () => {
  await withClient(async (client) => {
    await client.transaction(async () => {
      await assert.rejects(
        client.direct({
          model: "Entry",
          op: "create",
          identity: { id: "blocked" },
          values: { text: "A" },
        }),
        /transaction_active/,
      );
      await assert.rejects(
        client.invokeAction("Ping", 1, {}, () => undefined),
        (error) =>
          error instanceof CallError && error.code === "transaction_active",
      );
      await assert.rejects(
        client.invokeDirectAction("Ping", 1, {}, () => undefined),
        (error) =>
          error instanceof CallError && error.code === "transaction_active",
      );
    });
    assert.equal((await client.syncState()).pending, 0);
  });
});

test("close racing a committed submit still yields a terminal handle", async () => {
  const binding = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-action-close-race-"));
  const schema = {
    enums: [],
    models: [],
    actions: [{ name: "Ping", version: 1, inputs: [], outputs: [] }],
  };
  let entered;
  let release;
  const submitted = new Promise((resolve) => {
    entered = resolve;
  });
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  // Hold every event from the committed submit's completion on until the
  // gate opens, so close begins while the committed submit is unanswered.
  const submits = new Set();
  let held;
  let gateOpen = false;
  let wakeHeld;
  void gate.then(() => {
    gateOpen = true;
    wakeHeld?.();
  });
  const native = {
    runtimeOpen(request, wake) {
      const runtimeId = binding.runtimeOpen(request, wake);
      wakeHeld = () => wake(runtimeId);
      return runtimeId;
    },
    runtimeSubmit(runtimeId, message) {
      const parsed = JSON.parse(message);
      if (parsed.command?.kind === "submitAction")
        submits.add(parsed.requestId);
      binding.runtimeSubmit(runtimeId, message);
    },
    runtimeDrain(runtimeId) {
      const batch = JSON.parse(binding.runtimeDrain(runtimeId));
      if (
        !held &&
        batch.some(
          (event) =>
            event.type === "taskCompleted" && submits.has(event.requestId),
        )
      ) {
        held = [];
        entered();
      }
      if (!held) return JSON.stringify(batch);
      held.push(...batch);
      return JSON.stringify(gateOpen ? held.splice(0) : []);
    },
    runtimeDetach: (runtimeId) => binding.runtimeDetach(runtimeId),
  };
  const RaceClient = createClient(native, Transaction, () => {
    throw Error("network not configured");
  });
  const client = await RaceClient.open({ path: join(directory, "db"), schema });
  try {
    const callPromise = client.invokeAction("Ping", 1, {}, () => undefined);
    await submitted;
    const closing = client.close();
    release();
    const call = await callPromise;
    assert.equal((await call.wait()).error.code, "client.closed");
    await closing;
  } finally {
    release();
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("a completion in the same drained batch as its submission reaches the registered handle", async () => {
  // A fake carrier whose runtime publishes the `submitAction` completion and
  // the call's `callCompleted` in one batch: the handle is registered while
  // the completion is dispatched, so the outcome that follows it in the same
  // batch finds it (spec section 3).
  let wake;
  const outbox = [];
  const later = () => setImmediate(() => wake("1"));
  const native = {
    runtimeOpen(request, wakeRuntime) {
      wake = wakeRuntime;
      outbox.push({
        type: "taskCompleted",
        requestId: JSON.parse(request).requestId,
        ok: true,
        value: {
          clientId: "c",
          schema: { rebuilt: false, pending: null, lastRebuild: null },
        },
      });
      later();
      return "1";
    },
    runtimeSubmit(runtimeId, message) {
      const input = JSON.parse(message);
      if (input.command?.kind === "submitAction")
        outbox.push(
          {
            type: "taskCompleted",
            requestId: input.requestId,
            ok: true,
            value: { callId: "call-1", ordinal: 1 },
          },
          {
            type: "callCompleted",
            callId: "call-1",
            outcome: { status: "succeeded", result: { title: "A" } },
          },
        );
      else if (input.type === "close") outbox.push({ type: "runtimeClosed" });
      later();
    },
    runtimeDrain: () => JSON.stringify(outbox.splice(0)),
    runtimeDetach() {},
  };
  const FakeClient = createClient(native, Transaction, () => {
    throw Error("network not configured");
  });
  const client = await FakeClient.open({ path: "unused", schema: {} });
  try {
    const completions = [];
    client.onActionCompletion((completion) => completions.push(completion));
    const call = await client.invokeAction("Ping", 1, {}, (value) => ({
      title: value.title,
    }));
    assert.equal(call.status, "succeeded");
    assert.deepEqual(await call.wait(), { result: { title: "A" }, error: null });
    assert.deepEqual(completions.map((completion) => completion.callId), [
      "call-1",
    ]);
  } finally {
    await client.close();
  }
});

test("the store option travels beside args on both routes and is validated before submission", async () => {
  const binding = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-action-store-"));
  const schema = {
    enums: [],
    models: [],
    actions: [
      {
        name: "Ping",
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
    ],
  };
  const sent = [];
  const native = {
    runtimeOpen: (request, wake) => binding.runtimeOpen(request, wake),
    runtimeSubmit(runtimeId, message) {
      const { command } = JSON.parse(message);
      if (command?.kind === "submitAction" || command?.kind === "invoke")
        sent.push(command);
      binding.runtimeSubmit(runtimeId, message);
    },
    runtimeDrain: (runtimeId) => binding.runtimeDrain(runtimeId),
    runtimeDetach: (runtimeId) => binding.runtimeDetach(runtimeId),
  };
  const bodies = [];
  const StoreClient = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, bodyText) => {
      if (kind !== "action") throw Error(`unexpected ${kind}`);
      const body = JSON.parse(bodyText);
      bodies.push(body);
      return JSON.stringify({
        completion: {
          callId: body.call.callId,
          outcome: { status: "succeeded", result: null },
        },
        records: [],
      });
    },
  }));
  const client = await StoreClient.open({ path: join(directory, "db"), schema });
  try {
    await assert.rejects(
      client.invokeAction("Ping", 1, { store: "biz" }, () => undefined, {
        store: { missing: false },
      }),
      (error) => error instanceof CallError,
    );
    assert.equal((await client.syncState()).pending, 0);
    await client.invokeAction("Ping", 1, { store: "biz" }, () => undefined, {
      store: false,
    });
    await client.invokeAction("Ping", 1, { store: "plain" }, () => undefined);
    assert.equal((await client.syncState()).pending, 2);
    assert.deepEqual(sent[1].args, { store: "biz" });
    assert.equal(sent[1].store, false);
    assert.equal("store" in sent[2], false);
    const connection = await client.connect({
      url: "http://unused",
      token: "token",
    });
    await client.invokeDirectAction(
      "Ping",
      1,
      { store: "biz" },
      () => undefined,
      { store: false },
    );
    assert.equal(bodies[0].call.store, false);
    assert.deepEqual(bodies[0].call.args, { store: "biz" });
    await connection.close();
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});
