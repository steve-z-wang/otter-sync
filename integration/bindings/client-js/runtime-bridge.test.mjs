import test from "node:test";
import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { createRequire } from "node:module";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Bridge } from "../../../packages/client-js/bridge.mts";

// The SDK Bridge over the Rust-owned client runtime (#134): request routing,
// callback transactions, wake/drain dispatch and lifecycle, on the real
// Node carrier.
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const schemaUrl = new URL(
  "../../../fixtures/schemas/entry.json",
  import.meta.url,
);
const schema = JSON.parse(await readFile(schemaUrl, "utf8"));
const envelopes = JSON.parse(
  await readFile(
    new URL("../../../fixtures/bridge/envelopes.json", import.meta.url),
    "utf8",
  ),
);
const key = (id) => ({ model: "Entry", identity: { id } });
const create = (id, text) => ({
  kind: "direct",
  operation: {
    model: "Entry",
    op: "create",
    identity: { id },
    values: { text },
  },
});
const update = (id, text) => ({
  kind: "direct",
  operation: {
    model: "Entry",
    op: "update",
    identity: { id },
    values: { text },
  },
});
const read = (id) => ({ kind: "read", key: key(id) });
const deferred = () => {
  let resolve;
  const promise = new Promise((r) => (resolve = r));
  return { promise, resolve };
};
/** Whether `promise` is still pending after the actor had time to run it. */
async function pending(promise, ms = 50) {
  let timer;
  const marker = Symbol();
  try {
    return (
      (await Promise.race([
        promise.then(
          () => undefined,
          () => undefined,
        ),
        new Promise(
          (resolve) => (timer = setTimeout(() => resolve(marker), ms)),
        ),
      ])) === marker
    );
  } finally {
    clearTimeout(timer);
  }
}
async function withBridge(body, carrier = native) {
  const directory = await mkdtemp(join(tmpdir(), "axton-bridge-"));
  const { bridge, opened } = await Bridge.open(carrier, {
    path: join(directory, "client.sqlite"),
    schema,
  });
  try {
    await body(bridge, opened);
  } finally {
    await bridge.close();
    await rm(directory, { recursive: true, force: true });
  }
}

test("open answers the client id and schema state", async () => {
  await withBridge(async (bridge, opened) => {
    assert.equal(typeof opened.clientId, "string");
    assert.deepEqual(opened.schema, {
      rebuilt: false,
      pending: null,
      lastRebuild: null,
    });
    assert.equal(bridge.closed, false);
  });
});

test("overlapping tasks return to the correct waiter while a callback owns the transaction", async () => {
  await withBridge(async (bridge) => {
    await bridge.task(create("e", "A"));
    const order = [];
    const entered = deferred();
    const inside = deferred();
    const gate = deferred();
    const transaction = bridge.transaction(async (transactionId) => {
      entered.resolve(transactionId);
      await bridge.transactionCommand(
        transactionId,
        undefined,
        update("e", "B"),
      );
      order.push("tx write");
      const row = await bridge.transactionCommand(
        transactionId,
        undefined,
        read("e"),
      );
      order.push(`tx read ${row.text}`);
      inside.resolve();
      await gate.promise;
    });
    void transaction.then(() => order.push("transaction"));
    const transactionId = await entered.promise;
    assert.equal(typeof transactionId, "string");
    const outside = bridge.task(read("e")).then((row) => {
      order.push(`outside read ${row.text}`);
      return row;
    });
    const status = bridge.task({ kind: "status" }).then((state) => {
      order.push("status");
      return state;
    });
    await inside.promise;
    assert.equal(await pending(outside), true, "ordinary read waits");
    assert.equal(await pending(status), true, "status waits");
    assert.deepEqual(order, ["tx write", "tx read B"]);
    gate.resolve();
    await transaction;
    assert.equal((await outside).text, "B");
    assert.equal(typeof (await status).clientId, "string");
    assert.deepEqual(order, [
      "tx write",
      "tx read B",
      "transaction",
      "outside read B",
      "status",
    ]);
  });
});

test("a command in the wrong scope fails without joining and rolls the unit back", async () => {
  await withBridge(async (bridge) => {
    let refused;
    await assert.rejects(
      bridge.transaction(async (transactionId) => {
        await bridge.transactionCommand(
          transactionId,
          undefined,
          create("w", "A"),
        );
        refused = await bridge
          .transactionCommand(transactionId, "sp999", read("w"))
          .catch((error) => error);
      }),
      /invalid transaction scope/,
    );
    assert.match(refused.message, /invalid transaction scope/);
    assert.equal(await bridge.task(read("w")), null);
  });
});

test("savepoint scopes are issued by the runtime and carried by nested commands", async () => {
  await withBridge(async (bridge) => {
    await bridge.transaction(async (transactionId) => {
      const { scope } = await bridge.transactionCommand(
        transactionId,
        undefined,
        {
          kind: "savepoint",
        },
      );
      assert.equal(typeof scope, "string");
      await bridge.transactionCommand(transactionId, scope, create("s", "A"));
      await bridge.transactionCommand(transactionId, scope, {
        kind: "rollbackSavepoint",
        scope,
      });
      await bridge.transactionCommand(
        transactionId,
        undefined,
        create("t", "B"),
      );
    });
    assert.equal(await bridge.task(read("s")), null);
    assert.equal((await bridge.task(read("t"))).text, "B");
  });
});

test("admission failure rejects its waiter and leaves the bridge usable; after close tasks reject client_closed", async () => {
  let refuse = true;
  const carrier = {
    runtimeOpen: (request, wake) => native.runtimeOpen(request, wake),
    runtimeSubmit(runtimeId, message) {
      if (refuse && JSON.parse(message).command?.kind === "status") {
        refuse = false;
        throw Error("client_closed");
      }
      native.runtimeSubmit(runtimeId, message);
    },
    runtimeDrain: (runtimeId) => native.runtimeDrain(runtimeId),
    runtimeDetach: (runtimeId) => native.runtimeDetach(runtimeId),
  };
  let closedBridge;
  await withBridge(async (bridge) => {
    closedBridge = bridge;
    await assert.rejects(bridge.task({ kind: "status" }), /client_closed/);
    assert.equal(
      typeof (await bridge.task({ kind: "status" })).clientId,
      "string",
    );
  }, carrier);
  assert.equal(closedBridge.closed, true);
  await assert.rejects(closedBridge.task({ kind: "status" }), /client_closed/);
  await closedBridge.close();
});

test("a throwing callback rejects with the same value and rolls back", async () => {
  await withBridge(async (bridge) => {
    const thrown = Error("abort");
    await assert.rejects(
      bridge.transaction(async (transactionId) => {
        await bridge.transactionCommand(
          transactionId,
          undefined,
          create("rolled", "A"),
        );
        throw thrown;
      }),
      (error) => error === thrown,
    );
    assert.equal(await bridge.task(read("rolled")), null);
    const odd = { reason: "not an Error" };
    await assert.rejects(
      bridge.transaction(async () => {
        throw odd;
      }),
      (error) => error === odd,
    );
  });
});

test("the callback runs in the async context of the transaction's caller", async () => {
  await withBridge(async (bridge) => {
    const storage = new AsyncLocalStorage();
    let seen;
    await storage.run("caller", () =>
      bridge.transaction(async () => {
        seen = storage.getStore();
      }),
    );
    assert.equal(seen, "caller");
  });
});

test("a thrown value without a usable message still rolls back and rejects with it", async () => {
  await withBridge(async (bridge) => {
    const opaque = Object.create(null);
    await assert.rejects(
      bridge.transaction(async (transactionId) => {
        await bridge.transactionCommand(
          transactionId,
          undefined,
          create("o", "A"),
        );
        throw opaque;
      }),
      (error) => error === opaque,
    );
    assert.equal(await bridge.task(read("o")), null);
  });
});

test("a caught command failure rejects at commit with the engine message", async () => {
  await withBridge(async (bridge) => {
    let caught;
    await assert.rejects(
      bridge.transaction(async (transactionId) => {
        await bridge.transactionCommand(
          transactionId,
          undefined,
          create("kept", "A"),
        );
        await bridge
          .transactionCommand(transactionId, undefined, {
            kind: "direct",
            operation: {
              model: "Missing",
              op: "create",
              identity: { id: "m" },
              values: {},
            },
          })
          .catch((error) => {
            caught = error;
          });
      }),
      (error) =>
        error instanceof Error &&
        error !== caught &&
        error.message === caught.message,
    );
    assert.ok(caught instanceof Error);
    assert.equal(await bridge.task(read("kept")), null);
  });
});

test("a failed open rejects with the engine message and detaches its runtime", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-bridge-open-"));
  const opened = [];
  const detached = [];
  const carrier = {
    runtimeOpen(request, wake) {
      const id = native.runtimeOpen(request, wake);
      opened.push(id);
      return id;
    },
    runtimeSubmit: (runtimeId, message) =>
      native.runtimeSubmit(runtimeId, message),
    runtimeDrain: (runtimeId) => native.runtimeDrain(runtimeId),
    runtimeDetach(runtimeId) {
      detached.push(runtimeId);
      native.runtimeDetach(runtimeId);
    },
  };
  try {
    const missing = join(directory, "missing", "deeper", "client.sqlite");
    await assert.rejects(
      Bridge.open(carrier, { path: missing, schema }),
      (error) =>
        error instanceof Error &&
        error.message.length > 0 &&
        error.message !== "client_closed",
    );
    assert.equal(opened.length, 1);
    assert.deepEqual(detached, opened);
    assert.throws(
      () => native.runtimeSubmit(opened[0], JSON.stringify({ type: "close" })),
      /client_closed/,
    );
    const { bridge } = await Bridge.open(carrier, {
      path: join(directory, "client.sqlite"),
      schema,
    });
    assert.equal(await bridge.task(read("none")), null);
    await bridge.close();
    assert.deepEqual(detached, opened);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("close during an open callback rolls back and settles every waiter", async () => {
  await withBridge(async (bridge) => {
    const entered = deferred();
    const gate = deferred();
    let late;
    const transaction = bridge.transaction(async (transactionId) => {
      await bridge.transactionCommand(
        transactionId,
        undefined,
        create("c", "A"),
      );
      entered.resolve(transactionId);
      await gate.promise;
      late = await bridge
        .transactionCommand(transactionId, undefined, read("c"))
        .catch((error) => error);
    });
    await entered.promise;
    const queued = bridge.task(read("c"));
    const closing = bridge.close();
    assert.strictEqual(bridge.close(), closing);
    await assert.rejects(transaction, /client_closed/);
    await assert.rejects(queued, /client_closed/);
    gate.resolve();
    await closing;
    await new Promise((resolve) => setImmediate(resolve));
    assert.match(late.message, /transaction_closed|client_closed/);
    assert.equal(bridge.closed, true);
  });
});

test("the shared envelope fixtures carry the shapes the bridge sends and switches on", () => {
  const inputTypes = new Set(envelopes.inputs.map((input) => input.type));
  assert.deepEqual([...inputTypes].sort(), [
    "callbackResult",
    "close",
    "effectResult",
    "task",
    "transactionCommand",
  ]);
  for (const input of envelopes.inputs) {
    switch (input.type) {
      case "task":
        assert.equal(typeof input.requestId, "string");
        assert.equal(typeof input.command.kind, "string");
        break;
      case "transactionCommand":
        assert.equal(typeof input.requestId, "string");
        assert.equal(typeof input.transactionId, "string");
        assert.ok(input.scope === undefined || typeof input.scope === "string");
        assert.equal(typeof input.command.kind, "string");
        break;
      case "callbackResult":
        assert.equal(typeof input.effectId, "string");
        assert.equal(typeof input.transactionId, "string");
        assert.equal(typeof input.ok, "boolean");
        assert.ok(
          input.ok ? !("error" in input) : typeof input.error === "string",
        );
        break;
      case "effectResult":
        assert.equal(typeof input.effectId, "string");
        assert.equal(typeof input.outcome.ok, "boolean");
        if (!input.outcome.ok) {
          assert.equal(typeof input.outcome.error.message, "string");
          assert.ok(
            input.outcome.error.status === undefined ||
              typeof input.outcome.error.status === "number",
          );
        }
        break;
      case "close":
        assert.deepEqual(Object.keys(input), ["type"]);
        break;
    }
  }
  const eventTypes = new Set(envelopes.events.map((event) => event.type));
  assert.deepEqual([...eventTypes].sort(), [
    "callCompleted",
    "cancelEffect",
    "effect",
    "observerChanged",
    "report",
    "runtimeClosed",
    "taskCompleted",
  ]);
  const operations = new Set();
  const codes = new Set();
  const snapshots = new Set();
  for (const event of envelopes.events) {
    switch (event.type) {
      case "taskCompleted":
        assert.equal(typeof event.requestId, "string");
        assert.equal(typeof event.ok, "boolean");
        assert.ok("value" in event);
        assert.ok(
          event.ok ? !("error" in event) : typeof event.error === "string",
        );
        // The machine-readable reason a failure may carry beside its message.
        if ("details" in event) {
          assert.equal(event.ok, false);
          assert.equal(typeof event.details.code, "string");
          codes.add(event.details.code);
        }
        break;
      case "effect":
        assert.equal(typeof event.effectId, "string");
        assert.equal(typeof event.operation.kind, "string");
        operations.add(event.operation.kind);
        if (event.operation.kind === "callback") {
          assert.equal(typeof event.operation.transactionId, "string");
          assert.equal(typeof event.operation.requestId, "string");
        }
        break;
      case "cancelEffect":
        assert.equal(typeof event.effectId, "string");
        break;
      case "callCompleted":
        assert.equal(typeof event.callId, "string");
        assert.equal(typeof event.outcome, "object");
        break;
      case "observerChanged":
        assert.equal(typeof event.observerId, "string");
        assert.ok(["subscription", "watch"].includes(event.snapshot.kind));
        assert.ok(
          event.snapshot.closed === undefined || event.snapshot.closed === true,
        );
        if (event.snapshot.kind === "watch")
          assert.ok(Array.isArray(event.snapshot.rows));
        else
          assert.deepEqual(Object.keys(event.snapshot.status).sort(), [
            "active",
            "bootstrap",
            "connection",
            "initialization",
          ]);
        snapshots.add(
          `${event.snapshot.kind}${event.snapshot.closed ? ":closed" : ""}`,
        );
        break;
      case "report":
        assert.ok(
          ["records", "error", "protocol"].includes(event.diagnostic.kind),
        );
        break;
      case "runtimeClosed":
        assert.deepEqual(Object.keys(event), ["type"]);
        break;
    }
  }
  assert.deepEqual([...operations].sort(), [
    "callback",
    "http",
    "prerequisite",
    "refreshAuth",
    "socket",
    "timer",
  ]);
  // Every code the SDK maps a `scopeBootstrap` failure by, and every observer
  // snapshot it dispatches.
  assert.deepEqual([...codes].sort(), [
    "bootstrap.request_rejected",
    "bootstrap.superseded",
    "subscription.closed",
  ]);
  assert.deepEqual([...snapshots].sort(), [
    "subscription",
    "subscription:closed",
    "watch",
    "watch:closed",
  ]);
});

test("the bridge dispatches every fixture event and answers effects it has no handler for", async () => {
  // A carrier that delivers the shared fixture events after a successful
  // open: the bridge must route each one, never stop on one, and answer an
  // effect nobody handles.
  let wake;
  let batches = [];
  const submitted = [];
  const detached = [];
  const carrier = {
    runtimeOpen(request, wakeRuntime) {
      const { requestId } = JSON.parse(request);
      wake = wakeRuntime;
      batches.push([
        {
          type: "taskCompleted",
          requestId,
          ok: true,
          value: {
            clientId: "c",
            schema: { rebuilt: false, pending: null, lastRebuild: null },
          },
        },
      ]);
      setImmediate(() => wake("9"));
      return "9";
    },
    runtimeSubmit(runtimeId, message) {
      assert.equal(runtimeId, "9");
      submitted.push(JSON.parse(message));
    },
    runtimeDrain(runtimeId) {
      assert.equal(runtimeId, "9");
      return JSON.stringify(batches.shift() ?? []);
    },
    runtimeDetach(runtimeId) {
      detached.push(runtimeId);
    },
  };
  const { bridge, opened } = await Bridge.open(carrier, {
    path: "unused",
    schema,
  });
  assert.equal(opened.clientId, "c");
  const seen = [];
  for (const type of ["callCompleted", "observerChanged", "report"])
    bridge.on(type, (event) => seen.push(event.type));
  const handled = [];
  bridge.onEffect("timer", (effectId, operation) =>
    handled.push([effectId, operation.millis]),
  );
  // Split the fixture events over two drains to exercise the recheck.
  const events = envelopes.events.filter(
    (event) => event.type !== "runtimeClosed",
  );
  batches = [events.slice(0, 5), events.slice(5)];
  wake("9");
  assert.deepEqual(seen, [
    "callCompleted",
    ...Array(5).fill("observerChanged"),
    ...Array(4).fill("report"),
  ]);
  assert.deepEqual(handled, [["7", 250]]);
  const answered = submitted.filter((input) => input.type !== "callbackResult");
  assert.deepEqual(answered.map((input) => input.effectId).sort(), [
    "10",
    "11",
    "12",
    "6",
    "8",
    "9",
  ]);
  for (const input of answered)
    assert.deepEqual(input.outcome, {
      ok: false,
      error: { message: "unsupported effect" },
    });
  // A callback effect for a request the bridge does not route is refused.
  assert.deepEqual(
    submitted.filter((input) => input.type === "callbackResult"),
    [
      {
        type: "callbackResult",
        effectId: "5",
        transactionId: "tx7",
        ok: false,
        error: "unknown transaction",
      },
    ],
  );
  const task = bridge.task({ kind: "status" });
  const closing = bridge.close();
  assert.deepEqual(submitted.at(-1), { type: "close" });
  batches = [[{ type: "runtimeClosed" }]];
  wake("9");
  await assert.rejects(task, /client_closed/);
  await closing;
  assert.deepEqual(detached, ["9"]);
  assert.equal(bridge.closed, true);
});


/**
 * A Bridge over a scripted runtime: `respond(command, requestId)` answers each
 * submitted task with the events it publishes, and `publish` hands the Bridge
 * more, one drain batch per call, the way the runtime's outbox would.
 */
async function scripted(respond = () => []) {
  let wake;
  const outbox = [];
  const submitted = [];
  const later = () => setImmediate(() => wake("1"));
  const carrier = {
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
      submitted.push(input);
      if (input.type === "task")
        outbox.push(...respond(input.command, input.requestId));
      if (input.type === "close") outbox.push({ type: "runtimeClosed" });
      later();
    },
    runtimeDrain: () => JSON.stringify(outbox.splice(0)),
    runtimeDetach() {},
  };
  const { bridge } = await Bridge.open(carrier, { path: "unused", schema });
  return {
    bridge,
    submitted,
    publish(...events) {
      outbox.push(...events);
      wake("1");
    },
  };
}
const watchSnapshot = (rows, closed) => ({
  kind: "watch",
  rows,
  ...(closed ? { closed: true } : {}),
});

test("a throwing listener does not stop the completion in the same drain batch", async () => {
  const original = globalThis.reportError;
  const reported = [];
  globalThis.reportError = (error) => reported.push(error);
  try {
    const diagnostic = { kind: "error", message: "HTTP 503", status: 503 };
    const { bridge } = await scripted((command, requestId) => [
      { type: "report", diagnostic },
      { type: "taskCompleted", requestId, ok: true, value: "done" },
    ]);
    const failure = Error("listener failed");
    const seen = [];
    bridge.on("report", () => {
      throw failure;
    });
    bridge.on("report", (event) => seen.push(event.diagnostic));
    assert.equal(await bridge.task({ kind: "status" }), "done");
    assert.deepEqual(reported, [failure]);
    assert.deepEqual(seen, [diagnostic]);
  } finally {
    if (original === undefined) delete globalThis.reportError;
    else globalThis.reportError = original;
  }
});

test("a settled hook runs while the completion is dispatched and a throw fails its task", async () => {
  const { bridge } = await scripted((command, requestId) => [
    { type: "taskCompleted", requestId, ok: true, value: { callId: "c1" } },
    { type: "callCompleted", callId: "c1", outcome: { status: "succeeded" } },
  ]);
  const order = [];
  bridge.on("callCompleted", (event) => order.push(`completed ${event.callId}`));
  const value = await bridge.task(
    { kind: "submitAction" },
    { settled: ({ callId }) => order.push(`settled ${callId}`) },
  );
  order.push("continuation");
  assert.deepEqual(value, { callId: "c1" });
  assert.deepEqual(order, ["settled c1", "completed c1", "continuation"]);
  const failure = Error("hook failed");
  await assert.rejects(
    bridge.task(
      { kind: "submitAction" },
      {
        settled: () => {
          throw failure;
        },
      },
    ),
    (error) => error === failure,
  );
});

test("a failed task rejects with its message and the details the runtime gave it", async () => {
  const details = { code: "bootstrap.request_rejected", message: "HTTP 403" };
  const { bridge } = await scripted((command, requestId) => [
    command.kind === "coded"
      ? { type: "taskCompleted", requestId, ok: false, value: null, error: "HTTP 403", details }
      : { type: "taskCompleted", requestId, ok: false, value: null, error: "plain" },
  ]);
  const coded = await bridge.task({ kind: "coded" }).catch((error) => error);
  assert.equal(coded.message, "HTTP 403");
  assert.deepEqual(coded.details, details);
  const plain = await bridge.task({ kind: "plain" }).catch((error) => error);
  assert.equal(plain.message, "plain");
  assert.equal("details" in plain, false, "no details, no property");
});

test("an observer routed from its task's settled hook hears the snapshots published behind it", async () => {
  // The runtime queues an observer's first snapshot right after the task that
  // named it, in the same batch: it is dispatched before the task's
  // continuation runs, but after its settled hook.
  let next = 3;
  const { bridge, publish } = await scripted((command, requestId) => {
    const observerId = String(next++);
    return [
      { type: "taskCompleted", requestId, ok: true, value: { observerId } },
      { type: "observerChanged", observerId, snapshot: watchSnapshot([1]) },
      { type: "observerChanged", observerId, snapshot: watchSnapshot([2]) },
    ];
  });
  const seen = [];
  let detach;
  await bridge.task(
    { kind: "watch" },
    {
      settled: ({ observerId }) =>
        (detach = bridge.observe(observerId, (snapshot) =>
          seen.push(snapshot.rows),
        )),
    },
  );
  assert.deepEqual(seen, [[1], [2]], "every snapshot, in order, once");
  publish({ type: "observerChanged", observerId: "3", snapshot: watchSnapshot([3]) });
  assert.deepEqual(seen, [[1], [2], [3]]);
  detach();
  publish({ type: "observerChanged", observerId: "3", snapshot: watchSnapshot([4]) });
  assert.deepEqual(seen, [[1], [2], [3]], "a detached observer hears nothing");
  // Attached only in the continuation, an observer missed what was
  // dispatched before it: nothing is buffered for an unrouted observer.
  const late = [];
  const { observerId } = await bridge.task({ kind: "watch" });
  assert.equal(observerId, "4");
  bridge.observe(observerId, (snapshot) => late.push(snapshot.rows));
  assert.deepEqual(late, []);
  publish({ type: "observerChanged", observerId, snapshot: watchSnapshot([5]) });
  assert.deepEqual(late, [[5]]);
});

test("a terminal snapshot ends its observer; a throwing observer is reported and hears the next one", async () => {
  const original = globalThis.reportError;
  const reported = [];
  globalThis.reportError = (error) => reported.push(error);
  try {
    const { bridge, publish } = await scripted();
    const seen = [];
    const failure = Error("observer failed");
    bridge.observe("5", (snapshot) => {
      seen.push(snapshot.rows);
      if (seen.length === 1) throw failure;
    });
    publish(
      { type: "observerChanged", observerId: "5", snapshot: watchSnapshot([1]) },
      { type: "observerChanged", observerId: "5", snapshot: watchSnapshot([2], true) },
      { type: "observerChanged", observerId: "5", snapshot: watchSnapshot([3]) },
    );
    assert.deepEqual(reported, [failure]);
    assert.deepEqual(seen, [[1], [2]], "nothing follows a terminal snapshot");
  } finally {
    if (original === undefined) delete globalThis.reportError;
    else globalThis.reportError = original;
  }
});

/** Run a module script and answer its exit code and output. */
function script(source) {
  return new Promise((resolve) => {
    execFile(
      process.execPath,
      ["--input-type=module", "-e", source],
      { timeout: 20000 },
      (error, stdout, stderr) =>
        resolve({
          code: error ? (error.code ?? error.signal) : 0,
          stdout,
          stderr,
        }),
    );
  });
}
const bridgeModule = fileURLToPath(
  new URL("../../../packages/client-js/bridge.mts", import.meta.url),
);
const addon = fileURLToPath(
  new URL("../../../bindings/node/axton-node.node", import.meta.url),
);
const prelude = `
import { createRequire } from "node:module";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
const { Bridge } = await import(${JSON.stringify(bridgeModule)});
const native = createRequire(import.meta.url)(${JSON.stringify(addon)});
const schema = JSON.parse(readFileSync(${JSON.stringify(fileURLToPath(schemaUrl))}, "utf8"));
const path = join(mkdtempSync(join(tmpdir(), "axton-bridge-exit-")), "client.sqlite");
`;

test("an outstanding task keeps the process alive until it settles", async () => {
  const { code, stdout, stderr } = await script(`${prelude}
const { bridge } = await Bridge.open(native, { path, schema });
await bridge.task(${JSON.stringify(create("x", "kept"))});
const row = await bridge.task(${JSON.stringify(read("x"))});
console.log("read " + row.text);
`);
  assert.equal(code, 0, stderr);
  assert.equal(stdout.trim(), "read kept");
});

test("an idle open client does not keep the process alive", async () => {
  const started = Date.now();
  const { code, stdout, stderr } = await script(`${prelude}
await Bridge.open(native, { path, schema });
console.log("opened");
`);
  assert.equal(code, 0, stderr);
  assert.equal(stdout.trim(), "opened");
  assert.ok(Date.now() - started < 15000);
});

test("a callback whose task was refused in the same batch never runs", async () => {
  // A close admitted right behind the transaction: the runtime publishes the
  // callback effect, cancels it and refuses the task in one batch, and the
  // Bridge starts callbacks from a deferred continuation.
  const { bridge } = await scripted((command, requestId) =>
    command.kind === "transaction"
      ? [
          {
            type: "effect",
            effectId: "5",
            operation: { kind: "callback", transactionId: "tx1", requestId },
          },
          { type: "cancelEffect", effectId: "5" },
          {
            type: "taskCompleted",
            requestId,
            ok: false,
            value: null,
            error: "client_closed",
          },
          { type: "runtimeClosed" },
        ]
      : [],
  );
  let ran = false;
  await assert.rejects(
    bridge.transaction(async () => {
      ran = true;
    }),
    /client_closed/,
  );
  // The continuation that would start the body was queued before the
  // rejection was delivered; one more macrotask covers anything it chained.
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(ran, false, "the refused callback's body did not run");
  assert.equal(bridge.closed, true);
});

test("a callback whose effect was cancelled never runs, even while its task is pending", async () => {
  const { bridge, publish } = await scripted((command, requestId) =>
    command.kind === "transaction"
      ? [
          {
            type: "effect",
            effectId: "7",
            operation: { kind: "callback", transactionId: "tx2", requestId },
          },
          { type: "cancelEffect", effectId: "7" },
        ]
      : [],
  );
  let ran = false;
  const transaction = bridge.transaction(async () => {
    ran = true;
  });
  const outcome = transaction.catch((error) => error);
  await new Promise((resolve) => setImmediate(resolve));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(ran, false, "a cancelled callback effect does not start");
  publish({ type: "runtimeClosed" });
  assert.match((await outcome).message, /client_closed/);
  assert.equal(ran, false);
});
