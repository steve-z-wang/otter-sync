import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  AxtonReport,
  Effects,
  prerequisites,
  startConnection,
} from "../../../packages/client-js/connection.mts";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-js/transaction.mts";

// The connection is an effect executor (#134): the Rust runtime owns the
// lanes, direct calls, refresh, timeouts and retries, and asks the SDK for
// platform work. The first half drives the executor with a fake Bridge; the
// second half runs the real runtime over a scripted network.

const settled = (ms = 10) => new Promise((resolve) => setTimeout(resolve, ms));
/** Poll until `condition` holds; a real native client answers on its own schedule. */
const eventually = async (condition, what) => {
  const deadline = Date.now() + 5000;
  while (!condition()) {
    if (Date.now() > deadline) assert.fail(`${what} timed out`);
    await settled(5);
  }
};
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((a, b) => {
    resolve = a;
    reject = b;
  });
  return { promise, resolve, reject };
};

/** A Bridge that issues effects and events on demand and records every answer. */
function fakeBridge() {
  const handlers = new Map();
  const listeners = new Map();
  const results = [];
  return {
    results,
    effectResult(effectId, outcome) {
      results.push([effectId, outcome]);
    },
    onEffect(kind, handler) {
      handlers.set(kind, handler);
      return () => {
        if (handlers.get(kind) === handler) handlers.delete(kind);
      };
    },
    on(type, listener) {
      if (!listeners.has(type)) listeners.set(type, new Set());
      listeners.get(type).add(listener);
      return () => listeners.get(type).delete(listener);
    },
    handles: (kind) => handlers.has(kind),
    effect(effectId, operation) {
      const handler = handlers.get(operation.kind);
      if (!handler)
        return results.push([
          effectId,
          { ok: false, error: { message: "unsupported effect" } },
        ]);
      handler(effectId, operation);
    },
    emit(type, event) {
      for (const listener of [...(listeners.get(type) ?? [])])
        listener({ type, ...event });
    },
    cancel(effectId) {
      this.emit("cancelEffect", { effectId });
    },
  };
}
/** An executor over a fake Bridge with one connection installed on `network`. */
function executor(network = {}, options = {}) {
  const bridge = fakeBridge();
  const effects = new Effects(bridge);
  const stop = startConnection(
    bridge,
    effects,
    { push: async () => "", open() {}, ...network },
    options,
  );
  return { bridge, effects, stop };
}

test("an http effect posts to its route and answers with the response text", async () => {
  const posts = [];
  const { bridge } = executor({
    push: async (kind, body, signal) => {
      posts.push({ kind, body, aborted: signal.aborted });
      return `${kind}-answer`;
    },
  });
  bridge.effect("1", { kind: "http", route: "push", body: "frozen" });
  bridge.effect("2", { kind: "http", route: "pull", body: "cursors" });
  bridge.effect("3", { kind: "http", route: "action", body: "call" });
  await settled();
  assert.deepEqual(posts, [
    { kind: "push", body: "frozen", aborted: false },
    { kind: "pull", body: "cursors", aborted: false },
    { kind: "action", body: "call", aborted: false },
  ]);
  assert.deepEqual(bridge.results, [
    ["1", { ok: true, value: "push-answer" }],
    ["2", { ok: true, value: "pull-answer" }],
    ["3", { ok: true, value: "action-answer" }],
  ]);
});

test("a failed http effect answers its message and the HTTP status it carried", async () => {
  const { bridge } = executor({
    push: async (kind) => {
      if (kind === "pull")
        throw Object.assign(Error("pull failed: 503 unavailable"), {
          status: 503,
        });
      throw Error("fetch failed");
    },
  });
  bridge.effect("1", { kind: "http", route: "pull", body: "{}" });
  bridge.effect("2", { kind: "http", route: "push", body: "{}" });
  await settled();
  assert.deepEqual(bridge.results, [
    [
      "1",
      { ok: false, error: { message: "pull failed: 503 unavailable", status: 503 } },
    ],
    ["2", { ok: false, error: { message: "fetch failed" } }],
  ]);
});

test("a socket effect streams its frames, overflow and end under one id", async () => {
  let socket;
  const { bridge } = executor({
    open: (subscribe, signal, on) => (socket = { subscribe, signal, on }),
  });
  bridge.effect("7", { kind: "socket", subscribe: '{"type":"subscribe"}' });
  assert.equal(socket.subscribe, '{"type":"subscribe"}');
  await socket.on.message("page-1");
  await socket.on.overflow();
  await socket.on.message("page-2");
  socket.on.closed(Object.assign(Error("live failed: 401"), { status: 401 }));
  // The stream ended: nothing more is answered for it.
  await socket.on.message("late");
  socket.on.closed(Error("twice"));
  assert.deepEqual(bridge.results, [
    ["7", { ok: true, value: { event: "message", body: "page-1" } }],
    ["7", { ok: true, value: { event: "overflow" } }],
    ["7", { ok: true, value: { event: "message", body: "page-2" } }],
    ["7", { ok: false, error: { message: "live failed: 401", status: 401 } }],
  ]);
  assert.equal(socket.signal.aborted, false, "the socket ended on its own");
});

test("a socket that cannot be opened answers the failure", () => {
  const { bridge } = executor({
    open() {
      throw Error("no sockets here");
    },
  });
  bridge.effect("3", { kind: "socket", subscribe: "{}" });
  assert.deepEqual(bridge.results, [
    ["3", { ok: false, error: { message: "no sockets here" } }],
  ]);
});

test("a timer effect answers once it fires", async () => {
  const { bridge } = executor();
  bridge.effect("4", { kind: "timer", millis: 5 });
  assert.deepEqual(bridge.results, []);
  await settled(30);
  assert.deepEqual(bridge.results, [["4", { ok: true }]]);
});

test("cancelEffect aborts the request, the socket and the timer; late answers are silent", async () => {
  const request = deferred();
  let socket;
  const signals = [];
  const { bridge } = executor({
    // Ignores its abort signal: the executor must fence it on its own.
    push: (kind, body, signal) => {
      signals.push(signal);
      return request.promise;
    },
    open: (subscribe, signal, on) => (socket = { signal, on }),
  });
  bridge.effect("1", { kind: "http", route: "pull", body: "{}" });
  bridge.effect("2", { kind: "socket", subscribe: "{}" });
  bridge.effect("3", { kind: "timer", millis: 5 });
  await settled(0);
  bridge.cancel("1");
  bridge.cancel("2");
  bridge.cancel("3");
  assert.equal(signals[0].aborted, true, "the request was aborted");
  assert.equal(socket.signal.aborted, true, "the socket was aborted");
  request.resolve("late page");
  await socket.on.message("late frame");
  socket.on.closed(Error("late close"));
  await settled(30);
  assert.deepEqual(bridge.results, [], "nothing answers a cancelled effect");
  // Cancelling an effect that is gone, or was never issued, is harmless.
  bridge.cancel("1");
  bridge.cancel("99");
});

test("refreshAuth effects run the application's refresh and answer its outcome", async () => {
  let refreshes = 0;
  const { bridge } = executor(
    {},
    {
      refreshAuth: async () => {
        if (++refreshes === 2) throw Error("refresh failed");
      },
    },
  );
  bridge.effect("1", { kind: "refreshAuth" });
  await settled();
  bridge.effect("2", { kind: "refreshAuth" });
  await settled();
  assert.equal(refreshes, 2);
  assert.deepEqual(bridge.results, [
    ["1", { ok: true }],
    ["2", { ok: false, error: { message: "refresh failed" } }],
  ]);
  // Without an application refresh there is no handler: the runtime was told
  // at connect and never asks.
  const plain = executor();
  assert.equal(plain.bridge.handles("refreshAuth"), false);
});

test("prerequisite effects run the named handler and keep the failure's reason", async () => {
  const bridge = fakeBridge();
  const effects = new Effects(bridge);
  const seen = [];
  const stop = prerequisites(effects, {
    upload: async (args) => {
      seen.push(args);
    },
    fails: async () => {
      throw Error("offline");
    },
    throwsValue: () => {
      throw "plain reason";
    },
  });
  bridge.effect("1", {
    kind: "prerequisite",
    key: "k1",
    name: "upload",
    arguments: { id: "t" },
  });
  bridge.effect("2", { kind: "prerequisite", key: "k2", name: "fails", arguments: {} });
  bridge.effect("3", {
    kind: "prerequisite",
    key: "k3",
    name: "throwsValue",
    arguments: {},
  });
  await settled();
  assert.deepEqual(seen, [{ id: "t" }]);
  // Each answers when its own handler settles; the order among them is free.
  assert.deepEqual(bridge.results.sort(([a], [b]) => a.localeCompare(b)), [
    ["1", { ok: true }],
    ["2", { ok: false, error: { message: "offline" } }],
    ["3", { ok: false, error: { message: "plain reason" } }],
  ]);
  stop();
  assert.equal(bridge.handles("prerequisite"), false);
});

test("reports reach onError: records as AxtonReports, errors with the runtime's message and status", async () => {
  const errors = [];
  const { bridge } = executor(
    {
      push: async () => {
        throw Object.assign(Error("pull failed: 503 unavailable"), { status: 503 });
      },
    },
    { onError: (error) => errors.push(error) },
  );
  bridge.emit("report", {
    diagnostic: {
      kind: "records",
      reports: [
        { kind: "readFailed", model: "Entry", identity: { id: "a" }, stamp: 9, code: "loader.failed" },
        { kind: "skipped", model: "Entry", identity: { id: "b" }, stamp: 3 },
      ],
    },
  });
  // The runtime reports a lane failure itself, with the HTTP status the effect
  // answered with; the executor keeps no failure of its own to recall.
  bridge.effect("1", { kind: "http", route: "pull", body: "{}" });
  await settled();
  assert.deepEqual(bridge.results, [
    ["1", { ok: false, error: { message: "pull failed: 503 unavailable", status: 503 } }],
  ]);
  assert.equal(errors.length, 2, "an effect's failure is not reported by the executor");
  bridge.emit("report", {
    diagnostic: { kind: "error", message: "pull failed: 503 unavailable", status: 503 },
  });
  bridge.emit("report", { diagnostic: { kind: "error", message: "action lost" } });
  bridge.emit("report", {
    diagnostic: { kind: "protocol", message: "duplicate request id 4" },
  });
  assert.equal(errors.length, 5);
  assert.ok(errors[0] instanceof AxtonReport);
  assert.equal(errors[0].kind, "readFailed");
  assert.equal(errors[0].code, "loader.failed");
  assert.match(errors[0].message, /readFailed: Entry .* stamp 9 \(loader.failed\)/);
  assert.ok(errors[1] instanceof AxtonReport);
  assert.equal(errors[1].kind, "skipped");
  assert.ok(errors[2] instanceof Error && !(errors[2] instanceof AxtonReport));
  assert.equal(errors[2].message, "pull failed: 503 unavailable");
  assert.equal(errors[2].status, 503);
  assert.ok(errors[3] instanceof Error && !(errors[3] instanceof AxtonReport));
  assert.equal(errors[3].message, "action lost");
  assert.equal("status" in errors[3], false, "no status, no property");
  assert.equal(errors[4].message, "duplicate request id 4");
});

test("a throwing onError is reported and the rest of the reports still arrive", () => {
  const delivered = [];
  const thrown = Error("application diagnostic failed");
  const observed = [];
  const previous = globalThis.reportError;
  globalThis.reportError = (error) => observed.push(error);
  try {
    const { bridge } = executor(
      {},
      {
        onError: (error) => {
          delivered.push(error);
          throw thrown;
        },
      },
    );
    bridge.emit("report", {
      diagnostic: {
        kind: "records",
        reports: [
          { kind: "conflict", model: "Entry", identity: { id: "a" }, stamp: 1 },
          { kind: "conflict", model: "Entry", identity: { id: "b" }, stamp: 1 },
        ],
      },
    });
    bridge.emit("report", { diagnostic: { kind: "error", message: "socket closed" } });
    assert.equal(delivered.length, 3);
    assert.deepEqual(observed, [thrown, thrown, thrown]);
  } finally {
    globalThis.reportError = previous;
  }
});

test("stopping a connection removes its handlers and aborts what it still holds", async () => {
  const signals = [];
  const request = deferred();
  const errors = [];
  const { bridge, stop } = executor(
    {
      push: (kind, body, signal) => {
        signals.push(signal);
        return request.promise;
      },
      open: (subscribe, signal) => signals.push(signal),
    },
    { refreshAuth: async () => {}, onError: (error) => errors.push(error) },
  );
  bridge.effect("1", { kind: "http", route: "push", body: "{}" });
  bridge.effect("2", { kind: "socket", subscribe: "{}" });
  bridge.effect("3", { kind: "timer", millis: 5 });
  await settled(0);
  stop();
  assert.ok(signals.every((signal) => signal.aborted), "every resource aborted");
  request.resolve("late receipt");
  await settled(30);
  // The timer is the client's, not the connection's: it still fires.
  assert.deepEqual(bridge.results, [["3", { ok: true }]]);
  for (const kind of ["http", "socket", "refreshAuth"])
    assert.equal(bridge.handles(kind), false, `${kind} is uninstalled`);
  assert.equal(bridge.handles("timer"), true);
  bridge.emit("report", { diagnostic: { kind: "error", message: "after" } });
  assert.deepEqual(errors, [], "reports of a closed connection reach nobody");
});

// The real runtime over a scripted network.

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const pingSchema = {
  enums: [],
  models: [],
  actions: [{ name: "Ping", version: 1, inputs: [], outputs: [] }],
};
const entrySchema = async () => {
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  schema.actions = [{ name: "Ping", version: 1, inputs: [], outputs: [] }];
  return schema;
};
const answer = (body) =>
  JSON.stringify({
    completion: {
      callId: JSON.parse(body).call.callId,
      outcome: { status: "succeeded", result: null },
    },
    records: [],
  });
/** A client over `network(kind, body, signal)` and no socket; `carrier` wraps the native one. */
async function scripted(network, body, { schema = pingSchema, carrier = native } = {}) {
  const directory = await mkdtemp(join(tmpdir(), "axton-effects-"));
  const Client = createClient(carrier, Transaction, () => ({
    push: network,
    open() {},
  }));
  const client = await Client.open({ path: join(directory, "db"), schema });
  try {
    await body(client);
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
}

test("direct attempt is bounded even when transport ignores abort", async () => {
  await scripted(
    () => new Promise(() => {}),
    async (client) => {
      const connection = await client.connect(
        { url: "http://unused", token: "token" },
        { directTimeoutMs: 15 },
      );
      await assert.rejects(
        client.callAction("Ping", 1, {}),
        (error) =>
          error.code === "action.execution_unknown" &&
          error.execution === "unknown" &&
          error.cause instanceof Error &&
          error.cause.message === "direct call timed out" &&
          !("status" in error.cause),
      );
      await connection.close();
    },
  );
});

test("a direct call's transport failure and refused refresh become its cause", async () => {
  await scripted(
    async () => {
      throw Object.assign(Error("action failed: 503 busy"), { status: 503 });
    },
    async (client) => {
      const connection = await client.connect({ url: "http://unused", token: "token" });
      const error = await client.callAction("Ping", 1, {}).catch((error) => error);
      assert.equal(error.code, "action.execution_unknown");
      assert.equal(error.execution, "unknown");
      assert.ok(error.cause instanceof Error);
      assert.equal(error.cause.message, "action failed: 503 busy");
      assert.equal(error.cause.status, 503);
      // The typed path keeps the same cause.
      const typed = await client
        .invokeDirectAction("Ping", 1, {}, () => "unreachable")
        .catch((error) => error);
      assert.equal(typed.code, "action.execution_unknown");
      assert.equal(typed.cause.message, "action failed: 503 busy");
      assert.equal(typed.cause.status, 503);
      await connection.close();
    },
  );
  await scripted(
    async () => {
      throw Object.assign(Error("expired"), { status: 401 });
    },
    async (client) => {
      const connection = await client.connect(
        { url: "http://unused", token: "token" },
        {
          refreshAuth: async () => {
            throw Error("refresh refused");
          },
        },
      );
      const error = await client.callAction("Ping", 1, {}).catch((error) => error);
      assert.equal(error.code, "action.execution_unknown");
      assert.equal(error.cause.message, "refresh refused");
      await connection.close();
      // Stopped: unavailable, with no transport cause to report.
      const stopped = await client.callAction("Ping", 1, {}).catch((error) => error);
      assert.equal(stopped.code, "action.unavailable");
    },
  );
});

test("direct timeout rejects values outside the JavaScript timer range", async () => {
  await scripted(
    async (kind, body) => answer(body),
    async (client) => {
      for (const directTimeoutMs of [2_147_483_648, 0, 1.5])
        await assert.rejects(
          client.connect(
            { url: "http://unused", token: "token" },
            { directTimeoutMs },
          ),
          /directTimeoutMs must be an integer from 1 to 2147483647/,
        );
      // A refused option leaves no connection behind.
      const connection = await client.connect(
        { url: "http://unused", token: "token" },
        { directTimeoutMs: 2_147_483_647 },
      );
      assert.deepEqual(await client.callAction("Ping", 1, {}), {
        outcome: { status: "succeeded", result: null },
      });
      await connection.close();
    },
  );
});

test("direct request completes while durable delivery is blocked", async () => {
  const pushed = deferred();
  await scripted(
    async (kind, body) => {
      if (kind === "push") {
        pushed.resolve();
        return new Promise(() => {});
      }
      return answer(body);
    },
    async (client) => {
      const connection = await client.connect({
        url: "http://unused",
        token: "token",
      });
      await client.submitAction("Ping", 1, {});
      await pushed.promise;
      assert.equal(
        await client.invokeDirectAction("Ping", 1, {}, () => "direct"),
        "direct",
      );
      assert.equal((await client.syncState()).pending, 1, "the push is still out");
      await connection.close();
    },
  );
});

test("direct auth retry keeps the same body and close bounds a hanging refresh", async () => {
  const seen = [];
  let refreshes = 0;
  await scripted(
    async (kind, body) => {
      seen.push([kind, body]);
      if (seen.length === 1)
        throw Object.assign(Error("expired"), { status: 401 });
      return answer(body);
    },
    async (client) => {
      const connection = await client.connect(
        { url: "http://unused", token: "token" },
        { refreshAuth: async () => void refreshes++, directTimeoutMs: 500 },
      );
      assert.equal(
        await client.invokeDirectAction("Ping", 1, {}, () => "ok"),
        "ok",
      );
      assert.equal(refreshes, 1);
      assert.equal(seen.length, 2);
      assert.equal(seen[0][0], "action");
      assert.deepEqual(seen[1], seen[0], "the same bytes are sent again");
      await connection.close();
      await assert.rejects(
        client.callAction("Ping", 1, {}),
        (error) => error.code === "action.unavailable",
      );
    },
  );
  await scripted(
    async () => {
      throw Object.assign(Error("expired"), { status: 401 });
    },
    async (client) => {
      const connection = await client.connect(
        { url: "http://unused", token: "token" },
        { refreshAuth: () => new Promise(() => {}), directTimeoutMs: 15 },
      );
      await assert.rejects(
        client.callAction("Ping", 1, {}),
        (error) =>
          error.code === "action.execution_unknown" &&
          error.cause?.message === "direct call timed out",
      );
      await connection.close();
    },
  );
});

test("raw direct Action fails immediately without an active connection", async () => {
  await scripted(
    async () => {
      throw Error("no request is expected");
    },
    async (client) => {
      await assert.rejects(
        client.callAction("Ping", 1, {}),
        (error) =>
          error.code === "action.unavailable" && error.execution === "unknown",
      );
      assert.equal((await client.syncState()).pending, 0);
    },
  );
});

test("raw Action discard and rebuild deliver terminal call identities", async () => {
  const { Client } = await import("../../../packages/client-js/index.mts");
  const directory = await mkdtemp(join(tmpdir(), "axton-action-discard-"));
  const schema = await entrySchema();
  const breaking = structuredClone(schema);
  breaking.models[0].fields.push({ name: "due", nullable: false, type: { kind: "scalar", name: "string" } });
  try {
    for (const frozen of [false, true]) {
      const path = join(directory, frozen ? "frozen" : "unsent");
      const original = await Client.open({ path, schema });
      const delivered = [];
      original.onActionCompletion(value => delivered.push(value));
      const dropped = await original.submitAction("Ping", 1, {});
      await original.drop(dropped.ordinal);
      assert.equal(delivered.length, 1, "a dropped call completes once");
      assert.equal(delivered[0].callId, dropped.callId);
      assert.equal(delivered[0].outcome.code, "dropped");
      const pending = await original.submitAction("Ping", 1, {});
      if (frozen) await original.freeze();
      await original.close();
      const reopened = await Client.open({ path, schema: breaking });
      const abandoned = [];
      reopened.onActionCompletion(value => abandoned.push(value));
      try {
        const report = await reopened.rebuild({ discardPending: true });
        assert.deepEqual(report.abandonedCalls, [{ callId: pending.callId, frozen }]);
        assert.equal(abandoned.length, 1, "each abandoned call completes once");
        assert.equal(abandoned[0].callId, pending.callId);
        assert.equal(abandoned[0].outcome.code, "abandoned");
        assert.equal(abandoned[0].outcome.execution, frozen ? "unknown" : "rejected");
      } finally { await reopened.close(); }
    }
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("a waiting direct network response leaves local reads free and cannot apply after close", async () => {
  let release, entered;
  const waiting = new Promise(resolve => { release = resolve; });
  const begun = new Promise(resolve => { entered = resolve; });
  await scripted(
    (kind, body) => {
      if (kind === "action") { entered(body); return waiting; }
      return Promise.reject(Error("unexpected background request"));
    },
    async (client) => {
      const observed = [];
      const connection = await client.connect({ url: "http://unused", token: "token" });
      client.onActionCompletion(value => observed.push(value));
      const call = client.callAction("Ping", 1, {});
      const failure = assert.rejects(call, error => error.code === "action.unavailable" || error.code === "action.execution_unknown");
      const body = JSON.parse(await begun);
      assert.equal((await Promise.race([client.syncState(), new Promise((_, reject) => setTimeout(() => reject(Error("local queue blocked")), 100))])).pending, 0);
      await connection.close();
      await failure;
      release(JSON.stringify({ completion: { callId: body.call.callId, outcome: { status: "succeeded", result: null } }, records: [] }));
      await new Promise(resolve => setImmediate(resolve));
      assert.deepEqual(observed, []);
      assert.equal((await client.syncState()).pending, 0);
    },
  );
});

test("close aborts an uncooperative push and its late receipt never applies", async () => {
  const pushed = deferred();
  const late = deferred();
  let signal;
  let receipt;
  // The effect ids the runtime asked the SDK to push, and every effect answer
  // the SDK submitted.
  const pushes = [];
  const answered = [];
  const carrier = {
    runtimeOpen: (request, wake) => native.runtimeOpen(request, wake),
    runtimeSubmit(runtimeId, message) {
      const input = JSON.parse(message);
      if (input.type === "effectResult") answered.push(input.effectId);
      native.runtimeSubmit(runtimeId, message);
    },
    runtimeDrain(runtimeId) {
      const text = native.runtimeDrain(runtimeId);
      for (const event of JSON.parse(text))
        if (event.type === "effect" && event.operation.route === "push")
          pushes.push(event.effectId);
      return text;
    },
    runtimeDetach: (runtimeId) => native.runtimeDetach(runtimeId),
  };
  await scripted(
    (kind, body, abort) => {
      assert.equal(kind, "push");
      signal = abort;
      const batch = JSON.parse(body);
      pushed.resolve();
      // Ignores its abort signal: the receipt still arrives, when the test says.
      receipt = late.promise.then(() =>
        JSON.stringify({
          clientId: batch.clientId,
          batchSequence: batch.batchSequence,
          rejections: [],
          records: [],
          completions: batch.mutations.map((mutation) => ({
            callId: mutation.callId,
            outcome: { status: "succeeded", result: null },
          })),
        }),
      );
      return receipt;
    },
    async (client) => {
      const completions = [];
      client.onActionCompletion((completion) => completions.push(completion));
      const connection = await client.connect({ url: "http://unused", token: "token" });
      await client.submitAction("Ping", 1, {});
      await pushed.promise;
      await connection.close();
      assert.equal(signal.aborted, true, "close aborted the request");
      late.resolve();
      await receipt;
      // The executor's continuation of the resolved request has run.
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(pushes.length, 1);
      assert.equal(answered.includes(pushes[0]), false, "the late receipt was never answered");
      // A task admitted after it: whatever the runtime had to apply was applied first.
      assert.equal((await client.syncState()).pending, 1);
      assert.deepEqual(completions, [], "the late receipt settled nothing");
    },
    { carrier },
  );
});

test("client close is priority control while a callback holds the transaction", async () => {
  const within = async (promise, ms) => {
    let timer;
    try {
      return await Promise.race([
        promise.then(() => "settled"),
        new Promise((resolve) => (timer = setTimeout(() => resolve("timeout"), ms))),
      ]);
    } finally {
      clearTimeout(timer);
    }
  };
  const sockets = [];
  const directory = await mkdtemp(join(tmpdir(), "axton-close-callback-"));
  const Client = createClient(native, Transaction, () => ({
    push: () => new Promise(() => {}),
    open: (subscribe, signal) => sockets.push(signal),
  }));
  const client = await Client.open({
    path: join(directory, "db"),
    schema: await entrySchema(),
  });
  const gate = deferred();
  try {
    const connection = await client.connect({ url: "http://unused", token: "token" });
    await client.subscribe("scope");
    await eventually(() => sockets.length === 1, "the socket");
    const entered = deferred();
    const finished = deferred();
    const transaction = client.transaction(async (tx) => {
      try {
        await tx.direct({ model: "Entry", op: "create", identity: { id: "c" }, values: { text: "A", note: null } });
        entered.resolve();
        // External I/O the callback waits on while it holds the transaction.
        await gate.promise;
        await tx.read("Entry", { id: "c" });
      } catch (error) {
        finished.resolve(error);
        throw error;
      }
      finished.resolve(undefined);
    });
    const outcome = transaction.catch((error) => error);
    await entered.promise;
    assert.equal(await within(client.close(), 2000), "settled", "close waits for no task");
    assert.equal((await outcome).message, "client_closed", "the transaction rejects client_closed");
    assert.equal(sockets[0].aborted, true, "the connection's platform work was aborted");
    gate.resolve();
    const late = await finished.promise;
    assert.match(late?.message ?? "", /^(transaction_closed|client_closed)$/, "the callback's later command fails");
    await connection.close();
  } finally {
    gate.resolve();
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("closed connection controls cannot affect a replacement connection", async () => {
  const controls = [];
  const carrier = {
    runtimeOpen: (request, wake) => native.runtimeOpen(request, wake),
    runtimeSubmit(runtimeId, message) {
      const { command } = JSON.parse(message);
      if (command?.kind === "connection" || command?.kind === "connect")
        controls.push(command.event ?? command.kind);
      native.runtimeSubmit(runtimeId, message);
    },
    runtimeDrain: (runtimeId) => native.runtimeDrain(runtimeId),
    runtimeDetach: (runtimeId) => native.runtimeDetach(runtimeId),
  };
  await scripted(
    () => new Promise(() => {}),
    async (client) => {
      const first = await client.connect({ url: "http://unused", token: "token" });
      await first.pause();
      await first.resume();
      await first.wake();
      await first.close();
      assert.deepEqual(controls, ["connect", "pause", "resume", "wake", "stop"]);
      const second = await client.connect({ url: "http://unused", token: "token" });
      await first.pause();
      await first.resume();
      await first.wake();
      await first.close();
      assert.deepEqual(controls.slice(5), ["connect"], "the old handle submits nothing");
      await second.close();
      assert.deepEqual(controls.slice(5), ["connect", "stop"]);
    },
    { carrier },
  );
});

test("closing an old client connection twice preserves ownership of the replacement", async () => {
  const { Client } = await import("../../../packages/client-js/index.mts");
  const directory = await mkdtemp(join(tmpdir(), "axton-connection-"));
  const client = await Client.open({
    path: join(directory, "client.sqlite"),
    schema: await entrySchema(),
  });
  let second;
  try {
    const first = await client.connect({url:"http://127.0.0.1:1",token:"secret"});
    await first.close();
    second = await client.connect({url:"http://127.0.0.1:1",token:"secret"});
    await first.close();
    await assert.rejects(
      client.connect({url:"http://127.0.0.1:1",token:"secret"}),
      /already active/,
    );
  } finally {
    await second?.close();
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("client close waits for in-flight connection setup and remains idempotent", async () => {
  const { Client } = await import("../../../packages/client-js/index.mts");
  const directory = await mkdtemp(join(tmpdir(), "axton-connection-close-"));
  const client = await Client.open({
    path: join(directory, "client.sqlite"),
    schema: await entrySchema(),
  });
  const errors = [];
  try {
    const starting = client.connect(
      {url:"http://127.0.0.1:1",token:"secret"},
      { onError: (error) => errors.push(error) },
    );
    await Promise.all([starting, client.close()]);
    await new Promise((r) => setImmediate(r));
    assert.deepEqual(errors, []);
    await client.close();
    await (await starting).close();
    await assert.rejects(
      client.connect({url:"http://127.0.0.1:1",token:"secret"}),
      /closed/,
    );
  } finally {
    await client.close().catch(() => {});
    await rm(directory, { recursive: true, force: true });
  }
});

/**
 * A rebuild keeps the connected lane running: the runtime abandons the old
 * socket and subscribes the carried Scope again on a session of its own, with
 * no second `connect`. The Dart twin is `a rebuild wakes the sleeping
 * downlink lane without another start` in `packages/dart/test/client_test.dart`
 * ([#162](https://github.com/zanminwang/axton/issues/162)).
 */
test("a rebuild wakes the sleeping downlink lane without another start", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rebuild-wake-"));
  const path = join(directory, "client.sqlite");
  const schema = await entrySchema();
  const breaking = structuredClone(schema);
  breaking.models[0].fields.push({ name: "due", nullable: false, type: { kind: "scalar", name: "string" } });
  // Sockets open and are never acknowledged, and HTTP never answers: once it
  // opened its socket, the lane has nothing to do until it is woken.
  const sockets = [];
  let connects = 0;
  // The effects the runtime asked for and has not cancelled: none of them
  // answers here, so without a timer among them the runtime cannot act again
  // until an input wakes it.
  const outstanding = new Map();
  const carrier = {
    runtimeOpen: (request, wake) => native.runtimeOpen(request, wake),
    runtimeSubmit: (runtimeId, message) => native.runtimeSubmit(runtimeId, message),
    runtimeDrain(runtimeId) {
      const text = native.runtimeDrain(runtimeId);
      for (const event of JSON.parse(text))
        if (event.type === "effect") outstanding.set(event.effectId, event.operation.kind);
        else if (event.type === "cancelEffect") outstanding.delete(event.effectId);
      return text;
    },
    runtimeDetach: (runtimeId) => native.runtimeDetach(runtimeId),
  };
  const asleep = () => [...outstanding.values()].every((kind) => kind === "socket" || kind === "http");
  const Client = createClient(carrier, Transaction, () => {
    connects++;
    return {
      push: (kind, body, signal) =>
        new Promise((resolve, reject) =>
          signal?.addEventListener("abort", () => reject(Error("aborted")), { once: true }),
        ),
      open: (subscribe, signal) => sockets.push({ subscribe: JSON.parse(subscribe), signal }),
    };
  });
  let client;
  let connection;
  try {
    client = await Client.open({ path, schema });
    await client.subscribe("scope");
    // Unsent work keeps the incompatible file open, so the rebuild happens with
    // this client - and its lane - already connected.
    await client.mutate({ name: "Create", operations: [{ model: "Entry", op: "create", identity: { id: "e" }, values: { text: "A", note: null } }] });
    await client.close();
    client = await Client.open({ path, schema: breaking });
    const reported = [];
    connection = await client.connect({ url: "http://127.0.0.1:1", token: "t" }, { onError: (error) => reported.push(error) });
    await eventually(() => sockets.length === 1, "the first handshake");
    // A task admitted after the handshake: every effect the runtime asked for
    // before it has been dispatched when it completes.
    await client.syncState();
    assert.equal(sockets.length, 1);
    assert.ok(asleep(), `the lane sleeps until woken: ${[...outstanding.values()]}`);
    await client.rebuild({ discardPending: true });
    await eventually(() => sockets.length === 2, "the carried Scope subscribed again after the rebuild");
    assert.deepEqual(sockets[1].subscribe.channels, ["scope"]);
    assert.equal(sockets[0].signal.aborted, true, "the old socket was abandoned");
    assert.equal(sockets[1].signal.aborted, false, "the new socket stays");
    await client.syncState();
    assert.equal(sockets.length, 2, "one session per wake");
    assert.ok(asleep(), `the new session sleeps too: ${[...outstanding.values()]}`);
    assert.equal(connects, 1, "no second connect");
    assert.deepEqual(reported, []);
  } finally {
    await connection?.close();
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});
