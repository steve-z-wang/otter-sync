// Private bound Stream observer routing and Bootstrap handle lifecycle.
// Real native single-Stream behavior also runs in scope-protocol/bootstrap suites.
import test from "node:test";
import assert from "node:assert/strict";
import { Subscriptions } from "../../../packages/frontend/client-js/api/subscriptions.mts";
import { Bridge } from "../../../packages/frontend/client-js/bindings/bridge.mts";
const notRequested = { phase: "not-requested", error: null };
/**
 * The handles driven through a Bridge over a scripted runtime, so what the
 * runtime publishes is exact: the snapshot queued behind the task that names
 * its observer, a terminal snapshot, and every failure code a parked
 * `streamBootstrap` can end with ([#134](https://github.com/zanminwang/axton/issues/134)).
 * The runtime decides all of it; these tests pin what the handle makes of it.
 */
const statusOf = (over = {}) => ({
  active: true,
  initialization: "ready",
  connection: "live",
  bootstrap: notRequested,
  ...over,
});
const snapshot = (status, closed) => ({
  kind: "subscription",
  stream: "scope",
  subscriptionId: 1,
  status,
  ...(closed ? { closed: true } : {}),
});
const changed = (status, closed) => ({
  type: "observerChanged",
  observerId: "7",
  snapshot: snapshot(status, closed),
});
const done = (requestId, value = null) => ({
  type: "taskCompleted",
  requestId,
  ok: true,
  value,
});
const failed = (requestId, error, details) => ({
  type: "taskCompleted",
  requestId,
  ok: false,
  value: null,
  error,
  ...(details ? { details } : {}),
});
async function scriptedRuntime() {
  let wake;
  let observed = false;
  const outbox = [];
  const tasks = [];
  const reported = [];
  const later = () => setImmediate(() => wake("1"));
  const carrier = {
    runtimeOpen(request, wakeRuntime) {
      wake = wakeRuntime;
      outbox.push(
        done(JSON.parse(request).requestId, {
          clientId: "c",
        }),
      );
      later();
      return "1";
    },
    runtimeSubmit(runtimeId, message) {
      const input = JSON.parse(message);
      if (input.type === "close") outbox.push({ type: "runtimeClosed" });
      if (input.type !== "task") {
        later();
        return;
      }
      tasks.push({ ...input.command, requestId: input.requestId });
      switch (input.command.kind) {
        // The observer's first snapshot rides in the same batch, after the task.
        // A repeated call for a known identity publishes nothing: the status is unchanged.
        case "streamSubscribe":
          outbox.push(
            done(input.requestId, {
              state: {
                stream: "scope",
                subscriptionId: 1,
                startingCursor: 0,
                cursor: 0,
              },
              observerId: "7",
            }),
          );
          if (!observed) {
            observed = true;
            outbox.push(changed(statusOf()));
          }
          break;
        // A bootstrap parks until the test publishes its outcome.
      }
      later();
    },
    runtimeDrain: () => JSON.stringify(outbox.splice(0)),
    runtimeDetach() {},
  };
  const { bridge } = await Bridge.open(carrier, {
    path: "unused",
    schema: { enums: [], models: [] },
  });
  const subscriptions = new Subscriptions(bridge, (error) =>
    reported.push(error),
  );
  return {
    bridge,
    subscriptions,
    tasks,
    reported,
    publish(...events) {
      if (events.some((e) => e.snapshot?.closed)) observed = false;
      outbox.push(...events);
      wake("1");
    },
    bootstraps: () => tasks.filter((t) => t.kind === "streamBootstrap"),
  };
}

test("a status observer attached right after subscribe resolves sees the current snapshot once", async () => {
  const { subscriptions, publish } = await scriptedRuntime();
  const subscription = await subscriptions.subscribe("scope");
  const seen = [];
  subscription.watch((status) => seen.push(status));
  assert.deepEqual(
    seen,
    [statusOf()],
    "the snapshot published behind the task, exactly once",
  );
  assert.ok(
    Object.isFrozen(seen[0]) && Object.isFrozen(seen[0].bootstrap),
    "the runtime snapshot is immutable",
  );
  assert.equal(subscription.status, seen[0]);
  assert.equal(
    await subscriptions.subscribe("scope"),
    subscription,
    "the same identity answers the cached handle",
  );
  assert.equal(seen.length, 1, "a repeated subscribe publishes nothing new");
  const failure = { code: "bootstrap.request_rejected", message: "HTTP 403" };
  publish(
    changed(
      statusOf({
        connection: "catching-up",
        bootstrap: { phase: "failed", error: failure },
      }),
    ),
  );
  assert.deepEqual(
    seen.map((s) => s.connection),
    ["live", "catching-up"],
  );
  assert.deepEqual(subscription.status.bootstrap.error, failure);
  assert.ok(Object.isFrozen(subscription.status.bootstrap.error));
});

test("a terminal snapshot ends the handle: observers hear it once, then nothing, and work through it is local", async () => {
  const { subscriptions, publish, tasks } = await scriptedRuntime();
  const subscription = await subscriptions.subscribe("scope");
  const seen = [];
  subscription.watch((status) => seen.push(status));
  // A terminal status published by the runtime.
  publish(changed(statusOf({ active: false, connection: "stopped" }), true));
  assert.deepEqual(
    seen.map((s) => [s.active, s.connection]),
    [
      [true, "live"],
      [false, "stopped"],
    ],
  );
  publish(changed(statusOf()));
  assert.equal(seen.length, 2, "nothing follows a terminal snapshot");
  assert.equal(subscription.status.active, false);
  const late = [];
  subscription.watch((status) => late.push(status.connection))();
  assert.deepEqual(
    late,
    ["stopped"],
    "a closed handle delivers its last snapshot once",
  );
  const before = tasks.length;
  assert.equal(
    (
      await subscription.bootstrap().then(
        () => null,
        (error) => error,
      )
    )?.code,
    "subscription.closed",
  );
  assert.equal(tasks.length, before, "a closed handle submits nothing");
  const replacement = await subscriptions.subscribe("scope");
  assert.notEqual(
    replacement,
    subscription,
    "the forgotten identity is a new handle",
  );
});

test("a registration the engine refuses as closed rejects with subscription.closed", async () => {
  // The runtime ended the bound handle before its terminal snapshot arrived.
  const { subscriptions, publish, bootstraps } = await scriptedRuntime();
  const subscription = await subscriptions.subscribe("scope");
  const refused = subscription.bootstrap().then(
    () => null,
    (error) => error,
  );
  const other = subscription.bootstrap().then(
    () => null,
    (error) => error,
  );
  assert.equal(bootstraps().length, 2, "eager: submitted when called");
  assert.deepEqual(bootstraps()[0], {
    kind: "streamBootstrap",
    stream: "scope",
    subscriptionId: 1,
    requestId: bootstraps()[0].requestId,
  });
  const text =
    "subscription.closed: subscription 1 for scope is closed; it has no bootstrap state";
  publish(
    failed(bootstraps()[0].requestId, text, { code: "subscription.closed" }),
    failed(bootstraps()[1].requestId, "the database is locked"),
  );
  const rejected = await refused;
  assert.equal(
    rejected?.code,
    "subscription.closed",
    `the engine's text must not reach the caller: ${rejected?.message}`,
  );
  assert.equal(rejected.message, "subscription.closed");
  // An unrelated engine failure is still the caller's to see, unchanged.
  const unrelated = await other;
  assert.equal(unrelated?.message, "the database is locked");
  assert.equal(unrelated.code, undefined);
});

test("bootstrap maps every code the runtime fails a waiter with to its public error", async () => {
  const { subscriptions, publish, bootstraps } = await scriptedRuntime();
  const subscription = await subscriptions.subscribe("scope");
  const outcomes = [
    ["subscription.closed", { code: "subscription.closed" }],
    ["client_closed", { code: "client_closed" }],
    ["bootstrap.superseded", { code: "bootstrap.superseded" }],
    ["HTTP 403", { code: "bootstrap.request_rejected", message: "HTTP 403" }],
    // Tasks still queued when the runtime closed carry no details.
    ["client_closed", undefined],
  ];
  const calls = outcomes.map(() =>
    subscription.bootstrap().then(
      () => "resolved",
      (error) => error,
    ),
  );
  const resolved = subscription.bootstrap();
  publish(
    ...outcomes.map(([error, details], i) =>
      failed(bootstraps()[i].requestId, error, details),
    ),
    done(bootstraps().at(-1).requestId),
  );
  const errors = await Promise.all(calls);
  assert.deepEqual(
    errors.map((e) => e.code),
    [
      "subscription.closed",
      "client_closed",
      "bootstrap.superseded",
      "bootstrap.request_rejected",
      "client_closed",
    ],
  );
  assert.deepEqual(
    errors.map((e) => e.message),
    [
      "subscription.closed",
      "client_closed",
      "the bootstrap run of scope this call waited for was superseded",
      "HTTP 403",
      "client_closed",
    ],
  );
  assert.equal(
    await resolved,
    undefined,
    "a completed run resolves with nothing",
  );
});

test("a waiter whose run was superseded is rejected, never resolved by the newer one", async () => {
  // Run 1's waiter is failed by the runtime once run 2 is observed; the newer
  // run settles only the call that belongs to it.
  const { subscriptions, publish, bootstraps } = await scriptedRuntime();
  const subscription = await subscriptions.subscribe("scope");
  const first = subscription.bootstrap().then(
    () => null,
    (error) => error,
  );
  publish(changed(statusOf({ bootstrap: { phase: "loading", error: null } })));
  const second = subscription.bootstrap().then(
    () => null,
    (error) => error,
  );
  publish(
    failed(bootstraps()[0].requestId, "bootstrap.superseded", {
      code: "bootstrap.superseded",
    }),
  );
  assert.equal(
    (await first)?.code,
    "bootstrap.superseded",
    "run 1 must not resolve from run 2",
  );
  publish(
    changed(statusOf({ bootstrap: { phase: "complete", error: null } })),
    done(bootstraps()[1].requestId),
  );
  assert.equal(await second, null);
  assert.deepEqual(
    { ...subscription.status.bootstrap },
    { phase: "complete", error: null },
  );
});

test("a closing client marks its handles stopped; one the runtime never ended stops locally", async () => {
  const { subscriptions, publish, bootstraps, tasks } = await scriptedRuntime();
  const subscription = await subscriptions.subscribe("scope");
  const seen = [];
  subscription.watch((status) => seen.push(status.connection));
  const pending = subscription.bootstrap().then(
    () => null,
    (error) => error,
  );
  subscriptions.close();
  // The runtime's close: the waiter fails `client_closed`, then the terminal snapshot.
  publish(
    failed(bootstraps()[0].requestId, "client_closed", {
      code: "client_closed",
    }),
    changed(statusOf({ active: false, connection: "stopped" }), true),
  );
  assert.equal((await pending)?.code, "client_closed");
  assert.deepEqual(seen, ["live", "stopped"]);
  const before = tasks.length;
  assert.equal(
    (
      await subscription.bootstrap().then(
        () => null,
        (error) => error,
      )
    )?.code,
    "subscription.closed",
    "a stopped handle cannot commit work",
  );
  assert.equal(tasks.length, before);
  // A runtime that ended without a terminal snapshot for this one.
  const other = await scriptedRuntime();
  const orphan = await other.subscriptions.subscribe("scope");
  const heard = [];
  orphan.watch((status) => heard.push(status));
  other.subscriptions.close();
  await other.bridge.close();
  other.subscriptions.closed();
  assert.deepEqual(
    heard.map((s) => [s.active, s.connection]),
    [
      [true, "live"],
      [false, "stopped"],
    ],
  );
  assert.ok(Object.isFrozen(orphan.status));
});

test("an observer that throws is reported and hears every later snapshot", async () => {
  const { subscriptions, publish, reported } = await scriptedRuntime();
  const subscription = await subscriptions.subscribe("scope");
  const seen = [];
  subscription.watch((status) => {
    seen.push(status.connection);
    throw Error(`observer failed at ${status.connection}`);
  });
  publish(
    changed(statusOf({ connection: "offline" })),
    changed(statusOf({ connection: "connecting" })),
  );
  assert.deepEqual(seen, ["live", "offline", "connecting"]);
  assert.deepEqual(
    reported.map((e) => e.message),
    [
      "observer failed at live",
      "observer failed at offline",
      "observer failed at connecting",
    ],
  );
  assert.equal(
    subscription.status.connection,
    "connecting",
    "the exception changed nothing",
  );
});
