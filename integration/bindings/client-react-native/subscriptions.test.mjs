// Subscription handles on the mobile host: the shared runtime with the React
// Native transaction scope and the native carrier, so identity and the
// committed status are the same behaviour Node sees and nothing about
// subscriptions is platform-specific
// ([#150](https://github.com/zanminwang/axton/issues/150)). The package entry
// point itself pulls in `expo-modules-core`, so this suite assembles the client
// from the same pieces, with no network configured.
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { createClient } from "../../../packages/client-js/runtime.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const Client = createClient(native, Transaction, () => {
  throw Error("network not configured");
});
async function until(predicate, what = "condition") {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw Error(`${what} timed out`);
}

test("the mobile host shares the runtime subscription handles and their offline status", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-subscriptions-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  const client = await Client.open({
    path: join(directory, "client.sqlite"),
    schema,
  });
  try {
    const [first, second] = await Promise.all([
      client.streams.subscribe("project:123"),
      client.streams.subscribe("project:123"),
    ]);
    assert.equal(first, second, "concurrent calls obtain one cached handle");
    assert.equal(first.stream, "project:123");
    assert.deepEqual(
      { ...first.status },
      {
        active: true,
        initialization: "pending",
        connection: "offline",
        bootstrap: { phase: "not-requested", error: null },
      },
      "no network is configured: durable intent with no boundary and no transport",
    );
    const seen = [];
    const stop = first.watch((status) => seen.push(status.connection));
    assert.deepEqual(seen, ["offline"], "the current snapshot arrives at once");
    stop();
    assert.equal(
      await client.subscribe("project:123"),
      first,
      "the retained spelling is the same registration",
    );
    await first.unsubscribe();
    assert.deepEqual(
      { ...first.status },
      {
        active: false,
        initialization: "pending",
        connection: "stopped",
        bootstrap: { phase: "not-requested", error: null },
      },
    );
    assert.deepEqual(
      (await client.syncState()).streams,
      [],
      "the registration is gone",
    );
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

/**
 * `bootstrap()` is the same runtime code on the mobile host: registration is an
 * eager local command that needs no transport, concurrent calls share one run,
 * and the codes a closed handle answers with are the ones Node sees
 * ([#151](https://github.com/zanminwang/axton/issues/151)). Completion needs a
 * server, which this suite deliberately has none of, so the barrier itself is
 * covered by the Node host suite.
 */
test("the mobile host registers a bootstrap eagerly and rejects the waiters of a closed client", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-bootstrap-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  const client = await Client.open({
    path: join(directory, "client.sqlite"),
    schema,
  });
  try {
    const subscription = await client.streams.subscribe("project:123");
    assert.deepEqual(
      { ...subscription.status.bootstrap },
      { phase: "not-requested", error: null },
    );
    const first = subscription.bootstrap();
    const second = subscription.bootstrap();
    const outcome = Promise.all([first, second]).then(
      () => "resolved",
      (error) => error.code,
    );
    // Eager: the registration committed without anybody awaiting, and with no
    // boundary to bound its interval the run waits for initialization.
    await until(
      () =>
        subscription.status.bootstrap.phase === "waiting-for-initialization",
      "a registered load with no boundary yet",
    );
    await client.close();
    assert.equal(
      await outcome,
      "client_closed",
      "closing the client rejects the waiters of this process",
    );
    assert.equal(
      (await subscription.bootstrap().then(
        () => null,
        (error) => error,
      ))?.code,
      "subscription.closed",
      "a stopped handle starts nothing",
    );
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});
