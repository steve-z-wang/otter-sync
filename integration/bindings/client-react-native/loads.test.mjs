// The React Native host shares the TypeScript Load handles (#173); this runs
// them through its own transaction adapter and live carrier - real HTTP to
// `/sync/loads`, the real native runtime and SQLite underneath.
import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { LoadError } from "../../../packages/client-js/loads.mts";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { createServerConnection } from "../../../packages/client-react-native/live.mts";
import { args, page, schema, until } from "../client-js/loads-harness.mjs";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const code = (expected) => (error) =>
  error instanceof LoadError && error.code === expected;

async function backend() {
  const requests = [];
  const server = createServer(async (request, response) => {
    let text = "";
    for await (const chunk of request) text += chunk;
    requests.push({ path: request.url, body: JSON.parse(text) });
    const body = JSON.parse(text);
    response.setHeader("content-type", "application/json");
    response.end(
      JSON.stringify({
        loads: body.loads.map((intent) => page(intent, [["m", "Mobile"]])),
      }),
    );
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { server, requests, url: `http://127.0.0.1:${server.address().port}` };
}

test("mobile Load handles start offline, share once jobs and load over /sync/loads", async () => {
  const { server, requests, url } = await backend();
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-loads-"));
  const path = join(directory, "db");
  const Client = createClient(native, Transaction, (options) =>
    createServerConnection(options),
  );
  let client = await Client.open({ path, schema });
  try {
    const job = await client.startLoad("Entries", 1, args, { once: true });
    assert.equal(job.status.phase, "waiting");
    const joined = await client.startLoad("Entries", 1, args, { once: true });
    assert.equal(joined.id, job.id);
    const ordinary = await client.startLoad("Entries", 1, args);
    assert.notEqual(ordinary.id, job.id);
    const phases = [];
    const stop = job.watch((status) => phases.push(status.phase));
    joined.dispose();
    // The mobile adapter refuses Load work from a transaction callback.
    await client.transaction(async () => {
      await assert.rejects(
        client.startLoad("Entries", 1, args),
        code("transaction_active"),
      );
      await assert.rejects(job.wait(), code("transaction_active"));
      await assert.rejects(
        client.invalidateLoad("Entries", args),
        code("transaction_active"),
      );
    });
    await assert.rejects(
      client.startLoad("Entries", 1, args, { refresh: true }),
      code("load.invalid_options"),
    );
    const connection = await client.connect({ url, token: "token" });
    await Promise.all([job.wait(), ordinary.wait()]);
    stop();
    assert.equal(job.status.phase, "complete");
    assert.equal(phases.at(-1), "complete");
    assert.equal(
      joined.status.phase,
      "waiting",
      "a disposed handle heard nothing",
    );
    assert.deepEqual(await client.read("Entry", { id: "m" }), {
      id: "m",
      text: "Mobile",
      note: null,
    });
    assert.ok(requests.length >= 1);
    assert.ok(requests.every((r) => r.path === "/sync/loads"));
    await connection.close();
    // A complete once job is reused offline; invalidation needs no network.
    const sent = requests.length;
    const hit = await client.startLoad("Entries", 1, args, { once: true });
    assert.equal(hit.id, job.id);
    await hit.wait();
    await client.invalidateLoad("Entries", args);
    const fresh = await client.startLoad("Entries", 1, args, { once: true });
    assert.notEqual(fresh.id, job.id);
    assert.equal(requests.length, sent);
    // Close settles the waiter; the job survives and completes after reopen.
    const waiting = fresh.wait().then(
      () => assert.fail("resolved"),
      (error) => assert.ok(code("client_closed")(error), String(error)),
    );
    await client.close();
    await waiting;
    client = await Client.open({ path, schema });
    const restored = await client.getLoad(fresh.id);
    assert.equal(restored.status.phase, "waiting");
    const again = await client.connect({ url, token: "token" });
    await restored.wait();
    await until(() => restored.status.phase === "complete", "complete status");
    assert.equal((await client.getLoad(job.id)).status.phase, "complete");
    await again.close();
  } finally {
    await client.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});
