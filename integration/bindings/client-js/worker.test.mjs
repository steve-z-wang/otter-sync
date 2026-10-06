import { openStore } from "./store-fixture.mjs";
import test from "node:test";
import assert from "node:assert/strict";
import { Worker } from "node:worker_threads";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { Client } from "../../../packages/client-js/index.mts";

// Worker teardown (#134): the addon's env cleanup hook detaches every runtime
// opened in a terminated worker's environment, so the actor rolls back its
// open transaction and releases the database file.
const schema = JSON.parse(
  await readFile(
    new URL("../../../fixtures/schemas/entry.json", import.meta.url),
    "utf8",
  ),
);
async function within(promise, ms, what) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise(
        (_, reject) =>
          (timer = setTimeout(() => reject(Error(`${what} timed out`)), ms)),
      ),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

test("a terminated worker's runtime releases its database to the main thread", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-worker-"));
  const path = join(directory, "client.sqlite");
  try {
    const worker = new Worker(
      new URL("./worker-transaction.mjs", import.meta.url),
      { workerData: { path, schema } },
    );
    const [message] = await within(
      once(worker, "message"),
      10_000,
      "the worker's transaction",
    );
    assert.equal(message, "holding");
    await worker.terminate();
    const client = await within(
      openStore(Client, { path, schema }),
      5_000,
      "reopening the file",
    );
    try {
      // A write needs the lock the worker's transaction held.
      await within(
        client.transaction((tx) =>
          tx.direct({
            model: "Entry",
            op: "create",
            identity: { id: "main" },
            values: { text: "ok" },
          }),
        ),
        5_000,
        "a write after the worker ended",
      );
      assert.equal((await client.read("Entry", { id: "main" })).text, "ok");
      assert.equal(
        await client.read("Entry", { id: "worker" }),
        null,
        "the worker's transaction rolled back",
      );
    } finally {
      await client.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("terminating one worker releases every owned Store without closing the main environment", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-worker-many-"));
  const paths = ["one", "two", "three"].map((name) =>
    join(directory, `${name}.sqlite`),
  );
  const clients = [];
  let worker;
  try {
    const main = await openStore(Client, {
      path: join(directory, "main.sqlite"),
      schema,
    });
    clients.push(main);
    worker = new Worker(new URL("./worker-transaction.mjs", import.meta.url), {
      workerData: { paths, schema },
    });
    const [message] = await within(
      once(worker, "message"),
      10_000,
      "all worker transactions",
    );
    assert.equal(message, "holding");
    await worker.terminate();
    for (const path of paths) {
      const client = await within(
        openStore(Client, { path, schema }),
        5_000,
        "immediately reopening each owned file",
      );
      clients.push(client);
      await within(
        client.transaction((tx) =>
          tx.direct({
            model: "Entry",
            op: "create",
            identity: { id: "main" },
            values: { text: "released" },
          }),
        ),
        5_000,
        "writing each released file",
      );
      assert.equal(
        (await client.read("Entry", { id: "main" })).text,
        "released",
      );
      assert.equal(
        await client.read("Entry", { id: "worker" }),
        null,
        "each unfinished worker transaction rolled back",
      );
    }
    await main.transaction((tx) =>
      tx.direct({
        model: "Entry",
        op: "create",
        identity: { id: "still-live" },
        values: { text: "main env" },
      }),
    );
    assert.equal(
      (await main.read("Entry", { id: "still-live" })).text,
      "main env",
      "cleanup affects only the terminated environment",
    );
  } finally {
    await worker?.terminate();
    await Promise.all(clients.map((client) => client.close()));
    await rm(directory, { recursive: true, force: true });
  }
});
