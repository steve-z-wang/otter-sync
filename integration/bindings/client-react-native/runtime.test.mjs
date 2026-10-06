// RN transaction adapter over the real Rust runtime and SQLite. Device sockets
// and Expo loading are exercised separately by the platform smoke suite.
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { openStore, offlineNetwork } from "../client-js/store-fixture.mjs";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const Client = createClient(native, Transaction, offlineNetwork);
const schema = JSON.parse(
  await readFile(
    new URL("../../../fixtures/schemas/entry.json", import.meta.url),
    "utf8",
  ),
);
schema.actions = ["Edit", "Create"].map((name) => ({
  name,
  version: 1,
  kind: "mutation",
  inputs: [
    {
      kind: "model",
      name: "entry",
      model: "Entry",
      operation: name === "Edit" ? "update" : "create",
      cardinality: "single",
      allowedFields: ["text", "note"],
    },
  ],
  outputs: [],
}));
const write = (id, text, op = "create") => ({
  model: "Entry",
  op,
  identity: { id },
  values: { text, note: null },
});
async function fixture(body) {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-runtime-"));
  const path = join(directory, "store");
  let client;
  const open = async () => (client = await openStore(Client, { path, schema }));
  try {
    await body(await open(), open);
  } finally {
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
}
async function within(promise, ms = 1000) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise(
        (resolve) => (timer = setTimeout(() => resolve("timeout"), ms)),
      ),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

test("mobile bound Store has no hooks, anonymous mutations, Load jobs or public subscriptions", async () => {
  await fixture(async (client) => {
    for (const name of [
      "streams",
      "subscribe",
      "unsubscribe",
      "onStore",
      "mutate",
      "invokeAction",
      "startLoad",
    ])
      assert.equal(name in client, false, name);
    await client.transaction(async (tx) => {
      for (const name of ["streams", "mutate", "local"])
        assert.equal(name in tx, false, name);
      await tx.direct(write("e", "local"));
    });
    assert.deepEqual((await client.syncState()).streams, ["User:viewer"]);
    assert.equal((await client.read("Entry", { id: "e" })).text, "local");
  });
});

test("mobile scope preserves rollback, escaped scope refusal and persistent identity on real SQLite", async () => {
  await fixture(async (client, open) => {
    const id = client.clientId;
    await assert.rejects(
      client.transaction(async (tx) => {
        await tx.direct(write("rolled-back", "no"));
        throw Error("abort");
      }),
      /abort/,
    );
    assert.equal(await client.read("Entry", { id: "rolled-back" }), null);
    let retained;
    await client.transaction(async (tx) => {
      retained = tx;
      await tx.direct(write("kept", "yes"));
    });
    await assert.rejects(retained.read("Entry", { id: "kept" }), /closed/);
    await client.close();
    client = await open();
    assert.equal(client.clientId, id);
    assert.equal((await client.read("Entry", { id: "kept" })).text, "yes");
    let release, entered;
    const gate = new Promise((resolve) => (release = resolve)),
      ready = new Promise((resolve) => (entered = resolve));
    const transaction = client.transaction(async (tx) => {
      await tx.direct(write("kept", "after", "update"));
      entered();
      await gate;
    });
    await ready;
    let resolved = false;
    const read = client.read("Entry", { id: "kept" }).then((row) => {
      resolved = true;
      return row;
    });
    await new Promise(setImmediate);
    assert.equal(resolved, false);
    release();
    await transaction;
    assert.equal((await read).text, "after");
  });
});

test("mobile Mutation refuses callback and unrelated callers promptly under its coarse guard", async () => {
  await fixture(async (client) => {
    await client.direct(write("e", "before"));
    let release, entered;
    const gate = new Promise((resolve) => (release = resolve)),
      ready = new Promise((resolve) => (entered = resolve));
    const submit = (text) =>
      client.submitMutation(
        "Edit",
        1,
        { entry: { id: "e", text, note: null } },
        (value) => value,
      );
    const transaction = client.transaction(async () => {
      entered();
      assert.equal(
        await within(
          submit("inside").then(
            () => "",
            (error) => error.message,
          ),
        ),
        "transaction_active",
      );
      await gate;
    });
    try {
      await ready;
      assert.equal(
        await within(
          submit("unrelated").then(
            () => "",
            (error) => error.message,
          ),
        ),
        "transaction_active",
      );
    } finally {
      release();
      await transaction;
    }
    assert.equal((await client.syncState()).pending, 0);
    assert.equal((await submit("after")).status, "pending");
    assert.equal((await client.read("Entry", { id: "e" })).text, "after");
  });
});

test("mobile Bootstrap awaited inside its own callback refuses instead of parking behind it", async () => {
  await fixture(async (client) => {
    await client.transaction(async () => {
      assert.equal(
        await within(
          client.bootstrap().then(
            () => "",
            (error) => error.message,
          ),
        ),
        "transaction_active",
      );
    });
  });
});

test("mobile callback-before-input and optimism roll back together when typed input is invalid", async () => {
  await fixture(async (client) => {
    await assert.rejects(
      client.submitMutation(
        "Create",
        1,
        async (tx) => {
          await tx.direct(write("companion", "local"));
          return { entry: { id: "failed", unknown: "invalid" } };
        },
        (value) => value,
      ),
    );
    assert.equal((await client.syncState()).pending, 0);
    assert.equal(await client.read("Entry", { id: "companion" }), null);
    assert.equal(await client.read("Entry", { id: "failed" }), null);
    const call = await client.submitMutation(
      "Create",
      1,
      { entry: { id: "good", text: "written", note: null } },
      (value) => value,
    );
    assert.equal(call.status, "pending");
    assert.equal((await client.read("Entry", { id: "good" })).text, "written");
  });
});

test("priority close settles a held mobile transaction and rolls back its uncommitted write", async () => {
  await fixture(async (client, open) => {
    let release, entered, held;
    const gate = new Promise((resolve) => (release = resolve)),
      ready = new Promise((resolve) => (entered = resolve));
    const result = client
      .transaction(async (tx) => {
        held = tx;
        await tx.direct(write("uncommitted", "no"));
        entered();
        await gate;
      })
      .then(
        () => null,
        (error) => error,
      );
    await ready;
    await client.close();
    assert.match((await result).message, /closed/);
    await assert.rejects(held.read("Entry", { id: "uncommitted" }), /closed/);
    release();
    client = await open();
    assert.equal(await client.read("Entry", { id: "uncommitted" }), null);
  });
});

test("concurrent top-level mobile transactions serialize and the next observes the committed first", async () => {
  await fixture(async (client) => {
    let release, entered;
    const gate = new Promise((resolve) => (release = resolve)),
      ready = new Promise((resolve) => (entered = resolve)),
      order = [];
    const first = client.transaction(async (tx) => {
      order.push("first:begin");
      await tx.direct(write("a", "first"));
      entered();
      await gate;
      order.push("first:end");
    });
    await ready;
    const second = client.transaction(async (tx) => {
      order.push("second:begin");
      assert.equal((await tx.read("Entry", { id: "a" })).text, "first");
      await tx.direct(write("b", "second"));
      order.push("second:end");
    });
    await new Promise(setImmediate);
    assert.deepEqual(order, ["first:begin"]);
    release();
    await Promise.all([first, second]);
    assert.deepEqual(order, [
      "first:begin",
      "first:end",
      "second:begin",
      "second:end",
    ]);
    assert.equal((await client.query("Entry")).length, 2);
  });
});
