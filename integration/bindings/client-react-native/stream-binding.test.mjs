// A physical Store has one bound Stream; public subscribe/unsubscribe no
// longer changes its identity. These checks use RN's actual native carrier.
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "../../../packages/frontend/client-js/api/runtime.mts";
import { Transaction } from "../../../packages/frontend/client-react-native/api/transaction.mts";
import { openStore, offlineNetwork } from "../client-js/store-fixture.mjs";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const Client = createClient(native, Transaction, offlineNetwork),
  schema = {
    models: [],
    enums: [],
    actions: [
      { name: "Ping", version: 1, kind: "query", inputs: [], outputs: [] },
    ],
  };

test("mobile Stream binding survives close/reopen and rejects a different binding for the same file", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-binding-")),
    path = join(directory, "store");
  let client;
  try {
    client = await openStore(Client, { path, schema });
    const id = client.clientId;
    assert.deepEqual((await client.syncState()).streams, ["User:viewer"]);
    assert.equal("streams" in client, false);
    await client.close();
    await assert.rejects(
      openStore(Client, { path, schema, stream: "User:other" }),
      /Stream mismatch/,
    );
    client = await openStore(Client, { path, schema });
    assert.equal(client.clientId, id);
    assert.deepEqual((await client.syncState()).streams, ["User:viewer"]);
  } finally {
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("two mobile clients require separate physical files and hold independent Stream bindings", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-stores-"));
  let a, b;
  try {
    a = await openStore(Client, {
      path: join(directory, "a"),
      schema,
      stream: "Workspace:A",
    });
    await assert.rejects(
      openStore(Client, {
        path: join(directory, "a"),
        schema,
        stream: "Workspace:A",
      }),
      /already_open|owned|lock/i,
    );
    b = await openStore(Client, {
      path: join(directory, "b"),
      schema,
      stream: "Workspace:B",
    });
    assert.notEqual(a.clientId, b.clientId);
    assert.deepEqual((await a.syncState()).streams, ["Workspace:A"]);
    assert.deepEqual((await b.syncState()).streams, ["Workspace:B"]);
  } finally {
    await a?.close();
    await b?.close();
    await rm(directory, { recursive: true, force: true });
  }
});
