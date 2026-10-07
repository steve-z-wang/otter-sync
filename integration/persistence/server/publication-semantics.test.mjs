// Retained publication semantics through generated public API and native Rust.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { Pool } from "pg";
import { pg } from "../../../packages/postgres/index.mts";
import { createBackend } from "../../v05-sdk/backend.ts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
after(() => pool.end());
let serial = 0;
async function fixture(body) {
  const namespace = `publication_semantics_${++serial}`,
    admin = await pool.connect(),
    base = pg(pool);
  const database = {
    ...base,
    transaction: (body) =>
      base.transaction(async (tx) => {
        await tx.query(`SET LOCAL search_path=${namespace}`);
        return body(tx);
      }),
  };
  try {
    await admin.query(`CREATE SCHEMA ${namespace}`);
    await admin.query(`SET search_path=${namespace}`);
    await admin.query(
      await readFile(
        new URL("../../../packages/postgres/migration.sql", import.meta.url),
        "utf8",
      ),
    );
    await admin.query(
      "CREATE TABLE business_entry(id text PRIMARY KEY,text text NOT NULL); CREATE TABLE permission(user_id text,entry_id text,PRIMARY KEY(user_id,entry_id))",
    );
    const loader = async ({ tx, ids, userId }) =>
      Promise.all(
        ids.map(
          async ({ id }) =>
            (
              await tx.query(
                "SELECT b.id,b.text FROM business_entry b JOIN permission p ON p.entry_id=b.id WHERE b.id=$1 AND p.user_id=$2",
                [id, userId],
              )
            ).rows[0] ?? null,
        ),
      );
    const backend = createBackend({
      native,
      database,
      authenticate: () => "alice",
      protocol5: {
        authorizeStream: (owner, stream) => stream === `User:${owner}`,
      },
      mutations: { publish: async () => ({ entry: { id: "unused" } }) },
      queries: {
        find: async () => ({ entry: null }),
        peek: async () => ({ entry: null }),
      },
      loaders: { entry: loader, snapshot: loader },
    });
    const rows = async () =>
      (
        await admin.query(
          "SELECT stream,cursor,kind FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id'='e' ORDER BY stream",
        )
      ).rows;
    const heads = async () =>
      (
        await admin.query(
          "SELECT stream,head FROM axton_stream ORDER BY stream",
        )
      ).rows;
    const fetch = async (owner) =>
      JSON.parse(
        await backend.fetch(
          owner,
          JSON.stringify({
            protocol: 5,
            storeId: `viewer-${owner}`,
            stream: `User:${owner}`,
            materialization: backend.materializationId,
            requestId: `read-${owner}`,
            store: false,
            invocation: {
              kind: "fetch",
              key: { model: "Entry", identity: { id: "e" } },
              version: 1,
            },
          }),
        ),
      );
    await body({ backend, admin, rows, heads, fetch });
  } finally {
    await admin.query(`DROP SCHEMA ${namespace} CASCADE`);
    admin.release();
  }
}
test("explicit [] invalidation reaches nobody; global invalidation dominates targeted in either declaration order", () =>
  fixture(async ({ backend, admin, rows, heads }) => {
    await backend.transaction(async ({ tx, streams }) => {
      await tx.query("INSERT INTO business_entry VALUES('e','first')");
      streams(["User:alice", "User:bob"]).track.entry("e");
    });
    const before = await heads();
    await backend.transaction(async ({ tx, stream, invalidate }) => {
      await tx.query("UPDATE business_entry SET text='none' WHERE id='e'");
      stream([]).invalidate.entry("e");
      invalidate.entry("unheld");
    });
    assert.deepEqual(await heads(), before);
    assert.deepEqual(await rows(), [
      { stream: "User:alice", cursor: "1", kind: "upsert" },
      { stream: "User:bob", cursor: "1", kind: "upsert" },
    ]);
    assert.equal(
      (
        await admin.query(
          "SELECT count(*) n FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id'='unheld'",
        )
      ).rows[0].n,
      "0",
      "invalidating no holders never enrolls",
    );
    for (const reverse of [false, true]) {
      await backend.transaction(async ({ stream, invalidate }) => {
        const targeted = () => stream("User:alice").invalidate.entry("e"),
          global = () => invalidate.entry("e");
        if (reverse) {
          global();
          targeted();
        } else {
          targeted();
          global();
        }
      });
      const cursor = reverse ? "3" : "2";
      assert.deepEqual(await rows(), [
        { stream: "User:alice", cursor, kind: "upsert" },
        { stream: "User:bob", cursor, kind: "upsert" },
      ]);
      assert.deepEqual(await heads(), [
        { stream: "User:alice", head: cursor },
        { stream: "User:bob", head: cursor },
      ]);
    }
  }));
test("targeted permission loss affects only selected viewer; canonical absence preserves holders and recreation reaches them", () =>
  fixture(async ({ backend, admin, rows, fetch }) => {
    await backend.transaction(async ({ tx, streams }) => {
      await tx.query(
        "INSERT INTO business_entry VALUES('e','first'); INSERT INTO permission VALUES('alice','e'),('bob','e')",
      );
      streams(["User:alice", "User:bob"]).track.entry("e");
    });
    const id = (
      await admin.query(
        "SELECT id FROM axton_record WHERE model='Entry' AND identity->>'id'='e'",
      )
    ).rows[0].id;
    await backend.transaction(async ({ tx, stream }) => {
      await tx.query("DELETE FROM permission WHERE user_id='alice'");
      stream("User:alice").invalidate.entry("e");
    });
    assert.deepEqual(await rows(), [
      { stream: "User:alice", cursor: "2", kind: "upsert" },
      { stream: "User:bob", cursor: "1", kind: "upsert" },
    ]);
    assert.equal((await fetch("alice")).records[0].state, null);
    assert.equal((await fetch("bob")).records[0].state.text, "first");
    await backend.transaction(async ({ tx, invalidate }) => {
      await tx.query("DELETE FROM business_entry WHERE id='e'");
      invalidate.entry("e");
    });
    assert.equal((await fetch("bob")).records[0].state, null);
    assert.deepEqual(
      await rows(),
      [
        { stream: "User:alice", cursor: "3", kind: "upsert" },
        { stream: "User:bob", cursor: "2", kind: "upsert" },
      ],
      "absence retains both tracking rows",
    );
    await backend.transaction(async ({ tx, invalidate }) => {
      await tx.query(
        "INSERT INTO business_entry VALUES('e','back'); INSERT INTO permission VALUES('alice','e')",
      );
      invalidate.entry("e");
    });
    assert.equal((await fetch("alice")).records[0].state.text, "back");
    assert.equal((await fetch("bob")).records[0].state.text, "back");
    assert.deepEqual(await rows(), [
      { stream: "User:alice", cursor: "4", kind: "upsert" },
      { stream: "User:bob", cursor: "3", kind: "upsert" },
    ]);
    assert.equal(
      (
        await admin.query(
          "SELECT id FROM axton_record WHERE model='Entry' AND identity->>'id'='e'",
        )
      ).rows[0].id,
      id,
      "recreation retains canonical identity",
    );
  }));
