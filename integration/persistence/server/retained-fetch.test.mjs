// Requested read contracts and current cache projections share one SQL snapshot.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { Pool } from "pg";
import { createBackend } from "../../../packages/server/index.mts";
import { pg } from "../../../packages/postgres/index.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
after(() => pool.end());
let serial = 0;
const fixture = JSON.parse(
  await readFile(
    new URL("../../../fixtures/protocol/action-results.json", import.meta.url),
    "utf8",
  ),
);
for (const store of [false, true])
  test(`retained Fetch v1 returns caller projection and current v2 cache (store:${store})`, async () => {
    await run(store, async ({ backend, fetch, admin, seen }) => {
      const response = JSON.parse(await fetch(1));
      assert.deepEqual(response.outcome.result, {
        id: "01890f47-1234-7123-8123-123456789abc",
        title: "caller",
      });
      assert.deepEqual(response.records[0].state, {
        title: "cache",
        done: true,
      });
      assert.equal(response.records[0].cursor, null);
      assert.deepEqual(seen, [1, 2]);
      assert.equal(
        (await admin.query("SELECT count(*) n FROM axton_stream_record"))
          .rows[0].n,
        "0",
      );
      assert.equal(
        (await admin.query("SELECT count(*) n FROM axton_record")).rows[0].n,
        "0",
      );
    });
  });
for (const invalid of ["requested", "cache"])
  test(`Fetch refuses malformed ${invalid} version projection without binding or enrollment`, async () => {
    await run(
      true,
      async ({ fetch, admin, seen }) => {
        await assert.rejects(fetch(invalid === "requested" ? 2 : 1));
        assert.deepEqual(seen, invalid === "requested" ? [2] : [1, 2]);
        assert.equal(
          (await admin.query("SELECT count(*) n FROM axton_store")).rows[0].n,
          "0",
        );
        assert.equal(
          (await admin.query("SELECT count(*) n FROM axton_stream_record"))
            .rows[0].n,
          "0",
        );
      },
      invalid,
    );
  });
async function run(store, body, invalid) {
  const namespace = `retained_fetch_${++serial}`,
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
  const seen = [];
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
      "CREATE TABLE business(id uuid PRIMARY KEY,title text NOT NULL,done boolean NOT NULL); INSERT INTO business VALUES('01890f47-1234-7123-8123-123456789abc','stored',true)",
    );
    const schema = { ...fixture.schema, actions: [] };
    const models = [schema.resultModels[0], schema.models[0]];
    const loader =
      (version) =>
      async ({ tx, ids }) => {
        seen.push(version);
        return Promise.all(
          ids.map(async ({ id }) => {
            const row = (
              await tx.query("SELECT * FROM business WHERE id=$1", [id])
            ).rows[0];
            if (!row) return null;
            if (version === 1) return { id: row.id, title: "caller" };
            return invalid
              ? { id: row.id, title: "malformed v1" }
              : { id: row.id, title: "cache", done: row.done };
          }),
        );
      };
    const backend = createBackend({
      native,
      database,
      config: { schema, models, loaders: ["Todo"] },
      authenticate: () => "alice",
      protocol5: {
        authorizeStream: (owner, stream) => stream === `User:${owner}`,
      },
      mutations: {},
      queries: {},
      loaders: { todo: { v1: loader(1), v2: loader(2) } },
      onError: () => {},
    });
    const fetch = (version) =>
      backend.fetch(
        "alice",
        JSON.stringify({
          protocol: 5,
          storeId: `viewer-${serial}`,
          stream: "User:alice",
          materialization: backend.materializationId,
          requestId: "fetch",
          store,
          invocation: {
            kind: "fetch",
            key: {
              model: "Todo",
              identity: { id: "01890f47-1234-7123-8123-123456789abc" },
            },
            version,
          },
        }),
      );
    await body({ backend, fetch, admin, seen });
  } finally {
    await admin.query(`DROP SCHEMA ${namespace} CASCADE`);
    admin.release();
  }
}
