// Requested read contracts and current cache projections share one SQL snapshot.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { Pool } from "pg";
import { createBackend } from "../../../packages/backend/server/index.mts";
import { pg } from "../../../packages/backend/postgres/index.mts";
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
for (const kind of ["Fetch", "Query"])
  for (const store of [false, true])
    for (const fault of [undefined, "malformed", "throwing"])
      test(`retained ${kind} v1 ${store ? "validates current cache" : "ignores unused cache"} (${fault ?? "valid"})`, async () => {
        await run(
          store,
          async ({ fetch, admin, seen }) => {
            if (store && fault) {
              await assert.rejects(fetch(1));
              assert.deepEqual(seen, [1, 2]);
              assert.equal(
                (await admin.query("SELECT count(*) n FROM axton_store"))
                  .rows[0].n,
                "0",
              );
            } else {
              const response = JSON.parse(await fetch(1));
              const caller = {
                id: "01890f47-1234-7123-8123-123456789abc",
                title: "caller",
              };
              assert.deepEqual(
                response.outcome.result,
                kind === "Fetch" ? caller : { todo: caller },
              );
              // False carries requested-version evidence only; it is never installed as cache.
              assert.deepEqual(
                response.records[0].state,
                store ? { title: "cache", done: true } : { title: "caller" },
              );
              assert.equal(response.records[0].cursor, null);
              assert.deepEqual(seen, store ? [1, 2] : [1]);
              assert.equal(
                (await admin.query("SELECT count(*) n FROM axton_store"))
                  .rows[0].n,
                "1",
              );
            }
            for (const table of ["axton_stream_record", "axton_record"])
              assert.equal(
                (await admin.query(`SELECT count(*) n FROM ${table}`)).rows[0]
                  .n,
                "0",
              );
          },
          fault,
          kind,
        );
      });
for (const kind of ["Fetch", "Query"])
  for (const store of [false, true])
    test(`${kind} refuses malformed requested projection (store:${store}) without binding or enrollment`, async () => {
      await run(
        store,
        async ({ fetch, admin, seen }) => {
          await assert.rejects(fetch(1));
          assert.deepEqual(seen, [1]);
          for (const table of [
            "axton_store",
            "axton_stream_record",
            "axton_record",
          ])
            assert.equal(
              (await admin.query(`SELECT count(*) n FROM ${table}`)).rows[0].n,
              "0",
            );
        },
        "requested",
        kind,
      );
    });
for (const kind of ["Fetch", "Query"])
  for (const store of [false, true])
    test(`${kind} refuses malformed current requested v2 (store:${store})`, async () => {
      await run(
        store,
        async ({ fetch, admin, seen }) => {
          await assert.rejects(fetch(2));
          assert.deepEqual(seen, [2]);
          assert.equal(
            (await admin.query("SELECT count(*) n FROM axton_store")).rows[0].n,
            "0",
          );
        },
        "malformed",
        kind,
        2,
      );
    });
async function run(store, body, invalid, kind, requestedVersion = 1) {
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
        new URL("../../../packages/backend/postgres/migration.sql", import.meta.url),
        "utf8",
      ),
    );
    await admin.query(
      "CREATE TABLE business(id uuid PRIMARY KEY,title text NOT NULL,done boolean NOT NULL); INSERT INTO business VALUES('01890f47-1234-7123-8123-123456789abc','stored',true)",
    );
    const schema = {
      ...fixture.schema,
      resultModels:
        requestedVersion === 2
          ? [
              ...fixture.schema.resultModels,
              { ...fixture.schema.models[0], enums: [] },
            ]
          : fixture.schema.resultModels,
      actions:
        kind === "Query"
          ? fixture.schema.actions.map((action) => ({
              ...action,
              kind: "query",
              outputs: action.outputs.map((output) => ({
                ...output,
                modelReadVersion: requestedVersion,
              })),
            }))
          : [],
    };
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
            if (version === 1)
              return invalid === "requested"
                ? { id: row.id }
                : { id: row.id, title: "caller" };
            if (invalid === "throwing")
              throw new Error("unused cache Loader failed");
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
      queries:
        kind === "Query"
          ? {
              find: async () => ({
                todo: { id: "01890f47-1234-7123-8123-123456789abc" },
              }),
            }
          : {},
      loaders: { todo: { v1: loader(1), v2: loader(2) } },
      onError: () => {},
    });
    const fetch = (version) =>
      backend[kind === "Query" ? "action" : "fetch"](
        "alice",
        JSON.stringify({
          protocol: 5,
          storeId: `viewer-${serial}`,
          stream: "User:alice",
          materialization: backend.materializationId,
          requestId: "fetch",
          store,
          invocation:
            kind === "Query"
              ? {
                  kind: "query",
                  name: "Find",
                  version: 1,
                  args: { query: "first" },
                }
              : {
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
