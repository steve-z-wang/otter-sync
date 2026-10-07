// Generated public API -> lane native Rust -> real caller-owned SQL transactions.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { Pool } from "pg";
import { drizzle as orm } from "drizzle-orm/node-postgres";
import { pg, prisma } from "../../../packages/postgres/index.mts";
import { drizzle } from "../../../packages/postgres/src/drizzle.mts";
import { createBackend } from "../../v05-sdk/backend.ts";
const require = createRequire(import.meta.url),
  native = require("../../../bindings/node/axton-node.node");
const { PrismaClient } = require(
  process.env.AXTON_PRISMA_CLIENT ?? "../../bindings/node/generated/client",
);
const pool = new Pool({ connectionString: process.env.DATABASE_URL }),
  pr = new PrismaClient();
const adapters = [
  ["pg", pg(pool)],
  ["prisma", prisma(pr)],
  ["drizzle", drizzle(orm(pool))],
];
after(async () => {
  await pr.$disconnect();
  await pool.end();
});
let serial = 0;
for (const [name, database] of adapters)
  for (const prior of [false, true])
    for (const existing of [false, true])
      test(`[${name}] caller savepoint preserves ${prior ? "earlier kept reservation" : "first creation rollback"} on ${existing ? "existing" : "new"} Stream`, async () => {
        const namespace = `savepoint_${name}_${++serial}`,
          stream = "User:savepoint";
        const admin = await pool.connect(),
          query = (tx, sql, args = []) => database.driver.query(tx, sql, args);
        try {
          await admin.query(`CREATE SCHEMA ${namespace}`);
          await admin.query(`SET search_path=${namespace}`);
          await admin.query(
            await readFile(
              new URL(
                "../../../packages/postgres/migration.sql",
                import.meta.url,
              ),
              "utf8",
            ),
          );
          await admin.query(
            "CREATE TABLE business_entry(id text PRIMARY KEY,text text NOT NULL)",
          );
          if (existing)
            await admin.query(
              "INSERT INTO axton_stream(stream,head) VALUES($1,4)",
              [stream],
            );
          const backend = createBackend({
            native,
            database,
            authenticate: () => "alice",
            protocol5: { authorizeStream: () => true },
            mutations: { publish: async () => ({ entry: { id: "unused" } }) },
            queries: {
              find: async () => ({ entry: null }),
              peek: async () => ({ entry: null }),
            },
            loaders: {
              entry: async ({ ids }) => ids.map(() => null),
              snapshot: async ({ ids }) => ids.map(() => null),
            },
          });
          let woke = 0;
          const stop = backend.onCommitted(stream, () => woke++);
          try {
            const wakes = await database.transaction(async (tx) => {
              await query(tx, `SET LOCAL search_path=${namespace}`);
              // Force first-creation lifetime even when this driver's connection was reused.
              await query(
                tx,
                "DROP TABLE IF EXISTS pg_temp.axton_publication_cursor",
              );
              const publish = async (id) => {
                await query(tx, "INSERT INTO business_entry VALUES($1,$2)", [
                  id,
                  id,
                ]);
                await backend.acquirePublicationFence(tx);
                return backend.publish(tx, ({ stream: select, invalidate }) => {
                  invalidate.entry({ id });
                  select(stream).track.entry({ id });
                });
              };
              const kept = [];
              if (prior) kept.push(await publish("before"));
              await query(tx, "SAVEPOINT caller_act");
              await publish("undone");
              assert.notEqual(
                (
                  await query(
                    tx,
                    "SELECT to_regclass('pg_temp.axton_publication_cursor')::text relation",
                  )
                )[0].relation,
                null,
              );
              await query(tx, "ROLLBACK TO SAVEPOINT caller_act");
              const relation = (
                await query(
                  tx,
                  "SELECT to_regclass('pg_temp.axton_publication_cursor')::text relation",
                )
              )[0].relation;
              assert.equal(
                relation === null,
                !prior,
                "table creation follows SQL rollback",
              );
              kept.push(await publish("after"));
              assert.equal(woke, 0);
              return kept;
            });
            const ids = prior ? ["after", "before"] : ["after"],
              cursor = existing ? "5" : "1";
            assert.deepEqual(
              (
                await admin.query("SELECT id FROM business_entry ORDER BY id")
              ).rows.map((r) => r.id),
              ids,
            );
            assert.deepEqual(
              (
                await admin.query(
                  "SELECT identity->>'id' id FROM axton_record ORDER BY id",
                )
              ).rows
                .map((r) => r.id)
                .sort(),
              ids,
            );
            assert.deepEqual(
              (
                await admin.query(
                  "SELECT r.identity->>'id' id,s.cursor,h.head FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id JOIN axton_stream h USING(stream) ORDER BY r.identity->>'id'",
                )
              ).rows,
              ids.map((id) => ({ id, cursor, head: cursor })),
            );
            assert.equal(woke, 0, "no automatic caller-owned wake");
            for (const wake of wakes) wake();
            await Promise.resolve();
            assert.equal(woke, wakes.length);
          } finally {
            stop();
          }
        } finally {
          await admin.query(`DROP SCHEMA ${namespace} CASCADE`);
          admin.release();
        }
      });
