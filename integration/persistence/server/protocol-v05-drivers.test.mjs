// Real PostgreSQL adapters execute/replay the same Rust Batch path.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { Pool } from "pg";
import { drizzle as orm } from "drizzle-orm/node-postgres";
import {
  createBackend,
  MutationRejected,
} from "../../../packages/backend/server/index.mts";
import { pg, prisma } from "../../../packages/backend/postgres/index.mts";
import { drizzle } from "../../../packages/backend/postgres/src/drizzle.mts";
const require = createRequire(import.meta.url);
const native = require("../../../bindings/node/axton-node.node");
const { PrismaClient } = require(
  process.env.AXTON_PRISMA_CLIENT ?? "../../bindings/node/generated/client",
);
const check = new Pool({ connectionString: process.env.DATABASE_URL }),
  closers = [],
  adapters = [];
after(async () => {
  for (const close of closers) await close();
  await check.end();
});
{
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  adapters.push(["pg", pg(pool)]);
  closers.push(() => pool.end());
}
{
  const client = new PrismaClient();
  adapters.push(["prisma", prisma(client)]);
  closers.push(() => client.$disconnect());
}
{
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  adapters.push(["drizzle", drizzle(orm(pool))]);
  closers.push(() => pool.end());
}
const model = {
  name: "Task",
  version: 1,
  identity: ["id"],
  fields: ["id", "title"].map((name) => ({
    name,
    type: { kind: "scalar", name: "string" },
    nullable: false,
  })),
};
const config = {
  schema: {
    enums: [],
    models: [model],
    actions: [
      {
        name: "Write",
        version: 1,
        kind: "mutation",
        inputs: [
          {
            kind: "model",
            name: "task",
            model: "Task",
            operation: "create",
            cardinality: "single",
          },
        ],
        outputs: [],
      },
    ],
  },
  loaders: ["Task"],
  mutations: [],
};
const canonical = (v) =>
  Array.isArray(v)
    ? "[" + v.map(canonical).join(",") + "]"
    : v && typeof v === "object"
      ? "{" +
        Object.keys(v)
          .sort()
          .map((k) => JSON.stringify(k) + ":" + canonical(v[k]))
          .join(",") +
        "}"
      : JSON.stringify(v);
for (const [name, base] of adapters)
  test(`[${name}] v05 member transactions save independent refusal and replay immutable outcomes`, async () => {
    const namespace = `task3_${name}`;
    await check.query(`CREATE SCHEMA ${namespace}`);
    const setup = await check.connect();
    try {
      await setup.query("BEGIN");
      await setup.query(`SET LOCAL search_path=${namespace}`);
      await setup.query(
        await readFile(
          new URL("../../../packages/backend/postgres/migration.sql", import.meta.url),
          "utf8",
        ),
      );
      await setup.query(
        "CREATE TABLE business_task(id text PRIMARY KEY,title text NOT NULL)",
      );
      await setup.query("COMMIT");
    } catch (error) {
      await setup.query("ROLLBACK");
      throw error;
    } finally {
      setup.release();
    }
    const query = (tx, sql, params = []) => base.driver.query(tx, sql, params);
    const database = {
      ...base,
      transaction: (body) =>
        base.transaction(async (tx) => {
          await query(tx, `SET LOCAL search_path=${namespace}`);
          return body(tx);
        }),
    };
    let calls = 0,
      loads = 0;
    const app = createBackend({
      config,
      native,
      database,
      protocol5: {
        authorizeStream: async (principal, stream, tx) => {
          assert.equal(
            (await query(tx, "SHOW transaction_isolation"))[0]
              .transaction_isolation,
            "serializable",
          );
          return stream === `User:${principal}`;
        },
      },
      authenticate: () => "alice",
      mutations: {
        write: async ({ ctx, args }) => {
          calls++;
          await query(ctx.tx, "INSERT INTO business_task VALUES($1,$2)", [
            args.task.id,
            args.task.title,
          ]);
          ctx.stream("User:alice").track.task([{ id: args.task.id }]);
          if (args.task.title === "refuse")
            throw new MutationRejected("write.no");
          return {};
        },
      },
      loaders: {
        task: async ({ tx, ids }) => {
          loads++;
          return Promise.all(
            ids.map(
              async ({ id }) =>
                (
                  await query(
                    tx,
                    "SELECT id,title FROM business_task WHERE id=$1",
                    [id],
                  )
                )[0] ?? null,
            ),
          );
        },
      },
    });
    const r = {
      protocol: 5,
      storeId: `driver-${name}`,
      stream: "User:alice",
      materialization: app.materializationId,
      batchId: 1,
      digest: "",
      mutations: ["ok", "refuse", "ok"].map((title, i) => ({
        id: i + 1,
        name: "Write",
        version: 1,
        descriptor: "frozen-artifact",
        operations: [
          {
            step: 1,
            inputPath: "task",
            operation: "create",
            model: "Task",
            identity: { id: `t${i}` },
            value: { title },
          },
        ],
      })),
    };
    r.digest = createHash("sha256")
      .update("axton:mutation-batch:5\0")
      .update(
        canonical(
          Object.fromEntries(Object.entries(r).filter(([k]) => k !== "digest")),
        ),
      )
      .digest("hex");
    const wire = await app.push("alice", JSON.stringify(r));
    assert.deepEqual(
      JSON.parse(wire).results.map((x) => x.outcome.kind),
      ["accepted", "rejected", "accepted"],
    );
    assert.equal(calls, 3);
    const savedLoads = loads;
    assert.equal(await app.push("alice", JSON.stringify(r)), wire);
    assert.equal(calls, 3);
    assert.equal(loads, savedLoads);
    assert.equal(
      (
        await check.query(
          `SELECT count(*)::int n FROM ${namespace}.business_task`,
        )
      ).rows[0].n,
      2,
    );
    assert.equal(
      (
        await check.query(
          `SELECT count(*)::int n FROM ${namespace}.axton_mutation_result`,
        )
      ).rows[0].n,
      3,
    );
    assert.equal(
      (
        await check.query(
          `SELECT head FROM ${namespace}.axton_stream WHERE stream='User:alice'`,
        )
      ).rows[0].head,
      "2",
    );
    assert.equal(
      (
        await check.query(
          `SELECT last_processed_batch_id,progress FROM ${namespace}.axton_store`,
        )
      ).rows[0].last_processed_batch_id,
      "1",
    );
  });
