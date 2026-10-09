// A suspended ordinary Query must not serialize independent Batch progress.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { Pool } from "pg";
import { createBackend } from "../../../packages/backend/server/index.mts";
import { pg } from "../../../packages/backend/postgres/index.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
after(() => pool.end());
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
const hash = (domain, v) =>
  createHash("sha256")
    .update(domain + "\0")
    .update(canonical(v))
    .digest("hex");
const read = {
    name: "Read",
    version: 1,
    kind: "query",
    inputs: [],
    outputs: [],
  },
  write = { ...read, name: "Write", kind: "mutation" };
const config = {
  schema: {
    enums: [],
    models: [
      {
        name: "Task",
        version: 1,
        identity: ["id"],
        fields: ["id", "title"].map((name) => ({
          name,
          type: { kind: "scalar", name: "string" },
          nullable: false,
        })),
      },
    ],
    actions: [read, write],
  },
  loaders: ["Task"],
};
for (const kind of ["Query", "Fetch", "Query Model Loader"])
  for (const store of kind === "Query Model Loader" ? [false, true] : [false])
    for (const existing of [false, true])
      test(
        `suspended ${kind} admits same-Store Mutation on ${existing ? "existing" : "new"} Store (store:${store})`,
        { timeout: 10000 },
        async () => {
          const namespace = `read_liveness_${kind.replaceAll(" ", "_")}_${existing}_${store}`,
            admin = await pool.connect();
          let release, entered;
          const gate = new Promise((r) => (release = r)),
            arrived = new Promise((r) => (entered = r));
          const base = pg(pool),
            database = {
              ...base,
              transaction: (body) =>
                base.transaction(async (tx) => {
                  await tx.query(`SET LOCAL search_path=${namespace}`);
                  return body(tx);
                }),
            };
          let pending, mutation;
          try {
            await admin.query(`CREATE SCHEMA ${namespace}`);
            await admin.query(`SET search_path=${namespace}`);
            await admin.query(
              await readFile(
                new URL(
                  "../../../packages/backend/postgres/migration.sql",
                  import.meta.url,
                ),
                "utf8",
              ),
            );
            await admin.query(
              "CREATE TABLE business(id text PRIMARY KEY,title text NOT NULL)",
            );
            if (kind === "Query Model Loader")
              await admin.query(
                "INSERT INTO business VALUES('initial','before')",
              );
            const cfg =
              kind === "Query Model Loader"
                ? {
                    ...config,
                    schema: {
                      ...config.schema,
                      actions: [
                        {
                          ...read,
                          outputs: [
                            {
                              name: "task",
                              kind: "model",
                              model: "Task",
                              modelReadVersion: 1,
                              cardinality: "single",
                              source: "handlerIdentity",
                              handlerType: {
                                kind: "identity",
                                model: "Task",
                                fields: [
                                  {
                                    name: "id",
                                    type: { kind: "scalar", name: "string" },
                                  },
                                ],
                              },
                            },
                          ],
                        },
                        write,
                      ],
                      resultModels: config.schema.models.map((model) => ({
                        ...model,
                        enums: [],
                      })),
                    },
                  }
                : config;
            const app = createBackend({
              config: cfg,
              native,
              database,
              protocol5: {
                authorizeStream: (owner, stream) => stream === `User:${owner}`,
              },
              authenticate: () => "alice",
              loaders: {
                task: async ({ tx, ids }) => {
                  entered();
                  await gate;
                  return Promise.all(
                    ids.map(
                      async ({ id }) =>
                        (
                          await tx.query(
                            "SELECT id,title FROM business WHERE id=$1",
                            [id],
                          )
                        ).rows[0] ?? null,
                    ),
                  );
                },
              },
              queries: {
                read: async () => {
                  if (kind === "Query Model Loader")
                    return { task: { id: "initial" } };
                  entered();
                  await gate;
                  return {};
                },
              },
              mutations: {
                write: async ({ ctx }) => {
                  await ctx.tx.query(
                    "INSERT INTO business VALUES('committed','ok')",
                  );
                  return {};
                },
              },
            });
            const context = {
              protocol: 5,
              storeId: "same-store",
              stream: "User:alice",
              materialization: app.materializationId,
            };
            if (existing)
              await app.handshake(
                "alice",
                JSON.stringify({
                  protocol: 5,
                  storeId: context.storeId,
                  stream: context.stream,
                }),
              );
            pending = app.action(
              "alice",
              JSON.stringify({
                ...context,
                requestId: "suspended",
                store,
                invocation:
                  kind !== "Fetch"
                    ? { kind: "query", name: "Read", version: 1, args: {} }
                    : {
                        kind: "fetch",
                        key: { model: "Task", identity: { id: "committed" } },
                        version: 1,
                      },
              }),
            );
            pending.catch(() => {});
            await arrived;
            const request = {
              ...context,
              batchId: 1,
              digest: "",
              mutations: [
                {
                  id: 1,
                  name: "Write",
                  version: 1,
                  descriptor: hash("axton:mutation-descriptor:5", {
                    ...write,
                    input: null,
                    outputEnums: [],
                  }),
                  operations: [],
                },
              ],
            };
            request.digest = hash(
              "axton:mutation-batch:5",
              Object.fromEntries(
                Object.entries(request).filter(([key]) => key !== "digest"),
              ),
            );
            mutation = app.push("alice", JSON.stringify(request));
            mutation.catch(() => {});
            let timer;
            try {
              const ack = JSON.parse(
                await Promise.race([
                  mutation,
                  new Promise(
                    (_, reject) =>
                      (timer = setTimeout(
                        () =>
                          reject(
                            new Error("Mutation blocked by suspended Query"),
                          ),
                        1500,
                      )),
                  ),
                ]),
              );
              assert.equal(ack.results[0].outcome.kind, "accepted");
            } finally {
              clearTimeout(timer);
            }
            assert.deepEqual(
              (
                await admin.query(
                  "SELECT id FROM business WHERE id='committed'",
                )
              ).rows,
              [{ id: "committed" }],
            );
            release();
            assert.equal(JSON.parse(await pending).outcome.kind, "succeeded");
            assert.equal(
              (
                await admin.query(
                  "SELECT last_processed_batch_id FROM axton_store WHERE id='same-store'",
                )
              ).rows[0].last_processed_batch_id,
              "1",
            );
          } finally {
            release();
            await Promise.allSettled([pending, mutation]);
            await admin.query(`DROP SCHEMA ${namespace} CASCADE`);
            admin.release();
          }
        },
      );

test("ordinary read refuses existing Store owner or Stream mismatch before application work", async () => {
  const namespace = "read_binding_refusal",
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
  let handled = 0;
  try {
    await admin.query(`CREATE SCHEMA ${namespace}`);
    await admin.query(`SET search_path=${namespace}`);
    await admin.query(
      await readFile(
        new URL("../../../packages/backend/postgres/migration.sql", import.meta.url),
        "utf8",
      ),
    );
    const app = createBackend({
      config,
      native,
      database,
      authenticate: () => "alice",
      protocol5: {
        authorizeStream: (owner, stream) => stream === `User:${owner}`,
      },
      mutations: { write: async () => ({}) },
      queries: {
        read: async () => {
          handled++;
          return {};
        },
      },
      loaders: { task: async ({ ids }) => ids.map(() => null) },
    });
    await app.handshake(
      "alice",
      JSON.stringify({ protocol: 5, storeId: "owned", stream: "User:alice" }),
    );
    for (const [owner, stream] of [
      ["bob", "User:bob"],
      ["alice", "User:other"],
    ])
      await assert.rejects(
        app.action(
          owner,
          JSON.stringify({
            protocol: 5,
            storeId: "owned",
            stream,
            materialization: app.materializationId,
            requestId: "forged",
            store: false,
            invocation: { kind: "query", name: "Read", version: 1, args: {} },
          }),
        ),
        (error) => error.code === "store.binding",
      );
    assert.equal(handled, 0);
    assert.deepEqual(
      (
        await admin.query(
          "SELECT principal,stream,last_processed_batch_id FROM axton_store WHERE id='owned'",
        )
      ).rows,
      [
        {
          principal: "alice",
          stream: "User:alice",
          last_processed_batch_id: "0",
        },
      ],
    );
  } finally {
    await admin.query(`DROP SCHEMA ${namespace} CASCADE`);
    admin.release();
  }
});
