import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { pg, type PgClient } from "../../../packages/postgres/index.mts";
import { isRetryableTransactionError } from "../../../packages/server/index.mts";
import { createBackend, devAuth } from "./backend.ts";
import { createProxy } from "../proxy.mts";
import { drainedDatabase } from "../lifecycle.mts";
let sequence = 0;

export async function host({ projected = false } = {}) {
  const namespace = `behavior05_${++sequence}`;
  const admin = new Pool({ connectionString: process.env.DATABASE_URL });
  await admin.query(`CREATE SCHEMA ${namespace}`);
  await admin.end();
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    options: `-c search_path=${namespace}`,
  });
  await pool.query(
    await readFile(
      new URL("../../../packages/postgres/migration.sql", import.meta.url),
      "utf8",
    ),
  );
  await pool.query(
    "CREATE TABLE behavior_parent(id text PRIMARY KEY,title text NOT NULL);CREATE TABLE behavior_child(id text PRIMARY KEY,parent_id text NOT NULL,title text NOT NULL);CREATE TABLE behavior_item(id text PRIMARY KEY,project text NOT NULL,title text NOT NULL UNIQUE);CREATE TABLE behavior_tag(id text PRIMARY KEY,project text NOT NULL,label text NOT NULL);CREATE TABLE behavior_execution(id text NOT NULL)",
  );
  const lifecycle = drainedDatabase(pg(pool));
  const errors: unknown[] = [];
  let failItems = false,
    injectedLoader = false;
  let invalidIdentity = false;
  let loaderCalls = 0,
    queryCalls = 0;
  const backend = createBackend<PgClient>({
    database: lifecycle.database,
    authenticate: devAuth(),
    protocol5: {
      authorizeStream: (owner, stream) => stream === `User:${owner}`,
    },
    onError(error) {
      const message = error instanceof Error ? error.message : String(error);
      if (
        isRetryableTransactionError(error) ||
        message === "live handshake closed"
      )
        return;
      if (
        injectedLoader &&
        (message.includes("injected whole read Loader refusal") ||
          message === "loader.failed")
      )
        return;
      if (invalidIdentity && (/identity|output|title/.test(message) || message === "handler.invalid")) return;
      errors.push(error);
    },
    mutations: {
      async publishItem({ ctx, args }) {
        await ctx.tx.query("INSERT INTO behavior_execution VALUES($1)", [
          args.item.id,
        ]);
        await ctx.tx.query("INSERT INTO behavior_item VALUES($1,$2,$3)", [
          args.item.id,
          args.item.project,
          args.item.title.trim(),
        ]);
        if (!args.private) ctx.stream.track.item(args.item.id);
        return { item: { id: args.item.id } };
      },
      async renameItem({ ctx, args }) {
        await ctx.tx.query("UPDATE behavior_execution SET id=id WHERE id=$1", [
          args.item.id,
        ]);
        await ctx.tx.query("UPDATE behavior_item SET title=$2 WHERE id=$1", [
          args.item.id,
          args.item.title?.trim(),
        ]);
        ctx.invalidate.item(args.item.id);
        return { item: { id: args.item.id } };
      },
    },
    queries: {
      async projectItems({ ctx, args }) {
        queryCalls++;
        const items = await ctx.tx.query(
          "SELECT id FROM behavior_item WHERE project=$1 ORDER BY id",
          [args.project],
        );
        const tags = await ctx.tx.query(
          "SELECT id FROM behavior_tag WHERE project=$1 ORDER BY id",
          [args.project],
        );
        return {
          items: items.rows.map((row) => ({ id: String(row.id) })),
          tags: tags.rows.map((row) => ({ id: String(row.id) })),
        };
      },
      async findItem({ ctx, args }) {
        queryCalls++;
        const { rows } = await ctx.tx.query(
          "SELECT id FROM behavior_item WHERE id=$1",
          [args.id],
        );
        return {
          item: rows[0]
            ? {
                id: String(rows[0].id),
                ...(invalidIdentity ? { title: "not an identity" } : {}),
              }
            : null,
        };
      },
    },
    loaders: {
      async parent({ tx, ids }) {
        loaderCalls++;
        const { rows } = await tx.query(
          "SELECT id,title FROM behavior_parent WHERE id=ANY($1)",
          [ids.map((x) => x.id)],
        );
        return ids.map((x) => {
          const row = rows.find((row) => row.id === x.id);
          return row ? { id: String(row.id), title: String(row.title) } : null;
        });
      },
      async child({ tx, ids }) {
        loaderCalls++;
        const { rows } = await tx.query(
          "SELECT id,parent_id,title FROM behavior_child WHERE id=ANY($1)",
          [ids.map((x) => x.id)],
        );
        return ids.map((x) => {
          const row = rows.find((row) => row.id === x.id);
          return row
            ? {
                id: String(row.id),
                parentId: String(row.parent_id),
                title: String(row.title),
              }
            : null;
        });
      },
      async item({ tx, ids, userId }) {
        loaderCalls++;
        if (failItems) throw Error("injected whole read Loader refusal");
        const { rows } = await tx.query(
          "SELECT id,project,title FROM behavior_item WHERE id=ANY($1)",
          [ids.map((x) => x.id)],
        );
        return ids.map((x) => {
          const row = rows.find((row) => row.id === x.id);
          return row
            ? {
                id: String(row.id),
                project: String(row.project),
                title: projected ? `${userId}:${row.title}` : String(row.title),
              }
            : null;
        });
      },
      async tag({ tx, ids }) {
        loaderCalls++;
        const { rows } = await tx.query(
          "SELECT id,project,label FROM behavior_tag WHERE id=ANY($1)",
          [ids.map((x) => x.id)],
        );
        return ids.map((x) => {
          const row = rows.find((row) => row.id === x.id);
          return row
            ? {
                id: String(row.id),
                project: String(row.project),
                label: String(row.label),
              }
            : null;
        });
      },
      draft: undefined,
    },
    bootstrap: async () => {},
  });
  const listener = await backend.listen({ port: 0 });
  const proxy = await createProxy(listener.url);
  return {
    pool,
    backend,
    proxy,
    get loaderCalls() {
      return loaderCalls;
    },
    get queryCalls() {
      return queryCalls;
    },
    set invalidIdentity(value: boolean) {
      invalidIdentity = value;
    },
    set failItems(value: boolean) {
      failItems = value;
      if (value) injectedLoader = true;
    },
    async close() {
      proxy.releaseAll();
      await proxy.close();
      await listener.close();
      await lifecycle.drain();
      await pool.end();
      if (errors.length)
        throw new AggregateError(errors, "unexpected behavior host errors");
    },
  };
}
