import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { pg, type PgClient } from "../../packages/postgres/index.mts";
import { CallRejected, createBackend, devAuth, type Mutations, type Queries, type Loaders } from "./backend.ts";

export async function createFixture() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  let handlerCalls = 0;
  let loaderCalls = 0;
  let queryCalls = 0;
  /** Every AddNote argument exactly as a handler received it. */
  const notes: { id: string; body: string; mood: string; createdAt: Date; tag: string | null }[] = [];
  const onceCalls = { todoPage: 0, countTodos: 0 };
  let failQueries = false;
  const mutations: Mutations<PgClient> = {
    async addTodo({ ctx, args }) {
      handlerCalls++;
      await ctx.tx.query("INSERT INTO action_e2e_todo(id,title) VALUES($1,$2)", [args.todo.id, args.todo.title.trim()]);
      // Joins its Channel once: this change and every later one reach its subscribers.
      ctx.channel("todos:demo").todo.add(args.todo);
    },
    async updateTodo({ ctx, args }) {
      handlerCalls++;
      const changed = await ctx.tx.query("UPDATE action_e2e_todo SET title=$2 WHERE id=$1 RETURNING id", [args.todo.id, args.todo.title?.trim()]);
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
    },
    async deleteTodo({ ctx, args }) {
      handlerCalls++;
      const changed = await ctx.tx.query("DELETE FROM action_e2e_todo WHERE id=$1 RETURNING id", [args.todo.id]);
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
    },
    async editAndShow({ ctx, args }) {
      handlerCalls++;
      const changed = await ctx.tx.query("UPDATE action_e2e_todo SET title=$2 WHERE id=$1 RETURNING id", [args.todo.id, args.todo.title?.trim()]);
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
      // The `todo` output names the shown record, not the edited input.
      return { todo: { id: args.shown } };
    },
    async annotateTodo({ ctx, args }) {
      handlerCalls++;
      const changed = await ctx.tx.query("UPDATE action_e2e_todo SET title=$2 WHERE id=$1 RETURNING id", [args.todo.id, args.todo.title?.trim()]);
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
      await ctx.tx.query("UPDATE action_e2e_note SET body=$2 WHERE id=$1", [args.note, args.body]);
      ctx.touch.note({ id: args.note });
    },
    async sendEmail({ ctx, args }) {
      handlerCalls++;
      const inserted = await ctx.tx.query("INSERT INTO action_e2e_outbox(recipient,subject,body) VALUES($1,$2,$3) RETURNING id", [args.to, args.subject, args.body]);
      return { messageId: String(inserted.rows[0].id) };
    },
    // Retained v1 of SearchTodos was a Mutation; current clients call the v2 Query.
    async searchTodos() {
      throw new CallRejected("search.v1_retired");
    },
    async addNote({ ctx, args }) {
      handlerCalls++;
      // The handler sees the complete, client-expanded record; nothing is filled here.
      notes.push({ ...args.note });
      await ctx.tx.query("INSERT INTO action_e2e_note(id,body,mood,created_at,tag) VALUES($1,$2,$3,$4,$5)", [args.note.id, args.note.body, args.note.mood, args.note.createdAt.toISOString(), args.note.tag]);
      return { saved: { id: args.note.id } };
    },
    async retitleTodos({ ctx, args }) {
      handlerCalls++;
      const rows = (await ctx.tx.query("UPDATE action_e2e_todo SET title=$2 WHERE title ILIKE '%' || $1 || '%' RETURNING id", [args.query, args.title])).rows.map((row) => ({ id: String(row.id) })).sort((a, b) => a.id.localeCompare(b.id));
      // Explicit extra touches: distributed to their Channels, not caller authority.
      for (const todo of rows) ctx.touch.todo(todo);
      return { todos: rows, first: rows[0] ?? null };
    },
  };
  const queries: Queries<PgClient> = {
    searchTodos: {
      async v2({ ctx, args }) {
        handlerCalls++;
        queryCalls++;
        if (ctx.userId !== "alice") throw Error("untrusted Query owner");
        const rows = (await ctx.tx.query("SELECT id FROM action_e2e_todo WHERE ($1::text IS NULL OR title ILIKE '%' || $1 || '%') ORDER BY id", [args.query])).rows;
        return { todos: rows.map((row) => ({ id: String(row.id) })), first: rows[0] ? { id: String(rows[0].id) } : null, count: rows.length, labels: rows.map((row) => String(row.id)), hint: args.query };
      },
    },
    // Each execution has a distinct asOf, so a reused result is observable.
    async todoPage({ ctx, args }) {
      onceCalls.todoPage++;
      if (failQueries) throw new CallRejected("query.down");
      const rows = (await ctx.tx.query("SELECT id FROM action_e2e_todo WHERE ($1::text IS NULL OR title ILIKE '%' || $1 || '%') ORDER BY id", [args.query])).rows;
      return { todos: rows.map((row) => ({ id: String(row.id) })), count: rows.length, asOf: new Date(Date.UTC(2026, 0, 1, 0, 0, onceCalls.todoPage)), next: rows.length ? `after:${rows[rows.length - 1].id}` : null };
    },
    async countTodos({ ctx }) {
      onceCalls.countTodos++;
      return { count: Number((await ctx.tx.query("SELECT COUNT(*)::int AS n FROM action_e2e_todo")).rows[0].n) };
    },
  };
  const loaders: Loaders<PgClient> = {
    async note({ ids, tx }) {
      const rows: ({ id: string; body: string; mood: "calm" | "busy"; createdAt: Date; tag: string | null } | null)[] = [];
      for (const { id } of ids) {
        const row = (await tx.query("SELECT id,body,mood,created_at,tag FROM action_e2e_note WHERE id=$1", [id])).rows[0];
        rows.push(row ? { id: String(row.id), body: String(row.body), mood: row.mood === "busy" ? "busy" : "calm", createdAt: new Date(String(row.created_at)), tag: row.tag === null ? null : String(row.tag) } : null);
      }
      return rows;
    },
    async todo({ ids, tx }) {
      loaderCalls++;
      const rows: ({ id: string; title: string } | null)[] = [];
      for (const { id } of ids) {
        const row = (await tx.query("SELECT id,title FROM action_e2e_todo WHERE id=$1", [id])).rows[0];
        rows.push(row ? { id: String(row.id), title: String(row.title) } : null);
      }
      return rows;
    },
  };
  const backend = createBackend<PgClient>({ database: pg(pool), authenticate: devAuth(), mutations, queries, loaders });
  let listener: Awaited<ReturnType<typeof backend.listen>> | undefined;
  return {
    pool,
    backend,
    get handlerCalls() { return handlerCalls; },
    get queryCalls() { return queryCalls; },
    get loaderCalls() { return loaderCalls; },
    notes,
    /** Real handler executions of the once-test Queries. */
    onceCalls,
    set failQueries(value: boolean) { failQueries = value; },
    async initialize() {
      const migration = await readFile(new URL("../../packages/postgres/migration.sql", import.meta.url), "utf8");
      for (const sql of migration.split(";").map((statement) => statement.trim()).filter(Boolean)) await pool.query(sql);
      await pool.query("CREATE TABLE action_e2e_todo(id text PRIMARY KEY,title text NOT NULL)");
      await pool.query("CREATE TABLE action_e2e_note(id text PRIMARY KEY,body text NOT NULL,mood text NOT NULL,created_at text NOT NULL,tag text)");
      await pool.query("CREATE TABLE action_e2e_outbox(id bigserial PRIMARY KEY,recipient text NOT NULL,subject text NOT NULL,body text NOT NULL)");
    },
    async listen() { listener = await backend.listen({ port: 0 }); return listener; },
    async close() { await listener?.close(); await pool.end(); },
  };
}
