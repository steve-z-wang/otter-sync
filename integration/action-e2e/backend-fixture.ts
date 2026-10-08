import { drainedDatabase } from "../e2e/lifecycle.mts";
import { isRetryableTransactionError } from "../../packages/server/index.mts";
import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { pg, type PgClient } from "../../packages/postgres/index.mts";
import {
  CallRejected,
  createBackend,
  devAuth,
  type Mutations,
  type Queries,
  type Loaders,
} from "./backend.ts";

export async function createFixture() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  let handlerCalls = 0;
  let loaderCalls = 0;
  let queryCalls = 0;
  /** Every AddNote argument exactly as a handler received it. */
  const notes: {
    id: string;
    body: string;
    mood: string;
    createdAt: Date;
    tag: string | null;
  }[] = [];
  /** Every Restamp argument exactly as the handler received it (#189). */
  const restamps: {
    note?: { id: string; createdAt?: Date } | null;
    at: Date;
  }[] = [];
  const queryExecutions = { todoPage: 0, countTodos: 0 };
  let failQueries = false;
  let queryGate: (() => Promise<void>) | undefined;
  /** Every PublishEntry argument exactly as the handler received it. */
  const publishes: {
    entry: { id: string; title: string; body: string };
    media: { id: string; entryId: string; url: string }[];
    placement: {
      id: string;
      entryId: string;
      journal: string;
      position: number;
    };
  }[] = [];
  let rejectPublish = false;
  /** UpdateTodo attempts left that end in a real serialization failure (#202). */
  let conflictUpdates = 0;
  /** Entry identities PublishEntry rejects, so one push can mix outcomes. */
  const rejectedEntries = new Set<string>();
  const mutations: Mutations<PgClient> = {
    async addTodo({ ctx, args }) {
      handlerCalls++;
      await ctx.tx.query(
        "INSERT INTO action_e2e_todo(id,title) VALUES($1,$2)",
        [args.todo.id, args.todo.title.trim()],
      );
      // Joins its Scope once: this change and every later one reach its subscribers.
      ctx.stream.track.todo(args.todo.id);
      ctx.invalidate.todo(args.todo.id);
    },
    async updateTodo({ ctx, args }) {
      handlerCalls++;
      if (conflictUpdates > 0) {
        conflictUpdates--;
        // A real conflict, not a thrown code: read the row, let another
        // connection change and commit it, then write it from this snapshot.
        await ctx.tx.query("SELECT title FROM action_e2e_todo WHERE id=$1", [
          args.todo.id,
        ]);
        await pool.query("UPDATE action_e2e_todo SET title=title WHERE id=$1", [
          args.todo.id,
        ]);
      }
      const changed = await ctx.tx.query(
        "UPDATE action_e2e_todo SET title=$2 WHERE id=$1 RETURNING id",
        [args.todo.id, args.todo.title?.trim()],
      );
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
      ctx.invalidate.todo(args.todo.id);
    },
    async deleteTodo({ ctx, args }) {
      handlerCalls++;
      const changed = await ctx.tx.query(
        "DELETE FROM action_e2e_todo WHERE id=$1 RETURNING id",
        [args.todo.id],
      );
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
      ctx.invalidate.todo(args.todo.id);
    },
    async editAndShow({ ctx, args }) {
      handlerCalls++;
      const changed = await ctx.tx.query(
        "UPDATE action_e2e_todo SET title=$2 WHERE id=$1 RETURNING id",
        [args.todo.id, args.todo.title?.trim()],
      );
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
      ctx.invalidate.todo(args.todo.id);
      // The `todo` output names the shown record, not the edited input.
      return { todo: { id: args.shown } };
    },
    async annotateTodo({ ctx, args }) {
      handlerCalls++;
      const changed = await ctx.tx.query(
        "UPDATE action_e2e_todo SET title=$2 WHERE id=$1 RETURNING id",
        [args.todo.id, args.todo.title?.trim()],
      );
      if (changed.rows.length === 0) throw new CallRejected("todo.missing");
      ctx.invalidate.todo(args.todo.id);
      await ctx.tx.query("UPDATE action_e2e_note SET body=$2 WHERE id=$1", [
        args.note,
        args.body,
      ]);
      ctx.invalidate.note({ id: args.note });
    },
    async sendEmail({ ctx, args }) {
      handlerCalls++;
      const inserted = await ctx.tx.query(
        "INSERT INTO action_e2e_outbox(recipient,subject,body) VALUES($1,$2,$3) RETURNING id",
        [args.to, args.subject, args.body],
      );
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
      await ctx.tx.query(
        "INSERT INTO action_e2e_note(id,body,mood,created_at,tag) VALUES($1,$2,$3,$4,$5)",
        [
          args.note.id,
          args.note.body,
          args.note.mood,
          args.note.createdAt.toISOString(),
          args.note.tag,
        ],
      );
      ctx.stream.track.note(args.note.id);
      ctx.invalidate.note(args.note.id);
      return { saved: { id: args.note.id } };
    },
    // Moves a Note's createdAt when `note` is given and echoes `at` (#189).
    async restamp({ ctx, args }) {
      handlerCalls++;
      restamps.push(structuredClone(args));
      if (args.note?.createdAt)
        await ctx.tx.query(
          "UPDATE action_e2e_note SET created_at=$2 WHERE id=$1",
          [args.note.id, args.note.createdAt.toISOString()],
        );
      if (args.note?.createdAt) ctx.invalidate.note(args.note.id);
      return { at: args.at };
    },
    // Stores the Entry, its media and its Journal placement in one backend
    // transaction. While `rejectPublish` is set, or for an Entry listed in
    // `rejectedEntries`, it rejects after the Entry and media are inserted,
    // so the rejection must roll those back. A client's local companion
    // writes never arrive here.
    async publishEntry({ ctx, args }) {
      handlerCalls++;
      publishes.push(structuredClone(args));
      await ctx.tx.query(
        "INSERT INTO action_e2e_entry(id,title,body) VALUES($1,$2,$3)",
        [args.entry.id, args.entry.title, args.entry.body],
      );
      for (const media of args.media)
        await ctx.tx.query(
          "INSERT INTO action_e2e_media(id,entry_id,url) VALUES($1,$2,$3)",
          [media.id, media.entryId, media.url],
        );
      if (rejectPublish || rejectedEntries.has(args.entry.id))
        throw new CallRejected("publish.rejected");
      await ctx.tx.query(
        "INSERT INTO action_e2e_placement(id,entry_id,journal,position) VALUES($1,$2,$3,$4)",
        [
          args.placement.id,
          args.placement.entryId,
          args.placement.journal,
          args.placement.position,
        ],
      );
      ctx.stream.track.entry(args.entry.id);
      ctx.invalidate.entry(args.entry.id);
      ctx.stream.track.media(args.media.map((row) => row.id));
      ctx.invalidate.media(args.media.map((row) => row.id));
      ctx.stream.track.placement(args.placement.id);
      ctx.invalidate.placement(args.placement.id);
    },
    async retitleTodos({ ctx, args }) {
      handlerCalls++;
      const rows = (
        await ctx.tx.query(
          "UPDATE action_e2e_todo SET title=$2 WHERE title ILIKE '%' || $1 || '%' RETURNING id",
          [args.query, args.title],
        )
      ).rows
        .map((row) => ({ id: String(row.id) }))
        .sort((a, b) => a.id.localeCompare(b.id));
      // Explicit extra touches: distributed to their Scopes, not caller authority.
      for (const todo of rows) ctx.invalidate.todo(todo);
      return { todos: rows, first: rows[0] ?? null };
    },
  };
  const queries: Queries<PgClient> = {
    searchTodos: {
      async v2({ ctx, args }) {
        handlerCalls++;
        queryCalls++;
        if (ctx.userId !== "alice") throw Error("untrusted Query owner");
        const rows = (
          await ctx.tx.query(
            "SELECT id FROM action_e2e_todo WHERE ($1::text IS NULL OR title ILIKE '%' || $1 || '%') ORDER BY id",
            [args.query],
          )
        ).rows;
        return {
          todos: rows.map((row) => ({ id: String(row.id) })),
          first: rows[0] ? { id: String(rows[0].id) } : null,
          count: rows.length,
          labels: rows.map((row) => String(row.id)),
          hint: args.query,
        };
      },
    },
    // Each execution has a distinct asOf, so a reused result is observable.
    async todoPage({ ctx, args }) {
      if (queryGate) await queryGate();
      queryExecutions.todoPage++;
      if (failQueries) throw new CallRejected("query.down");
      const rows = (
        await ctx.tx.query(
          "SELECT id FROM action_e2e_todo WHERE ($1::text IS NULL OR title ILIKE '%' || $1 || '%') ORDER BY id",
          [args.query],
        )
      ).rows;
      return {
        todos: rows.map((row) => ({ id: String(row.id) })),
        count: rows.length,
        asOf: new Date(Date.UTC(2026, 0, 1, 0, 0, queryExecutions.todoPage)),
        next: rows.length ? `after:${rows[rows.length - 1].id}` : null,
      };
    },
    async countTodos({ ctx }) {
      queryExecutions.countTodos++;
      return {
        count: Number(
          (await ctx.tx.query("SELECT COUNT(*)::int AS n FROM action_e2e_todo"))
            .rows[0].n,
        ),
      };
    },
  };
  const loaders: Loaders<PgClient> = {
    async note({ ids, tx }) {
      const rows: ({
        id: string;
        body: string;
        mood: "calm" | "busy";
        createdAt: Date;
        tag: string | null;
      } | null)[] = [];
      for (const { id } of ids) {
        const row = (
          await tx.query(
            "SELECT id,body,mood,created_at,tag FROM action_e2e_note WHERE id=$1",
            [id],
          )
        ).rows[0];
        rows.push(
          row
            ? {
                id: String(row.id),
                body: String(row.body),
                mood: row.mood === "busy" ? "busy" : "calm",
                createdAt: new Date(String(row.created_at)),
                tag: row.tag === null ? null : String(row.tag),
              }
            : null,
        );
      }
      return rows;
    },
    // No Composition Loader: Compositions are device-only (#187). The backend
    // stores none, publishes none, and would refuse to start if a Mutation
    // named one on the wire.
    async entry({ ids, tx }) {
      const rows: ({ id: string; title: string; body: string } | null)[] = [];
      for (const { id } of ids) {
        const row = (
          await tx.query(
            "SELECT id,title,body FROM action_e2e_entry WHERE id=$1",
            [id],
          )
        ).rows[0];
        rows.push(
          row
            ? {
                id: String(row.id),
                title: String(row.title),
                body: String(row.body),
              }
            : null,
        );
      }
      return rows;
    },
    async media({ ids, tx }) {
      const rows: ({ id: string; entryId: string; url: string } | null)[] = [];
      for (const { id } of ids) {
        const row = (
          await tx.query(
            "SELECT id,entry_id,url FROM action_e2e_media WHERE id=$1",
            [id],
          )
        ).rows[0];
        rows.push(
          row
            ? {
                id: String(row.id),
                entryId: String(row.entry_id),
                url: String(row.url),
              }
            : null,
        );
      }
      return rows;
    },
    async placement({ ids, tx }) {
      const rows: ({
        id: string;
        entryId: string;
        journal: string;
        position: number;
      } | null)[] = [];
      for (const { id } of ids) {
        const row = (
          await tx.query(
            "SELECT id,entry_id,journal,position FROM action_e2e_placement WHERE id=$1",
            [id],
          )
        ).rows[0];
        rows.push(
          row
            ? {
                id: String(row.id),
                entryId: String(row.entry_id),
                journal: String(row.journal),
                position: Number(row.position),
              }
            : null,
        );
      }
      return rows;
    },
    async todo({ ids, tx }) {
      loaderCalls++;
      const rows: ({ id: string; title: string } | null)[] = [];
      for (const { id } of ids) {
        const row = (
          await tx.query("SELECT id,title FROM action_e2e_todo WHERE id=$1", [
            id,
          ])
        ).rows[0];
        rows.push(
          row ? { id: String(row.id), title: String(row.title) } : null,
        );
      }
      return rows;
    },
  };
  const lifecycle = drainedDatabase(pg(pool));
  const unexpected: unknown[] = [];
  const backend = createBackend<PgClient>({
    database: lifecycle.database,
    authenticate: devAuth(),
    protocol5: {
      authorizeStream: (viewer, stream) => stream === `User:${viewer}`,
    },
    mutations,
    queries,
    loaders,
    onError(error) {
      if (
        isRetryableTransactionError(error) ||
        (error instanceof Error && error.message === "live handshake closed")
      )
        return;
      unexpected.push(error);
    },
  });
  let listener: Awaited<ReturnType<typeof backend.listen>> | undefined;
  return {
    pool,
    backend,
    get handlerCalls() {
      return handlerCalls;
    },
    get queryCalls() {
      return queryCalls;
    },
    get loaderCalls() {
      return loaderCalls;
    },
    notes,
    /** Restamp arguments in arrival order. */
    restamps,
    /** Real handler executions of the fresh Queries. */
    queryExecutions,
    set queryGate(value: (() => Promise<void>) | undefined) {
      queryGate = value;
    },
    set failQueries(value: boolean) {
      failQueries = value;
    },
    /** PublishEntry arguments in arrival order. */
    publishes,
    /** While set, PublishEntry rejects with `publish.rejected` after partial inserts, which its transaction rolls back. */
    set rejectPublish(value: boolean) {
      rejectPublish = value;
    },
    /** Entry identities PublishEntry rejects like `rejectPublish`, leaving other calls of the same push accepted. */
    rejectedEntries,
    /** The next this-many UpdateTodo attempts each fail PostgreSQL serialization (40001) on their write. */
    get conflictUpdates() {
      return conflictUpdates;
    },
    set conflictUpdates(value: number) {
      conflictUpdates = value;
    },
    async initialize() {
      const migration = await readFile(
        new URL("../../packages/postgres/migration.sql", import.meta.url),
        "utf8",
      );
      await pool.query(migration); // one simple-protocol call: the file holds dollar-quoted functions
      await pool.query(
        "CREATE TABLE action_e2e_todo(id text PRIMARY KEY,title text NOT NULL)",
      );
      await pool.query(
        "CREATE TABLE action_e2e_note(id text PRIMARY KEY,body text NOT NULL,mood text NOT NULL,created_at text NOT NULL,tag text)",
      );
      await pool.query(
        "CREATE TABLE action_e2e_outbox(id bigserial PRIMARY KEY,recipient text NOT NULL,subject text NOT NULL,body text NOT NULL)",
      );
      await pool.query(
        "CREATE TABLE action_e2e_entry(id text PRIMARY KEY,title text NOT NULL,body text NOT NULL)",
      );
      await pool.query(
        "CREATE TABLE action_e2e_media(id text PRIMARY KEY,entry_id text NOT NULL REFERENCES action_e2e_entry(id),url text NOT NULL)",
      );
      await pool.query(
        "CREATE TABLE action_e2e_placement(id text PRIMARY KEY,entry_id text NOT NULL REFERENCES action_e2e_entry(id),journal text NOT NULL,position integer NOT NULL)",
      );
    },
    async listen() {
      listener = await backend.listen({ port: 0 });
      return listener;
    },
    async close() {
      await listener?.close();
      await lifecycle.drain();
      await pool.end();
      if (unexpected.length)
        throw new AggregateError(
          unexpected,
          "Unexpected Action fixture errors",
        );
    },
  };
}
