import { PrismaClient, type Prisma } from "@prisma/client";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { prisma } from "../../../../packages/postgres/index.mts";
import {
  createBackend,
  devAuth,
  Entry,
  CallRejected,
  type Mutations,
  type Loaders,
} from "./generated/backend.ts";
import { schema } from "./generated/generated.ts";
import { sqlStatements } from "../../../../packages/postgres/src/statements.mts";

import { drainedDatabase } from "../../../load-e2e/lifecycle.mts";
import { isRetryableTransactionError } from "../../../../packages/server/index.mts";

type Tx = Prisma.TransactionClient;

export async function createExample() {
  const db = new PrismaClient();
  let calls = 0;
  let loaderCalls = 0;
  const mutations: Mutations<Tx> = {
    async editEntry({ ctx, args }) {
      calls++;
      const { id, ...patch } = args.entry;
      if (patch.text === "reject") throw new CallRejected("entry.denied");
      await ctx.tx.entry.update({
        where: { id },
        data: {
          ...patch,
          ...(typeof patch.text === "string"
            ? { text: patch.text.trim() }
            : {}),
        },
      });
      ctx.invalidate.entry(id);
      return { entry: { id } };
    },
  };
  const refusing = new Set<string>();
  let expectedLoaderFailure = false;
  const loaders: Loaders<Tx> = {
    async entry({ ids, tx }) {
      loaderCalls++;
      if (ids.some((identity) => refusing.has(identity.id)))
        throw new Error(
          `the Entry loader refuses ${ids.map((i) => i.id).join(", ")}`,
        );
      return Promise.all(
        ids.map((identity) => tx.entry.findUnique({ where: identity })),
      );
    },
  };
  const lifecycle = drainedDatabase(prisma(db));
  const unexpected: unknown[] = [];
  const backend = createBackend<Tx>({
    database: lifecycle.database,
    onError(error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message.startsWith("the Entry loader refuses") && refusing.size > 0)
        return;
      // The native refusal reports the same explicitly injected Loader fault.
      if (message === "loader.failed" && expectedLoaderFailure) return;
      if (
        message === "live handshake closed" ||
        isRetryableTransactionError(error)
      )
        return;
      unexpected.push(error);
    },
    authenticate: devAuth(),
    protocol4: {
      backendId: "round-trip",
      contractId: "round-trip-v04",
      authorizeStream: (viewer, stream) => stream === `User:${viewer}`,
    },
    handlers: {
      async edit({ input, tx }) {
        const { identity, patch } = input.entry;
        await tx.entry.update({ where: identity, data: patch });
      },
    },
    mutations,
    queries: {
      async findEntry({ ctx, args }) {
        const row = await ctx.tx.entry.findUnique({ where: { id: args.id } });
        return { entry: row ? { id: row.id } : null };
      },
    },
    bootstrap: async ({ ctx }) => {
      const rows = await ctx.tx.entry.findMany();
      for (const row of rows) ctx.stream.track.entry(row.id);
    },
    loaders,
  });
  let server: Awaited<ReturnType<typeof backend.listen>> | undefined;
  return {
    db,
    backend,
    schema,
    get loaderCalls() {
      return loaderCalls;
    },
    async members(scope: string) {
      const rows = await db.$queryRawUnsafe<{ identity_key: string }[]>(
        "SELECT r.identity_key FROM axton_stream_member m JOIN axton_record r ON r.id=m.record_id WHERE m.stream=$1 AND r.model='Entry' ORDER BY r.identity_key",
        scope,
      );
      return rows.map((row) => JSON.parse(row.identity_key).id);
    },
    get handlerCalls() {
      return calls;
    },
    async initialize() {
      const migration = await readFile(
        new URL("../../../../packages/postgres/migration.sql", import.meta.url),
        "utf8",
      );
      // Prisma runs one statement per call.
      for (const sql of sqlStatements(migration))
        await db.$executeRawUnsafe(sql);
      await db.$executeRawUnsafe(
        'CREATE TABLE IF NOT EXISTS "Entry" (id TEXT PRIMARY KEY,text TEXT NOT NULL,note TEXT)',
      );
      await backend.transaction(
        async ({ tx, streams: scopes, invalidate: touch }) => {
          await tx.entry.upsert({
            where: { id: "entry-1" },
            create: { id: "entry-1", text: "Hello from the server" },
            update: { text: "Hello from the server", note: null },
          });
          touch.entry({ id: "entry-1" });
          scopes(["User:demo-user"]).track.entry({ id: "entry-1" });
        },
      );
    },
    async notify(ids: string[] = ["entry-1"], name = "User:demo-user") {
      await backend.transaction(
        async ({ streams: scopes, invalidate: touch }) => {
          for (const id of ids) {
            touch.entry({ id });
            scopes([name]).track.entry({ id });
          }
        },
      );
    },
    async publishMany(
      count: number,
      options: { scope: string; prefix: string; from?: number; batch?: number },
    ): Promise<string[]> {
      const from = options.from ?? 1;
      const size = options.batch ?? 20;
      const ids = Array.from(
        { length: count },
        (_, i) => `${options.prefix}-${from + i}`,
      );
      for (let start = 0; start < ids.length; start += size) {
        const batch = ids.slice(start, start + size);
        await backend.transaction(
          async ({ tx, streams: scopes, invalidate: touch }) => {
            for (const id of batch) {
              await tx.entry.upsert({
                where: { id },
                create: { id, text: `${id} text` },
                update: { text: `${id} text` },
              });
              touch.entry({ id });
              scopes([options.scope]).track.entry({ id });
            }
          },
        );
      }
      return ids;
    },
    async publishOne(id: string, text: string, names: string[]): Promise<void> {
      await backend.transaction(
        async ({ tx, streams: scopes, invalidate: touch }) => {
          await tx.entry.upsert({
            where: { id },
            create: { id, text },
            update: { text },
          });
          touch.entry({ id });
          for (const name of names) scopes([name]).track.entry({ id });
        },
      );
    },
    async invalidateRecords(ids: string[], name: string): Promise<void> {
      await backend.transaction(async (ctx) =>
        ctx.streams([name]).invalidate(ids.map((id) => Entry({ id }))),
      );
    },
    async tombstone(id: string): Promise<void> {
      await backend.transaction(async ({ tx, invalidate: touch }) => {
        await tx.entry.delete({ where: { id } });
        touch.entry({ id });
      });
    },
    failLoads(...ids: string[]) {
      expectedLoaderFailure = true;
      for (const id of ids) refusing.add(id);
    },
    allowLoads(...ids: string[]) {
      for (const id of ids) refusing.delete(id);
    },
    async head(scope: string): Promise<number> {
      const rows = await db.$queryRawUnsafe<{ head: bigint }[]>(
        "SELECT head FROM axton_stream WHERE stream = $1",
        scope,
      );
      return rows.length === 0 ? 0 : Number(rows[0]!.head);
    },
    async positionOf(scope: string, id: string): Promise<number | null> {
      const rows = await db.$queryRawUnsafe<{ cursor: bigint }[]>(
        "SELECT l.cursor FROM axton_stream_log l JOIN axton_record r ON r.id = l.record_id WHERE l.stream = $1 AND r.model = 'Entry' AND r.identity_key = $2 AND l.kind = 'upsert'",
        scope,
        JSON.stringify({ id }),
      );
      return rows.length === 0 ? null : Number(rows[0]!.cursor);
    },
    async reset(): Promise<void> {
      await db.$executeRawUnsafe(
        "TRUNCATE axton_bootstrap_range, axton_bootstrap_identity, axton_bootstrap_manifest, axton_publication_group, axton_stream_member, axton_stream_log, axton_stream, axton_record, axton_client, axton_call",
      );
      await db.$executeRawUnsafe('DELETE FROM "Entry"');
      refusing.clear();
      expectedLoaderFailure = false;
    },
    listen(port: number) {
      return backend.listen({ port }).then((started) => {
        server = started;
        return started;
      });
    },
    async close() {
      await server?.close();
      await lifecycle.drain();
      await db.$disconnect();
      if (unexpected.length)
        throw new AggregateError(
          unexpected,
          "Unexpected round-trip backend errors",
        );
    },
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const app = await createExample();
  await app.initialize();
  const server = await app.listen(Number(process.env.PORT ?? 4242));
  console.log(`Example listening at ${server.url}`);
  for (const signal of ["SIGINT", "SIGTERM"] as const)
    process.once(signal, () => void server.close().then(() => app.close()));
}
