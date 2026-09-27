import { PrismaClient, type Prisma } from "@prisma/client";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { prisma } from "../../../../packages/postgres/index.mts";
import {
  createBackend,
  devAuth,
  Entry,
  MutationRejected,
  type Handlers,
  type Loaders,
} from "./generated/backend.ts";
import { schema } from "./generated/generated.ts";

type Tx = Prisma.TransactionClient;

export async function createExample() {
  const db = new PrismaClient();
  let calls = 0;
  const handlers: Handlers<Tx> = {
    async edit({ input, tx }) {
      calls++;
      const { identity, patch } = input.entry;
      if (patch.text === "reject") throw new MutationRejected("entry.denied");
      await tx.entry.update({
        where: identity,
        data: { ...patch, ...(typeof patch.text === "string" ? { text: patch.text.trim() } : {}) },
      });
      // The edited entry is stamped and read back for the receipt, and that same
      // version reaches every Channel it is a member of: no enrollment here.
    },
  };
  /**
   * Entry ids whose read fails, as `failLoads` sets them. A Loader may throw;
   * the framework then asks for each identity on its own, so exactly these
   * become `loader.failed` error records and the rest of the page still
   * resolves (guarantee D7).
   */
  const refusing = new Set<string>();
  const loaders: Loaders<Tx> = {
    async entry({ ids, tx }) {
      if (ids.some((identity) => refusing.has(identity.id)))
        throw new Error(`the Entry loader refuses ${ids.map((i) => i.id).join(", ")}`);
      return Promise.all(ids.map((identity) => tx.entry.findUnique({ where: identity })));
    },
  };
  const backend = createBackend<Tx>({
    database: prisma(db),
    authenticate: devAuth(),
    handlers,
    loaders,
  });
  let server: Awaited<ReturnType<typeof backend.listen>> | undefined;
  return {
    db,
    backend,
    schema,
    get handlerCalls() {
      return calls;
    },
    async initialize() {
      const migration = await readFile(
        new URL("../../../../packages/postgres/migration.sql", import.meta.url),
        "utf8",
      );
      for (const sql of migration.split(";").map((s) => s.trim()).filter(Boolean))
        await db.$executeRawUnsafe(sql);
      await db.$executeRawUnsafe(
        'CREATE TABLE IF NOT EXISTS "Entry" (id TEXT PRIMARY KEY,text TEXT NOT NULL,note TEXT)',
      );
      await backend.transaction(async ({ tx, channel, touch }) => {
        await tx.entry.upsert({
          where: { id: "entry-1" },
          create: { id: "entry-1", text: "Hello from the server" },
          update: {},
        });
        touch.entry({ id: "entry-1" });
        channel("book:demo").entry.add({ id: "entry-1" });
      });
    },
    /**
     * Touch the current `Entry` rows, enrolling them on `name`: a new stamp and a
     * new position on every Channel they belong to. A subscription's origin is
     * the first head its handshake acknowledges
     * ([#150](https://github.com/zanminwang/axton/issues/150)), so a client that
     * subscribes after `initialize` meets the seeded rows either this way or
     * through `subscription.bootstrap()`
     * ([#151](https://github.com/zanminwang/axton/issues/151)); `readd` below is
     * the version that moves a position without touching the stamp.
     */
    async notify(ids: string[] = ["entry-1"], name = "book:demo") {
      await backend.transaction(async ({ channel, touch }) => {
        for (const id of ids) {
          touch.entry({ id });
          channel(name).entry.add({ id });
        }
      });
    },
    /**
     * Write `count` new `Entry` rows and publish them on `channel`, each at its
     * own cursor. Written in batches, because one interactive PostgreSQL
     * transaction per hundred records runs into the driver's transaction
     * timeout; the returned ids are in publication order.
     */
    async publishMany(
      count: number,
      options: { channel: string; prefix: string; from?: number; batch?: number },
    ): Promise<string[]> {
      const from = options.from ?? 1;
      const size = options.batch ?? 20;
      const ids = Array.from({ length: count }, (_, i) => `${options.prefix}-${from + i}`);
      for (let start = 0; start < ids.length; start += size) {
        const batch = ids.slice(start, start + size);
        await backend.transaction(async ({ tx, channel, touch }) => {
          for (const id of batch) {
            await tx.entry.upsert({
              where: { id },
              create: { id, text: `${id} text` },
              update: { text: `${id} text` },
            });
            touch.entry({ id });
            channel(options.channel).entry.add({ id });
          }
        });
      }
      return ids;
    },
    /** Write one `Entry` and enroll it on every named Channel: one stamp, one position on each. */
    async publishOne(id: string, text: string, channels: string[]): Promise<void> {
      await backend.transaction(async ({ tx, channel, touch }) => {
        await tx.entry.upsert({ where: { id }, create: { id, text }, update: { text } });
        touch.entry({ id });
        for (const name of channels) channel(name).entry.add({ id });
      });
    },
    /**
     * Remove existing members from `name`, then add them back in a second
     * settlement, without changing them. Adding an absent member publishes its
     * current state, so each takes a new cursor at the stamp it already has
     * (guarantee D3); adding a present member would do nothing. This is how a
     * record leaves a subscription's historical interval and becomes the
     * subscription's own delivery ([#151](https://github.com/zanminwang/axton/issues/151)).
     */
    async readd(ids: string[], name: string): Promise<void> {
      await backend.transaction(async ({ channel }) => {
        channel(name).remove(ids.map((id) => Entry({ id })));
      });
      await backend.transaction(async ({ channel }) => {
        channel(name).add(ids.map((id) => Entry({ id })));
      });
    },
    /** Delete the row and touch it: its members' Loader answers `null`, an authoritative deletion (D6). */
    async tombstone(id: string): Promise<void> {
      await backend.transaction(async ({ tx, touch }) => {
        await tx.entry.delete({ where: { id } });
        touch.entry({ id });
      });
    },
    /** Make the `Entry` Loader fail for these ids until `allowLoads` clears them. */
    failLoads(...ids: string[]) {
      for (const id of ids) refusing.add(id);
    },
    allowLoads(...ids: string[]) {
      for (const id of ids) refusing.delete(id);
    },
    /** The channel head: the highest cursor the invalidation log has allocated. */
    async head(channel: string): Promise<number> {
      const rows = await db.$queryRawUnsafe<{ head: bigint }[]>(
        "SELECT head FROM axton_channel WHERE channel = $1",
        channel,
      );
      return rows.length === 0 ? 0 : Number(rows[0]!.head);
    },
    /** The one retained position `id` has on `channel`, or `null`; a later publication replaces it in place. */
    async positionOf(channel: string, id: string): Promise<number | null> {
      const rows = await db.$queryRawUnsafe<{ cursor: bigint }[]>(
        'SELECT cursor FROM axton_invalidation WHERE channel = $1 AND model = \'Entry\' AND identity_key = $2',
        channel,
        JSON.stringify({ id }),
      );
      return rows.length === 0 ? null : Number(rows[0]!.cursor);
    },
    /**
     * Empty every AXTON table and the `Entry` rows. A bootstrap scenario asserts
     * cursors and channel heads, so it starts from an empty log rather than from
     * whatever an earlier scenario in the same database left behind.
     */
    async reset(): Promise<void> {
      await db.$executeRawUnsafe(
        "TRUNCATE axton_membership, axton_invalidation, axton_channel, axton_record, axton_client, axton_call",
      );
      await db.$executeRawUnsafe('DELETE FROM "Entry"');
      refusing.clear();
    },
    listen(port: number) {
      return backend.listen({ port }).then((started) => {
        server = started;
        return started;
      });
    },
    async close() {
      await server?.close();
      await db.$disconnect();
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
