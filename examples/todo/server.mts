import { PrismaClient, Prisma } from "./prisma/client/index.js";
import type { IncomingMessage } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { prisma } from "../../packages/postgres/index.mts";
import {
  createBackend,
  CallRejected,
  type Mutations,
  type Loaders,
} from "./generated/node/backend.ts";
import { schema } from "./generated/node/generated.ts";
import { CHANNEL, seed } from "./seed.mts";

type Tx = Prisma.TransactionClient;

/** The two demo identities. Development only: a bearer token equal to the id is the whole credential. */
export const DEMO_USERS: ReadonlySet<string> = new Set(["alice", "bob"]);

/** Accepts only the demo bearer tokens `alice` and `bob`. Never use in production. */
export function demoAuth(request: IncomingMessage): string | null {
  const header = request.headers.authorization;
  if (typeof header !== "string" || !header.startsWith("Bearer ")) return null;
  const id = header.slice("Bearer ".length).trim();
  return DEMO_USERS.has(id) ? id : null;
}

function titleForInsert(title: string): string {
  const value = title.trim();
  if (!value) throw new CallRejected("todo.title_empty");
  return value;
}

function validateCreate(userId: string, values: { createdById: string; done: boolean }): void {
  if (values.createdById !== userId) throw new CallRejected("todo.creator_invalid");
  if (values.done !== false) throw new CallRejected("todo.initial_state_invalid");
}

function prismaCode(error: unknown): string | undefined {
  return error instanceof Prisma.PrismaClientKnownRequestError ? error.code : undefined;
}

/** True only for a unique violation on the Todo primary key, never for any other database failure. */
function isTodoIdConflict(error: unknown): boolean {
  if (!(error instanceof Prisma.PrismaClientKnownRequestError) || error.code !== "P2002") return false;
  const meta = error.meta as { target?: unknown; modelName?: unknown } | undefined;
  const target = meta?.target;
  const columns = Array.isArray(target) ? target : typeof target === "string" ? [target] : [];
  return columns.length === 1 && columns[0] === "id" && (meta?.modelName === undefined || meta.modelName === "Todo");
}

/** Sequential savepoint names keep the transaction usable after a rejected insert. */
let savepoints = 0;

export async function createExample() {
  const db = new PrismaClient();
  let calls = 0;
  const mutations: Mutations<Tx> = {
    async addTodo({ args, ctx }) {
      const { tx, userId } = ctx;
      calls++;
      const { todo } = args;
      const title = titleForInsert(todo.title);
      validateCreate(userId, todo);
      const savepoint = `todo_create_${++savepoints}`;
      await tx.$executeRawUnsafe(`SAVEPOINT ${savepoint}`);
      try {
        await tx.todo.create({ data: { id: todo.id, title, done: false, createdById: todo.createdById } });
      } catch (error) {
        if (!isTodoIdConflict(error)) throw error;
        await tx.$executeRawUnsafe(`ROLLBACK TO SAVEPOINT ${savepoint}`);
        throw new CallRejected("todo.id_conflict");
      }
      await tx.$executeRawUnsafe(`RELEASE SAVEPOINT ${savepoint}`);
      // The created input is already a change the caller receives authority for.
      // Joining the demo Channel once is what distributes it, and every later
      // change to it, to the other subscribers.
      ctx.channel(CHANNEL).todo.add({ id: todo.id });
    },
    async setTodoDone({ args, ctx }) {
      const { tx } = ctx;
      calls++;
      const { id, done } = args.todo;
      // An empty patch is a no-op (#49): the record is still read back and
      // distributed at a new stamp, but nothing is written. The Todo is already
      // a member of the demo Channel, so no enrollment is needed here.
      if (typeof done === "boolean") {
        try {
          await tx.todo.update({ where: { id }, data: { done } });
        } catch (error) {
          if (prismaCode(error) !== "P2025") throw error;
          throw new CallRejected("todo.missing");
        }
      }
      // The explicit `todo` output: its identity, which the framework reads
      // through the Loader for the caller's result.
      return { todo: { id } };
    },
  };
  const loaders: Loaders<Tx> = {
    async user({ ids, tx }) {
      const rows = await tx.user.findMany({ where: { id: { in: ids.map((identity) => identity.id) } } });
      const byId = new Map(rows.map((row) => [row.id, row]));
      return ids.map((identity) => byId.get(identity.id) ?? null);
    },
    async todo({ ids, tx }) {
      const rows = await tx.todo.findMany({ where: { id: { in: ids.map((identity) => identity.id) } } });
      const byId = new Map(rows.map((row) => [row.id, row]));
      return ids.map((identity) => byId.get(identity.id) ?? null);
    },
  };
  const backend = createBackend<Tx>({
    database: prisma(db),
    authenticate: demoAuth,
    mutations,
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
        new URL("../../packages/postgres/migration.sql", import.meta.url),
        "utf8",
      );
      for (const sql of migration.split(";").map((s) => s.trim()).filter(Boolean))
        await db.$executeRawUnsafe(sql);
      await db.$executeRawUnsafe(
        'CREATE TABLE IF NOT EXISTS "User" (id TEXT PRIMARY KEY, name TEXT NOT NULL)',
      );
      await db.$executeRawUnsafe(
        'CREATE TABLE IF NOT EXISTS "Todo" (id TEXT PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN NOT NULL, "createdById" TEXT NOT NULL REFERENCES "User"(id))',
      );
      await seed(backend);
    },
    /**
     * Touch the seed users and tasks again, creating nothing new: what a
     * backend job does when it wants existing rows redistributed. An app meets
     * them instead through `subscription.bootstrap()`
     * ([#151](https://github.com/zanminwang/axton/issues/151)), which is what
     * `mobile/src/todo.ts` calls; this stays for the tests that are about
     * republication itself.
     */
    publishSeeds() {
      return seed(backend);
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
  console.log(`To-do backend listening at ${server.url}`);
  for (const signal of ["SIGINT", "SIGTERM"] as const)
    process.once(signal, () => void server.close().then(() => app.close()));
}
