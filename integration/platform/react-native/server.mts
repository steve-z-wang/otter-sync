import {
  PrismaClient,
  type Prisma,
} from "../../e2e/fixtures/round-trip/node_modules/@prisma/client/default.js";
import { readFile, writeFile } from "node:fs/promises";
import { createServer, request as httpRequest } from "node:http";
import { connect as netConnect, type Socket } from "node:net";
import { prisma } from "../../../packages/postgres/index.mts";
import {
  createBackend,
  devAuth,
  MutationRejected,
  type Handlers,
  type Loaders,
} from "./generated/backend.ts";

const db = new PrismaClient();
const calls = { add: 0, edit: 0 };
const handlers: Handlers<Prisma.TransactionClient> = {
  async addEntry({ input, tx, channel }) {
    calls.add++;
    await tx.entry.create({ data: input.entry });
    // A new Entry joins the Channel once; its later edits reach it with no enrollment.
    channel("book:demo").entry.add(input.entry);
  },
  async edit({ input, tx }) {
    calls.edit++;
    if (input.entry.patch.text === "reject")
      throw new MutationRejected("entry.denied");
    await tx.entry.update({
      where: input.entry.identity,
      data: input.entry.patch,
    });
  },
};
const loaders: Loaders<Prisma.TransactionClient> = {
  async entry({ ids, tx }) {
    return Promise.all(
      ids.map((identity) => tx.entry.findUnique({ where: identity })),
    );
  },
};
const backend = createBackend({
  database: prisma(db),
  authenticate: devAuth(),
  handlers,
  loaders,
});
const migration = await readFile(
  new URL(
    "../../../packages/postgres/migration.sql",
    import.meta.url,
  ),
  "utf8",
);
for (const sql of migration
  .split(";")
  .map((x) => x.trim())
  .filter(Boolean))
  await db.$executeRawUnsafe(sql);
await db.$executeRawUnsafe(
  'CREATE TABLE IF NOT EXISTS "Entry" (id TEXT PRIMARY KEY,text TEXT NOT NULL,note TEXT)',
);
await backend.transaction(async ({ tx, channel, touch }) => {
  await tx.entry.upsert({
    where: { id: "entry-1" },
    create: { id: "entry-1", text: "seed", note: null },
    update: {},
  });
  touch.entry({ id: "entry-1" });
  channel("book:demo").entry.add({ id: "entry-1" });
});
const started = await backend.listen({ port: 0, host: "127.0.0.1" });
const target = new URL(started.url);

// The rows seeded above were published before any phone had a subscription, so
// subscribing does not deliver them
// ([#150](https://github.com/zanminwang/axton/issues/150)). The app asks for
// them with `subscription.bootstrap()`
// ([#151](https://github.com/zanminwang/axton/issues/151)); this harness
// republishes nothing: the seeded state arrives through the app's own load.

async function proxy(dropFirstPush: boolean) {
  let online = true;
  let droppedResponses = 0;
  const sockets = new Set<Socket>();
  const server = createServer((req, res) => {
    if (!online) {
      res.writeHead(503);
      res.end("disconnected by test harness");
      return;
    }
    const upstream = httpRequest(
      {
        hostname: target.hostname,
        port: target.port,
        path: req.url,
        method: req.method,
        headers: req.headers,
      },
      (response) => {
        if (
          dropFirstPush &&
          req.url === "/sync/mutations" &&
          response.statusCode === 200
        ) {
          dropFirstPush = false;
          response.resume();
          response.on("end", () => {
            droppedResponses++;
            res.destroy();
          });
        } else {
          res.writeHead(response.statusCode ?? 502, response.headers);
          response.pipe(res);
        }
      },
    );
    upstream.on("error", () => {
      if (!res.headersSent) res.writeHead(502);
      res.end();
    });
    req.on("error", () => upstream.destroy());
    req.pipe(upstream);
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  server.on("upgrade", (req, socket, head) => {
    // The HTTP server drops its own error listener on upgrade; a reset from the phone
    // (for example when the runner terminates it) must not crash the proxy.
    socket.on("error", () => {});
    if (!online) {
      socket.end(
        "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n",
      );
      return;
    }
    const upstream = netConnect(Number(target.port), target.hostname, () => {
      const headers = Object.entries(req.headers).flatMap(([name, value]) =>
        Array.isArray(value)
          ? value.map((v) => `${name}: ${v}`)
          : [`${name}: ${value}`],
      );
      upstream.write(
        `${req.method} ${req.url} HTTP/1.1\r\n${headers.join("\r\n")}\r\n\r\n`,
      );
      if (head.length) upstream.write(head);
      socket.pipe(upstream);
      upstream.pipe(socket);
    });
    upstream.on("error", () => socket.destroy());
    socket.on("error", () => upstream.destroy());
    socket.on("close", () => upstream.destroy());
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    url: `http://127.0.0.1:${(server.address() as { port: number }).port}`,
    setOnline(value: boolean) {
      online = value;
      if (!online) for (const socket of sockets) socket.destroy();
    },
    get droppedResponses() {
      return droppedResponses;
    },
    async close() {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
const alice = await proxy(true);
const bob = await proxy(false);
const control = createServer(async (req, res) => {
  try {
    if (req.method === "POST" && req.url === "/alice/offline")
      alice.setOnline(false);
    if (req.method === "POST" && req.url === "/alice/online")
      alice.setOnline(true);
    res.setHeader("content-type", "application/json");
    res.end(
      JSON.stringify({
        calls,
        droppedResponses: alice.droppedResponses,
        rows: await db.entry.findMany({ orderBy: { id: "asc" } }),
      }),
    );
  } catch (error) {
    res.writeHead(500);
    res.end(String(error));
  }
});
await new Promise<void>((resolve) => control.listen(0, "127.0.0.1", resolve));
const ports = {
  alice: alice.url,
  bob: bob.url,
  control: `http://127.0.0.1:${(control.address() as { port: number }).port}`,
};
await writeFile(process.env.AXTON_SMOKE_PORTS!, JSON.stringify(ports));
console.log(JSON.stringify(ports));
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  await Promise.all([alice.close(), bob.close()]);
  await new Promise<void>((resolve) => control.close(() => resolve()));
  await started.close();
  await db.$disconnect();
}
for (const signal of ["SIGINT", "SIGTERM"] as const)
  process.once(signal, () => void close());
