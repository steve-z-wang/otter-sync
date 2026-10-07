// Actual PostgreSQL + native Rust host. Faults interrupt HTTP bytes after cloud commit.
import { readFile } from "node:fs/promises";
import { createServer, request } from "node:http";
import { Pool } from "pg";
import { pg } from "../../packages/postgres/index.mts";
import {
  createBackend,
  devAuth,
  CallRejected as MutationRejected,
} from "./backend.ts";
import { createBackend as createRolloverBackend } from "./rollover/backend.ts";
import { createBackend as createVersionedBackend } from "./versioned/backend.ts";

export async function host({
  rollover = false,
  versioned = false,
  materializations = {},
} = {}) {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  await pool.query(
    await readFile(
      new URL("../../packages/postgres/migration.sql", import.meta.url),
      "utf8",
    ),
  );
  await pool.query(
    "CREATE TABLE IF NOT EXISTS sdk05_entry(id text PRIMARY KEY,text text NOT NULL,owner text NOT NULL)",
  );

  const executions = [],
    queries = [],
    batches = [],
    requests = [],
    errors = [];
  const loader = async ({ tx, ids, userId }) => {
    const { rows } = await tx.query(
      "SELECT id,text FROM sdk05_entry WHERE id=ANY($1) AND owner=$2",
      [ids.map((x) => x.id), userId],
    );
    const byId = new Map(
      rows.map((row) => [row.id, { id: row.id, text: row.text }]),
    );
    return ids.map((x) => byId.get(x.id) ?? null);
  };
  const options = {
    database: pg(pool),
    authenticate: devAuth(),
    protocol5: {
      materializations,
      authorizeStream: (owner, stream) => stream === `User:${owner}`,
    },
    onError: (error) => {
      errors.push(String(error));
      if (process.env.AXTON_GATE_TRACE)
        console.error("host error", String(error));
    },
    mutations: {
      publish: async ({ ctx, args }) => {
        executions.push(args.entry.id);
        if (args.entry.text === "refuse")
          throw new MutationRejected("publish.refused");
        await ctx.tx.query("INSERT INTO sdk05_entry VALUES($1,$2,$3)", [
          args.entry.id,
          args.entry.text.trim(),
          ctx.userId,
        ]);
        if (args.call !== "private") ctx.stream.track.entry(args.entry.id);
        if (args.call === "multi")
          ctx.streams(["User:alice", "User:bob"]).track.entry(args.entry.id);
        return { entry: { id: args.entry.id } };
      },
    },
    queries: {
      find: async ({ ctx, args }) => {
        queries.push(args.id);
        if (args.id === "refused-query")
          throw new MutationRejected("find.denied");
        const { rows } = await ctx.tx.query(
          "SELECT id FROM sdk05_entry WHERE id=$1 AND owner=$2",
          [args.id, ctx.userId],
        );
        if (args.id.startsWith("explicit-track-") && rows[0])
          ctx.stream.track.entry(rows[0].id);
        return { entry: rows[0] ?? null };
      },
      peek: async ({ ctx, args }) => {
        queries.push(args.id);
        const { rows } = await ctx.tx.query(
          "SELECT id FROM sdk05_entry WHERE id=$1 AND owner=$2",
          [args.id, ctx.userId],
        );
        return { entry: rows[0] ?? null };
      },
    },
    loaders: {
      entry: loader,
      snapshot: loader,
    },
    bootstrap: async () => {},
  };
  if (versioned) {
    options.mutations.publish = {
      v1: options.mutations.publish,
      v2: options.mutations.publish,
    };
    options.queries.find = {
      v1: options.queries.find,
      v2: options.queries.find,
    };
    options.loaders.entry = {
      v1: loader,
      v2: async (call) =>
        (await loader(call)).map((row) =>
          row ? { ...row, note: "new schema" } : null,
        ),
    };
  }
  const backend = (
    versioned
      ? createVersionedBackend
      : rollover
        ? createRolloverBackend
        : createBackend
  )(options);
  const real = await backend.listen({ port: 0 });
  let lose = 0,
    hold = false,
    held = [],
    holdReads = false,
    heldReads = [],
    holdHandshake = false,
    heldHandshake = [],
    holdSchema = false,
    heldSchema = [];
  const proxy = createServer(async (incoming, outgoing) => {
    const chunks = [];
    for await (const chunk of incoming) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    requests.push({ route: incoming.url, body: body.toString() });
    if (process.env.AXTON_GATE_TRACE)
      console.error("host request", incoming.url, body.toString());
    const mutation = incoming.url === "/sync/mutations";
    if (mutation) batches.push(body.toString());
    const forward = () => {
      const upstream = request(
        new URL(incoming.url, real.url),
        { method: incoming.method, headers: incoming.headers },
        (response) => {
          if (process.env.AXTON_GATE_TRACE) {
            const trace = [];
            response.on("data", (chunk) => trace.push(chunk));
            response.on("end", () =>
              console.error(
                "host response",
                incoming.url,
                response.statusCode,
                Buffer.concat(trace).toString().slice(0, 2500),
              ),
            );
          }
          if (mutation && lose > 0) {
            lose--;
            response.resume();
            response.on("end", () => outgoing.destroy());
            return;
          }
          if (holdSchema && incoming.url === "/sync/materialize") {
            const bytes = [];
            response.on("data", (chunk) => bytes.push(chunk));
            response.on("end", () =>
              heldSchema.push(() => {
                outgoing.writeHead(response.statusCode, response.headers);
                outgoing.end(Buffer.concat(bytes));
              }),
            );
            return;
          }
          if (
            holdReads &&
            (incoming.url === "/sync/actions" || incoming.url === "/sync/fetch")
          ) {
            const bytes = [];
            response.on("data", (chunk) => bytes.push(chunk));
            response.on("end", () =>
              heldReads.push(() => {
                outgoing.writeHead(response.statusCode, response.headers);
                outgoing.end(Buffer.concat(bytes));
              }),
            );
            return;
          }
          outgoing.writeHead(response.statusCode, response.headers);
          response.pipe(outgoing);
        },
      );
      upstream.on("error", (error) => outgoing.destroy(error));
      upstream.end(body);
    };
    if (incoming.url === "/sync/handshake" && holdHandshake)
      heldHandshake.push(forward);
    else if (mutation && hold) held.push(forward);
    else forward();
  });
  // Pass real WebSocket bytes through without interpreting authority or cursors.
  const sockets = new Set();
  proxy.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });
  proxy.on("upgrade", (incoming, socket, head) => {
    const forward = () => {
      const upstream = request(new URL(incoming.url, real.url), {
        method: incoming.method,
        headers: incoming.headers,
      });
      upstream.on("upgrade", (response, remote, remoteHead) => {
        socket.write(
          `HTTP/1.1 ${response.statusCode} Switching Protocols\r\n` +
            Object.entries(response.headers)
              .map(([k, v]) => `${k}: ${v}`)
              .join("\r\n") +
            "\r\n\r\n",
        );
        if (remoteHead.length) socket.write(remoteHead);
        if (head.length) remote.write(head);
        socket.pipe(remote).pipe(socket);
        socket.on("error", () => remote.destroy());
        remote.on("error", () => socket.destroy());
      });
      upstream.on("response", (response) => {
        socket.end(
          `HTTP/1.1 ${response.statusCode} Refused\r\nConnection: close\r\n\r\n`,
        );
        response.resume();
      });
      upstream.on("error", () => socket.destroy());
      upstream.end();
    };
    if (holdHandshake) heldHandshake.push(forward);
    else forward();
  });
  await new Promise((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  return {
    pool,
    backend,
    executions,
    queries,
    batches,
    requests,
    errors,
    url: `http://127.0.0.1:${proxy.address().port}`,
    holdSchema() {
      holdSchema = true;
    },
    get heldSchemaCount() {
      return heldSchema.length;
    },
    releaseSchema() {
      holdSchema = false;
      const pending = heldSchema;
      heldSchema = [];
      for (const respond of pending) respond();
    },
    holdHandshake() {
      holdHandshake = true;
    },
    releaseHandshake() {
      holdHandshake = false;
      const pending = heldHandshake;
      heldHandshake = [];
      for (const forward of pending) forward();
    },
    holdReads() {
      holdReads = true;
    },
    get heldReadCount() {
      return heldReads.length;
    },
    releaseReads() {
      holdReads = false;
      const pending = heldReads;
      heldReads = [];
      for (const respond of pending) respond();
    },
    loseNext(count = 1) {
      lose = count;
    },
    holdMutations() {
      hold = true;
    },
    releaseMutations() {
      hold = false;
      const pending = held;
      held = [];
      for (const forward of pending) forward();
    },
    async close() {
      for (const socket of sockets) socket.destroy();
      proxy.closeAllConnections();
      await new Promise((resolve) => proxy.close(resolve));
      await real.close();
      await pool.end();
    },
  };
}
