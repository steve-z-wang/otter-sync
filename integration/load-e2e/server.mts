// Real PostgreSQL backend and HTTP/WebSocket fault proxy for bound protocol 4.
import { readFile } from "node:fs/promises";
import { createServer, type IncomingMessage } from "node:http";
import { connect, type Socket } from "node:net";
import { Pool } from "pg";
import { pg, type PgClient } from "../../packages/postgres/index.mts";
import { CallRejected, createBackend, devAuth, Item } from "./backend.ts";
import { drainedDatabase } from "./lifecycle.mts";
import { isRetryableTransactionError } from "../../packages/server/index.mts";
export async function createFixture() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const handled: { name: string; callId: string; project?: string }[] = [];
  const failing = new Set<string>();
  const absent = new Set<string>();
  const loaderErrors = new Set<string>();
  const lifecycle = drainedDatabase(pg(pool));
  const unexpected: unknown[] = [];
  const backend = createBackend<PgClient>({
    database: lifecycle.database,
    onError(error) {
      const message = error instanceof Error ? error.message : String(error);
      if (message === "fixture Loader error" && loaderErrors.size > 0) return;
      if (
        message === "live handshake closed" ||
        isRetryableTransactionError(error)
      )
        return;
      unexpected.push(error);
    },
    authenticate: devAuth(),
    protocol4: {
      backendId: "read-e2e",
      contractId: "read-v04",
      authorizeStream: (viewer, stream) => stream === `User:${viewer}`,
    },
    mutations: {
      addItem: {
        async v1({ ctx, args }) {
          await ctx.tx.query("INSERT INTO load_e2e_item VALUES($1,$2,$3)", [
            args.item.id,
            args.item.project,
            args.item.title,
          ]);
          ctx.stream.track.item(args.item.id);
          ctx.invalidate.item(args.item.id);
        },
        async v2({ ctx, args }) {
          await ctx.tx.query("INSERT INTO load_e2e_item VALUES($1,$2,$3)", [
            args.item.id,
            args.item.project,
            args.item.title,
          ]);
          ctx.stream.track.item(args.item.id);
          ctx.invalidate.item(args.item.id);
          return { item: { id: args.item.id } };
        },
      },
      renameItem: {
        async v1({ ctx, args }) {
          await ctx.tx.query("UPDATE load_e2e_item SET title=$2 WHERE id=$1", [
            args.item.id,
            args.item.title,
          ]);
          ctx.invalidate.item(args.item.id);
        },
        async v2({ ctx, args }) {
          const rows = await ctx.tx.query(
            "UPDATE load_e2e_item SET title=$2 WHERE id=$1 RETURNING id",
            [args.item.id, args.item.title],
          );
          if (rows.rows.length === 0) throw new CallRejected("item.missing");
          ctx.invalidate.item(args.item.id);
          return { item: { id: args.item.id } };
        },
      },
      async ping({ ctx, args }) {
        if (args.note === "reject") throw new CallRejected("ping.denied");
        await ctx.tx.query("INSERT INTO load_e2e_ping(note) VALUES($1)", [
          args.note,
        ]);
      },
    },
    queries: {
      async projectItems({ ctx, args }) {
        handled.push({
          name: "ProjectItems",
          callId: ctx.callId,
          project: args.project,
        });
        if (failing.has(args.project)) throw new CallRejected("project.closed");
        return {
          items: (
            await ctx.tx.query(
              "SELECT id FROM load_e2e_item WHERE project=$1 ORDER BY id",
              [args.project],
            )
          ).rows.map((row) => ({ id: String(row.id) })),
          tags: (
            await ctx.tx.query(
              "SELECT id FROM load_e2e_tag WHERE project=$1 ORDER BY id",
              [args.project],
            )
          ).rows.map((row) => ({ id: String(row.id) })),
        };
      },
      async catalog({ ctx, args }) {
        handled.push({ name: "Catalog", callId: ctx.callId });
        return {
          items: (
            await ctx.tx.query(
              "SELECT id FROM load_e2e_item WHERE project=$1 ORDER BY id",
              [`shelf:${args.shelf ?? "all"}`],
            )
          ).rows.map((row) => ({ id: String(row.id) })),
        };
      },
    },
    loaders: {
      item: async ({ tx, ids }) => {
        if (ids.some(({ id }) => loaderErrors.has(id)))
          throw Error("fixture Loader error");
        const rows = (
          await tx.query(
            "SELECT id,project,title FROM load_e2e_item WHERE id=ANY($1::text[])",
            [ids.map(({ id }) => id)],
          )
        ).rows;
        const byId = new Map(
          rows.map((row) => [
            String(row.id),
            {
              id: String(row.id),
              project: String(row.project),
              title: String(row.title),
            },
          ]),
        );
        return ids.map(({ id }) =>
          absent.has(id) ? null : (byId.get(id) ?? null),
        );
      },
      tag: async ({ tx, ids }) => {
        const rows = (
          await tx.query(
            "SELECT id,label FROM load_e2e_tag WHERE id=ANY($1::text[])",
            [ids.map(({ id }) => id)],
          )
        ).rows;
        return ids.map(({ id }) =>
          (() => {
            const row = rows.find((row) => row.id === id);
            return row
              ? { id: String(row.id), label: String(row.label) }
              : null;
          })(),
        );
      },
      seen: undefined,
    },
    bootstrap: async ({ ctx }) => {
      const rows = (
        await ctx.tx.query(
          "SELECT id FROM load_e2e_item WHERE project=$1 ORDER BY id",
          [ctx.userId],
        )
      ).rows;
      for (const row of rows) ctx.stream.track.item(String(row.id));
    },
  });
  let listener: Awaited<ReturnType<typeof backend.listen>> | undefined;
  let proxy: Proxy | undefined;
  return {
    pool,
    backend,
    handled,
    failing,
    absent,
    loaderErrors,
    get proxy() {
      return proxy!;
    },
    async initialize() {
      await pool.query(
        await readFile(
          new URL("../../packages/postgres/migration.sql", import.meta.url),
          "utf8",
        ),
      );
      await pool.query(
        "CREATE TABLE load_e2e_item(id text PRIMARY KEY,project text NOT NULL,title text NOT NULL);CREATE TABLE load_e2e_tag(id text PRIMARY KEY,project text NOT NULL,label text NOT NULL);CREATE TABLE load_e2e_ping(id bigserial PRIMARY KEY,note text NOT NULL)",
      );
    },
    async seed(project: string, count: number, prefix = project) {
      const ids = Array.from({ length: count }, (_, i) => `${prefix}-${i + 1}`);
      for (const id of ids)
        await pool.query("INSERT INTO load_e2e_item VALUES($1,$2,$3)", [
          id,
          project,
          `${id} title`,
        ]);
      return ids;
    },
    async seedTags(project: string, labels: string[]) {
      const ids = labels.map((_, i) => `${project}-tag-${i + 1}`);
      for (const [i, id] of ids.entries())
        await pool.query("INSERT INTO load_e2e_tag VALUES($1,$2,$3)", [
          id,
          project,
          labels[i],
        ]);
      return ids;
    },
    async tracked(stream: string) {
      return (
        await pool.query(
          "SELECT r.model,r.identity_key FROM axton_stream_member m JOIN axton_record r ON r.id=m.record_id WHERE m.stream=$1 ORDER BY r.model,r.identity_key",
          [stream],
        )
      ).rows;
    },
    async retitle(id: string, title: string) {
      await backend.transaction(async ({ tx, invalidate }) => {
        await tx.query("UPDATE load_e2e_item SET title=$2 WHERE id=$1", [
          id,
          title,
        ]);
        invalidate.item(id);
      });
    },
    async publish(ids: string[], streams: string[]) {
      await backend.transaction(async (ctx) => {
        ctx.streams(streams).track(ids.map((id) => Item({ id })));
        ctx.invalidate(ids.map((id) => Item({ id })));
      });
    },
    async invalidate(ids: string[], streams?: string[]) {
      await backend.transaction(async (ctx) => {
        if (streams === undefined)
          ctx.invalidate(ids.map((id) => Item({ id })));
        else ctx.streams(streams).invalidate(ids.map((id) => Item({ id })));
      });
    },
    async listen() {
      listener = await backend.listen({ port: 0 });
      proxy = await createProxy(listener.url);
      return proxy;
    },
    async close() {
      proxy?.releaseAll();
      await proxy?.close();
      await listener?.close();
      await lifecycle.drain();
      await pool.end();
      if (unexpected.length)
        throw new AggregateError(
          unexpected,
          "Unexpected backend fixture errors",
        );
    },
  };
}

/** One HTTP request through the proxy and what became of it. */
export type Exchange = {
  path: string;
  body: string;
  status?: number;
  response?: string;
  /** `request`: cut before the backend saw it; `response`: the backend answered, the client never got it. */
  dropped?: "request" | "response";
};
type Rule = {
  match: (exchange: Exchange) => boolean;
  used: boolean;
  run: (exchange: Exchange, socket: Socket) => Promise<void>;
};
export type Proxy = Awaited<ReturnType<typeof createProxy>>;

/**
 * Forwards `/sync/*` HTTP and the live WebSocket to the backend. Rules act on
 * answered exchanges; `down()` cuts every connection and answers nothing
 * until `up()`.
 */
export async function createProxy(target: string) {
  const upstream = new URL(target);
  const exchanges: Exchange[] = [];
  const sockets = new Set<Socket>();
  const rules: Rule[] = [];
  const waiters = new Set<() => void>();
  const requestHolds: {
    match: (exchange: Exchange) => boolean;
    used: boolean;
    arrive: () => void;
    released: Promise<void>;
  }[] = [];
  let down = false;
  const changed = () => {
    for (const wake of [...waiters]) wake();
  };
  const cut = () => {
    for (const socket of sockets) socket.destroy();
    sockets.clear();
  };
  const track = (socket: Socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  };
  const server = createServer(async (request: IncomingMessage, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const exchange: Exchange = {
      path: request.url?.split("?")[0] ?? "",
      body: Buffer.concat(chunks).toString("utf8"),
    };
    exchanges.push(exchange);
    const held = requestHolds.find(
      (candidate) => !candidate.used && candidate.match(exchange),
    );
    if (held) {
      held.used = true;
      held.arrive();
      await held.released;
    }
    if (down) {
      exchange.dropped = "request";
      request.socket.destroy();
      changed();
      return;
    }
    try {
      const answer = await fetch(new URL(request.url ?? "/", upstream), {
        method: request.method,
        headers: {
          authorization: String(request.headers.authorization ?? ""),
          "content-type": String(
            request.headers["content-type"] ?? "application/json",
          ),
        },
        body: exchange.body,
      });
      exchange.status = answer.status;
      exchange.response = await answer.text();
    } catch {
      exchange.dropped = "request";
      request.socket.destroy();
      changed();
      return;
    }
    const rule = rules.find(
      (candidate) => !candidate.used && candidate.match(exchange),
    );
    if (rule) {
      rule.used = true;
      await rule.run(exchange, request.socket);
    }
    if (down || request.socket.destroyed) {
      exchange.dropped = "response";
      request.socket.destroy();
      changed();
      return;
    }
    response.writeHead(exchange.status!, {
      "content-type": "application/json; charset=utf-8",
    });
    response.end(exchange.response);
    changed();
  });
  server.on("connection", track);
  server.on(
    "upgrade",
    (request: IncomingMessage, socket: Socket, head: Buffer) => {
      if (down) {
        socket.destroy();
        return;
      }
      const backend = connect(Number(upstream.port), upstream.hostname, () => {
        const lines = [`${request.method} ${request.url} HTTP/1.1`];
        for (let n = 0; n < request.rawHeaders.length; n += 2)
          lines.push(`${request.rawHeaders[n]}: ${request.rawHeaders[n + 1]}`);
        backend.write(`${lines.join("\r\n")}\r\n\r\n`);
        if (head.length) backend.write(head);
        socket.pipe(backend).pipe(socket);
      });
      track(backend);
      backend.on("error", () => socket.destroy());
      socket.on("error", () => backend.destroy());
      socket.on("close", () => backend.destroy());
    },
  );
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as { port: number };
  const rule = (match: (exchange: Exchange) => boolean, run: Rule["run"]) => {
    rules.push({ match, used: false, run });
  };
  const releases = new Set<() => void>();
  return {
    url: `http://127.0.0.1:${address.port}`,
    exchanges,
    requests(path: string) {
      return exchanges
        .filter((exchange) => exchange.path === path)
        .map((exchange) => ({
          ...exchange,
          request: JSON.parse(exchange.body),
          answer:
            exchange.response === undefined
              ? undefined
              : JSON.parse(exchange.response),
        }));
    },
    /** Resolves once `predicate` holds over the exchanges so far. */
    until(
      predicate: () => boolean,
      label: string,
      timeout = 20_000,
    ): Promise<void> {
      if (predicate()) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          waiters.delete(check);
          reject(Error(`Timed out waiting for ${label}`));
        }, timeout);
        const check = () => {
          if (predicate()) {
            clearTimeout(timer);
            waiters.delete(check);
            resolve();
          }
        };
        waiters.add(check);
      });
    },
    /**
     * Hold the first matching answered exchange's response until released;
     * `clientGone` resolves when the client's connection has closed.
     */
    holdResponse(match: (exchange: Exchange) => boolean) {
      let arrive!: (exchange: Exchange) => void;
      let release!: () => void;
      let gone!: () => void;
      const arrived = new Promise<Exchange>((resolve) => {
        arrive = resolve;
      });
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      const clientGone = new Promise<void>((resolve) => {
        gone = resolve;
      });
      releases.add(release);
      rule(match, async (exchange, socket) => {
        if (socket.destroyed) gone();
        else socket.once("close", gone);
        arrive(exchange);
        await released;
      });
      return { arrived, release, clientGone };
    },
    /** Hold the first matching request before the backend sees it, until released. */
    holdRequest(match: (exchange: Exchange) => boolean) {
      let arrive!: () => void;
      let release!: () => void;
      const arrived = new Promise<void>((resolve) => {
        arrive = resolve;
      });
      const released = new Promise<void>((resolve) => {
        release = resolve;
      });
      releases.add(release);
      requestHolds.push({ match, used: false, arrive, released });
      return { arrived, release };
    },
    /** Answer nothing for the first matching exchange and cut the network (`down()`). */
    dropResponseAndGoDown(match: (exchange: Exchange) => boolean) {
      rule(match, async () => {
        down = true;
        cut();
      });
    },
    down() {
      down = true;
      cut();
    },
    up() {
      down = false;
    },
    releaseAll() {
      for (const release of releases) release();
    },
    async close() {
      cut();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
