// The Load end-to-end backend: the generated backend over disposable
// PostgreSQL, and an HTTP/WebSocket proxy in front of it that every client
// (in-process, child process and Dart) talks through. The handlers record
// every page they executed; the proxy records every exchange and can hold a
// request or a response, drop one, or cut the network.
import { readFile } from "node:fs/promises";
import { createServer, type IncomingMessage } from "node:http";
import { connect, type Socket } from "node:net";
import { Pool } from "pg";
import { pg, type PgClient } from "../../packages/postgres/index.mts";
import { CallRejected, createBackend, devAuth, Item, Tag, type Loaders, type Loads, type LoadNext, type Mutations } from "./backend.ts";

/** One page as a handler executed it. */
/** One handler run; `xact` is its transaction's ID, so a run retried after a serialization failure is told apart from its committed run. */
export type Handled = { load: "ProjectItems" | "Catalog"; loadId: string; callId: string; key: string | null; continuation: LoadNext; xact: string };

/**
 * A single-use pause at a named point: the first arrival matching `match`
 * waits there until `release()`. `arrived` resolves with what arrived.
 */
export class Hold<T> {
  readonly match: (value: T) => boolean;
  readonly arrived: Promise<T>;
  #arrive!: (value: T) => void;
  #released: Promise<void>;
  #release!: () => void;
  used = false;
  constructor(match: (value: T) => boolean) {
    this.match = match;
    this.arrived = new Promise((resolve) => { this.#arrive = resolve; });
    this.#released = new Promise((resolve) => { this.#release = resolve; });
  }
  pass(value: T): Promise<void> { this.used = true; this.#arrive(value); return this.#released; }
  release(): void { this.#release(); }
}
class Holds<T> {
  #armed: Hold<T>[] = [];
  arm(match: (value: T) => boolean): Hold<T> { const hold = new Hold(match); this.#armed.push(hold); return hold; }
  async pass(value: T): Promise<void> {
    const hold = this.#armed.find((candidate) => !candidate.used && candidate.match(value));
    if (hold) await hold.pass(value);
  }
  releaseAll(): void { for (const hold of this.#armed) hold.release(); this.#armed = []; }
}

/** Items a page names per request: two per page, in id order after the keyset. */
const PAGE = 2;

export async function createFixture() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const handled: Handled[] = [];
  const pings: string[] = [];
  const failing = new Set<string>();
  const enrolling = new Set<string>();
  const handlerHolds = new Holds<Handled>();
  const loaderHolds = new Holds<string[]>();
  const xact = async (tx: PgClient) => String((await tx.query("SELECT pg_current_xact_id()::text AS xact")).rows[0].xact);
  const itemIds = async (tx: PgClient, sql: string, parameters: unknown[]) =>
    (await tx.query(sql, parameters)).rows.map((row) => ({ id: String(row.id) }));

  const mutations: Mutations<PgClient> = {
    async addItem({ ctx, args }) {
      await ctx.tx.query("INSERT INTO load_e2e_item(id,project,title) VALUES($1,$2,$3)", [args.item.id, args.item.project, args.item.title]);
      ctx.channel(`items:${args.item.project}`).item.add(args.item);
    },
    async renameItem({ ctx, args }) {
      const changed = await ctx.tx.query("UPDATE load_e2e_item SET title=$2 WHERE id=$1 RETURNING id", [args.item.id, args.item.title]);
      if (changed.rows.length === 0) throw new CallRejected("item.missing");
    },
    async ping({ ctx, args }) {
      await ctx.tx.query("INSERT INTO load_e2e_ping(note) VALUES($1)", [args.note]);
      pings.push(args.note);
    },
  };
  const loads: Loads<PgClient> = {
    /**
     * A project's Items two at a time, then one final empty page. The state
     * is structured: the keyset, the page number, every id seen so far and
     * nested metadata that must round-trip exactly.
     */
    async projectItems({ ctx, args, continuation }) {
      const page: Handled = { load: "ProjectItems", loadId: ctx.loadId, callId: ctx.callId, key: args.project, continuation, xact: await xact(ctx.tx) };
      handled.push(page);
      await handlerHolds.pass(page);
      if (failing.has(args.project)) throw new CallRejected("project.closed");
      const state = (continuation?.state ?? null) as { after: string; page: number; trail: string[]; meta: unknown } | null;
      const items = await itemIds(ctx.tx, "SELECT id FROM load_e2e_item WHERE project=$1 AND id>$2 ORDER BY id LIMIT $3", [args.project, state?.after ?? "", PAGE]);
      const tags = await itemIds(ctx.tx, "SELECT id FROM load_e2e_tag WHERE project=$1 ORDER BY id", [args.project]);
      if (items.length === 0) return { data: { items, tags: [] }, next: null };
      const pageNumber = (state?.page ?? 0) + 1;
      const pageTags = pageNumber === 1 ? tags : [];
      if (enrolling.has(args.project)) {
        // Only what this page returns, in both forms; an Item declared twice is one addition.
        const channel = ctx.channel(`items:${args.project}`);
        for (const item of items) channel.item.add(item);
        channel.add([...items.map((item) => Item(item)), ...pageTags.map((tag) => Tag(tag))]);
      }
      return {
        data: { items, tags: pageTags },
        next: { state: { after: items.at(-1)!.id, page: pageNumber, trail: [...(state?.trail ?? []), ...items.map((item) => item.id)], meta: { size: PAGE, nested: { flags: [true, false, null], label: `p${pageNumber}` } } } },
      };
    },
    /**
     * The shelf's Items in three requests: the first answers `{state: null}`,
     * which is not the end; the second an offset in a mixed tuple; the last
     * is empty and completes.
     */
    async catalog({ ctx, args, continuation }) {
      const page: Handled = { load: "Catalog", loadId: ctx.loadId, callId: ctx.callId, key: args.shelf, continuation, xact: await xact(ctx.tx) };
      handled.push(page);
      await handlerHolds.pass(page);
      const shelf = `shelf:${args.shelf ?? "all"}`;
      const all = await itemIds(ctx.tx, "SELECT id FROM load_e2e_item WHERE project=$1 ORDER BY id", [shelf]);
      if (continuation === null) return { data: { items: all.slice(0, PAGE) }, next: { state: null } };
      if (continuation.state === null) return { data: { items: all.slice(PAGE, PAGE * 2) }, next: { state: { offset: [PAGE * 2, "items", { big: "9007199254740993" }] } } };
      const [offset] = (continuation.state as { offset: [number, string, object] }).offset;
      return { data: { items: all.slice(offset) }, next: null };
    },
  };
  const loaders: Loaders<PgClient> = {
    async item({ ids, tx }) {
      const rows = (await tx.query("SELECT id,project,title FROM load_e2e_item WHERE id = ANY($1::text[])", [ids.map(({ id }) => id)])).rows;
      const byId = new Map(rows.map((row) => [String(row.id), { id: String(row.id), project: String(row.project), title: String(row.title) }]));
      // Read first, then pause: a held page carries the rows as they were.
      await loaderHolds.pass(ids.map(({ id }) => id));
      return ids.map(({ id }) => byId.get(id) ?? null);
    },
    async tag({ ids, tx }) {
      const rows = (await tx.query("SELECT id,label FROM load_e2e_tag WHERE id = ANY($1::text[])", [ids.map(({ id }) => id)])).rows;
      const byId = new Map(rows.map((row) => [String(row.id), { id: String(row.id), label: String(row.label) }]));
      return ids.map(({ id }) => byId.get(id) ?? null);
    },
    // Seen is client bookkeeping; the backend never names one.
    async seen({ ids }) { return ids.map(() => null); },
  };
  const backend = createBackend<PgClient>({ database: pg(pool), authenticate: devAuth(), mutations, loads, loaders, onError: () => {} });
  let listener: Awaited<ReturnType<typeof backend.listen>> | undefined;
  let proxy: Proxy | undefined;
  return {
    pool,
    backend,
    /** Every page a handler executed, in order. A replayed page is not here again. */
    handled,
    pings,
    /** Projects whose ProjectItems pages reject with `project.closed`. */
    failing,
    /** Projects whose ProjectItems pages add what they return to Channel `items:${project}`. */
    enrolling,
    /** Pause a handler at its first matching page, before it reads. */
    holdHandler: (match: (page: Handled) => boolean) => handlerHolds.arm(match),
    /** Pause the Item Loader after it read the rows, at its first matching identity list. */
    holdLoader: (match: (ids: string[]) => boolean) => loaderHolds.arm(match),
    get proxy() { return proxy!; },
    async initialize() {
      const migration = await readFile(new URL("../../packages/postgres/migration.sql", import.meta.url), "utf8");
      await pool.query(migration); // one simple-protocol call: the file holds dollar-quoted functions
      await pool.query("CREATE TABLE load_e2e_item(id text PRIMARY KEY, project text NOT NULL, title text NOT NULL)");
      await pool.query("CREATE TABLE load_e2e_tag(id text PRIMARY KEY, project text NOT NULL, label text NOT NULL)");
      await pool.query("CREATE TABLE load_e2e_ping(id bigserial PRIMARY KEY, note text NOT NULL)");
    },
    /** Insert `count` Items of `project` as `${prefix}-1`…, bypassing any Mutation. */
    async seed(project: string, count: number, prefix = project) {
      const ids = Array.from({ length: count }, (_, n) => `${prefix}-${n + 1}`);
      for (const id of ids) await pool.query("INSERT INTO load_e2e_item(id,project,title) VALUES($1,$2,$3)", [id, project, `${id} title`]);
      return ids;
    },
    /** Insert one Tag per label for `project`, as `${project}-tag-1`…. */
    async seedTags(project: string, labels: string[]) {
      const ids = labels.map((_, n) => `${project}-tag-${n + 1}`);
      for (const [n, id] of ids.entries()) await pool.query("INSERT INTO load_e2e_tag(id,project,label) VALUES($1,$2,$3)", [id, project, labels[n]]);
      return ids;
    },
    /** Retitle an Item in a backend transaction that `touch`es it: no Mutation and no Channel declaration. */
    async retitle(id: string, title: string) {
      await backend.transaction(async ({ tx, touch }) => {
        await tx.query("UPDATE load_e2e_item SET title=$2 WHERE id=$1", [id, title]);
        touch.item({ id });
      });
    },
    /** Relabel a Tag the same way. */
    async relabel(id: string, label: string) {
      await backend.transaction(async ({ tx, touch }) => {
        await tx.query("UPDATE load_e2e_tag SET label=$2 WHERE id=$1", [id, label]);
        touch.tag({ id });
      });
    },
    /** Create an Item in a backend transaction, adding it to `channel` only when one is named. */
    async create(id: string, project: string, channel?: string) {
      await backend.transaction(async ({ tx, touch, channel: join }) => {
        await tx.query("INSERT INTO load_e2e_item(id,project,title) VALUES($1,$2,$3)", [id, project, `${id} title`]);
        touch.item({ id });
        if (channel !== undefined) join(channel).item.add({ id });
      });
    },
    /** A Channel's head: it moves only when something is published to it. */
    async head(channel: string) {
      return Number((await pool.query("SELECT head FROM axton_channel WHERE channel=$1", [channel])).rows[0]?.head ?? 0);
    },
    async listen() {
      listener = await backend.listen({ port: 0 });
      proxy = await createProxy(listener.url);
      return proxy;
    },
    async close() {
      handlerHolds.releaseAll();
      loaderHolds.releaseAll();
      proxy?.releaseAll();
      await proxy?.close();
      await listener?.close();
      await pool.end();
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
type Rule = { match: (exchange: Exchange) => boolean; used: boolean; run: (exchange: Exchange, socket: Socket) => Promise<void> };
export type Proxy = Awaited<ReturnType<typeof createProxy>>;

/**
 * Forwards `/sync/*` HTTP and the live WebSocket to the backend. Rules act on
 * answered exchanges; `down()` cuts every connection and answers nothing
 * until `up()`.
 */
async function createProxy(target: string) {
  const upstream = new URL(target);
  const exchanges: Exchange[] = [];
  const sockets = new Set<Socket>();
  const rules: Rule[] = [];
  const waiters = new Set<() => void>();
  const requestHolds: { match: (exchange: Exchange) => boolean; used: boolean; arrive: () => void; released: Promise<void> }[] = [];
  let down = false;
  const changed = () => { for (const wake of [...waiters]) wake(); };
  const cut = () => { for (const socket of sockets) socket.destroy(); sockets.clear(); };
  const track = (socket: Socket) => { sockets.add(socket); socket.on("close", () => sockets.delete(socket)); };
  const server = createServer(async (request: IncomingMessage, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const exchange: Exchange = { path: request.url?.split("?")[0] ?? "", body: Buffer.concat(chunks).toString("utf8") };
    exchanges.push(exchange);
    const held = requestHolds.find((candidate) => !candidate.used && candidate.match(exchange));
    if (held) { held.used = true; held.arrive(); await held.released; }
    if (down) {
      exchange.dropped = "request";
      request.socket.destroy();
      changed();
      return;
    }
    try {
      const answer = await fetch(new URL(request.url ?? "/", upstream), {
        method: request.method,
        headers: { authorization: String(request.headers.authorization ?? ""), "content-type": String(request.headers["content-type"] ?? "application/json") },
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
    const rule = rules.find((candidate) => !candidate.used && candidate.match(exchange));
    if (rule) { rule.used = true; await rule.run(exchange, request.socket); }
    if (down || request.socket.destroyed) {
      exchange.dropped = "response";
      request.socket.destroy();
      changed();
      return;
    }
    response.writeHead(exchange.status!, { "content-type": "application/json; charset=utf-8" });
    response.end(exchange.response);
    changed();
  });
  server.on("connection", track);
  server.on("upgrade", (request: IncomingMessage, socket: Socket, head: Buffer) => {
    if (down) { socket.destroy(); return; }
    const backend = connect(Number(upstream.port), upstream.hostname, () => {
      const lines = [`${request.method} ${request.url} HTTP/1.1`];
      for (let n = 0; n < request.rawHeaders.length; n += 2) lines.push(`${request.rawHeaders[n]}: ${request.rawHeaders[n + 1]}`);
      backend.write(`${lines.join("\r\n")}\r\n\r\n`);
      if (head.length) backend.write(head);
      socket.pipe(backend).pipe(socket);
    });
    track(backend);
    backend.on("error", () => socket.destroy());
    socket.on("error", () => backend.destroy());
    socket.on("close", () => backend.destroy());
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address() as { port: number };
  const rule = (match: (exchange: Exchange) => boolean, run: Rule["run"]) => { rules.push({ match, used: false, run }); };
  const releases = new Set<() => void>();
  return {
    url: `http://127.0.0.1:${address.port}`,
    exchanges,
    /** Load exchanges only, with their request and response items decoded. */
    loads() {
      return exchanges.filter((exchange) => exchange.path === "/sync/loads").map((exchange) => ({
        ...exchange,
        items: (JSON.parse(exchange.body) as { loads: LoadRequestItem[] }).loads,
        answers: exchange.response === undefined || exchange.status !== 200 ? [] : (JSON.parse(exchange.response) as { loads: LoadResponseItem[] }).loads,
      }));
    },
    /** Resolves once `predicate` holds over the exchanges so far. */
    until(predicate: () => boolean, label: string, timeout = 20_000): Promise<void> {
      if (predicate()) return Promise.resolve();
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => { waiters.delete(check); reject(Error(`Timed out waiting for ${label}`)); }, timeout);
        const check = () => { if (predicate()) { clearTimeout(timer); waiters.delete(check); resolve(); } };
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
      const arrived = new Promise<Exchange>((resolve) => { arrive = resolve; });
      const released = new Promise<void>((resolve) => { release = resolve; });
      const clientGone = new Promise<void>((resolve) => { gone = resolve; });
      releases.add(release);
      rule(match, async (exchange, socket) => {
        if (socket.destroyed) gone(); else socket.once("close", gone);
        arrive(exchange);
        await released;
      });
      return { arrived, release, clientGone };
    },
    /** Hold the first matching request before the backend sees it, until released. */
    holdRequest(match: (exchange: Exchange) => boolean) {
      let arrive!: () => void;
      let release!: () => void;
      const arrived = new Promise<void>((resolve) => { arrive = resolve; });
      const released = new Promise<void>((resolve) => { release = resolve; });
      releases.add(release);
      requestHolds.push({ match, used: false, arrive, released });
      return { arrived, release };
    },
    /** Answer nothing for the first matching exchange and cut the network (`down()`). */
    dropResponseAndGoDown(match: (exchange: Exchange) => boolean) {
      rule(match, async () => { down = true; cut(); });
    },
    down() { down = true; cut(); },
    up() { down = false; },
    releaseAll() { for (const release of releases) release(); },
    async close() {
      cut();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}

export type LoadRequestItem = { loadId: string; callId: string; name: string; version: number; args: Record<string, unknown>; continuation: LoadNext; models: Record<string, number> };
export type LoadResponseItem = {
  loadId: string;
  callId: string;
  outcome: { status: "succeeded"; data: Record<string, { id: string }[]>; next: LoadNext } | { status: "failed" | "retryable"; error: { code: string; message: string } };
  records: { model: string; identity: { id: string }; stamp: number; state: Record<string, unknown> }[];
};
