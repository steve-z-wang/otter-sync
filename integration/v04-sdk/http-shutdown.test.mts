import test from "node:test";
import assert from "node:assert/strict";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate as turn } from "node:timers/promises";
import { Pool } from "pg";
import { pg, type PgClient } from "../../packages/postgres/index.mts";
import { createBackend, devAuth } from "./backend.ts";
import { GeneratedClient } from "./client.ts";

function gate() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

test("listener close drains an admitted HTTP Fetch after the native requester closes its socket", async () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const dir = await mkdtemp(join(tmpdir(), "axton-http-shutdown-"));
  const entered = gate(),
    release = gate(),
    finished = gate(),
    socketClosed = gate();
  const diagnostics: unknown[] = [];
  const authenticate = devAuth();
  let armed = false,
    held = false,
    queryResult: unknown,
    queryError: unknown;
  let closing: Promise<void> | undefined,
    closed = false;
  let client: Awaited<ReturnType<typeof GeneratedClient.open>> | undefined;
  let server:
    | Awaited<ReturnType<ReturnType<typeof createBackend<PgClient>>["listen"]>>
    | undefined;
  try {
    await pool.query(
      await readFile(
        new URL("../../packages/postgres/migration.sql", import.meta.url),
        "utf8",
      ),
    );
    await pool.query(
      "CREATE TABLE http_shutdown_entry(id text PRIMARY KEY,text text NOT NULL)",
    );
    await pool.query(
      "INSERT INTO http_shutdown_entry VALUES('read','snapshot')",
    );
    const backend = createBackend<PgClient>({
      database: pg(pool),
      authenticate: (request) => {
        if (request.url === "/sync/fetch")
          request.socket.once("close", socketClosed.resolve);
        return authenticate(request);
      },
      protocol4: {
        backendId: "sdk",
        contractId: "sdk-v04",
        authorizeStream: (viewer, stream) => stream === `User:${viewer}`,
      },
      onError: (error) => diagnostics.push(error),
      mutations: { publish: async () => ({ entry: { id: "unused" } }) },
      queries: { find: async () => ({ entry: null }) },
      loaders: {
        entry: async ({ tx, ids }) => {
          if (armed) {
            armed = false;
            held = true;
            // This transaction is genuinely connected before the complete Fetch body
            // has been admitted and its Loader enters the gate.
            await tx.query("SELECT 1 AS connected");
            entered.resolve();
            await release.promise;
            try {
              queryResult = (
                await tx.query(
                  "SELECT id,text FROM http_shutdown_entry WHERE id=$1",
                  ["read"],
                )
              ).rows;
              return ids.map(() => ({ id: "read", text: "snapshot" }));
            } catch (error) {
              queryError = error;
              throw error;
            } finally {
              finished.resolve();
            }
          }
          return ids.map(() => null);
        },
        draft: undefined,
      },
      bootstrap: async () => {},
    });
    server = await backend.listen({ port: 0 });
    client = await GeneratedClient.open({
      path: join(dir, "db"),
      stream: "User:alice",
      connection: {
        url: server.url,
        token: "alice",
        identity: { backend: "sdk", viewer: "alice", contract: "sdk-v04" },
      },
    });
    await client.bootstrap();
    armed = true;
    // Observe cancellation without creating an unhandled rejected Fetch.
    const fetching = client.fetch.entry({ id: "read" }, { store: false }).then(
      (value) => ({ value }),
      (error) => ({ error }),
    );
    await entered.promise;
    await client.close();
    await socketClosed.promise;
    const outcome = await fetching;
    assert.ok(
      "error" in outcome,
      "closing the native client cancels its pending Fetch",
    );
    closing = server.close().then(() => {
      closed = true;
    });
    let closedAgain = false;
    const again = server.close().then(() => {
      closedAgain = true;
    });
    await turn();
    await turn();
    assert.equal(
      closed,
      false,
      "listener close resolved after the HTTP socket closed while its admitted real database Loader was still held",
    );
    assert.equal(closedAgain, false);
    release.resolve();
    await finished.promise;
    await Promise.all([closing, again]);
    assert.equal(queryError, undefined);
    assert.deepEqual(queryResult, [{ id: "read", text: "snapshot" }]);
    assert.deepEqual(diagnostics, []);
  } finally {
    release.resolve();
    if (held) await finished.promise;
    await client?.close();
    await (closing ?? server?.close());
    await pool.end();
    await rm(dir, { recursive: true, force: true });
  }
});

test("close owns both aborted HTTP admissions, refuses new work, and preserves admission failures", async () => {
  const { request } = await import("node:http");
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const held = [0, 1].map(() => ({
    entered: gate(),
    release: gate(),
    finished: gate(),
    gone: gate(),
  }));
  const expected = new Error("held admission failed");
  const diagnostics: unknown[] = [];
  let admissions = 0,
    closed = false;
  const backend = createBackend<PgClient>({
    database: pg(pool),
    authenticate: async (req) => {
      const index = admissions++;
      const item = held[index];
      req.socket.once("close", item.gone.resolve);
      const tx = await pool.connect();
      try {
        await tx.query("SELECT 1 AS connected");
        item.entered.resolve();
        await item.release.promise;
        assert.deepEqual((await tx.query("SELECT 1 AS admitted")).rows, [
          { admitted: 1 },
        ]);
        if (index === 1) throw expected;
        return "alice";
      } finally {
        tx.release();
        item.finished.resolve();
      }
    },
    protocol4: {
      backendId: "sdk",
      contractId: "sdk-v04",
      authorizeStream: (viewer, stream) => stream === `User:${viewer}`,
    },
    onError: (error) => diagnostics.push(error),
    mutations: { publish: async () => ({ entry: { id: "unused" } }) },
    queries: { find: async () => ({ entry: null }) },
    loaders: {
      entry: async ({ ids }) => ids.map(() => null),
      draft: undefined,
    },
    bootstrap: async () => {},
  });
  const server = await backend.listen({ port: 0 });
  const callers = held.map(() => {
    const req = request(server.url + "/sync/fetch", { method: "POST" });
    req.on("error", () => {}); // Socket destruction is deliberate; server Reporter remains asserted.
    req.end("{}");
    return req;
  });
  let closing: Promise<void> | undefined;
  try {
    await Promise.all(held.map((item) => item.entered.promise));
    callers.forEach((req) => req.destroy());
    await Promise.all(held.map((item) => item.gone.promise));
    closing = server.close().then(() => {
      closed = true;
    });
    const status = await new Promise<number | undefined>((resolve, reject) => {
      const req = request(
        server.url + "/sync/fetch",
        { method: "POST" },
        (res) => {
          res.resume();
          res.once("end", () => resolve(res.statusCode));
        },
      );
      req.on("error", reject);
      req.end("{}");
    });
    assert.equal(status, 503);
    assert.equal(
      admissions,
      2,
      "close refuses requests before authentication can own new database work",
    );
    assert.equal(closed, false);
    held[0].release.resolve();
    await held[0].finished.promise;
    await turn();
    assert.equal(
      closed,
      false,
      "draining one request must not release the other admission",
    );
    held[1].release.resolve();
    await held[1].finished.promise;
    await closing;
    assert.ok(
      diagnostics.includes(expected),
      "original admission failure reaches Reporter after socket close",
    );
  } finally {
    held.forEach((item) => item.release.resolve());
    callers.forEach((req) => req.destroy());
    await (closing ?? server.close());
    await pool.end();
  }
});
