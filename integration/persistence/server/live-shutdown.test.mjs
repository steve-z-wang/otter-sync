import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { setImmediate as turn } from "node:timers/promises";
import { Pool } from "pg";
import { WebSocket } from "ws";
import { createBackend } from "../../../packages/backend/server/index.mts";
import { pg } from "../../../packages/backend/postgres/index.mts";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const admin = new Pool({ connectionString: process.env.DATABASE_URL });
const fields = ["id", "name"].map((name) => ({
  name,
  nullable: false,
  type: { kind: "scalar", name: "string" },
}));
const config = {
  schema: {
    enums: [],
    models: [{ name: "Space", version: 1, identity: ["id"], fields }],
  },
  mutations: [],
};
const gate = () => {
  let resolve;
  const promise = new Promise((done) => {
    resolve = done;
  });
  return { promise, resolve };
};
before(async () => {
  await admin.query(
    await readFile(
      new URL("../../../packages/backend/postgres/migration.sql", import.meta.url),
      "utf8",
    ),
  );
  await admin.query(
    "CREATE TABLE shutdown_space(id text PRIMARY KEY, name text NOT NULL)",
  );
});
after(() => admin.end());

for (const carrier of ["protocol5"]) {
  test(`listener close drains an admitted ${carrier} Live Space Loader before database disconnect`, async () => {
    const pool = new Pool({ connectionString: process.env.DATABASE_URL });
    // Capture genuine connection/Reporter failures, including after the socket closes.
    const diagnostics = [];
    pool.on("error", (error) => diagnostics.push(error));
    const entered = gate(),
      release = gate(),
      read = gate();
    let armed = false,
      pid,
      queryResult,
      queryError,
      readSettled = false,
      disconnected = false;
    const viewer = `shutdown-${carrier}`,
      stream = `User:${viewer}`,
      identity = { id: viewer };
    await admin.query("INSERT INTO shutdown_space VALUES($1,$2)", [
      viewer,
      "Journal",
    ]);
    const app = createBackend({
      config,
      native,
      database: pg(pool),
      authenticate: () => viewer,
      protocol5: {
        projectionGeneration: "1",
        authorizeStream: (owner, name) => name === `User:${owner}`,
      },
      onError: (error) => diagnostics.push(error),
      loaders: {
        space: async ({ tx, ids }) => {
          if (armed) {
            armed = false;
            tx.on("error", (error) => diagnostics.push(error));
            pid = (await tx.query("SELECT pg_backend_pid() AS pid")).rows[0]
              .pid;
            entered.resolve();
            await release.promise;
            try {
              queryResult = (
                await tx.query(
                  "SELECT id,name FROM shutdown_space WHERE id=$1",
                  [viewer],
                )
              ).rows;
            } catch (error) {
              queryError = error;
              throw error;
            } finally {
              readSettled = true;
              read.resolve();
            }
            return ids.map(() => queryResult[0] ?? null);
          }
          return Promise.all(
            ids.map(
              async ({ id }) =>
                (
                  await tx.query(
                    "SELECT id,name FROM shutdown_space WHERE id=$1",
                    [id],
                  )
                ).rows[0] ?? null,
            ),
          );
        },
      },
    });
    await app.transaction(({ streams }) =>
      streams(stream).track.space(identity),
    );
    armed = true;
    const listener = await app.listen({ port: 0 });
    const socket = new WebSocket(
      listener.url.replace("http:", "ws:") + "/sync/live",
    );
    let closing, disconnectedWork;
    try {
      await once(socket, "open");
      const ack = once(socket, "message");
      socket.send(JSON.stringify({ protocol: 5, storeId: viewer, stream }));
      await ack;
      await app.transaction(({ invalidate }) => invalidate.space(identity));
      await entered.promise;
      let closed = false,
        closedAgain = false;
      const socketClosed = once(socket, "close");
      closing = listener.close();
      disconnectedWork = closing.then(async () => {
        closed = true;
        disconnected = true;
        // Reproduce the application releasing its DB immediately after close.
        // Terminate only this fixture's held transaction, if close returned early.
        if (!readSettled)
          await admin.query("SELECT pg_terminate_backend($1)", [pid]);
        await pool.end();
      });
      const again = listener.close().then(() => {
        closedAgain = true;
      });
      await socketClosed;
      // A socket-close event and an event-loop barrier, never a timing sleep.
      await turn();
      assert.equal(
        closed,
        false,
        "listener close resolved while its admitted Loader was still gated",
      );
      assert.equal(
        closedAgain,
        false,
        "a concurrent close must await the same drain",
      );
      assert.equal(disconnected, false);
      release.resolve();
      await read.promise;
      await Promise.all([disconnectedWork, again]);
      assert.equal(queryError, undefined);
      assert.deepEqual(queryResult, [{ id: viewer, name: "Journal" }]);
      assert.deepEqual(diagnostics, []);
    } finally {
      // On RED, ensure the real premature disconnect happens before releasing
      // the query, and retain its exact diagnostic instead of suppressing it.
      if (disconnected && !readSettled)
        await admin.query("SELECT pg_terminate_backend($1)", [pid]);
      release.resolve();
      socket.close();
      if (closing) await read.promise;
      await (closing ?? listener.close());
      await (disconnectedWork ?? pool.end());
      if (queryError || diagnostics.length)
        console.error("LIVE_SHUTDOWN_DIAGNOSTICS", { queryError, diagnostics });
    }
  });
}

test("listener close drains a held real upgrade admission before database release", async () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const entered = gate(),
    release = gate(),
    finished = gate(),
    peerGone = gate();
  const diagnostics = [];
  let queryResult,
    admissionSocket,
    closed = false,
    closing;
  const app = createBackend({
    config,
    native,
    database: pg(pool),
    authenticate: () => "admission",
    admit: async (request) => {
      request.socket.once("close", () => peerGone.resolve());
      admissionSocket = request.socket;
      const tx = await pool.connect();
      try {
        entered.resolve();
        await release.promise;
        queryResult = (await tx.query("SELECT 1 AS admitted")).rows;
        return null;
      } finally {
        tx.release();
        finished.resolve();
      }
    },
    onError: (error) => diagnostics.push(error),
    loaders: { space: async ({ ids }) => ids.map(() => null) },
  });
  const listener = await app.listen({ port: 0 });
  const socket = new WebSocket(
    listener.url.replace("http:", "ws:") + "/sync/live",
  );
  const refused = new Promise((resolve) => socket.once("error", resolve));
  try {
    await entered.promise;
    const disconnected = new Promise((resolve) =>
      socket.once("close", resolve),
    );
    socket.terminate();
    admissionSocket.destroy();
    await disconnected;
    await peerGone.promise;
    closing = listener.close().then(() => {
      closed = true;
    });
    await turn();
    await turn();
    assert.equal(
      closed,
      false,
      "listener close resolved while its upgrade admission still owned database work",
    );
    release.resolve();
    await finished.promise;
    await closing;
    assert.deepEqual(queryResult, [{ admitted: 1 }]);
    assert.match(
      (await refused).message,
      /closed before the connection was established/,
    );
    assert.deepEqual(diagnostics, []);
  } finally {
    release.resolve();
    await finished.promise;
    await (closing ?? listener.close());
    socket.close();
    await pool.end();
  }
});

for (const scenario of ["negotiation", "multiple pulls", "rejected pull"]) {
  test(`listener close drains ${scenario} with real native sessions`, async () => {
    const pool = new Pool({ connectionString: process.env.DATABASE_URL });
    const database = pg(pool),
      diagnostics = [],
      expected = new Error("held pull rejected");
    const rejectedTransactions = new Set(),
      initialTransactions = new Map();
    const count = scenario === "multiple pulls" ? 2 : 1;
    const held = Array.from({ length: count }, () => ({
      entered: gate(),
      release: gate(),
      finished: gate(),
    }));
    const initial = held.map(() => gate());
    const viewers = held.map((_, i) => `shutdown-${scenario}-${i}`);
    let armed = false,
      negotiationHeld = false,
      pendingIndex = 0;
    const hold = async (tx, index) => {
      const item = held[index];
      item.entered.resolve();
      await item.release.promise;
      assert.deepEqual((await tx.query("SELECT 1 AS live")).rows, [
        { live: 1 },
      ]);
    };
    const app = createBackend({
      config,
      native,
      database: {
        ...database,
        transaction: async (body) => {
          const pending =
            armed && scenario === "multiple pulls" ? pendingIndex++ : undefined;
          if (pending !== undefined) {
            held[pending].entered.resolve();
            await held[pending].release.promise;
          }
          let transaction;
          const result = await database.transaction(async (tx) => {
            transaction = tx;
            if (pending !== undefined)
              assert.deepEqual((await tx.query("SELECT 1 AS live")).rows, [
                { live: 1 },
              ]);
            const blocked =
              armed && scenario === "negotiation" && !negotiationHeld;
            if (blocked) {
              negotiationHeld = true;
              await hold(tx, 0);
            }
            try {
              const result = await body(tx);
              if (rejectedTransactions.delete(tx)) throw expected;
              return result;
            } finally {
              if (blocked) held[0].finished.resolve();
            }
          });
          if (pending !== undefined) held[pending].finished.resolve();
          const index = initialTransactions.get(transaction);
          if (index !== undefined) initial[index].resolve();
          return result;
        },
      },
      authenticate: (request) => request.headers["x-viewer"],
      protocol5: {
        projectionGeneration: "1",
        authorizeStream: (owner, name) => name === `User:${owner}`,
      },
      onError: (error) => diagnostics.push(error),
      loaders: {
        space: async ({ tx, ids, userId }) => {
          const index = viewers.indexOf(userId);

          if (!armed) initialTransactions.set(tx, index);
          if (armed && scenario === "rejected pull") {
            try {
              await hold(tx, index);
              if (scenario === "rejected pull") rejectedTransactions.add(tx);
            } finally {
              held[index].finished.resolve();
            }
          }
          return ids.map(({ id }) => ({ id, name: "Journal" }));
        },
      },
    });
    for (const viewer of viewers)
      await app.transaction(({ streams }) =>
        streams(`User:${viewer}`).track.space({ id: viewer }),
      );
    armed = scenario === "negotiation";
    const listener = await app.listen({ port: 0 }),
      sockets = [];
    let closing,
      closed = false;
    try {
      for (const viewer of viewers) {
        const socket = new WebSocket(
          listener.url.replace("http:", "ws:") + "/sync/live",
          { headers: { "x-viewer": viewer } },
        );
        sockets.push(socket);
        await once(socket, "open");
        const ready = once(socket, "message");
        socket.send(
          JSON.stringify({
            protocol: 5,
            storeId: viewer,
            stream: `User:${viewer}`,
          }),
        );
        if (scenario !== "negotiation") await ready;
      }
      if (scenario !== "negotiation") {
        await app.transaction(({ invalidate }) => {
          for (const id of viewers) invalidate.space({ id });
          armed = true;
        });
      }
      await Promise.all(held.map((item) => item.entered.promise));
      const stopped = sockets.map((socket) => once(socket, "close"));
      closing = listener.close().then(() => {
        closed = true;
      });
      await Promise.all(stopped);
      await turn();
      assert.equal(closed, false);
      held[0].release.resolve();
      await held[0].finished.promise;
      if (count === 2) {
        await turn();
        assert.equal(
          closed,
          false,
          "first drained session must not release the second transaction",
        );
        held[1].release.resolve();
        await held[1].finished.promise;
      }
      await closing;
      if (scenario === "rejected pull")
        assert.ok(
          diagnostics.some((error) => error === expected),
          "Reporter must retain the pull failure",
        );
      else assert.deepEqual(diagnostics, []);
    } finally {
      held.forEach((item) => item.release.resolve());
      sockets.forEach((socket) => socket.close());
      await (closing ?? listener.close());
      await pool.end();
    }
  });
}

test("throwing upgrade Reporter cannot release another admission or an aborted Live Loader", async () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const held = [0, 1].map(() => ({
    entered: gate(),
    release: gate(),
    finished: gate(),
    gone: gate(),
  }));
  const session = { entered: gate(), release: gate(), finished: gate() };
  const expected = new Error("upgrade admission failed"),
    reporterFailure = new Error("upgrade Reporter failed");
  const diagnostics = [];
  let admissions = 0,
    closed = false,
    closing,
    sessionResult;
  const backend = createBackend({
    config,
    native,
    database: pg(pool),
    authenticate: async (request) => {
      if (request.headers["x-viewer"] === "held-session") return "held-session";
      const index = admissions++,
        item = held[index];
      request.socket.once("close", item.gone.resolve);
      item.socket = request.socket;
      const tx = await pool.connect();
      try {
        await tx.query("SELECT 1 AS connected");
        item.entered.resolve();
        await item.release.promise;
        assert.deepEqual((await tx.query("SELECT 1 AS admitted")).rows, [
          { admitted: 1 },
        ]);
        if (index === 0) throw expected;
        return "admitted";
      } finally {
        tx.release();
        item.finished.resolve();
      }
    },
    protocol5: {
      projectionGeneration: "1",
      authorizeStream: (viewer, name) => name === `User:${viewer}`,
    },
    onError: (error) => {
      diagnostics.push(error);
      if (error === expected) throw reporterFailure;
    },
    loaders: {
      space: async ({ tx, ids }) => {
        await tx.query("SELECT 1 AS connected");
        session.entered.resolve();
        await session.release.promise;
        try {
          sessionResult = (await tx.query("SELECT 1 AS live")).rows;
          return ids.map(({ id }) => ({ id, name: "Journal" }));
        } finally {
          session.finished.resolve();
        }
      },
    },
  });
  await backend.transaction(({ streams }) =>
    streams("User:held-session").track.space({ id: "held-session" }),
  );
  const listener = await backend.listen({ port: 0 });
  const url = listener.url.replace("http:", "ws:") + "/sync/live";
  const socket = new WebSocket(url, {
    headers: { "x-viewer": "held-session" },
  });
  const upgrades = held.map(() => {
    const ws = new WebSocket(url);
    ws.on("error", () => {});
    return ws;
  });
  try {
    await once(socket, "open");
    const ready = once(socket, "message");
    socket.send(
      JSON.stringify({
        protocol: 5,
        storeId: "held-session",
        stream: "User:held-session",
      }),
    );
    await ready;
    await backend.transaction(({ invalidate }) =>
      invalidate.space({ id: "held-session" }),
    );
    await Promise.all([
      ...held.map((item) => item.entered.promise),
      session.entered.promise,
    ]);
    const sessionGone = once(socket, "close");
    socket.terminate();
    upgrades.forEach((ws) => ws.terminate());
    held.forEach((item) => item.socket.destroy());
    await Promise.all([...held.map((item) => item.gone.promise), sessionGone]);
    closing = listener.close().then(
      () => {
        closed = true;
      },
      (error) => {
        closed = true;
        throw error;
      },
    );
    void closing.catch(() => {});
    held[0].release.resolve();
    await held[0].finished.promise;
    await turn();
    await turn();
    assert.equal(
      closed,
      false,
      "upgrade Reporter rejection must not release another admitted database callback",
    );
    held[1].release.resolve();
    await held[1].finished.promise;
    await turn();
    await turn();
    assert.equal(
      closed,
      false,
      "failed upgrade must still drain the already-aborted Live session",
    );
    session.release.resolve();
    await session.finished.promise;
    await assert.rejects(closing, (error) => error === reporterFailure);
    await assert.rejects(
      listener.close(),
      (error) => error === reporterFailure,
    );
    await assert.rejects(
      fetch(listener.url + "/sync/fetch", { method: "POST", body: "{}" }),
    );
    assert.deepEqual(sessionResult, [{ live: 1 }]);
    assert.ok(diagnostics.includes(expected));
  } finally {
    held.forEach((item) => item.release.resolve());
    session.release.resolve();
    upgrades.forEach((ws) => ws.terminate());
    socket.terminate();
    await Promise.allSettled([closing ?? listener.close()]);
    await pool.end();
  }
});
