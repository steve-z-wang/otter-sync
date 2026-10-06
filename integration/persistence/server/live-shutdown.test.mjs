import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { setImmediate as turn } from "node:timers/promises";
import { Pool } from "pg";
import { WebSocket } from "ws";
import { createBackend } from "../../../packages/server/index.mts";
import { pg } from "../../../packages/postgres/index.mts";

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
      new URL("../../../packages/postgres/migration.sql", import.meta.url),
      "utf8",
    ),
  );
  await admin.query(
    "CREATE TABLE shutdown_space(id text PRIMARY KEY, name text NOT NULL)",
  );
});
after(() => admin.end());

for (const carrier of ["protocol4", "legacy"]) {
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
      ...(carrier === "protocol4"
        ? {
            protocol4: {
              backendId: "shutdown",
              contractId: "app",
              projectionGeneration: "1",
              authorizeStream: (owner, name) => name === `User:${owner}`,
            },
          }
        : {}),
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
      socket.send(
        JSON.stringify(
          carrier === "protocol4"
            ? {
                context: {
                  protocol: 4,
                  binding: {
                    backend: "shutdown",
                    viewer,
                    stream,
                    contract: "app",
                  },
                  materialization: app.materializationId,
                  incarnation: randomUUID(),
                },
                models: { Space: 1 },
                cursor: 0,
              }
            : {
                capabilities: ["stream-authority-v1"],
                type: "subscribe",
                streams: [stream],
                models: { Space: 1 },
              },
        ),
      );
      await ack;
      if (carrier === "legacy")
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
      protocol4: {
        backendId: "shutdown",
        contractId: "app",
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
        socket.send(
          JSON.stringify({
            context: {
              protocol: 4,
              binding: {
                backend: "shutdown",
                viewer,
                stream: `User:${viewer}`,
                contract: "app",
              },
              materialization: app.materializationId,
              incarnation: randomUUID(),
            },
            models: { Space: 1 },
            cursor: 0,
          }),
        );
      }
      if (scenario !== "negotiation") {
        await Promise.all(initial.map((item) => item.promise));
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
