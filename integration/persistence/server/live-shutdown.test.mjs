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
