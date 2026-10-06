// Actual PostgreSQL/listener -> native actor process -> SQLite, no SDK state machine.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRequire } from "node:module";
import { Pool } from "pg";
import { DatabaseSync } from "node:sqlite";
import { createBackend } from "../../packages/server/index.mts";
import { pg } from "../../packages/postgres/index.mts";
import { NativeHost } from "./native-host.mjs";
const native = createRequire(import.meta.url)(
  "../../bindings/node/axton-node.node",
);
const X = "01890f47-1234-7123-8123-000000000057";
let sequence = 0;
const fields = ["id", "title"].map((name) => ({
  name,
  nullable: false,
  type: { kind: "scalar", name: "string" },
}));
const model = { name: "Task", version: 1, identity: ["id"], fields };
const marker = { ...model, name: "Marker", bootstrap: true };
function configuration(models = [model, marker]) {
  return {
    schema: {
      enums: [],
      models,
      resultModels: models.map((m) => ({ ...m, enums: [] })),
      actions: [
        {
          name: "Edit",
          version: 1,
          kind: "mutation",
          inputs: [
            {
              kind: "model",
              name: "task",
              model: "Task",
              operation: "update",
              cardinality: "single",
              allowedPatchFields: ["title"],
            },
          ],
          outputs: [],
        },
        {
          name: "Ping",
          version: 1,
          kind: "mutation",
          inputs: [],
          outputs: [],
        },
        {
          name: "Find",
          version: 1,
          kind: "query",
          inputs: [],
          outputs: [
            {
              name: "task",
              kind: "model",
              model: "Task",
              modelReadVersion: 1,
              source: "handlerIdentity",
              cardinality: "single",
              handlerType: {
                kind: "identity",
                model: "Task",
                fields: [
                  { name: "id", type: { kind: "scalar", name: "string" } },
                ],
              },
            },
          ],
        },
      ],
    },
    mutations: [],
    loaders: models.map((m) => m.name),
  };
}
async function fixture() {
  const namespace = `accept04_${++sequence}`;
  const admin = new Pool({ connectionString: process.env.DATABASE_URL });
  await admin.query(`CREATE SCHEMA ${namespace}`);
  await admin.end();
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    options: `-c search_path=${namespace}`,
  });
  await pool.query(
    await readFile(
      new URL("../../packages/postgres/migration.sql", import.meta.url),
      "utf8",
    ),
  );
  await pool.query(
    "CREATE TABLE business_task(id text PRIMARY KEY,title text NOT NULL UNIQUE,label text);CREATE TABLE business_marker(id text PRIMARY KEY,title text NOT NULL);CREATE TABLE executions(n int NOT NULL);INSERT INTO executions VALUES(0)",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton04-"));
  const hosts = [];
  const listeners = [];
  const apps = [];
  const inFlight = new Set();
  let queryCalls = 0;
  let queryGate = null;
  const binding = {
    backend: "accept04",
    viewer: "alice",
    stream: "User:alice",
    contract: "app",
  };
  function backend(
    config = configuration(),
    extra = {},
    projected = false,
    extraLoaders = {},
  ) {
    const taskLoader =
      (version) =>
      async ({ tx, ids, userId }) =>
        Promise.all(
          ids.map(async ({ id }) => {
            const row = (
              await tx.query(
                "SELECT id,title,label FROM business_task WHERE id=$1",
                [id],
              )
            ).rows[0];
            return row
              ? {
                  id: row.id,
                  title: projected ? `${userId}:${row.title}` : row.title,
                  ...(version === 2 ? { label: row.label } : {}),
                  ...(config.schema.models
                    .find((m) => m.name === "Task")
                    .fields.some((f) => f.name === "parentId")
                    ? { parentId: row.label }
                    : {}),
                }
              : null;
          }),
        );
    const taskRegistration =
      config.schema.models.find((m) => m.name === "Task").version === 2
        ? { v1: taskLoader(1), v2: taskLoader(2) }
        : taskLoader(1);
    const database = pg(pool);
    const transaction = database.transaction.bind(database);
    database.transaction = (body) => {
      const pending = transaction(body);
      inFlight.add(pending);
      pending.then(
        () => inFlight.delete(pending),
        () => inFlight.delete(pending),
      );
      return pending;
    };
    const app = createBackend({
      config,
      native,
      database,
      protocol4: {
        backendId: binding.backend,
        contractId: binding.contract,
        authorizeStream: (viewer, stream) => stream === `User:${viewer}`,
        ...extra,
      },
      authenticate: (request) => request.headers["x-viewer"] ?? "alice",
      loaders: {
        task: taskRegistration,
        marker: async ({ tx, ids }) =>
          Promise.all(
            ids.map(
              async ({ id }) =>
                (
                  await tx.query("SELECT * FROM business_marker WHERE id=$1", [
                    id,
                  ])
                ).rows[0] ?? null,
            ),
          ),
        ...extraLoaders,
      },
      queries: {
        find: async ({ ctx }) => {
          queryCalls++;
          await queryGate?.();
          ctx.stream.track.task({ id: X });
          return { task: { id: X } };
        },
      },
      mutations: {
        ping: async ({ ctx }) => {
          await ctx.tx.query("UPDATE executions SET n=n+1");
        },
        edit: async ({ ctx, args }) => {
          await ctx.tx.query("UPDATE executions SET n=n+1");
          if (args.task.title !== "noop") {
            await ctx.tx.query(
              "UPDATE business_task SET title=$2 WHERE id=$1",
              [args.task.id, args.task.title],
            );
            ctx.invalidate.task({ id: args.task.id });
          }
        },
      },
    });
    apps.push(app);
    return app;
  }
  async function client(
    app,
    name = "client",
    schema = app.config?.schema ?? configuration().schema,
    clientBinding = binding,
  ) {
    const listener = await app.listen({ port: 0 });
    listeners.push(listener);
    const host = await new NativeHost(listener.url).open(
      join(directory, name + ".sqlite"),
      schema,
      clientBinding,
    );
    hosts.push(host);
    return host;
  }
  async function close() {
    const errors = [];
    for (const host of hosts) {
      try {
        await host.close();
      } catch (error) {
        errors.push(error);
        try {
          await host.kill();
        } catch (killError) {
          errors.push(killError);
        }
      }
    }
    for (const listener of listeners) {
      try {
        await listener.close();
      } catch (error) {
        errors.push(error);
      }
    }
    // Drain actual entered transaction/retry promises after closing admission.
    while (inFlight.size) await Promise.allSettled([...inFlight]);
    await pool.end();
    await rm(directory, { recursive: true, force: true });
    if (errors.length)
      throw new AggregateError(errors, "acceptance cleanup failed");
  }
  return {
    pool,
    directory,
    hosts,
    listeners,
    binding,
    backend,
    client,
    close,
    queryCalls: () => queryCalls,
    setQueryGate: (gate) => {
      queryGate = gate;
    },
  };
}
async function held(predicate, host) {
  let resolve;
  const promise = new Promise((r) => (resolve = r));
  host.hold = async (data) => {
    if (predicate(data)) {
      resolve(data);
      return true;
    }
    return false;
  };
  return promise;
}
const rowKey = { model: "Task", identity: { id: X } };
async function cache(host) {
  await host.task({ kind: "connect" });
  await host.task({
    kind: "fetch",
    model: "Task",
    version: 1,
    identity: { id: X },
  });
}

// Seed retained historical Remove evidence under the real publication fence.
// 0.4 exposes no application untrack API; this is a storage compatibility case.
async function historicalRemove(f, app) {
  const database = pg(f.pool);
  await database.transaction(async (tx) => {
    await app.acquirePublicationFence(tx);
    const record = (
      await tx.query(
        "SELECT id FROM axton_record WHERE model='Task' AND identity_key=$1",
        [JSON.stringify({ id: X })],
      )
    ).rows[0].id;
    const cursor = Number(
      (
        await tx.query(
          "UPDATE axton_stream SET head=head+1 WHERE stream='User:alice' RETURNING head",
        )
      ).rows[0].head,
    );
    await tx.query(
      "DELETE FROM axton_stream_member WHERE stream='User:alice' AND record_id=$1",
      [record],
    );
    await tx.query(
      "UPDATE axton_stream_log SET kind='remove',cursor=$2 WHERE stream='User:alice' AND record_id=$1",
      [record, cursor],
    );
    await database.persistence(tx).call({
      op: "savePublicationGroups",
      positions: [
        {
          stream: "User:alice",
          model: "Task",
          identityKey: JSON.stringify({ id: X }),
          cursor,
          kind: "remove",
        },
      ],
    });
  });
}

for (const laterDirect of [false, true]) {
  test(`retained Remove before a delayed tracked receipt settles its original owned fallback${laterDirect ? " beneath later direct work" : ""}`, async () => {
    const f = await fixture();
    try {
      const app = f.backend();
      await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
        X,
        "original",
      ]);
      await app.transaction(async ({ stream }) =>
        stream("User:alice").track.task({ id: X }),
      );
      const host = await f.client(app, "removed-receipt");
      host.frameHold = (body) => Array.isArray(body.units);
      await cache(host);
      const response = held(
        ({ route, body }) => route === "action" && body.models,
        host,
      );
      const call = await host.task({
        kind: "submitAction",
        name: "Edit",
        version: 1,
        args: { task: { id: X, title: "accepted" } },
      });
      const answer = await response;
      assert.equal(answer.response.targets[0].kind, "stream");
      assert.equal(answer.response.targets[0].cursor, 2);
      await historicalRemove(f, app);
      // A retained storage Remove has no current application publication wake.
      // Reconnect the real socket so catch-up reads the committed compacted log.
      await host.until(
        "SELECT cursor FROM axton_v04_store",
        () => host.heldFrames.length > 0,
      );
      const closeSocket = host.effects.get(host.heldFrames[0].effect.effectId);
      assert.equal(typeof closeSocket, "function");
      host.frameHold = null;
      closeSocket();
      await host.until(
        "SELECT cursor FROM axton_v04_store",
        (rows) => rows[0].cursor === 3,
      );
      if (laterDirect)
        await host.task({
          kind: "direct",
          operation: {
            model: "Task",
            identity: { id: X },
            op: "update",
            values: { title: "later-local" },
          },
        });
      host.reply(answer.effect, {
        status: 200,
        body: JSON.stringify(answer.response),
      });
      const completion = await host.wait(
        (e) => e.type === "callCompleted" && e.callId === call.callId,
      );
      assert.equal(completion.outcome.status, "succeeded");
      assert.equal(
        (await host.sql("SELECT title FROM Task"))[0].title,
        laterDirect ? "later-local" : "accepted",
      );
      const evidence = JSON.parse(
        (await host.sql("SELECT evidence FROM axton_v04_record"))[0].evidence,
      );
      assert.deepEqual(evidence.membership, { cursor: 3, live: false });
      assert.equal(evidence.current, null);
      assert.equal(
        (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
        3,
      );
      assert.equal(
        (await host.sql("SELECT count(*) n FROM axton_mutation"))[0].n,
        0,
      );
      assert.equal(
        (await f.pool.query("SELECT n FROM executions")).rows[0].n,
        1,
      );
    } finally {
      await f.close();
    }
  });
}

for (const variant of ["untracked", "other Stream", "untracked no-op"]) {
  test(`private Mutation settlement for ${variant} keeps initiating Store outside authority`, async () => {
    const f = await fixture();
    try {
      const app = f.backend();
      await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
        X,
        "original",
      ]);
      if (variant === "other Stream")
        await app.transaction(async ({ stream }) =>
          stream("User:bob").track.task({ id: X }),
        );
      const host = await f.client(app, "private");
      await cache(host);
      const response = held(
        ({ route, body }) => route === "action" && body.models,
        host,
      );
      const call = await host.task({
        kind: "submitAction",
        name: "Edit",
        version: 1,
        args: {
          task: {
            id: X,
            title: variant === "untracked no-op" ? "noop" : "accepted",
          },
        },
      });
      const answer = await response;
      assert.equal(answer.response.targets.length, 1);
      assert.equal(answer.response.targets[0].kind, "private");
      assert.equal(answer.response.targets[0].record.cursor, null);
      host.reply(answer.effect, {
        status: 200,
        body: JSON.stringify(answer.response),
      });
      const completion = await host.wait(
        (e) => e.type === "callCompleted" && e.callId === call.callId,
      );
      assert.equal(completion.outcome.status, "succeeded");
      assert.equal(
        (await host.sql("SELECT title FROM Task"))[0].title,
        variant === "untracked no-op" ? "original" : "accepted",
      );
      assert.equal(
        (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
        0,
      );
      const evidence = JSON.parse(
        (await host.sql("SELECT evidence FROM axton_v04_record"))[0].evidence,
      );
      assert.deepEqual(evidence.history, {});
      assert.equal(evidence.current, null);
      assert.equal(
        (await host.sql("SELECT count(*) n FROM axton_mutation"))[0].n,
        0,
      );
      assert.equal(
        (
          await f.pool.query(
            "SELECT count(*)::int n FROM axton_stream_member WHERE stream='User:alice'",
          )
        ).rows[0].n,
        0,
      );
      assert.equal(
        (await f.pool.query("SELECT n FROM executions")).rows[0].n,
        1,
      );
    } finally {
      await f.close();
    }
  });
}

test("delayed private Mutation fallback cannot overwrite a subsequent direct write", async () => {
  const f = await fixture();
  try {
    const app = f.backend();
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "original",
    ]);
    const host = await f.client(app, "private-local");
    await cache(host);
    const response = held(
      ({ route, body }) => route === "action" && body.models,
      host,
    );
    const call = await host.task({
      kind: "submitAction",
      name: "Edit",
      version: 1,
      args: { task: { id: X, title: "accepted" } },
    });
    const answer = await response;
    assert.equal(answer.response.targets[0].kind, "private");
    await host.task({
      kind: "direct",
      operation: {
        model: "Task",
        identity: { id: X },
        op: "update",
        values: { title: "later-local" },
      },
    });
    host.reply(answer.effect, {
      status: 200,
      body: JSON.stringify(answer.response),
    });
    const completion = await host.wait(
      (e) => e.type === "callCompleted" && e.callId === call.callId,
    );
    assert.equal(completion.outcome.status, "succeeded");
    assert.equal(
      (await host.sql("SELECT title FROM Task"))[0].title,
      "later-local",
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM axton_mutation"))[0].n,
      0,
    );
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      0,
    );
    assert.equal(
      (await f.pool.query("SELECT title FROM business_task WHERE id=$1", [X]))
        .rows[0].title,
      "accepted",
    );
  } finally {
    await f.close();
  }
});

test("Mutation with no Model target completes durably without advancing Store cursor", async () => {
  const f = await fixture();
  try {
    const app = f.backend();
    const host = await f.client(app, "no-target");
    await host.task({ kind: "connect" });
    const call = await host.task({
      kind: "submitAction",
      name: "Ping",
      version: 1,
      args: {},
    });
    const completion = await host.wait(
      (e) => e.type === "callCompleted" && e.callId === call.callId,
    );
    assert.equal(completion.outcome.status, "succeeded");
    assert.equal(
      (await host.task({ kind: "callCompletion", callId: call.callId })).outcome
        .status,
      "succeeded",
    );
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      0,
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM axton_v04_record"))[0].n,
      0,
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM axton_mutation"))[0].n,
      0,
    );
    assert.equal((await f.pool.query("SELECT n FROM executions")).rows[0].n, 1);
  } finally {
    await f.close();
  }
});

test("historical target57 behind C80 materializes during a separately held public Bootstrap", async () => {
  const f = await fixture();
  try {
    const app = f.backend();
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "canonical",
    ]);
    const fillers = Array.from({ length: 79 }, (_, i) => ({ id: `f${i}` }));
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task(fillers.slice(0, 56)),
    );
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task({ id: X }),
    );
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task(fillers.slice(56)),
    );
    await f.pool.query("INSERT INTO business_marker VALUES('root','marked')");
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.marker({ id: "root" }),
    );
    const host = await f.client(app);
    const publicPage = held(
      ({ route, body }) => route === "pull" && body.kind === "page",
      host,
    );
    await host.task({ kind: "connect" });
    const blocked = await publicPage;
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      81,
    );
    assert.equal(
      (
        await host.sql(
          "SELECT count(*) n FROM axton_v04_record WHERE model='Task'",
        )
      )[0].n,
      0,
    );
    await host.task({
      kind: "fetch",
      model: "Task",
      version: 1,
      identity: { id: X },
    });
    host.hold = async (data) =>
      data.effect.effectId === blocked.effect.effectId; // public response remains held; other manifest pages proceed.
    const call = await host.task({
      kind: "submitAction",
      name: "Edit",
      version: 1,
      args: { task: { id: X, title: "noop" } },
    });
    const completed = await host.wait(
      (e) => e.type === "callCompleted" && e.callId === call.callId,
    );
    assert.equal(completed.outcome.status, "succeeded");
    const evidence = JSON.parse(
      (
        await host.sql(
          "SELECT evidence FROM axton_v04_record WHERE model='Task'",
        )
      )[0].evidence,
    );
    assert.equal(evidence.history[host.context.materialization], 57);
    assert.equal(evidence.current.cursor, 57);
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      81,
    );
    assert.equal(
      (
        await host.sql(
          "SELECT coverage FROM axton_v04_bootstrap WHERE purpose='bootstrap'",
        )
      )[0].coverage.includes('"covered":0'),
      true,
    );
    assert.ok(host.requests.some((r) => r.body.kind === "materialize"));
    const position = (
      await f.pool.query(
        "SELECT l.cursor FROM axton_stream_log l JOIN axton_record r ON r.id=l.record_id WHERE r.identity_key=$1 AND l.stream='User:alice'",
        [JSON.stringify({ id: X })],
      )
    ).rows[0].cursor;
    assert.equal(Number(position), 57);
    host.reply(blocked.effect, {
      status: 200,
      body: JSON.stringify(blocked.response),
    });
    await host.until(
      "SELECT coverage FROM axton_v04_bootstrap WHERE purpose='bootstrap'",
      (rows) => JSON.parse(rows[0].coverage).covered === 1,
    );
    assert.equal((await f.pool.query("SELECT n FROM executions")).rows[0].n, 1);
  } finally {
    await f.close();
  }
});

test("SIGKILL after accepted HTTP response retries frozen old intent under added Model/current read shape", async () => {
  const f = await fixture();
  try {
    const old = configuration();
    const app = f.backend(old);
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,$3)", [
      X,
      "old",
      "new-field",
    ]);
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task({ id: X }),
    );
    const host = await f.client(app, "crash", old.schema);
    await cache(host);
    const response = held(
      ({ route, body }) => route === "action" && body.models,
      host,
    );
    const call = await host.task({
      kind: "submitAction",
      name: "Edit",
      version: 1,
      args: { task: { id: X, title: "accepted" } },
    });
    const accepted = await response;
    assert.equal(accepted.response.completion.outcome.status, "succeeded");
    const beforeIntent = accepted.body;
    assert.equal((await f.pool.query("SELECT n FROM executions")).rows[0].n, 1);
    await host.kill();
    const next = structuredClone(old);
    next.schema.models[0].version = 2;
    next.schema.models[0].bootstrap = true;
    next.schema.models[0].fields.push({
      name: "label",
      nullable: true,
      type: { kind: "scalar", name: "string" },
    });
    next.schema.models.push({ ...marker, name: "Added", bootstrap: false });
    next.schema.resultModels = [
      ...old.schema.resultModels,
      { ...next.schema.models[0], enums: [] },
      { ...next.schema.models.at(-1), enums: [] },
    ];
    next.models = next.schema.resultModels;
    next.loaders.push("Added");
    // Added is device-only in this fixture, so not loaded; only original read models register handlers.
    next.loaders = next.loaders.filter((name) => name !== "Added");
    const current = f.backend(next, {
      materializations: {
        [host.context.materialization]: {
          schema: old.schema,
          projectionGeneration: "1",
        },
      },
    });
    const reopened = await f.client(current, "crash", next.schema);
    assert.notEqual(
      reopened.context.materialization,
      host.context.materialization,
    );
    assert.equal(reopened.context.incarnation, host.context.incarnation);
    await reopened.task({ kind: "connect" });
    const completed = await reopened.wait(
      (e) => e.type === "callCompleted" && e.callId === call.callId,
    );
    assert.equal(completed.outcome.status, "succeeded");
    assert.deepEqual(
      reopened.requests.find((r) => r.route === "action" && r.body.models).body,
      beforeIntent,
    );
    assert.equal(
      (await f.pool.query("SELECT n FROM executions")).rows[0].n,
      1,
      "saved server receipt prevents repeated handler",
    );
    const row = (await reopened.sql("SELECT * FROM Task WHERE id=?", [X]))[0];
    assert.equal(row.title, "accepted");
    assert.equal(row.label, "new-field");
    const evidence = JSON.parse(
      (
        await reopened.sql(
          "SELECT evidence FROM axton_v04_record WHERE model='Task'",
        )
      )[0].evidence,
    );
    assert.ok(
      evidence.history[reopened.context.materialization] >=
        accepted.response.targets[0].cursor,
    );
    assert.equal(
      (await reopened.sql("SELECT count(*) n FROM axton_mutation"))[0].n,
      0,
    );
    const saved = await reopened.task({
      kind: "callCompletion",
      callId: call.callId,
    });
    assert.equal(saved.outcome.status, "succeeded");
  } finally {
    await f.close();
  }
});

test("actual Query modes/once and stale Fetch distinguish invocation snapshots from Stream source", async () => {
  const f = await fixture();
  try {
    const app = f.backend();
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "A",
    ]);
    const host = await f.client(app, "modes");
    host.frameHold = (body) => Array.isArray(body.units);
    const heldPulls = [];
    host.hold = async (data) => {
      if (data.route !== "pull") return false;
      heldPulls.push(data);
      return true;
    };
    await host.task({ kind: "connect" });
    await host.until(
      "SELECT cursor FROM axton_v04_store",
      () => heldPulls.length > 0,
    );
    assert.equal(heldPulls[0].response.start, 0);
    const snapshot = await host.task({
      kind: "invoke",
      name: "Find",
      version: 1,
      args: {},
      once: true,
      store: false,
    });
    assert.equal(snapshot.outcome.result.task.title, "A");
    assert.equal((await host.sql("SELECT count(*) n FROM Task"))[0].n, 0);
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      0,
    );
    assert.equal(
      Number(
        (
          await f.pool.query(
            "SELECT head FROM axton_stream WHERE stream='User:alice'",
          )
        ).rows[0].head,
      ),
      1,
      "false Query explicit track still publishes",
    );
    const firstAttempts = f.queryCalls();
    const again = await host.task({
      kind: "invoke",
      name: "Find",
      version: 1,
      args: {},
      once: true,
      store: false,
    });
    assert.deepEqual(again, snapshot);
    assert.equal(
      f.queryCalls(),
      firstAttempts,
      "once returns saved invocation without another handler attempt",
    );
    const stored = await host.task({
      kind: "invoke",
      name: "Find",
      version: 1,
      args: {},
      once: true,
      store: true,
    });
    assert.deepEqual(stored, snapshot);
    assert.ok(
      f.queryCalls() > firstAttempts,
      "mode true has its own once identity (serializable handlers may retry)",
    );
    assert.equal((await host.sql("SELECT title FROM Task"))[0].title, "A");
    assert.deepEqual(
      JSON.parse(
        (await host.sql("SELECT evidence FROM axton_v04_record"))[0].evidence,
      ).history,
      {},
    );
    host.hold = null;
    host.frameHold = null;
    for (const pull of heldPulls)
      host.reply(pull.effect, {
        status: 200,
        body: JSON.stringify(pull.response),
      });
    for (const frame of host.heldFrames.splice(0))
      host.reply(frame.effect, frame.value);
    await host.until(
      "SELECT cursor FROM axton_v04_store",
      (rows) => rows[0].cursor === 1,
    );
    const heldRead = held(({ route }) => route === "fetch", host);
    host.send({
      type: "task",
      requestId: "late-read",
      command: {
        kind: "fetch",
        model: "Task",
        version: 1,
        identity: { id: X },
      },
    });
    const stale = await heldRead;
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("DELETE FROM business_task WHERE id=$1", [X]);
      invalidate.task({ id: X });
    });
    // The real server's second page is delivered before its older ordinary read.
    await host.until("SELECT cursor FROM axton_v04_store", (rows) => {
      for (const frame of host.heldFrames.splice(0))
        host.reply(frame.effect, frame.value);
      return rows[0].cursor === 2;
    });
    host.reply(stale.effect, {
      status: 200,
      body: JSON.stringify(stale.response),
    });
    const read = await host.wait(
      (e) => e.type === "taskCompleted" && e.requestId === "late-read",
    );
    assert.equal(read.ok, true);
    assert.equal(read.value.outcome.result.title, "A");
    assert.equal((await host.sql("SELECT count(*) n FROM Task"))[0].n, 0);
    assert.equal(
      JSON.parse(
        (await host.sql("SELECT evidence FROM axton_v04_record"))[0].evidence,
      ).current.deleted,
      true,
    );
  } finally {
    await f.close();
  }
});

test("independent server transactions transfer UNIQUE values through actual manifest commit", async () => {
  const f = await fixture();
  try {
    const unique = { ...model, bootstrap: true, unique: [["title"]] };
    const cfg = configuration([unique, marker]);
    const app = f.backend(cfg);
    await f.pool.query(
      "INSERT INTO business_task VALUES('zz','X',null),('aa','Y',null)",
    );
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task([{ id: "zz" }, { id: "aa" }]),
    );
    const host = await f.client(app, "unique", cfg.schema);
    const starting = held(
      ({ route, body }) => route === "pull" && body.kind === "start",
      host,
    );
    host.frameHold = (body) => Array.isArray(body.units);
    await host.task({ kind: "connect" });
    const start = await starting;
    for (const id of ["zz", "aa"])
      await host.task({
        kind: "fetch",
        model: "Task",
        version: 1,
        identity: { id },
      });
    assert.ok(
      (
        await host.sql(
          "SELECT name FROM sqlite_master WHERE type='index' AND name='Task_title_unique'",
        )
      ).length === 1,
    );
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("UPDATE business_task SET title='Z' WHERE id='zz'");
      invalidate.task({ id: "zz" });
    });
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("UPDATE business_task SET title='X' WHERE id='aa'");
      invalidate.task({ id: "aa" });
    });
    let firstPage;
    let captureFirst;
    const firstPageReady = new Promise((resolve) => {
      captureFirst = resolve;
    });
    host.hold = async (answer) => {
      const { route, body } = answer;
      if (route !== "pull") return false;
      if (!body.kind) return true; // keep independent Delta from hiding manifest evidence
      if (body.kind !== "page") return false;
      if (body.limit > 1) {
        // Drop a real computed response to exercise the actual transport retry policy.
        host.send({
          type: "effectResult",
          effectId: answer.effect.effectId,
          outcome: {
            ok: false,
            error: { message: "injected-link-refusal", status: 503 },
          },
        });
        return true;
      }
      if (body.from === 0) {
        firstPage = answer;
        captureFirst();
        return true;
      }
      return false;
    };
    host.reply(start.effect, {
      status: 200,
      body: JSON.stringify(start.response),
    });
    await firstPageReady;
    assert.equal(firstPage.body.limit, 1);
    assert.equal(firstPage.response.items.length, 1);
    assert.equal(firstPage.response.items[0].ordinal, 0);
    assert.equal(firstPage.response.items[0].change.record.identity.id, "aa");
    assert.ok(
      firstPage.response.companions.some(
        (c) =>
          c.kind === "upsert" &&
          c.record.identity.id === "zz" &&
          c.record.state.title === "Z",
      ),
      "later release ordinal is required companion of first acquire page",
    );
    host.reply(firstPage.effect, {
      status: 200,
      body: JSON.stringify(firstPage.response),
    });
    await host.until(
      "SELECT coverage FROM axton_v04_bootstrap WHERE purpose='bootstrap'",
      (rows) => rows.length && JSON.parse(rows[0].coverage).covered === 2,
    );
    assert.deepEqual(await host.sql("SELECT id,title FROM Task ORDER BY id"), [
      { id: "aa", title: "X" },
      { id: "zz", title: "Z" },
    ]);
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      2,
      "manifest installs newer G without advancing initial boundary",
    );
    const evidence = await host.sql(
      "SELECT identity,evidence FROM axton_v04_record ORDER BY identity",
    );
    assert.deepEqual(
      evidence.map((row) => JSON.parse(row.evidence).current.cursor),
      [4, 3],
    );
  } finally {
    await f.close();
  }
});

test("moving identity after captured tail cannot replace the finite Bootstrap target", async () => {
  const f = await fixture();
  try {
    const cfg = configuration([{ ...model, bootstrap: true }, marker]);
    const app = f.backend(cfg);
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "initial",
    ]);
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task({ id: X }),
    );
    const host = await f.client(app, "moving", cfg.schema);
    host.frameHold = (body) => Array.isArray(body.units);
    const pagePromise = held(
      ({ route, body }) => route === "pull" && body.kind === "page",
      host,
    );
    await host.task({ kind: "connect" });
    const page = await pagePromise;
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("UPDATE business_task SET title=$2 WHERE id=$1", [
        X,
        "at-tail",
      ]);
      invalidate.task({ id: X });
    });
    const tailPromise = held(
      ({ route, body }) => route === "pull" && body.kind === "tail",
      host,
    );
    host.reply(page.effect, {
      status: 200,
      body: JSON.stringify(page.response),
    });
    const tail = await tailPromise;
    assert.equal(tail.response.head, 2);
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("UPDATE business_task SET title=$2 WHERE id=$1", [
        X,
        "after-tail",
      ]);
      invalidate.task({ id: X });
    });
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      1,
    );
    host.hold = null;
    host.reply(tail.effect, {
      status: 200,
      body: JSON.stringify(tail.response),
    });
    await host.until(
      "SELECT cursor FROM axton_v04_store",
      (rows) => rows[0].cursor >= 2,
    );
    const coverage = JSON.parse(
      (
        await host.sql(
          "SELECT coverage FROM axton_v04_bootstrap WHERE purpose='bootstrap'",
        )
      )[0].coverage,
    );
    assert.equal(coverage.tail, 2, "later head3 never replaces fixed H2");
    assert.equal(coverage.covered, 1);
    assert.equal(
      (await host.sql("SELECT title FROM Task"))[0].title,
      "after-tail",
    );
    assert.equal(
      JSON.parse(
        (await host.sql("SELECT evidence FROM axton_v04_record"))[0].evidence,
      ).current.cursor,
      3,
    );
  } finally {
    await f.close();
  }
});

test("actual SQLite deferred commit refusal preserves accepted-awaiting through SIGKILL and later direct content", async () => {
  const f = await fixture();
  try {
    const cfg = configuration();
    const app = f.backend(cfg);
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "old",
    ]);
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task({ id: X }),
    );
    let host = await f.client(app, "commit-fault", cfg.schema);
    // Install the fault only after the native owner has released SQLite.
    await host.close();
    const db = new DatabaseSync(join(f.directory, "commit-fault.sqlite"));
    try {
      db.exec(
        "CREATE TABLE fault_parent(id INTEGER PRIMARY KEY);CREATE TABLE fault_marker(parent INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER fail_settlement_commit BEFORE DELETE ON axton_mutation BEGIN INSERT INTO fault_marker VALUES(99);END",
      );
    } finally {
      db.close();
    }
    host = await f.client(app, "commit-fault", cfg.schema);
    await cache(host);
    const reply = held(
      ({ route, body }) => route === "action" && body.models,
      host,
    );
    const call = await host.task({
      kind: "submitAction",
      name: "Edit",
      version: 1,
      args: { task: { id: X, title: "accepted" } },
    });
    const answer = await reply;
    await host.until(
      "SELECT evidence FROM axton_v04_record",
      (rows) =>
        JSON.parse(rows[0].evidence).history[host.context.materialization] >=
        answer.response.targets[0].cursor,
    );
    await host.task({
      kind: "direct",
      operation: {
        model: "Task",
        identity: { id: X },
        op: "update",
        values: { title: "later-direct" },
      },
    });
    host.reply(answer.effect, {
      status: 200,
      body: JSON.stringify(answer.response),
    });
    await host.until(
      "SELECT status FROM axton_v04_call",
      (rows) => rows[0].status === "acceptedAwaiting",
    );
    await host.wait(
      (event) =>
        event.type === "report" &&
        JSON.stringify(event).includes("FOREIGN KEY"),
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM axton_mutation"))[0].n,
      1,
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM axton_v04_completion"))[0].n,
      0,
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM fault_marker"))[0].n,
      0,
      "deferred commit failure rolls back trigger insert and settlement",
    );
    assert.equal(
      (await host.sql("SELECT title FROM Task"))[0].title,
      "later-direct",
    );
    await host.kill();
    const repair = new DatabaseSync(join(f.directory, "commit-fault.sqlite"));
    try {
      repair.exec("DROP TRIGGER fail_settlement_commit");
    } finally {
      repair.close();
    }
    const reopened = await f.client(app, "commit-fault", cfg.schema);
    await reopened.task({ kind: "connect" });
    const completion = await reopened.wait(
      (e) => e.type === "callCompleted" && e.callId === call.callId,
    );
    assert.equal(completion.outcome.status, "succeeded");
    assert.equal(
      (await reopened.sql("SELECT title FROM Task"))[0].title,
      "later-direct",
    );
    assert.equal(
      (await reopened.sql("SELECT count(*) n FROM axton_mutation"))[0].n,
      0,
    );
    assert.equal((await f.pool.query("SELECT n FROM executions")).rows[0].n, 1);
  } finally {
    await f.close();
  }
});

test("reset fences an old ordinary response while another same-Stream file remains unchanged", async () => {
  const f = await fixture();
  try {
    const app = f.backend();
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "cached",
    ]);
    const first = await f.client(app, "first");
    const second = await f.client(app, "second");
    await cache(second);
    await first.task({ kind: "connect" });
    await second.task({
      kind: "direct",
      operation: {
        model: "Task",
        identity: { id: X },
        op: "update",
        values: { title: "second-only" },
      },
    });
    const pending = held(({ route }) => route === "fetch", first);
    first.send({
      type: "task",
      requestId: "old-read",
      command: {
        kind: "fetch",
        model: "Task",
        version: 1,
        identity: { id: X },
      },
    });
    const stale = await pending;
    const old = first.context;
    const reset = await first.task({ kind: "resetStore" });
    assert.equal(reset.context.materialization, old.materialization);
    assert.notEqual(reset.context.incarnation, old.incarnation);
    assert.deepEqual(reset.context.binding, old.binding);
    first.context = reset.context;
    first.reply(stale.effect, {
      status: 200,
      body: JSON.stringify(stale.response),
    });
    const oldRead = await first.wait(
      (e) => e.type === "taskCompleted" && e.requestId === "old-read",
    );
    assert.equal(oldRead.ok, false);
    assert.equal((await first.sql("SELECT count(*) n FROM Task"))[0].n, 0);
    assert.equal(
      (await first.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      0,
    );
    assert.equal(
      (await second.sql("SELECT title FROM Task"))[0].title,
      "second-only",
    );
    assert.equal(second.context.incarnation === old.incarnation, false);
    first.hold = null;
    await first.task({
      kind: "fetch",
      model: "Task",
      version: 1,
      identity: { id: X },
    });
    assert.equal(
      (await first.sql("SELECT title FROM Task"))[0].title,
      "cached",
    );
    assert.equal(
      (await second.sql("SELECT title FROM Task"))[0].title,
      "second-only",
    );
  } finally {
    await f.close();
  }
});

test("real authenticated viewers keep independent Store projections and reject confused receipt context", async () => {
  const f = await fixture();
  try {
    const app = f.backend(configuration(), {}, true);
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "canonical",
    ]);
    await app.transaction(async ({ stream }) => {
      stream("User:alice").track.task({ id: X });
      stream("User:bob").track.task({ id: X });
    });
    const alice = await f.client(app, "viewer-alice");
    const bob = await f.client(app, "viewer-bob", configuration().schema, {
      ...f.binding,
      viewer: "bob",
      stream: "User:bob",
    });
    await cache(alice);
    await cache(bob);
    assert.equal(
      (await alice.sql("SELECT title FROM Task"))[0].title,
      "alice:canonical",
    );
    assert.equal(
      (await bob.sql("SELECT title FROM Task"))[0].title,
      "bob:canonical",
    );
    assert.notDeepEqual(alice.context.binding, bob.context.binding);
    const before = await f.pool.query(
      "SELECT count(*) n FROM axton_call WHERE call_id='confused-viewer'",
    );
    const response = await fetch(f.listeners[0].url + "/sync/fetch", {
      method: "POST",
      headers: { "x-viewer": "bob" },
      body: JSON.stringify({
        context: alice.context,
        callId: "confused-viewer",
        model: "Task",
        version: 1,
        identity: { id: X },
        store: true,
      }),
    });
    assert.equal(response.ok, false, await response.text());
    assert.equal(
      (
        await f.pool.query(
          "SELECT count(*) n FROM axton_call WHERE call_id='confused-viewer'",
        )
      ).rows[0].n,
      before.rows[0].n,
      "refused context never executes or saves a Call",
    );
    assert.equal(
      (await alice.sql("SELECT title FROM Task"))[0].title,
      "alice:canonical",
    );
    assert.equal(
      (await bob.sql("SELECT title FROM Task"))[0].title,
      "bob:canonical",
    );
  } finally {
    await f.close();
  }
});

test("actual Loader refusal shrinks finite manifest requests to commit unrelated coverage without skipping", async () => {
  const f = await fixture();
  try {
    const zebra = { ...model, name: "Zebra", bootstrap: true };
    const cfg = configuration([{ ...model, bootstrap: true }, marker, zebra]);
    let broken = true;
    const app = f.backend(cfg, {}, false, {
      zebra: async ({ ids }) => {
        if (broken) throw new Error("persistent-zebra-loader-fault");
        return ids.map(({ id }) => ({ id, title: "repaired" }));
      },
    });
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "good",
    ]);
    await app.transaction(async ({ stream }) => {
      stream("User:alice").track.task({ id: X });
      stream("User:alice").track.zebra({ id: "bad" });
    });
    const host = await f.client(app, "adaptive", cfg.schema);
    await host.task({ kind: "connect" });
    await host.until("SELECT coverage FROM axton_v04_bootstrap", (rows) =>
      rows.some((r) => JSON.parse(r.coverage).covered === 1),
    );
    assert.equal((await host.sql("SELECT title FROM Task"))[0].title, "good");
    assert.equal((await host.sql("SELECT count(*) n FROM Zebra"))[0].n, 0);
    const pages = host.requests
      .filter((r) => r.body.kind === "page")
      .map((r) => r.body);
    assert.ok(pages.some((p) => p.limit === 1));
    assert.ok(pages.some((p) => p.limit > 1));
    assert.ok(
      pages.every((p) => p.from <= 1),
      "no request skips the refused ordinal",
    );
    assert.equal(
      (await host.sql("SELECT cursor FROM axton_v04_store"))[0].cursor,
      2,
      "only initial Start boundary, no invented Delta progress",
    );
    broken = false;
    await host.until("SELECT coverage FROM axton_v04_bootstrap", (rows) =>
      rows.some((r) => JSON.parse(r.coverage).covered === 2),
    );
    assert.equal(
      (await host.sql("SELECT title FROM Zebra"))[0].title,
      "repaired",
    );
  } finally {
    await f.close();
  }
});

test("a suspended real Query handler does not block native sync or committed local transactions", async () => {
  const f = await fixture();
  let release;
  try {
    const app = f.backend();
    await f.pool.query("INSERT INTO business_task VALUES($1,$2,null)", [
      X,
      "before",
    ]);
    await app.transaction(async ({ stream }) =>
      stream("User:alice").track.task({ id: X }),
    );
    const host = await f.client(app, "slow-query");
    await cache(host);
    let entered;
    const entering = new Promise((resolve) => {
      entered = resolve;
    });
    const paused = new Promise((resolve) => {
      release = resolve;
    });
    f.setQueryGate(async () => {
      entered();
      await paused;
    });
    host.send({
      type: "task",
      requestId: "slow-query",
      command: {
        kind: "invoke",
        name: "Find",
        version: 1,
        args: {},
        store: false,
      },
    });
    await entering;
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query(
        "UPDATE business_task SET title='during-query' WHERE id=$1",
        [X],
      );
      invalidate.task({ id: X });
    });
    await host.until(
      "SELECT title FROM Task",
      (rows) => rows[0]?.title === "during-query",
    );
    await host.task({
      kind: "direct",
      operation: {
        model: "Task",
        identity: { id: X },
        op: "update",
        values: { title: "local-during-query" },
      },
    });
    assert.equal(
      (await host.sql("SELECT title FROM Task"))[0].title,
      "local-during-query",
    );
    assert.ok(
      !host.events.some(
        (e) => e.type === "taskCompleted" && e.requestId === "slow-query",
      ),
    );
    release();
    const result = await host.wait(
      (e) => e.type === "taskCompleted" && e.requestId === "slow-query",
    );
    assert.equal(result.ok, true);
    assert.equal(result.value.outcome.result.task.title, "during-query");
    assert.equal(
      (await host.sql("SELECT title FROM Task"))[0].title,
      "local-during-query",
    );
  } finally {
    release?.();
    await f.close();
  }
});

test("declared cascade follows real parent Stream tombstone and blocks a delayed ordinary child Fetch", async () => {
  const f = await fixture();
  try {
    const child = {
      ...model,
      fields: [
        ...fields,
        {
          name: "parentId",
          nullable: false,
          type: { kind: "scalar", name: "string" },
        },
      ],
      relations: [
        {
          name: "parent",
          target: "Marker",
          fields: ["parentId"],
          targetFields: ["id"],
          onDelete: "delete",
        },
      ],
    };
    const cfg = configuration([child, marker]);
    const app = f.backend(cfg);
    await f.pool.query(
      "INSERT INTO business_marker VALUES('parent','alive');INSERT INTO business_task VALUES('child','cached-child','parent')",
    );
    await app.transaction(async ({ stream }) => {
      stream("User:alice").track.marker({ id: "parent" });
      stream("User:alice").track.task({ id: "child" });
    });
    const host = await f.client(app, "cascade", cfg.schema);
    await host.task({ kind: "connect" });
    await host.until("SELECT coverage FROM axton_v04_bootstrap", (rows) =>
      rows.some((r) => JSON.parse(r.coverage).covered === 1),
    );
    await host.task({
      kind: "fetch",
      model: "Task",
      version: 1,
      identity: { id: "child" },
    });
    assert.equal(
      (await host.sql("SELECT title FROM Task"))[0].title,
      "cached-child",
    );
    const delayed = held(
      ({ route, body }) => route === "fetch" && body.model === "Task",
      host,
    );
    host.send({
      type: "task",
      requestId: "delayed-child",
      command: {
        kind: "fetch",
        model: "Task",
        version: 1,
        identity: { id: "child" },
      },
    });
    const answer = await delayed;
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("DELETE FROM business_marker WHERE id='parent'");
      invalidate.marker({ id: "parent" });
    });
    await host.until(
      "SELECT cursor FROM axton_v04_store",
      (rows) => rows[0].cursor === 3,
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM Task"))[0].n,
      0,
      "declared device cascade removes cache child",
    );
    host.reply(answer.effect, {
      status: 200,
      body: JSON.stringify(answer.response),
    });
    const returned = await host.wait(
      (e) => e.type === "taskCompleted" && e.requestId === "delayed-child",
    );
    assert.equal(returned.ok, true);
    assert.equal(
      returned.value.outcome.result.title,
      "cached-child",
      "invocation snapshot remains available",
    );
    assert.equal(
      (await host.sql("SELECT count(*) n FROM Task"))[0].n,
      0,
      "current parent Stream-null suppresses cache revival",
    );
    const evidence = await host.sql(
      "SELECT model,evidence FROM axton_v04_record ORDER BY model",
    );
    assert.equal(
      JSON.parse(evidence.find((r) => r.model === "Marker").evidence).current
        .deleted,
      true,
    );
    assert.deepEqual(
      JSON.parse(evidence.find((r) => r.model === "Task").evidence).history,
      {},
      "parent cascade invents no child Stream position",
    );
    assert.equal(
      (
        await f.pool.query(
          "SELECT count(*) n FROM business_task WHERE id='child'",
        )
      ).rows[0].n,
      "1",
      "device cascade is not a backend FK deletion policy",
    );
  } finally {
    await f.close();
  }
});
