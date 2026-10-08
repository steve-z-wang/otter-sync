// Real PostgreSQL transactions, native Rust executor and server carrier.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { Pool } from "pg";
import { spawnSync } from "node:child_process";
import {
  createBackend,
  MutationRejected,
} from "../../../packages/server/index.mts";
import { pg } from "../../../packages/postgres/index.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const database = pg(pool, { retries: 10 });
const q = async (sql, args = []) => (await pool.query(sql, args)).rows;
const canonical = (v) =>
  Array.isArray(v)
    ? "[" + v.map(canonical).join(",") + "]"
    : v && typeof v === "object"
      ? "{" +
        Object.keys(v)
          .sort()
          .map((k) => JSON.stringify(k) + ":" + canonical(v[k]))
          .join(",") +
        "}"
      : JSON.stringify(v);
const digest = (r) =>
  createHash("sha256")
    .update("axton:mutation-batch:5\0")
    .update(
      canonical(
        Object.fromEntries(Object.entries(r).filter(([k]) => k !== "digest")),
      ),
    )
    .digest("hex");
const model = {
  name: "Todo",
  version: 1,
  identity: ["id"],
  fields: ["id", "title"].map((name) => ({
    name,
    nullable: false,
    type: { kind: "scalar", name: "string" },
  })),
};
const action = {
  name: "Write",
  version: 1,
  kind: "mutation",
  inputs: [
    {
      kind: "model",
      name: "todo",
      model: "Todo",
      operation: "create",
      cardinality: "single",
    },
    {
      kind: "value",
      name: "mode",
      type: { kind: "scalar", name: "string" },
      nullable: false,
      list: false,
    },
  ],
  outputs: [],
};
const config = {
  schema: { enums: [], models: [model], actions: [action] },
  mutations: [],
  loaders: ["Todo"],
};
let serial = 0;
const store = () => `batch-store-${++serial}`;
const materialization = native.serverMaterializationId05(
  JSON.stringify(config),
  "1",
);
function batch(s, modes = ["ok"], batchId = 1) {
  const r = {
    protocol: 5,
    storeId: s,
    stream: "User:alice",
    materialization,
    batchId,
    digest: "",
    mutations: modes.map((mode, i) => ({
      id: i + 1,
      name: "Write",
      version: 1,
      descriptor: createHash("sha256")
        .update("axton:mutation-descriptor:5\0")
        .update(
          canonical({
            ...action,
            input: null,
            outputEnums: [],
            inputs: action.inputs.map((i) =>
              i.kind === "model" ? { ...i, allowedPatchFields: null } : i,
            ),
          }),
        )
        .digest("hex"),
      operations: [
        {
          step: 1,
          inputPath: "todo",
          operation: "create",
          model: "Todo",
          identity: { id: `${s}-${batchId}-${i}` },
          value: { title: mode },
        },
        {
          step: 2,
          inputPath: "mode",
          operation: "argument",
          model: null,
          identity: null,
          value: mode,
        },
      ],
    })),
  };
  r.digest = digest(r);
  return r;
}
let calls = 0,
  loads = 0,
  prepares = 0;
let fault = "";
let injectedError;
function app(cfg = config, materializations = {}) {
  const options = {
    config: cfg,
    native,
    database,
    protocol5: {
      materializations,
      authorizeStream: (principal, stream) => stream === `User:${principal}`,
    },
    authenticate: () => "alice",
    onError: () => {},
    mutations: {
      write: async ({ ctx, args }) => {
        calls++;
        await ctx.tx.query("INSERT INTO v05_business VALUES($1,$2)", [
          args.todo.id,
          args.todo.title,
        ]);
        if (args.mode !== "private" && args.mode !== "other")
          ctx.stream.track.todo({ id: args.todo.id });
        if (args.mode !== "private")
          ctx.stream("User:other").track.todo({ id: args.todo.id });
        if (args.mode === "multi") {
          const extra = args.todo.id + "-extra";
          await ctx.tx.query("INSERT INTO v05_business VALUES($1,$2)", [
            extra,
            "multi",
          ]);
          ctx.streams(["User:alice", "User:other"]).track.todo({ id: extra });
        }
        if (args.mode === "refuse") throw new MutationRejected("write.no");
        if (args.mode === "transient" && fault === "handler") {
          fault = "";
          throw injectedError ?? new Error("temporary handler crash");
        }
        return {};
      },
    },
    loaders: {
      todo: async ({ tx, ids }) => {
        loads++;
        if (fault === "loader") {
          fault = "";
          throw injectedError ?? new Error("temporary loader crash");
        }
        if (fault === "loader-refusal") {
          fault = "";
          throw new MutationRejected("write.read_no");
        }
        return Promise.all(
          ids.map(
            async ({ id }) =>
              (
                await tx.query(
                  "SELECT id,title FROM v05_business WHERE id=$1",
                  [id],
                )
              ).rows[0] ?? null,
          ),
        );
      },
    },
    loaderHooks: {
      todo: {
        prepareForViewer: async ({ tx, ids, streams, invalidate }) => {
          prepares++;
          for (const { id } of ids) {
            const row = (
              await tx.query("SELECT title FROM v05_business WHERE id=$1", [id])
            ).rows[0];
            if (row?.title === "private") continue;
            if (row?.title !== "other")
              streams("User:alice").track.todo({ id });
            invalidate.todo({ id });
            streams("User:other").track.todo({ id });
          }
        },
      },
    },
  };
  if (cfg.models?.some((m) => m.version === 2)) {
    const loader = options.loaders.todo;
    options.loaders.todo = { v1: loader, v2: loader };
  }
  return createBackend(options);
}
before(async () => {
  await q(
    await readFile(
      new URL("../../../packages/postgres/migration.sql", import.meta.url),
      "utf8",
    ),
  );
  await q("CREATE TABLE v05_business(id text PRIMARY KEY,title text NOT NULL)");
});
after(() => pool.end());
async function state(s) {
  return (await q("SELECT * FROM axton_store WHERE id=$1", [s]))[0];
}
async function streamHeads() {
  return new Map(
    (await q("SELECT stream,head FROM axton_stream")).map((row) => [
      row.stream,
      Number(row.head),
    ]),
  );
}
async function assertPrefixPositions(s, prior, count, streams) {
  const heads = await streamHeads();
  const positions = await q(
    "SELECT s.stream,s.cursor,r.identity FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id' LIKE $1 ORDER BY s.stream,s.cursor",
    [s + "-%"],
  );
  for (const stream of streams) {
    assert.equal(
      heads.get(stream),
      (prior.get(stream) ?? 0) + count,
      `${stream}: only committed prefix consumes head`,
    );
    assert.deepEqual(
      positions
        .filter((row) => row.stream === stream)
        .map((row) => [Number(row.cursor), row.identity.id]),
      Array.from({ length: count }, (_, i) => [
        (prior.get(stream) ?? 0) + i + 1,
        `${s}-1-${i}`,
      ]),
    );
  }
}
async function results(s) {
  return q(
    "SELECT * FROM axton_mutation_result WHERE store_id=$1 ORDER BY ordinal",
    [s],
  );
}
test("Batch commits each Mutation separately and crash after k resumes durable progress", async () => {
  const s = store(),
    r = batch(s, ["ok", "transient", "ok"]);
  const prior = await streamHeads();
  fault = "handler";
  let a = app();
  await assert.rejects(
    a.push("alice", JSON.stringify(r)),
    /temporary handler crash/,
  );
  assert.equal((await state(s)).progress, "1");
  assert.equal((await state(s)).last_processed_batch_id, "0");
  await assertPrefixPositions(s, prior, 1, ["User:alice", "User:other"]);
  assert.equal((await results(s)).length, 1);
  const changed = structuredClone(r);
  changed.mutations.pop();
  changed.digest = digest(changed);
  const beforeReplay = calls;
  await assert.rejects(
    a.push("alice", JSON.stringify(changed)),
    /batch.conflict/,
  );
  assert.equal(calls, beforeReplay);
  assert.equal((await state(s)).progress, "1");
  assert.equal(
    (await q("SELECT * FROM v05_business WHERE id LIKE $1", [s + "-%"])).length,
    1,
  );
  a = app();
  const ack = JSON.parse(await a.push("alice", JSON.stringify(r)));
  assert.equal(ack.results.length, 3);
  assert.equal((await state(s)).progress, "0");
  assert.equal((await state(s)).last_processed_batch_id, "1");
  assert.equal((await results(s)).length, 3);
});
test("explicit middle refusal rolls back only its business effects and continues", async () => {
  const s = store(),
    r = batch(s, ["ok", "refuse", "ok"]);
  const ack = JSON.parse(await app().push("alice", JSON.stringify(r)));
  assert.deepEqual(
    ack.results.map((x) => x.outcome.kind),
    ["accepted", "rejected", "accepted"],
  );
  assert.equal(
    (await q("SELECT * FROM v05_business WHERE id LIKE $1", [s + "-%"])).length,
    2,
  );
  assert.equal((await results(s)).length, 3);
  assert.equal(ack.results[1].outcome.code, "write.no");
});
test("lost ack and concurrent duplicate execute each logical member once", async () => {
  const s = store(),
    r = batch(s, ["ok", "ok"]);
  const a = app(),
    before = calls;
  const [x, y] = await Promise.all([
    a.push("alice", JSON.stringify(r)),
    a.push("alice", JSON.stringify(r)),
  ]);
  assert.equal(x, y);
  assert.equal(calls - before, 2);
  const savedLoads = loads,
    savedPrepares = prepares;
  assert.equal(await app().push("alice", JSON.stringify(r)), x);
  assert.equal(loads, savedLoads);
  assert.equal(prepares, savedPrepares);
});
test("changed body, skipped/stale ID and principal/Stream mismatch execute no handlers or prune", async () => {
  const s = store(),
    r = batch(s);
  const a = app();
  await a.push("alice", JSON.stringify(r));
  const before = calls;
  const changed = structuredClone(r);
  changed.mutations[0].operations[1].value = "changed";
  changed.digest = digest(changed);
  await assert.rejects(
    a.push("alice", JSON.stringify(changed)),
    /batch.conflict/,
  );
  await assert.rejects(
    a.push("alice", JSON.stringify(batch(s, ["ok"], 3))),
    /batch.sequence/,
  );
  await assert.rejects(
    a.push("bob", JSON.stringify(r)),
    /stream.forbidden|store.binding/,
  );
  const foreign = structuredClone(r);
  foreign.stream = "User:bob";
  foreign.digest = digest(foreign);
  await assert.rejects(a.push("bob", JSON.stringify(foreign)), /store.binding/);
  assert.equal((await results(s)).length, 1);
  assert.equal(calls, before);
  await a.push("alice", JSON.stringify(batch(s, ["ok"], 2)));
  assert.equal((await results(s)).length, 1);
  await assert.rejects(a.push("alice", JSON.stringify(r)), /batch.sequence/);
});
test("Loader infrastructure failure aborts acceptance with unchanged progress and retries", async () => {
  const s = store(),
    r = batch(s);
  fault = "loader";
  await assert.rejects(
    app().push("alice", JSON.stringify(r)),
    /temporary loader crash/,
  );
  assert.equal(await state(s), undefined);
  assert.equal((await results(s)).length, 0);
  assert.equal(
    (await q("SELECT * FROM v05_business WHERE id LIKE $1", [s + "-%"])).length,
    0,
  );
  assert.equal(
    JSON.parse(await app().push("alice", JSON.stringify(r))).results[0].outcome
      .kind,
    "accepted",
  );
});
test("one cursor per Mutation per Stream includes repeated preparation publications", async () => {
  const s = store(),
    r = batch(s, ["ok", "ok"]);
  const prior = new Map(
    (await q("SELECT stream,head FROM axton_stream")).map((x) => [
      x.stream,
      Number(x.head),
    ]),
  );
  const ack = JSON.parse(await app().push("alice", JSON.stringify(r)));
  const pairs = await q(
    "SELECT s.stream,s.cursor,r.identity FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id' LIKE $1 ORDER BY s.stream,s.cursor",
    [s + "-%"],
  );
  for (const stream of ["User:alice", "User:other"]) {
    assert.equal(
      Number(
        (await q("SELECT head FROM axton_stream WHERE stream=$1", [stream]))[0]
          .head,
      ),
      prior.get(stream) + 2,
    );
    assert.deepEqual(
      pairs.filter((p) => p.stream === stream).map((p) => Number(p.cursor)),
      [prior.get(stream) + 1, prior.get(stream) + 2],
    );
  }
  for (const result of ack.results) {
    assert.equal(result.outcome.targets[0].cursor, result.outcome.syncCursor);
  }
});
test("typed default Mutation Stream and explicit multi-Stream handles publish positive positions", async () => {
  const s = store(),
    r = batch(s, ["multi"]);
  const prior = new Map(
    (await q("SELECT stream,head FROM axton_stream")).map((x) => [
      x.stream,
      Number(x.head),
    ]),
  );
  const ack = JSON.parse(await app().push("alice", JSON.stringify(r)));
  assert.equal(ack.results[0].outcome.kind, "accepted");
  const pairs = await q(
    "SELECT s.stream,s.cursor FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id' LIKE $1",
    [s + "-%"],
  );
  for (const stream of ["User:alice", "User:other"]) {
    const cursors = pairs
      .filter((p) => p.stream === stream)
      .map((p) => Number(p.cursor));
    assert.deepEqual(cursors, [
      (prior.get(stream) ?? 0) + 1,
      (prior.get(stream) ?? 0) + 1,
    ]);
    assert.ok(cursors.every((c) => c > 0));
  }
  assert.equal(
    ack.results[0].outcome.syncCursor,
    (prior.get("User:alice") ?? 0) + 1,
  );
});
test("multiple records share one Mutation cursor in each affected Stream", async () => {
  const s = store(),
    r = batch(s, ["multi"]);
  const prior = new Map(
    (await q("SELECT stream,head FROM axton_stream")).map((x) => [
      x.stream,
      Number(x.head),
    ]),
  );
  await app().push("alice", JSON.stringify(r));
  const pairs = await q(
    "SELECT s.stream,s.cursor FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id' LIKE $1",
    [s + "-%"],
  );
  for (const stream of ["User:alice", "User:other"]) {
    const values = pairs
      .filter((p) => p.stream === stream)
      .map((p) => Number(p.cursor));
    assert.deepEqual(values, [prior.get(stream) + 1, prior.get(stream) + 1]);
  }
});
test("private and other-Stream-only targets retain owned null-cursor fallback without implicit track", async () => {
  for (const mode of ["private", "other"]) {
    const s = store(),
      r = batch(s, [mode]),
      prior = Number(
        (await q("SELECT head FROM axton_stream WHERE stream='User:alice'"))[0]
          .head,
      );
    const ack = JSON.parse(await app().push("alice", JSON.stringify(r)));
    const outcome = ack.results[0].outcome;
    assert.equal(outcome.syncCursor, prior);
    assert.equal(outcome.targets[0].kind, "private");
    assert.equal(outcome.targets[0].record.cursor, null);
    assert.equal(outcome.targets[0].record.state.title, mode);
    assert.equal(
      (
        await q(
          "SELECT * FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE s.stream='User:alice' AND r.identity->>'id'=$1",
          [`${s}-1-0`],
        )
      ).length,
      0,
    );
  }
});
test("real publication fence serializes tracking against global invalidation and preserves holders", async () => {
  const a = app();
  await q("INSERT INTO v05_business VALUES('fence-record','before')");
  await a.transaction(async ({ stream }) =>
    stream("User:alice").track.todo({ id: "fence-record" }),
  );
  let entered, release;
  const waiting = new Promise((resolve) => (entered = resolve)),
    gate = new Promise((resolve) => (release = resolve));
  const tracking = a.transaction(async ({ tx, stream }) => {
    await tx.query("SELECT title FROM v05_business WHERE id='fence-record'");
    entered();
    await gate;
    stream("User:new-holder").track.todo({ id: "fence-record" });
  });
  await waiting;
  const invalidate = a.transaction(async ({ tx, invalidate }) => {
    await tx.query(
      "UPDATE v05_business SET title='after' WHERE id='fence-record'",
    );
    invalidate.todo({ id: "fence-record" });
  });
  release();
  await Promise.all([tracking, invalidate]);
  const rows = await q(
    "SELECT s.stream,s.cursor FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id'='fence-record' ORDER BY s.stream",
  );
  assert.deepEqual(
    rows.map((r) => r.stream),
    ["User:alice", "User:new-holder"],
  );
  for (const row of rows) {
    const head = Number(
      (
        await q("SELECT head FROM axton_stream WHERE stream=$1", [row.stream])
      )[0].head,
    );
    assert.equal(Number(row.cursor), head);
  }
  assert.equal(
    (
      await q(
        "SELECT count(*) n FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='axton_record' AND column_name='stamp'",
      )
    )[0].n,
    "0",
  );
});
test("no-op with no Model targets accepts at head zero and replays without business execution", async () => {
  const emptyAction = {
    name: "Noop",
    version: 1,
    kind: "mutation",
    inputs: [],
    outputs: [],
  };
  const cfg = {
    ...config,
    schema: { ...config.schema, actions: [emptyAction] },
  };
  let executions = 0;
  const a = createBackend({
    config: cfg,
    native,
    database,
    protocol5: {
      authorizeStream: (principal, stream, tx) => {
        assert.ok(tx);
        return stream === `User:${principal}`;
      },
    },
    authenticate: () => "fresh",
    loaders: { todo: async () => [] },
    mutations: {
      noop: async () => {
        executions++;
        return {};
      },
    },
  });
  const r = {
    protocol: 5,
    storeId: store(),
    stream: "User:fresh-empty",
    materialization: native.serverMaterializationId05(JSON.stringify(cfg), "1"),
    batchId: 1,
    digest: "",
    mutations: [
      {
        id: 1,
        name: "Noop",
        version: 1,
        descriptor: createHash("sha256")
          .update("axton:mutation-descriptor:5\0")
          .update(canonical({ ...emptyAction, input: null, outputEnums: [] }))
          .digest("hex"),
        operations: [],
      },
    ],
  };
  r.digest = digest(r);
  const wire = await a.push("fresh-empty", JSON.stringify(r));
  assert.deepEqual(JSON.parse(wire).results[0].outcome, {
    kind: "accepted",
    syncCursor: 0,
    result: null,
    targets: [],
  });
  assert.equal(await a.push("fresh-empty", JSON.stringify(r)), wire);
  assert.equal(executions, 1);
});
test("process exit after committed member leaves durable prefix and rolls back disconnected member", async () => {
  const s = store(),
    r = batch(s, ["ok", "exit", "ok"]);
  const prior = await streamHeads();
  const script = `import {createRequire} from 'node:module';import {Pool} from 'pg';import {createBackend} from ${JSON.stringify(new URL("../../../packages/server/index.mts", import.meta.url).href)};import {pg} from ${JSON.stringify(new URL("../../../packages/postgres/index.mts", import.meta.url).href)};const cfg=JSON.parse(process.env.TASK3_CONFIG),r=JSON.parse(process.env.TASK3_REQUEST);const pool=new Pool({connectionString:process.env.DATABASE_URL});const native=createRequire(${JSON.stringify(import.meta.url)})('../../../bindings/node/axton-node.node');const a=createBackend({config:cfg,native,database:pg(pool),protocol5:{authorizeStream:()=>true},authenticate:()=> 'alice',loaders:{todo:async({tx,ids})=>Promise.all(ids.map(async({id})=>(await tx.query('SELECT id,title FROM v05_business WHERE id=$1',[id])).rows[0]??null))},mutations:{write:async({ctx,args})=>{await ctx.tx.query('INSERT INTO v05_business VALUES($1,$2)',[args.todo.id,args.todo.title]);if(args.mode==='exit')process.exit(77);ctx.stream('User:alice').track.todo({id:args.todo.id});return {};}}});await a.push('alice',JSON.stringify(r));`;
  const child = spawnSync(
    process.execPath,
    ["--input-type=module", "-e", script],
    {
      cwd: new URL("../../..", import.meta.url),
      env: {
        ...process.env,
        TASK3_CONFIG: JSON.stringify(config),
        TASK3_REQUEST: JSON.stringify(r),
      },
      encoding: "utf8",
      timeout: 10000,
    },
  );
  assert.equal(child.status, 77, child.stderr);
  await assertPrefixPositions(s, prior, 1, ["User:alice"]);
  assert.equal(
    (await streamHeads()).get("User:other"),
    prior.get("User:other"),
    "disconnected member consumes no other Stream position",
  );
  assert.equal((await state(s)).progress, "1");
  assert.equal((await results(s)).length, 1);
  const rows = await q("SELECT id FROM v05_business WHERE id LIKE $1", [
    s + "-%",
  ]);
  assert.deepEqual(
    rows.map((x) => x.id),
    [`${s}-1-0`],
  );
  const ack = JSON.parse(await app().push("alice", JSON.stringify(r)));
  assert.equal(ack.results.length, 3);
  assert.equal((await state(s)).last_processed_batch_id, "1");
  assert.equal(
    (await q("SELECT id FROM v05_business WHERE id LIKE $1", [s + "-%"]))
      .length,
    3,
  );
});
test("reused caller-owned PoolClient starts a fresh publication reservation after COMMIT", async () => {
  const a = app(),
    tx = await pool.connect();
  const id = "reused-transaction";
  await q("INSERT INTO v05_business VALUES($1,$2)", [id, "before"]);
  try {
    await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    await a.acquirePublicationFence(tx);
    await a.publish(tx, async ({ stream }) =>
      stream("User:reused").track.todo({ id }),
    );
    await tx.query("COMMIT");
    await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    await a.acquirePublicationFence(tx);
    await tx.query("UPDATE v05_business SET title=$2 WHERE id=$1", [
      id,
      "after",
    ]);
    await a.publish(tx, async ({ invalidate }) => invalidate.todo({ id }));
    await tx.query("COMMIT");
    const [row] = await q(
      "SELECT s.cursor,h.head FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id JOIN axton_stream h ON h.stream=s.stream WHERE s.stream='User:reused' AND r.identity->>'id'=$1",
      [id],
    );
    assert.equal(row.head, "2");
    assert.equal(row.cursor, "2");
  } catch (error) {
    await tx.query("ROLLBACK");
    throw error;
  } finally {
    tx.release();
  }
});
test("explicit Loader business refusal rolls back publications and persists refusal once", async () => {
  const s = store(),
    r = batch(s),
    a = app();
  const prior = new Map(
    (await q("SELECT stream,head FROM axton_stream")).map((x) => [
      x.stream,
      x.head,
    ]),
  );
  fault = "loader-refusal";
  const wire = await a.push("alice", JSON.stringify(r));
  assert.equal(JSON.parse(wire).results[0].outcome.kind, "rejected");
  assert.equal(JSON.parse(wire).results[0].outcome.code, "write.read_no");
  assert.equal(
    (await q("SELECT id FROM v05_business WHERE id LIKE $1", [s + "-%"]))
      .length,
    0,
  );
  for (const [stream, head] of prior)
    assert.equal(
      (await q("SELECT head FROM axton_stream WHERE stream=$1", [stream]))[0]
        .head,
      head,
    );
  const savedLoads = loads;
  assert.equal(await a.push("alice", JSON.stringify(r)), wire);
  assert.equal(loads, savedLoads);
});
test("frozen old artifact executes once under same-version reordered and widened trusted input", async () => {
  const s = store(),
    r = batch(s);
  const cfg = structuredClone(config);
  const current = cfg.schema.models[0];
  current.version = 2;
  current.fields.reverse();
  current.fields.push({
    name: "note",
    nullable: true,
    type: { kind: "scalar", name: "string" },
  });
  cfg.models = [
    { ...structuredClone(model), enums: [] },
    { ...current, enums: [] },
  ];
  cfg.schema.actions[0].input = {
    models: [structuredClone(current)],
    enums: [],
  };
  const a = app(cfg, {
    [materialization]: { schema: config.schema, projectionGeneration: "1" },
  });
  const before = calls;
  const wire = await a.push("alice", JSON.stringify(r));
  assert.equal(JSON.parse(wire).results[0].outcome.kind, "accepted");
  assert.equal(await a.push("alice", JSON.stringify(r)), wire);
  assert.equal(calls - before, 1);
  const changed = structuredClone(r);
  changed.mutations[0].descriptor = "another-artifact";
  changed.digest = digest(changed);
  await assert.rejects(
    a.push("alice", JSON.stringify(changed)),
    /batch.conflict/,
  );
  assert.equal(calls - before, 1);
});
test("HTTP Batch endpoint replays identical ack and answers changed immutable body as conflict", async () => {
  const a = app(),
    server = await a.listen({ port: 0 }),
    r = batch(store());
  try {
    const send = (body) =>
      fetch(`${server.url}/sync/mutations`, {
        method: "POST",
        body: JSON.stringify(body),
      });
    const first = await send(r);
    assert.equal(first.status, 200);
    const bytes = await first.text();
    const replay = await send(r);
    assert.equal(replay.status, 200);
    assert.equal(await replay.text(), bytes);
    const changed = structuredClone(r);
    changed.mutations[0].descriptor = "different-artifact";
    changed.digest = digest(changed);
    const conflict = await send(changed);
    assert.equal(conflict.status, 409);
    assert.equal((await conflict.json()).code, "batch.conflict");
  } finally {
    await server.close();
  }
});
test("raw SQLSTATE and wrapped Prisma retry errors survive Handler and Loader native callbacks", async () => {
  const errors = [
    { code: "40001" },
    { code: "40P01" },
    { code: "P2034" },
    { code: "P2010", meta: { code: "40001" } },
    {
      code: "P2010",
      meta: {
        driverAdapterError: {
          name: "DriverAdapterError",
          cause: { kind: "TransactionWriteConflict" },
        },
      },
    },
  ];
  try {
    for (const kind of ["handler", "loader"])
      for (const fields of errors) {
        const s = store(),
          before = calls;
        injectedError = Object.assign(
          new Error("injected native retry"),
          fields,
        );
        fault = kind;
        const ack = JSON.parse(
          await app().push(
            "alice",
            JSON.stringify(batch(s, [kind === "handler" ? "transient" : "ok"])),
          ),
        );
        assert.equal(ack.results.length, 1);
        assert.equal(
          calls,
          before + 2,
          "whole member transaction retries once",
        );
        assert.equal(
          (
            await q("SELECT count(*) n FROM v05_business WHERE id=$1", [
              `${s}-1-0`,
            ])
          )[0].n,
          "1",
        );
      }
  } finally {
    injectedError = undefined;
    fault = "";
  }
});

for (const existing of [false, true])
  test(`caller savepoint publication rolls back and publishes again on ${existing ? "existing" : "new"} Stream`, async () => {
    const backend = app(),
      tx = await pool.connect();
    const stream = `User:savepoint-${existing}`,
      undone = `savepoint-${existing}-undone`,
      kept = `savepoint-${existing}-kept`;
    if (existing)
      await q("INSERT INTO axton_stream(stream,head) VALUES($1,4)", [stream]);
    let woke = 0;
    const stop = backend.onCommitted(stream, () => woke++);
    try {
      await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
      await tx.query("SAVEPOINT caller_act");
      await tx.query("INSERT INTO v05_business VALUES($1,'undone')", [undone]);
      await backend.acquirePublicationFence(tx);
      const discardedWake = await backend.publish(
        tx,
        ({ stream: select, invalidate }) => {
          invalidate.todo({ id: undone });
          select(stream).track.todo({ id: undone });
        },
      );
      assert.equal(typeof discardedWake, "function");
      await tx.query("ROLLBACK TO SAVEPOINT caller_act");
      await tx.query("INSERT INTO v05_business VALUES($1,'kept')", [kept]);
      await backend.acquirePublicationFence(tx);
      const wake = await backend.publish(
        tx,
        ({ stream: select, invalidate }) => {
          invalidate.todo({ id: kept });
          select(stream).track.todo({ id: kept });
        },
      );
      assert.equal(woke, 0, "publication cannot wake before commit");
      await tx.query("COMMIT");
      assert.deepEqual(
        await q(
          "SELECT id,title FROM v05_business WHERE id=ANY($1) ORDER BY id",
          [[undone, kept]],
        ),
        [{ id: kept, title: "kept" }],
      );
      assert.deepEqual(
        await q(
          "SELECT identity->>'id' id FROM axton_record WHERE identity->>'id'=ANY($1)",
          [[undone, kept]],
        ),
        [{ id: kept }],
      );
      const rows = await q(
        "SELECT r.identity->>'id' id,s.cursor,h.head,s.kind FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id JOIN axton_stream h ON h.stream=s.stream WHERE s.stream=$1",
        [stream],
      );
      assert.deepEqual(rows, [
        {
          id: kept,
          cursor: existing ? "5" : "1",
          head: existing ? "5" : "1",
          kind: "upsert",
        },
      ]);
      assert.equal(woke, 0, "caller owns the postcommit wake");
      wake();
      await Promise.resolve();
      assert.equal(woke, 1, "only kept publication wake is delivered");
    } finally {
      await tx.query("ROLLBACK");
      tx.release();
      stop();
    }
  });

test("reused caller-owned PoolClient restores reservations after full ROLLBACK", async () => {
  const backend = app(),
    tx = await pool.connect(),
    stream = "User:rollback-reused";
  try {
    for (const [id, commit] of [
      ["rolled", false],
      ["kept", true],
    ]) {
      await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
      await backend.acquirePublicationFence(tx);
      await tx.query("INSERT INTO v05_business VALUES($1,$2)", [
        "reuse-" + id,
        id,
      ]);
      await backend.publish(tx, ({ stream: select }) =>
        select(stream).track.todo({ id: "reuse-" + id }),
      );
      await tx.query(commit ? "COMMIT" : "ROLLBACK");
    }
    assert.deepEqual(
      await q("SELECT id FROM v05_business WHERE id LIKE 'reuse-%'"),
      [{ id: "reuse-kept" }],
    );
    assert.deepEqual(
      await q(
        "SELECT r.identity->>'id' id,h.head,s.cursor FROM axton_record r JOIN axton_stream_record s ON s.record_id=r.id JOIN axton_stream h USING(stream) WHERE stream=$1",
        [stream],
      ),
      [{ id: "reuse-kept", head: "1", cursor: "1" }],
    );
    assert.deepEqual(
      await q(
        "SELECT id FROM axton_record WHERE identity->>'id'='reuse-rolled'",
      ),
      [],
    );
  } finally {
    await tx.query("ROLLBACK");
    tx.release();
  }
});

test("safe head overflow aborts business/result/progress atomically", async () => {
  const s = store(),
    r = batch(s);
  await q(
    "UPDATE axton_stream SET head=9007199254740991 WHERE stream='User:other'",
  );
  await assert.rejects(app().push("alice", JSON.stringify(r)), /overflow/);
  assert.equal(await state(s), undefined);
  assert.equal((await results(s)).length, 0);
  assert.equal(
    (await q("SELECT * FROM v05_business WHERE id LIKE $1", [s + "-%"])).length,
    0,
  );
});
