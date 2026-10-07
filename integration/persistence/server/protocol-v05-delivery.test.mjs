import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { performance } from "node:perf_hooks";
import { Pool } from "pg";
import {
  createBackend,
  WebSocket,
  MutationRejected,
} from "../../../packages/server/index.mts";
import { pg } from "../../../packages/postgres/index.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const database = pg(pool, { retries: 10 });
const fenceStarts = new WeakMap();
let lastFenceHeldMs = 0;
const originalQuery = database.driver.query.bind(database.driver);
database.driver.query = async (tx, sql, params) => {
  if (sql.startsWith("UPDATE axton_publication_fence") && !fenceStarts.has(tx))
    fenceStarts.set(tx, performance.now());
  return originalQuery(tx, sql, params);
};
const originalTransaction = database.transaction.bind(database);
database.transaction = async (body) => {
  let tx;
  try {
    return await originalTransaction(async (t) => {
      tx = t;
      fenceStarts.delete(t);
      return body(t);
    });
  } finally {
    lastFenceHeldMs =
      tx && fenceStarts.has(tx) ? performance.now() - fenceStarts.get(tx) : 0;
  }
};

const q = async (sql, args = []) => (await pool.query(sql, args)).rows;
const model = {
  name: "Entry",
  version: 1,
  bootstrap: true,
  identity: ["id"],
  unique: [["text"]],
  fields: ["id", "text"].map((name) => ({
    name,
    nullable: false,
    type: { kind: "scalar", name: "string" },
  })),
};
const config = {
  schema: { enums: [], models: [model], actions: [] },
  mutations: [],
  loaders: ["Entry"],
};
const materialization = native.serverMaterializationId05(
  JSON.stringify(config),
  "1",
);
const previous = native.serverMaterializationId05(
  JSON.stringify(config),
  "old",
);
let preparations = 0,
  loaderCalls = 0,
  fault = false,
  bootstrapTrack = false;
function appFor(config) {
  return createBackend({
    config,
    native,
    database,
    protocol5: {
      materializations: {
        [previous]: { schema: config.schema, projectionGeneration: "old" },
      },
      authorizeStream: (p, s) => s === `User:${p}`,
    },
    authenticate: () => "alice",
    onError: () => {},
    bootstrap: async ({ ctx }) => {
      preparations++;
      if (bootstrapTrack) ctx.stream.track.entry({ id: "e1" });
    },
    loaders: {
      entry: async ({ tx, ids }) => {
        loaderCalls++;
        if (fault) throw new Error("DB offline");
        const rows = (
          await tx.query(
            "SELECT id,text FROM delivery_business WHERE id=ANY($1)",
            [ids.map((i) => i.id)],
          )
        ).rows;
        const by = new Map(rows.map((r) => [r.id, r]));
        return ids.map((i) => by.get(i.id) ?? null);
      },
    },
  });
}
const app = appFor(config);
const context = (storeId) => ({
  protocol: 5,
  storeId,
  stream: "User:alice",
  materialization,
});
const delta = (s, after = 0, through = 40, bootstrap = true) => ({
  ...context(s),
  after,
  through,
  bootstrap,
});
async function fixture(n, unique = true) {
  await q(
    "TRUNCATE axton_delivery_plan,axton_mutation_result,axton_store,axton_stream_record,axton_stream,axton_record,delivery_business RESTART IDENTITY CASCADE",
  );
  await q(
    "INSERT INTO delivery_business SELECT 'e'||i,'value'||i FROM generate_series(1,$1) i",
    [n],
  );
  await q(
    "INSERT INTO axton_record(model,identity_key,stamp) SELECT 'Entry',json_build_object('id','e'||i)::text::jsonb::text ,1 FROM generate_series(1,$1) i",
    [0],
  ); // canonical keys without pg JSON spacing
  await q(
    "INSERT INTO axton_record(model,identity_key,stamp) SELECT 'Entry','{\"id\":\"e'||i||'\"}',1 FROM generate_series(1,$1) i",
    [n],
  );
  await q("INSERT INTO axton_stream VALUES('User:alice',45)");
  await q(
    "INSERT INTO axton_stream_record SELECT 'User:alice',id,45,'upsert' FROM axton_record",
  );
}
async function pull(r) {
  return JSON.parse(await app.pull("alice", JSON.stringify(r)));
}
before(async () => {
  await q(
    await readFile(
      new URL("../../../packages/postgres/migration.sql", import.meta.url),
      "utf8",
    ),
  );
  await q(
    "CREATE TABLE IF NOT EXISTS delivery_business(id text PRIMARY KEY,text text NOT NULL)",
  );
});
after(async () => {
  await pool.end();
});
test("Bootstrap freezes e@45, retries preparation, authenticates pages and expires without fabricated coverage", async () => {
  await fixture(1500);
  await q("UPDATE axton_stream_record SET cursor=20");
  await q("UPDATE axton_stream SET head=40");
  preparations = 0;
  loaderCalls = 0;
  const h = JSON.parse(
    await app.handshake(
      "alice",
      JSON.stringify({ protocol: 5, storeId: "s", stream: "User:alice" }),
    ),
  );
  assert.equal(h.head, 40);
  await app.handshake(
    "alice",
    JSON.stringify({ protocol: 5, storeId: "s", stream: "User:alice" }),
  );
  assert.equal(preparations, 1);
  await q("UPDATE axton_stream_record SET cursor=45");
  await q("UPDATE axton_stream SET head=45");
  const r = delta("s"),
    first = await pull(r);
  assert.equal(first.header.units.length, 1);
  assert.equal(first.header.units[0].through, 40);
  assert.equal(first.header.units[0].parts.length, 3);
  await q("UPDATE delivery_business SET text='changed46' WHERE id='e1000'");
  await q(
    'UPDATE axton_stream_record SET cursor=46 WHERE record_id=(SELECT id FROM axton_record WHERE identity_key=\'{"id":"e1000"}\')',
  );
  await q("UPDATE axton_stream SET head=46");
  const calls = loaderCalls;
  let parts = [...first.parts];
  for (let part = 1; part < 3; part++) {
    const page = await pull({
      ...r,
      continuation: {
        planId: first.header.planId,
        digest: first.header.digest,
        unit: 0,
        part,
      },
    });
    parts.push(...page.parts);
  }
  assert.equal(loaderCalls, calls);
  assert.equal(
    parts.flatMap((p) => p.changes).find((c) => c.key.identity.id === "e1000")
      .state.text,
    "value1000",
  );
  await assert.rejects(
    app.pull(
      "other",
      JSON.stringify({
        ...r,
        continuation: {
          planId: first.header.planId,
          digest: first.header.digest,
          unit: 0,
          part: 1,
        },
      }),
    ),
  );
  await q("UPDATE axton_delivery_plan SET expires_at=0");
  await assert.rejects(
    pull({
      ...r,
      continuation: {
        planId: first.header.planId,
        digest: first.header.digest,
        unit: 0,
        part: 1,
      },
    }),
    (e) => e.code === "delivery.expired",
  );
  assert.equal(
    (await q("SELECT count(*) n FROM axton_delivery_unit"))[0].n,
    "0",
  );
  const renewed = await pull(r);
  assert.equal(renewed.header.observedHead, 46);
  const sync = await pull(delta("s", 40, 46, false));
  assert.equal(sync.header.through, 46);
});
test("S=0 empty Bootstrap and null-cursor Fetch are explicit; loader fault stages nothing", async () => {
  await fixture(0);
  await q("UPDATE axton_stream SET head=0");
  const first = await pull(delta("zero", 0, 0));
  assert.equal(first.header.units[0].through, 0);
  assert.deepEqual(first.parts[0].changes, []);
  const read = {
    ...context("zero"),
    requestId: "fetch",
    store: false,
    invocation: {
      kind: "fetch",
      key: { model: "Entry", identity: { id: "absent" } },
      version: 1,
    },
  };
  const answer = JSON.parse(await app.fetch("alice", JSON.stringify(read)));
  assert.equal(answer.records[0].cursor, null);
  assert.equal(answer.records[0].state, null);
  assert.equal(
    (await q("SELECT count(*) n FROM axton_stream_record"))[0].n,
    "0",
  );
  await fixture(1);
  fault = true;
  await assert.rejects(pull(delta("fault")));
  ((fault = false), (bootstrapTrack = false));
  assert.equal(
    (await q("SELECT count(*) n FROM axton_delivery_plan"))[0].n,
    "0",
  );
});
test("real selected and unique-model capacity measurements", async () => {
  for (const kind of ["selected", "unique"])
    for (const n of [10000, 100000]) {
      await fixture(n);
      let chosen = app,
        r = delta(`capacity-${kind}-${n}`, 0, n);
      if (kind === "selected") {
        const cfg = {
          ...config,
          schema: { ...config.schema, models: [{ ...model, unique: [] }] },
        };
        chosen = appFor(cfg);
        r.materialization = native.serverMaterializationId05(
          JSON.stringify(cfg),
          "1",
        );
        await q("UPDATE axton_stream_record SET cursor=record_id");
        await q("UPDATE axton_stream SET head=$1", [n]);
      } else {
        await q("UPDATE axton_stream SET head=$1", [n]);
      }
      const start = performance.now();
      const first = JSON.parse(await chosen.pull("alice", JSON.stringify(r)));
      const frozenMs = performance.now() - start;
      const fenceHeldMs = lastFenceHeldMs;
      const [row] = await q(
        "SELECT staged_bytes FROM axton_delivery_plan WHERE plan_id=$1",
        [first.header.planId],
      );
      const lastUnit = first.header.units.length - 1,
        lastPart = first.header.units[lastUnit].parts.length - 1;
      const c = {
        planId: first.header.planId,
        digest: first.header.digest,
        unit: lastUnit,
        part: lastPart,
      };
      const reads = performance.now();
      for (let i = 0; i < 5; i++)
        await chosen.pull("alice", JSON.stringify({ ...r, continuation: c }));
      const validationMs = (performance.now() - reads) / 5;
      const allParts = (
        await q("SELECT payload FROM axton_delivery_unit WHERE plan_id=$1", [
          first.header.planId,
        ])
      ).map((p) => p.payload);
      const headerBytes = Buffer.byteLength(JSON.stringify(first.header));
      const transportedBytes = allParts.reduce(
        (n, p) =>
          n +
          Buffer.byteLength(
            JSON.stringify({ header: first.header, parts: [p] }),
          ),
        0,
      );
      const cleanupStart = performance.now();
      await q("DELETE FROM axton_delivery_plan");
      const cleanupMs = performance.now() - cleanupStart;
      assert.equal(
        (await q("SELECT count(*) n FROM axton_delivery_unit"))[0].n,
        "0",
      );
      console.log(
        JSON.stringify({
          measurement: kind,
          n,
          stagedBytes: Number(row.staged_bytes),
          fencedRequestMs: frozenMs,
          fenceHeldMs,
          continuationMs: validationMs,
          cleanupMs,
          headerBytes,
          responseCount: allParts.length,
          totalTransportBytes: transportedBytes,
        }),
      );
    }
});
test("target materialization returns current Remove, never enrolls unknown keys", async () => {
  await fixture(2);
  await q(
    "UPDATE axton_stream_record SET kind='remove' WHERE record_id=(SELECT id FROM axton_record WHERE identity_key='{\"id\":\"e1\"}')",
  );
  // A schema owner may reconcile held keys only. Unknown explicit keys are refused.

  const r = {
    ...context("materialize"),
    requestId: "m",
    owner: { kind: "schema", previousMaterialization: previous },
    keys: [{ model: "Entry", identity: { id: "e1" } }],
    models: {},
  };
  const d = JSON.parse(await app.materialize("alice", JSON.stringify(r)));
  assert.equal(d.delivery.header.after, null);
  assert.equal(d.delivery.header.units[0].through, null);
  assert.equal(d.delivery.parts[0].changes[0].kind, "remove");
  await assert.rejects(
    app.materialize(
      "alice",
      JSON.stringify({
        ...r,
        requestId: "unknown",
        keys: [{ model: "Entry", identity: { id: "unknown" } }],
      }),
    ),
  );
  assert.equal(
    (await q("SELECT count(*) n FROM axton_stream_record"))[0].n,
    "2",
  );
});
test("compacted unique ownership transfer remains one final-state unit across three responses", async () => {
  await fixture(1500);
  await q("UPDATE delivery_business SET text='free' WHERE id='e1'");
  await q("UPDATE delivery_business SET text='x' WHERE id='e2'");
  await q(
    "UPDATE axton_stream_record SET cursor=CASE WHEN record_id=1 THEN 5 ELSE 3 END",
  );
  await q("UPDATE axton_stream SET head=5");
  const r = delta("transfer", 0, 5, false),
    first = await pull(r);
  assert.equal(first.header.units.length, 1);
  assert.equal(first.header.units[0].minimumCursor, 3);
  assert.equal(first.header.units[0].parts.length, 3);
  const parts = [...first.parts];
  for (let part = 1; part < 3; part++)
    parts.push(
      ...(
        await pull({
          ...r,
          continuation: {
            planId: first.header.planId,
            digest: first.header.digest,
            unit: 0,
            part,
          },
        })
      ).parts,
    );
  const states = new Map(
    parts
      .flatMap((p) => p.changes)
      .map((c) => [c.key.identity.id, c.state.text]),
  );
  assert.equal(states.get("e1"), "free");
  assert.equal(states.get("e2"), "x");
  assert.equal(first.header.units[0].through, 5);
});
test("capacity refusal preserves existing plan and stages no partial payload", async () => {
  await fixture(1);
  const first = await pull(delta("capacity-refusal"));
  await q(
    "INSERT INTO delivery_business SELECT 'more'||i,'morevalue'||i FROM generate_series(1,100000) i",
  );
  await q(
    "INSERT INTO axton_record(model,identity_key,stamp) SELECT 'Entry','{\"id\":\"more'||i||'\"}',1 FROM generate_series(1,100000) i",
  );
  await q(
    "INSERT INTO axton_stream_record SELECT 'User:alice',id,45,'upsert' FROM axton_record WHERE id>1",
  );
  await assert.rejects(
    pull(delta("capacity-refusal")),
    (e) => e.code === "delivery.capacity",
  );
  assert.equal(
    (await q("SELECT count(*) n FROM axton_delivery_plan"))[0].n,
    "1",
  );
  assert.equal(
    (await q("SELECT count(*) n FROM axton_delivery_unit"))[0].n,
    "1",
  );
  const replay = await pull({
    ...delta("capacity-refusal"),
    continuation: {
      planId: first.header.planId,
      digest: first.header.digest,
      unit: 0,
      part: 0,
    },
  });
  assert.equal(replay.parts[0].changes.length, 1);
});
test("preparation publishes before the initial head, retries only replay committed preparation", async () => {
  await fixture(1);
  await q("DELETE FROM axton_stream_record");
  await q("UPDATE axton_stream SET head=0");
  preparations = 0;
  bootstrapTrack = true;
  try {
    for (let i = 0; i < 2; i++) {
      const ack = JSON.parse(
        await app.handshake(
          "alice",
          JSON.stringify({
            protocol: 5,
            storeId: "prepare",
            stream: "User:alice",
          }),
        ),
      );
      assert.equal(ack.head, 1);
    }
    assert.equal(preparations, 1);
    const [store] = await q(
      "SELECT bootstrap_prepared,start_cursor FROM axton_store WHERE id='prepare'",
    );
    assert.equal(store.bootstrap_prepared, true);
    assert.equal(store.start_cursor, "1");
  } finally {
    bootstrapTrack = false;
  }
});
test("WebSocket offers frozen complete units; reconnect head leaves HTTP gap repair available", async () => {
  await fixture(1);
  const errors = [];
  const listening = await app.listen({ port: 0 });
  const connect = () =>
    new WebSocket(listening.url.replace("http:", "ws:") + "/sync/live");
  const socket = connect(),
    frames = [];
  let signal;
  const waitFrame = async (predicate) => {
    for (let i = 0; i < 100; i++) {
      const found = frames.find(predicate);
      if (found) return found;
      await new Promise((r) => {
        signal = r;
        setTimeout(r, 25);
      });
    }
    throw new Error("socket timed out " + JSON.stringify(errors));
  };
  socket.on("message", (data) => {
    frames.push(JSON.parse(String(data)));
    signal?.();
  });
  socket.on("error", (e) => errors.push(String(e)));
  try {
    await new Promise((r, j) => {
      socket.once("open", r);
      socket.once("error", j);
    });
    socket.send(
      JSON.stringify({ protocol: 5, storeId: "live", stream: "User:alice" }),
    );
    const ack = await waitFrame((f) => f.head !== undefined);
    assert.equal(ack.head, 45);
    await waitFrame((f) => f.header?.through === 45);
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("UPDATE delivery_business SET text='at46' WHERE id='e1'");
      invalidate.entry({ id: "e1" });
    });
    const delta = await waitFrame((f) => f.header?.through === 46);
    assert.equal(delta.parts[0].changes[0].cursor, 46);
    assert.equal(delta.parts[0].changes[0].state.text, "at46");
    socket.close();
    await new Promise((r) => socket.once("close", r));
    await app.transaction(async ({ tx, invalidate }) => {
      await tx.query("UPDATE delivery_business SET text='at47' WHERE id='e1'");
      invalidate.entry({ id: "e1" });
    });
    const next = JSON.parse(
      await app.handshake(
        "alice",
        JSON.stringify({ protocol: 5, storeId: "live", stream: "User:alice" }),
      ),
    );
    assert.equal(next.head, 47);
    const repair = await pull(deltaRequest("live", 46, 47));
    assert.equal(repair.header.through, 47);
    assert.equal(repair.parts[0].changes[0].state.text, "at47");
  } finally {
    socket.close();
    await listening.close();
  }
});
function deltaRequest(s, after, through) {
  return { ...context(s), after, through, bootstrap: false };
}
test("Query carrier normalizes result snapshots without enrollment; refusal rolls back", async () => {
  await fixture(1);
  const output = {
    name: "entry",
    kind: "model",
    cardinality: "single",
    source: "handlerIdentity",
    model: "Entry",
    modelReadVersion: 1,
    handlerType: {
      kind: "identity",
      model: "Entry",
      fields: [{ name: "id", type: { kind: "scalar", name: "string" } }],
    },
  };
  const cfg = {
    ...config,
    schema: {
      ...config.schema,
      resultModels: [{ ...model, enums: [] }],
      actions: [
        {
          name: "Find",
          version: 1,
          kind: "query",
          inputs: [],
          outputs: [output],
        },
      ],
    },
  };
  let mode = "ok";
  const chosen = createBackend({
    config: cfg,
    native,
    database,
    protocol5: { authorizeStream: (p, s) => s === `User:${p}` },
    authenticate: () => "alice",
    onError: () => {},
    queries: {
      find: async ({ ctx }) => {
        if (mode === "track") {
          await ctx.tx.query(
            "UPDATE delivery_business SET text='forbidden' WHERE id='e1'",
          );
          assert.equal(ctx.stream, undefined);
          throw new MutationRejected("find.denied");
        }
        return { entry: { id: "e1" } };
      },
    },
    loaders: {
      entry: async ({ tx, ids }) =>
        Promise.all(
          ids.map(
            async ({ id }) =>
              (
                await tx.query(
                  "SELECT id,text FROM delivery_business WHERE id=$1",
                  [id],
                )
              ).rows[0] ?? null,
          ),
        ),
    },
  });
  const r = {
    ...context("query"),
    materialization: native.serverMaterializationId05(JSON.stringify(cfg), "1"),
    requestId: "q",
    store: false,
    invocation: { kind: "query", name: "Find", version: 1, args: {} },
  };
  const answer = JSON.parse(await chosen.action("alice", JSON.stringify(r)));
  assert.equal(answer.outcome.kind, "succeeded");
  assert.equal(answer.outcome.result.entry.text, "value1");
  assert.equal(answer.records[0].cursor, null);
  assert.equal((await q("SELECT head FROM axton_stream"))[0].head, "45");
  mode = "track";
  const refused = JSON.parse(
    await chosen.action("alice", JSON.stringify({ ...r, requestId: "bad" })),
  );
  assert.equal(refused.outcome.kind, "failed");
  assert.equal(refused.outcome.code, "find.denied");
  assert.deepEqual(refused.records, []);
  assert.equal(
    (await q("SELECT text FROM delivery_business"))[0].text,
    "value1",
  );
});
test('retained read context repairs only its known Models while covering unrelated publications',async()=>{
 await fixture(1);await q("INSERT INTO axton_record(model,identity_key,stamp) VALUES('Extra','{\"id\":\"extra\"}',1)");await q("INSERT INTO axton_stream_record SELECT 'User:alice',id,45,'upsert' FROM axton_record WHERE model='Extra'");
 const cfg={...config,schema:{...config.schema,models:[model,{...model,name:'Extra'}]},loaders:['Entry','Extra']};
 const chosen=createBackend({config:cfg,native,database,protocol5:{materializations:{[materialization]:{schema:config.schema,projectionGeneration:'1'}},authorizeStream:(p,s)=>s===`User:${p}`},authenticate:()=> 'alice',onError:()=>{},loaders:{entry:async()=>[{id:'e1',text:'value1'}],extra:async()=>{throw new Error('excluded Model Loader ran');}}});
 const answer=JSON.parse(await chosen.pull('alice',JSON.stringify(delta('retained',0,45,false))));assert.equal(answer.header.through,45);assert.equal(answer.parts[0].changes.length,1);assert.equal(answer.parts[0].changes[0].key.model,'Entry');
});
