import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { createBackend, devAuth } from '../../../packages/server/index.mts';
import { isRetryableTransactionError } from '../../../packages/server/retryable.mts';
import { prisma, pg, drizzle } from '../../../packages/postgres/index.mts';
import { Pool } from 'pg';
import { drizzle as drizzleOrm } from 'drizzle-orm/node-postgres';
const require = createRequire(import.meta.url);
const { PrismaClient } = require('../../bindings/node/generated/client');
const native = require('../../../bindings/node/axton-node.node');
const db = new PrismaClient();
const fields = [
  { name: 'id', type: { kind: 'scalar', name: 'string' }, nullable: false },
  { name: 'title', type: { kind: 'scalar', name: 'string' }, nullable: false },
];
// An explicit `todo` output: the handler returns the identity, the Loader reads it.
const todoOutput = { name: 'todo', kind: 'model', model: 'Todo', modelReadVersion: 1, cardinality: 'single', source: 'handlerIdentity', handlerType: { kind: 'identity', model: 'Todo', fields: [{ name: 'id', type: { kind: 'scalar', name: 'string' } }] } };
const config = { schema: {
  enums: [], models: [{ name: 'Todo', version: 1, identity: ['id'], fields }],
  resultModels: [{ name: 'Todo', version: 1, identity: ['id'], fields, enums: [] }],
  actions: [{ name: 'Add', version: 1, inputs: [{ kind: 'model', name: 'todo', model: 'Todo', operation: 'create', cardinality: 'single' }], outputs: [todoOutput] }],
}, mutations: [], loaders: ['Todo'] };
let handlers = 0, loaders = 0;
const handler = async ({ ctx, args }) => {
  handlers++;
  await ctx.tx.$executeRawUnsafe('INSERT INTO action_todo(id,title) VALUES($1,$2)', args.todo.id, args.todo.title);
  return { todo: { id: args.todo.id } };
};
const loader = async ({ tx, ids }) => {
  loaders++;
  return Promise.all(ids.map(async ({ id }) => (await tx.$queryRawUnsafe('SELECT id,title FROM action_todo WHERE id=$1', id))[0] ?? null));
};
const backend = database => createBackend({ config, native, database, authenticate: () => 'alice', mutations: { add: handler }, loaders: { todo: loader } });
const request = (clientId, callId, id, title) => JSON.stringify({ clientId, batchSequence: 1, models: { Todo: 1 }, mutations: [{ ordinal: 1, callId, name: 'Add', version: 1, args: { todo: { id, title } } }] });
const openShims = () => {
  const poolPg = new Pool({ connectionString: process.env.DATABASE_URL });
  const poolDrizzle = new Pool({ connectionString: process.env.DATABASE_URL });
  return {
    shims: [{ name: 'prisma', database: prisma(db) }, { name: 'pg', database: pg(poolPg) }, { name: 'drizzle', database: drizzle(drizzleOrm(poolDrizzle)) }],
    close: () => Promise.all([poolPg.end(), poolDrizzle.end()]),
  };
};
before(async () => {
  for (const sql of (await readFile(new URL('../../../packages/postgres/migration.sql', import.meta.url), 'utf8')).split(';').map(s => s.trim()).filter(Boolean)) await db.$executeRawUnsafe(sql);
  await db.$executeRawUnsafe('CREATE TABLE action_todo(id text PRIMARY KEY,title text NOT NULL)');
  await db.$executeRawUnsafe('CREATE TABLE action_counter(id integer PRIMARY KEY,n integer NOT NULL)');
  await db.$executeRawUnsafe('INSERT INTO action_counter(id,n) VALUES(1,0)');
});
after(() => db.$disconnect());

test("direct HTTP Action authenticates, commits, and replays the same call ID across UUID casing", async () => {
  const app = createBackend({
    config,
    native,
    database: prisma(db),
    authenticate: devAuth(),
    mutations: { add: handler },
    loaders: { todo: loader },
  });
  const listening = await app.listen({ port: 0 });
  const callId = "01890f47-1234-7123-8123-123456789aee";
  const body = JSON.stringify({
    call: {
      callId: callId.toUpperCase(),
      name: "Add",
      version: 1,
      args: { todo: { id: "direct-http", title: "D" } },
    },
    models: { Todo: 1 },
  });
  try {
    const denied = await fetch(`${listening.url}/sync/actions`, {
      method: "POST",
      body,
    });
    assert.equal(denied.status, 401);
    const send = (request) =>
      fetch(`${listening.url}/sync/actions`, {
        method: "POST",
        headers: { authorization: "Bearer alice" },
        body: request,
      });
    const first = await send(body);
    assert.equal(first.status, 200);
    const result = await first.json();
    assert.equal(result.completion.callId, callId);
    assert.equal(result.completion.outcome.status, "succeeded");
    assert.equal(result.records[0].state.title, "D");
    const priorHandlers = handlers;
    const replay = await send(
      JSON.stringify({
        call: {
          callId,
          name: "Add",
          version: 1,
          args: { todo: { id: "direct-http", title: "D" } },
        },
        models: { Todo: 1 },
      }),
    );
    assert.equal(replay.status, 200);
    assert.deepEqual(await replay.json(), result);
    assert.equal(handlers, priorHandlers);
    const durable = JSON.parse(await app.push('alice', request('direct-replay-batch', callId.toUpperCase(), 'direct-http', 'D')));
    assert.equal(durable.completions[0].callId, callId);
    assert.equal(handlers, priorHandlers, 'durable replay shares the canonical direct claim');
    const invalid = await send(JSON.stringify({ call: { callId: '01890f47-1234-7123-8123-123456789aef', name: 'Add', version: 1, args: { todo: { id: 'missing-title' } } }, models: { Todo: 1 } }));
    assert.equal(invalid.status, 200, 'semantic validation is a per-call outcome');
    assert.equal((await invalid.json()).completion.outcome.code, 'action.invalid');
    assert.equal(
      (
        await db.$queryRawUnsafe(
          "SELECT call_id FROM axton_call WHERE call_id=$1",
          callId,
        )
      ).length,
      1,
    );
  } finally {
    await listening.close();
  }
});

test('committed Action replays its original result after losing the response', async () => {
  const app = backend(prisma(db));
  const callId = '01890f47-1234-7123-8123-123456789abc';
  const first = JSON.parse(await app.push('alice', request('a', callId, 'one', 'A')));
  const handled = handlers, loaded = loaders;
  const replay = JSON.parse(await app.push('alice', request('b', callId, 'one', 'A')));
  assert.deepEqual(replay.completions, first.completions);
  assert.deepEqual(replay.records, first.records);
  assert.equal(handlers, handled);
  assert.equal(loaders, loaded);
  assert.deepEqual(await db.$queryRawUnsafe("SELECT title FROM action_todo WHERE id='one'"), [{ title: 'A' }]);
});

test('same frozen call replays after a nullable Model input/read field is added', async () => {
  const callId = '01890f47-1234-7123-8123-123456789ac0';
  const frozen = request('before-addition', callId, 'three', 'C');
  const original = JSON.parse(await backend(prisma(db)).push('alice', frozen));
  const upgraded = structuredClone(config);
  const note = { name: 'note', type: { kind: 'scalar', name: 'string' }, nullable: true };
  upgraded.schema.models[0].fields.push(note);
  upgraded.schema.resultModels[0].fields = structuredClone(upgraded.schema.models[0].fields);
  const priorHandlers = handlers, priorLoaders = loaders;
  const replay = JSON.parse(await createBackend({ config: upgraded, native, database: prisma(db), authenticate: () => 'alice', mutations: { add: handler }, loaders: { todo: loader } }).push('alice', request('after-addition', callId, 'three', 'C')));
  assert.deepEqual(replay.completions, original.completions, 'cached result retains its original snapshot');
  assert.deepEqual(replay.records, [{ model: 'Todo', identity: { id: 'three' }, stamp: 1, state: { title: 'C', note: null } }], 'receipt authority is normalized for the current read contract');
  assert.equal(handlers, priorHandlers);
  assert.equal(loaders, priorLoaders);
});

test('saveCall persistence fault rolls back business row and call claim', async () => {
  const normal = prisma(db);
  const broken = { transaction: normal.transaction, persistence: tx => ({ call: async req => {
    if (req.op === 'saveCall') throw new Error('forced saveCall fault');
    return normal.persistence(tx).call(req);
  } }) };
  const callId = '01890f47-1234-7123-8123-123456789abd';
  await assert.rejects(() => backend(broken).push('alice', request('fault', callId, 'two', 'B')), /forced saveCall fault/);
  assert.deepEqual(await db.$queryRawUnsafe("SELECT id FROM action_todo WHERE id='two'"), []);
  assert.deepEqual(await db.$queryRawUnsafe('SELECT call_id FROM axton_call WHERE call_id=$1', callId), []);
  const retry = JSON.parse(await backend(normal).push('alice', request('retry', callId, 'two', 'B')));
  assert.equal(retry.completions[0].outcome.status, 'succeeded');
  assert.deepEqual(await db.$queryRawUnsafe("SELECT title FROM action_todo WHERE id='two'"), [{ title: 'B' }]);
});

test('two distinct calls racing on one row retry without saving a transient rejection', async () => {
  const bumpConfig = { schema: { enums: [], models: [], actions: [{ name: 'Bump', version: 1, inputs: [], outputs: [{ name: 'n', kind: 'value', type: { kind: 'scalar', name: 'int' }, cardinality: 'single', source: 'handlerValue' }] }] }, mutations: [], loaders: [] };
  const body = (clientId, callId) => JSON.stringify({ clientId, batchSequence: 1, models: {}, mutations: [{ ordinal: 1, callId, name: 'Bump', version: 1, args: {} }] });
  const { shims, close } = openShims();
  try {
    for (const [index, { name, database }] of shims.entries()) {
      const rowId = index + 1;
      if (rowId > 1) await db.$executeRawUnsafe('INSERT INTO action_counter(id,n) VALUES($1,0)', rowId);
      let arrived = 0, release;
      const both = new Promise(resolve => { release = resolve; });
      let attempts = 0;
      const bump = createBackend({ config: bumpConfig, native, database, authenticate: () => 'alice', mutations: { async bump({ ctx }) {
        attempts++;
        await database.driver.query(ctx.tx, 'SELECT n FROM action_counter WHERE id=$1', [rowId]);
        if (arrived < 2) { arrived++; if (arrived === 2) release(); await both; }
        const rows = await database.driver.query(ctx.tx, 'UPDATE action_counter SET n=n+1 WHERE id=$1 RETURNING n', [rowId]);
        return { n: Number(rows[0].n) };
      } }, loaders: {} });
      const a = `01890f47-1234-7123-8123-123456789ad${index * 2}`;
      const b = `01890f47-1234-7123-8123-123456789ad${index * 2 + 1}`;
      const receipts = await Promise.all([bump.push('alice', body(`race-${name}-a`, a)), bump.push('alice', body(`race-${name}-b`, b))]);
      assert.deepEqual(receipts.map(text => JSON.parse(text).completions[0].outcome.result.n).sort(), [1, 2], name);
      assert.equal(Number((await db.$queryRawUnsafe('SELECT n FROM action_counter WHERE id=$1', rowId))[0].n), 2, name);
      assert.ok(attempts >= 3, `${name} retried the whole transaction after a real row conflict`);
      for (const callId of [a, b]) {
        const rows = await db.$queryRawUnsafe('SELECT response FROM axton_call WHERE call_id=$1', callId);
        assert.equal(rows.length, 1, name);
        assert.equal(JSON.parse(rows[0].response).completion.outcome.status, 'succeeded', name);
      }
    }
  } finally {
    await close();
  }
});

test('retry classifier retains raw SQLSTATE and wrapped Prisma failures', () => {
  for (const code of ['40001', '40P01', 'P2034']) assert.equal(isRetryableTransactionError({ code }), true);
  assert.equal(isRetryableTransactionError({ code: 'P2010', meta: { code: '40001' } }), true);
  assert.equal(isRetryableTransactionError({ code: 'P2010', meta: { code: '40P01' } }), true);
  assert.equal(isRetryableTransactionError({ cause: { code: '40P01' } }), true);
  assert.equal(isRetryableTransactionError({ code: 'P2010', meta: { code: '23505' } }), false);
});

test('retryable handler and Loader errors cross native bridge unchanged on every adapter', async () => {
  const { shims, close } = openShims();
  const bumpConfig = { schema: { enums: [], models: [], actions: [{ name: 'Bump', version: 1, inputs: [], outputs: [{ name: 'n', kind: 'value', type: { kind: 'scalar', name: 'int' }, cardinality: 'single', source: 'handlerValue' }] }] }, mutations: [], loaders: [] };
  const findConfig = { schema: { enums: [], models: config.schema.models, resultModels: config.schema.resultModels, actions: [{ name: 'Find', version: 1, inputs: [], outputs: [{ name: 'todo', kind: 'model', model: 'Todo', modelReadVersion: 1, cardinality: 'single', source: 'handlerIdentity', handlerType: { kind: 'identity', model: 'Todo', fields: [{ name: 'id', type: { kind: 'scalar', name: 'string' } }] } }] }] }, mutations: [], loaders: ['Todo'] };
  const body = (clientId, callId, name, models) => JSON.stringify({ clientId, batchSequence: 1, models, mutations: [{ ordinal: 1, callId, name, version: 1, args: {} }] });
  try {
    for (const [index, { name, database }] of shims.entries()) {
      const fault = () => name === 'prisma'
        ? Object.assign(new Error('retryable Prisma fault'), { code: 'P2010', meta: { code: '40P01' } })
        : Object.assign(new Error('retryable SQL fault'), { code: '40P01' });
      const reported = [];
      let handlerAttempts = 0;
      const handlerBackend = createBackend({ config: bumpConfig, native, database, authenticate: () => 'alice', onError: error => reported.push(error), mutations: { async bump() {
        if (++handlerAttempts === 1) throw fault();
        return { n: 7 };
      } }, loaders: {} });
      const handlerReceipt = JSON.parse(await handlerBackend.push('alice', body(`handler-${name}`, `01890f47-1234-7123-8123-123456789ae${index}`, 'Bump', {})));
      assert.equal(handlerReceipt.completions[0].outcome.result.n, 7, name);
      assert.equal(handlerAttempts, 2, `${name} retried handler failure`);
      let loaderAttempts = 0, findAttempts = 0;
      const loaderBackend = createBackend({ config: findConfig, native, database, authenticate: () => 'alice', onError: error => reported.push(error), mutations: { async find() { findAttempts++; return { todo: { id: 'one' } }; } }, loaders: { async todo({ tx }) {
        if (++loaderAttempts === 1) throw fault();
        const rows = await database.driver.query(tx, 'SELECT id,title FROM action_todo WHERE id=$1', ['one']);
        return rows;
      } } });
      const loaderReceipt = JSON.parse(await loaderBackend.push('alice', body(`loader-${name}`, `01890f47-1234-7123-8123-123456789af${index}`, 'Find', { Todo: 1 })));
      assert.equal(loaderReceipt.completions[0].outcome.result.todo.title, 'A', name);
      assert.equal(loaderAttempts, 2, `${name} retried loader failure`);
      assert.equal(findAttempts, 2, `${name} reran the whole Action`);
      assert.deepEqual(reported, [], `${name} retryable faults did not become application failures`);
    }
  } finally { await close(); }
});

test('read-only identity holds its stamp lock through Loader read and commit', async () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const database = pg(pool);
  const schema = { enums: [], models: config.schema.models, resultModels: config.schema.resultModels, actions: [
    { name: 'Find', version: 1, inputs: [], outputs: [{ name: 'todo', kind: 'model', model: 'Todo', modelReadVersion: 1, cardinality: 'single', source: 'handlerIdentity', handlerType: { kind: 'identity', model: 'Todo', fields: [{ name: 'id', type: { kind: 'scalar', name: 'string' } }] } }] },
    { name: 'Edit', version: 1, inputs: [{ kind: 'model', name: 'todo', model: 'Todo', operation: 'update', cardinality: 'single', allowedPatchFields: ['title'] }], outputs: [todoOutput] },
  ] };
  await db.$executeRawUnsafe("INSERT INTO action_todo(id,title) VALUES('lock','old')");
  await database.transaction(async tx => database.persistence(tx).call({ op: 'ensureStamp', model: 'Todo', identityKey: '{"id":"lock"}' }));
  let enteredLoad, releaseLoad, enteredAdvance, writerPid;
  const loading = new Promise(resolve => { enteredLoad = resolve; });
  const release = new Promise(resolve => { releaseLoad = resolve; });
  const advancing = new Promise(resolve => { enteredAdvance = resolve; });
  const read = createBackend({ config: { schema, mutations: [], loaders: ['Todo'] }, native, database, authenticate: () => 'alice', mutations: { async find() { return { todo: { id: 'lock' } }; }, async edit() { return { todo: { id: 'lock' } }; } }, loaders: { async todo({ tx }) {
    enteredLoad();
    await release;
    return database.driver.query(tx, 'SELECT id,title FROM action_todo WHERE id=$1', ['lock']);
  } } });
  const writerDatabase = { ...database, persistence: tx => ({ call: async req => {
    if (req.op === 'advanceStamp') enteredAdvance();
    return database.persistence(tx).call(req);
  } }) };
  const write = createBackend({ config: { schema, mutations: [], loaders: ['Todo'] }, native, database: writerDatabase, authenticate: () => 'alice', mutations: { async find() { return { todo: { id: 'lock' } }; }, async edit({ ctx }) {
    writerPid = Number((await database.driver.query(ctx.tx, 'SELECT pg_backend_pid() AS pid', []))[0].pid);
    await database.driver.query(ctx.tx, 'UPDATE action_todo SET title=$1 WHERE id=$2', ['new', 'lock']);
    return { todo: { id: 'lock' } };
  } }, loaders: { async todo({ tx }) { return database.driver.query(tx, 'SELECT id,title FROM action_todo WHERE id=$1', ['lock']); } } });
  const body = (clientId, callId, name, args) => JSON.stringify({ clientId, batchSequence: 1, models: { Todo: 1 }, mutations: [{ ordinal: 1, callId, name, version: 1, args }] });
  try {
    const reader = read.push('alice', body('locked-read', '01890f47-1234-7123-8123-123456789ac8', 'Find', {}));
    await loading;
    const writer = write.push('alice', body('locked-write', '01890f47-1234-7123-8123-123456789ac9', 'Edit', { todo: { id: 'lock', title: 'new' } }));
    await advancing;
    let waitedOnLock = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      const rows = await db.$queryRawUnsafe('SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1', writerPid);
      if (rows[0]?.wait_event_type === 'Lock') { waitedOnLock = true; break; }
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    assert.equal(waitedOnLock, true, 'changed-record writer waits on the read-only output stamp lock');
    releaseLoad();
    const [readReceipt, writeReceipt] = await Promise.all([reader, writer].map(async promise => JSON.parse(await promise)));
    assert.equal(readReceipt.completions[0].outcome.result.todo.title, 'old');
    assert.equal(readReceipt.records[0].stamp, 1);
    assert.equal(readReceipt.records[0].state.title, 'old');
    assert.equal(writeReceipt.completions[0].outcome.result.todo.title, 'new');
    assert.equal(writeReceipt.records[0].stamp, 2);
    assert.equal(writeReceipt.records[0].state.title, 'new');
  } finally {
    releaseLoad?.();
    await pool.end();
  }
});

test('store policy is part of the saved call identity and replays without Loader work', async () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const database = pg(pool);
  const schema = { enums: [], models: config.schema.models, resultModels: config.schema.resultModels, actions: [
    { name: 'Find', version: 1, inputs: [], outputs: [{ name: 'todo', kind: 'model', model: 'Todo', modelReadVersion: 1, cardinality: 'single', source: 'handlerIdentity', handlerType: { kind: 'identity', model: 'Todo', fields: [{ name: 'id', type: { kind: 'scalar', name: 'string' } }] } }] },
  ] };
  let found = 0, read = 0;
  const app = createBackend({ config: { schema, mutations: [], loaders: ['Todo'] }, native, database, authenticate: () => 'alice',
    mutations: { async find() { found++; return { todo: { id: 'store-a' } }; } },
    loaders: { async todo({ tx, ids }) { read++; return Promise.all(ids.map(async ({ id }) => (await database.driver.query(tx, 'SELECT id,title FROM action_todo WHERE id=$1', [id]))[0] ?? null)); } } });
  const batch = (clientId, calls) => JSON.stringify({ clientId, batchSequence: 1, models: { Todo: 1 }, mutations: calls.map((call, index) => ({ ordinal: index + 1, name: 'Find', version: 1, args: {}, ...call })) });
  const stamps = async () => (await db.$queryRawUnsafe("SELECT stamp FROM axton_record WHERE model='Todo' AND identity_key=$1", '{"id":"store-a"}')).length;
  const callId = '01890f47-1234-7123-8123-1234567890f0';
  try {
    await db.$executeRawUnsafe("INSERT INTO action_todo(id,title) VALUES('store-a','SA')");
    const first = JSON.parse(await app.push('alice', batch('store-first', [{ callId, store: false }])));
    assert.equal(first.completions[0].outcome.result.todo.title, 'SA', 'the Loader snapshot is returned');
    assert.deepEqual(first.records, [], 'no output-only authority');
    assert.equal(await stamps(), 0, 'no output-only stamp allocation');
    const [saved] = await db.$queryRawUnsafe('SELECT request FROM axton_call WHERE call_id=$1', callId);
    assert.equal(JSON.parse(saved.request).store, false, 'the policy is part of the saved identity');
    await db.$executeRawUnsafe("UPDATE action_todo SET title='SA2' WHERE id='store-a'");
    const handled = found, loaded = read;
    const replay = JSON.parse(await app.push('alice', batch('store-replay', [{ callId, store: false }])));
    assert.deepEqual(replay.completions, first.completions, 'lost response replays the saved result');
    assert.deepEqual(replay.records, first.records);
    for (const store of [undefined, true, { todo: false }]) {
      const conflict = JSON.parse(await app.push('alice', batch(`store-conflict-${JSON.stringify(store)}`, [{ callId, ...(store === undefined ? {} : { store }) }])));
      assert.equal(conflict.completions[0].outcome.code, 'call.identity_conflict');
    }
    assert.equal(found, handled, 'Handler was not rerun');
    assert.equal(read, loaded, 'Loader was not rerun');
    const mixed = JSON.parse(await app.push('alice', batch('store-mixed', [
      { callId: '01890f47-1234-7123-8123-1234567890f1', store: { nope: false } },
      { callId: '01890f47-1234-7123-8123-1234567890f2', store: { todo: true } },
    ])));
    assert.deepEqual(mixed.rejections, [{ ordinal: 1, code: 'action.invalid' }], 'an invalid key rejects only its own call');
    assert.equal(mixed.completions[1].outcome.result.todo.title, 'SA2');
    assert.deepEqual(mixed.records.map(record => [record.identity.id, record.state.title]), [['store-a', 'SA2']], 'enabled output stores its authority');
    assert.equal(await stamps(), 1);
  } finally {
    await pool.end();
  }
});

test('a forged Query settlement rolls back its own transaction writes and keeps adjacent calls', async () => {
  // The TypeScript runtime gives a Query no touch or Channel handles. A host that forges
  // them anyway (a defect or another language host) is refused by the shared
  // Rust executor; this wrapper forges them on the settlement it forwards.
  const forging = {
    ...native,
    processAction: (config, owner, request, callback) => native.processAction(config, owner, request, forge(callback)),
    processPush: (config, owner, request, callback) => native.processPush(config, owner, request, forge(callback)),
  };
  const forge = callback => async raw => {
    const answer = await callback(raw);
    const request = JSON.parse(raw);
    if (request.op !== 'handleAction' || request.name !== 'Leak') return answer;
    const settled = JSON.parse(answer);
    const leaked = { model: 'Todo', identity: { id: `leak-${request.callId}` } };
    return JSON.stringify({ ...settled, changes: [leaked], memberships: [{ channel: 'todos', ...leaked, present: true }] });
  };
  const leakConfig = { schema: { ...config.schema, actions: [
    ...config.schema.actions,
    { name: 'Leak', version: 1, kind: 'query', inputs: [], outputs: [{ name: 'n', kind: 'value', type: { kind: 'scalar', name: 'int' }, cardinality: 'single', source: 'handlerValue' }] },
  ] }, mutations: [], loaders: ['Todo'] };
  const { shims, close } = openShims();
  try {
    for (const [index, { name, database }] of shims.entries()) {
      const leak = async ({ ctx }) => {
        // A same-transaction write the Query contract forbids; it must roll back.
        await database.driver.query(ctx.tx, 'INSERT INTO action_todo(id,title) VALUES($1,$2)', [`leak-${ctx.callId}`, 'forbidden']);
        return { n: 1 };
      };
      const add = async ({ ctx, args }) => { await database.driver.query(ctx.tx, 'INSERT INTO action_todo(id,title) VALUES($1,$2)', [args.todo.id, args.todo.title]); return { todo: { id: args.todo.id } }; };
      const todo = async ({ tx, ids }) => Promise.all(ids.map(async ({ id }) => (await database.driver.query(tx, 'SELECT id,title FROM action_todo WHERE id=$1', [id]))[0] ?? null));
      const app = createBackend({ config: leakConfig, native: forging, database, authenticate: () => 'alice', mutations: { add }, queries: { leak }, loaders: { todo } });
      const ids = [0, 1, 2].map(n => `01890f47-1234-7123-8123-1234567891${index}${n}`);
      const receipt = JSON.parse(await app.push('alice', JSON.stringify({ clientId: `forged-${name}`, batchSequence: 1, models: { Todo: 1 }, mutations: [
        { ordinal: 1, callId: ids[0], name: 'Leak', version: 1, args: {} },
        { ordinal: 2, callId: ids[1], name: 'Add', version: 1, args: { todo: { id: `kept-${name}`, title: 'K' } } },
      ] })));
      assert.deepEqual(receipt.rejections, [{ ordinal: 1, code: 'query.effects_forbidden' }], name);
      assert.equal(receipt.completions[1].outcome.status, 'succeeded', name);
      assert.deepEqual(receipt.records.map(record => record.identity.id), [`kept-${name}`], name);
      const direct = JSON.parse(await app.action('alice', JSON.stringify({ call: { callId: ids[2], name: 'Leak', version: 1, args: {} }, models: { Todo: 1 } })));
      assert.equal(direct.completion.outcome.code, 'query.effects_forbidden', name);
      assert.deepEqual(await db.$queryRawUnsafe("SELECT id FROM action_todo WHERE id LIKE 'leak-%'"), [], `${name}: forbidden Query writes rolled back`);
      assert.deepEqual(await db.$queryRawUnsafe('SELECT title FROM action_todo WHERE id=$1', `kept-${name}`), [{ title: 'K' }], name);
      assert.deepEqual(await db.$queryRawUnsafe("SELECT count(*)::int AS n FROM axton_record WHERE identity_key LIKE '%leak-%'"), [{ n: 0 }], `${name}: no stamp for a forged change`);
      // The rejection is the saved outcome: a retry replays it without running the handler.
      const retried = JSON.parse(await app.action('alice', JSON.stringify({ call: { callId: ids[2], name: 'Leak', version: 1, args: {} }, models: { Todo: 1 } })));
      assert.deepEqual(retried, direct, name);
    }
  } finally {
    await close();
  }
});
