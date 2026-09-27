// Native Load pages against real PostgreSQL through every shim: each batch
// item runs and commits in its own application transaction, a repeated page
// call ID replays its saved outcome, and transaction faults are retryable items.
import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { Pool } from 'pg';
import { drizzle as drizzleOrm } from 'drizzle-orm/node-postgres';
import { createBackend, devAuth, MutationRejected } from '../../../packages/server/index.mts';
import { prisma, pg, drizzle, answer } from '../../../packages/postgres/index.mts';
import { READ_STAMPS } from '../../../packages/postgres/src/sql.mts';
const require = createRequire(import.meta.url);
const { PrismaClient } = require('../../bindings/node/generated/client');
const native = require('../../../bindings/node/axton-node.node');
const db = new PrismaClient();
const check = new Pool({ connectionString: process.env.DATABASE_URL });
const q = async (sql, params = []) => (await check.query(sql, params)).rows;

const fields = [
  { name: 'id', type: { kind: 'scalar', name: 'string' }, nullable: false },
  { name: 'title', type: { kind: 'scalar', name: 'string' }, nullable: false },
];
const todos = { name: 'todos', kind: 'model', cardinality: 'list', source: 'handlerIdentity', model: 'Todo', modelReadVersion: 1, handlerType: { kind: 'identity', model: 'Todo', fields: [{ name: 'id', type: { kind: 'scalar', name: 'string' } }] } };
const config = { schema: {
  enums: [], models: [{ name: 'Todo', version: 1, identity: ['id'], fields }],
  resultModels: [{ name: 'Todo', version: 1, identity: ['id'], fields, enums: [] }],
  actions: [],
  loads: [{ name: 'ProjectTodos', version: 1, inputs: [{ kind: 'value', name: 'projectId', type: { kind: 'scalar', name: 'string' }, nullable: false, list: false, required: true, cardinality: 'single' }], outputs: [todos], input: { models: [], enums: [] }, outputEnums: [] }],
}, mutations: [], loaders: ['Todo'] };

const uuid = n => `01890f47-1234-7123-8123-${n.toString(16).padStart(12, '0')}`;
let next = 0x10000;
/** One page of `ProjectTodos(projectId)` with fresh IDs unless given. */
const page = (projectId, { continuation = null, loadId = uuid(next++), callId = uuid(next++) } = {}) =>
  ({ loadId, callId, name: 'ProjectTodos', version: 1, args: { projectId }, continuation, models: { Todo: 1 } });
const batch = (...items) => JSON.stringify({ loads: items });

/**
 * The page handler over `load_todo`: two identities per page in id order
 * after the continuation, only the caller's own rows. `closed` writes an
 * audit row and then rejects; every call records its transaction ID.
 */
const handlers = (database, seen) => ({
  async projectTodos({ ctx, args, continuation }) {
    const [{ id: txid }] = await database.driver.query(ctx.tx, 'SELECT txid_current()::text AS id', []);
    seen.handled.push({ callId: ctx.callId, loadId: ctx.loadId, userId: ctx.userId, continuation, txid, keys: Object.keys(ctx).sort() });
    await database.driver.query(ctx.tx, 'INSERT INTO load_audit(note) VALUES($1)', [`${args.projectId}:${ctx.callId}`]);
    if (args.projectId === 'closed') throw new MutationRejected('project.closed');
    const rows = await database.driver.query(ctx.tx, 'SELECT id FROM load_todo WHERE project=$1 AND owner_id=$2 AND id>$3 ORDER BY id LIMIT 2', [args.projectId, ctx.userId, continuation?.state?.after ?? '']);
    return { data: { todos: rows.map(row => ({ id: row.id })) }, next: rows.length < 2 ? null : { state: { after: rows.at(-1).id } } };
  },
});
const loaders = (database, seen) => ({
  async todo({ tx, ids, userId }) {
    const [{ id: txid }] = await database.driver.query(tx, 'SELECT txid_current()::text AS id', []);
    seen.loaded.push({ ids: ids.length, txid });
    const rows = await database.driver.query(tx, 'SELECT id,title FROM load_todo WHERE owner_id=$1 AND id = ANY(SELECT jsonb_array_elements_text($2::jsonb))', [userId, JSON.stringify(ids.map(({ id }) => id))]);
    const byId = new Map(rows.map(row => [row.id, row]));
    return ids.map(({ id }) => byId.get(id) ?? null);
  },
});
const backend = (database, seen = { handled: [], loaded: [] }, extra = {}) =>
  createBackend({ config, native, database, authenticate: devAuth(), loads: handlers(database, seen), loaders: loaders(database, seen), onError: () => {}, ...extra });

const shims = [];
before(async () => {
  for (const sql of (await readFile(new URL('../../../packages/postgres/migration.sql', import.meta.url), 'utf8')).split(';').map(s => s.trim()).filter(Boolean)) await q(sql);
  await q('CREATE TABLE load_todo(id text PRIMARY KEY, project text NOT NULL, owner_id text NOT NULL, title text NOT NULL)');
  await q('CREATE TABLE load_audit(id serial PRIMARY KEY, note text NOT NULL)');
  const poolPg = new Pool({ connectionString: process.env.DATABASE_URL });
  const poolDrizzle = new Pool({ connectionString: process.env.DATABASE_URL });
  shims.push({ name: 'prisma', database: prisma(db), close: async () => {} });
  shims.push({ name: 'pg', database: pg(poolPg), close: () => poolPg.end() });
  shims.push({ name: 'drizzle', database: drizzle(drizzleOrm(poolDrizzle)), close: () => poolDrizzle.end() });
});
after(async () => { for (const shim of shims) await shim.close(); await db.$disconnect(); await check.end(); });

const seed = async (prefix, project, owner, count) => {
  for (let n = 1; n <= count; n++) await q('INSERT INTO load_todo(id,project,owner_id,title) VALUES($1,$2,$3,$4)', [`${prefix}-${n}`, project, owner, `${prefix} ${n}`]);
};
const saved = async callId => (await q('SELECT owner_id, response FROM axton_call WHERE call_id=$1', [callId])).map(row => ({ owner: row.owner_id, response: row.response && JSON.parse(row.response) }));
const outcomes = text => JSON.parse(text).loads;

test('each batch item is its own transaction: a rejected handler rolls back its writes while its sibling commits, on every shim', async () => {
  for (const { name, database } of shims) {
    await seed(`iso-${name}`, `open-${name}`, 'alice', 3);
    const seen = { handled: [], loaded: [] };
    const app = backend(database, seen);
    const good = page(`open-${name}`), bad = page('closed'), unknown = { ...page(`open-${name}`), name: 'Missing' };
    const [ok, rejected, missing] = outcomes(await app.loads('alice', batch(good, bad, unknown)));
    assert.equal(ok.callId, good.callId, name);
    assert.equal(ok.outcome.status, 'succeeded', name);
    assert.deepEqual(ok.outcome.data, { todos: [{ id: `iso-${name}-1` }, { id: `iso-${name}-2` }] }, name);
    assert.deepEqual(ok.outcome.next, { state: { after: `iso-${name}-2` } }, name);
    assert.deepEqual(ok.records.map(record => [record.identity.id, record.stamp, record.state.title]), [[`iso-${name}-1`, 1, `iso-${name} 1`], [`iso-${name}-2`, 1, `iso-${name} 2`]], name);
    assert.deepEqual(rejected.outcome, { status: 'failed', error: { code: 'project.closed', message: 'project.closed' } }, name);
    assert.deepEqual(missing.outcome.error.code, 'load_version_unsupported', `${name}: an unknown Load fails only its own item`);
    // The rejected page's audit write rolled back to its savepoint; its sibling's committed.
    assert.deepEqual(await q('SELECT note FROM load_audit WHERE note=$1', [`closed:${bad.callId}`]), [], name);
    assert.equal((await q('SELECT note FROM load_audit WHERE note=$1', [`open-${name}:${good.callId}`])).length, 1, name);
    for (const item of [good, bad, unknown]) assert.equal((await saved(item.callId)).length, 1, `${name}: every terminal outcome is saved`);
    // Same-transaction identity: the handler and its Loader read one transaction; siblings never share it.
    const handled = new Map(seen.handled.map(call => [call.callId, call.txid]));
    assert.equal(seen.loaded.length, 1, name);
    assert.equal(seen.loaded[0].txid, handled.get(good.callId), `${name}: Loader runs in the page's transaction`);
    assert.notEqual(handled.get(good.callId), handled.get(bad.callId), `${name}: batch items share no transaction`);
    assert.deepEqual(seen.handled[0].keys, ['callId', 'loadId', 'tx', 'userId'], `${name}: read-only Load context`);
    assert.equal(seen.handled.find(call => call.callId === good.callId).loadId, good.loadId);
  }
});

test('a repeated page call ID replays its exact saved page after business rows change, without handler, Loader or new stamps', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('replay', 'replay', 'alice', 3);
  const seen = { handled: [], loaded: [] };
  const app = backend(database, seen);
  const first = page('replay');
  const [original] = outcomes(await app.loads('alice', batch(first)));
  const second = page('replay', { loadId: first.loadId, continuation: original.outcome.next });
  const [last] = outcomes(await app.loads('alice', batch(second)));
  assert.deepEqual(last.outcome, { status: 'succeeded', data: { todos: [{ id: 'replay-3' }] }, next: null });
  await q("UPDATE load_todo SET title='changed' WHERE project='replay'");
  await q("INSERT INTO load_todo(id,project,owner_id,title) VALUES('replay-0','replay','alice','new')");
  const stamps = await q("SELECT identity_key, stamp, xmin::text FROM axton_record WHERE identity_key LIKE '%replay-%' ORDER BY identity_key");
  const handled = seen.handled.length, loaded = seen.loaded.length;
  const [replayed] = outcomes(await app.loads('alice', batch({ ...first, callId: first.callId.toUpperCase() })));
  assert.deepEqual(replayed, original, 'the saved data, next and authority');
  assert.equal(replayed.records[0].state.title, 'replay 1');
  assert.equal(seen.handled.length, handled, 'no handler');
  assert.equal(seen.loaded.length, loaded, 'no Loader');
  assert.deepEqual(await q("SELECT identity_key, stamp, xmin::text FROM axton_record WHERE identity_key LIKE '%replay-%' ORDER BY identity_key"), stamps, 'no stamp written');
  const [conflict] = outcomes(await app.loads('alice', batch({ ...first, continuation: { state: null } })));
  assert.equal(conflict.outcome.error.code, 'call.identity_conflict');
  assert.equal((await saved(first.callId))[0].response.outcome.status, 'succeeded', 'a conflict saves nothing over the original');
});

test('another principal cannot retrieve a saved page by its IDs', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('owner-a', 'shared', 'alice', 2);
  await seed('owner-b', 'shared', 'bob', 1);
  const seen = { handled: [], loaded: [] };
  const app = backend(database, seen);
  const item = page('shared');
  const [alice] = outcomes(await app.loads('alice', batch(item)));
  const [bob] = outcomes(await app.loads('bob', batch(item)));
  assert.deepEqual(bob.outcome.data, { todos: [{ id: 'owner-b-1' }] }, "bob's own execution, never alice's saved page");
  assert.equal(seen.handled.at(-1).userId, 'bob');
  assert.deepEqual((await saved(item.callId)).map(row => row.owner).sort(), ['alice', 'bob'], 'claims are owner scoped');
  const [again] = outcomes(await app.loads('alice', batch(item)));
  assert.deepEqual(again, alice);
});

test('an unavailable identity fails the whole page and keeps none of its stamps; existing stamps are read without rewriting', async () => {
  for (const { name, database } of shims) {
    await seed(`stamp-${name}`, `stamp-${name}`, 'alice', 2);
    const key = id => JSON.stringify({ id });
    const existing = key(`stamp-${name}-1`);
    const answerIn = request => database.transaction(tx => answer(database.driver, tx, request));
    await answerIn({ op: 'advanceStamp', model: 'Todo', identityKey: existing });
    await answerIn({ op: 'advanceStamp', model: 'Todo', identityKey: existing });
    const [{ xmin }] = await q("SELECT xmin::text FROM axton_record WHERE model='Todo' AND identity_key=$1", [existing]);
    const stamps = await answerIn({ op: 'readStamps', model: 'Todo', identityKeys: [key(`stamp-${name}-2`), existing, key(`stamp-${name}-9`)] });
    assert.deepEqual(stamps, [1, 2, 1], `${name}: request order, existing kept, missing at 1`);
    assert.deepEqual(await q("SELECT xmin::text FROM axton_record WHERE model='Todo' AND identity_key=$1", [existing]), [{ xmin }], `${name}: the existing row was not rewritten`);
    await q("DELETE FROM axton_record WHERE identity_key IN ($1,$2)", [key(`stamp-${name}-2`), key(`stamp-${name}-9`)]);

    const app = createBackend({ config, native, database, authenticate: () => 'alice', onError: () => {}, loaders: loaders(database, { handled: [], loaded: [] }),
      loads: { async projectTodos() { return { data: { todos: [{ id: `stamp-${name}-2` }, { id: `stamp-${name}-gone` }] }, next: null }; } } });
    const [failed] = outcomes(await app.loads('alice', batch(page('any'))));
    assert.equal(failed.outcome.error.code, 'load.record_unavailable', name);
    assert.deepEqual(await q('SELECT identity_key FROM axton_record WHERE identity_key IN ($1,$2)', [key(`stamp-${name}-2`), key(`stamp-${name}-gone`)]), [], `${name}: stamps rolled back`);
  }
});

test('a 1,000-identity page resolves with one stamp and one Loader round trip', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await q("INSERT INTO load_todo(id,project,owner_id,title) SELECT 'bulk-'||lpad(n::text,4,'0'),'bulk','alice','B' FROM generate_series(1,1000) n");
  const calls = [];
  const counted = { ...database, persistence: tx => ({ call: request => { calls.push(request.op); return database.persistence(tx).call(request); } }) };
  const seen = { handled: [], loaded: [] };
  const app = createBackend({ config, native, database: counted, authenticate: () => 'alice', loaders: loaders(database, seen),
    loads: { async projectTodos({ ctx }) { const rows = await database.driver.query(ctx.tx, "SELECT id FROM load_todo WHERE project='bulk' ORDER BY id", []); return { data: { todos: rows.map(row => ({ id: row.id })) }, next: null }; } } });
  const [bulk] = outcomes(await app.loads('alice', batch(page('bulk'))));
  assert.equal(bulk.outcome.status, 'succeeded');
  assert.equal(bulk.records.length, 1000);
  assert.deepEqual(calls, ['claimCall', 'savepoint', 'readStamps', 'release', 'saveCall']);
  assert.deepEqual(seen.loaded.map(call => call.ids), [1000]);
});

test('transaction faults are retryable items with nothing saved, and a resend lets replay decide what committed', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('fault', 'fault', 'alice', 1);
  const reported = [];
  const seen = { handled: [], loaded: [] };
  // COMMIT connection loss: the page committed, but the carrier cannot know.
  const lostCommit = { ...database, transaction: async body => { await database.transaction(body); throw new Error('Connection terminated unexpectedly'); } };
  const item = page('fault');
  const [lost] = outcomes(await backend(lostCommit, seen, { onError: error => reported.push(error) }).loads('alice', batch(item)));
  assert.deepEqual(lost, { loadId: item.loadId, callId: item.callId, outcome: { status: 'retryable', error: { code: 'server.unavailable', message: 'the page transaction did not complete; resend the same call ID' } }, records: [] });
  const handled = seen.handled.length;
  const [resent] = outcomes(await backend(database, seen).loads('alice', batch(item)));
  assert.equal(resent.outcome.status, 'succeeded');
  assert.equal(seen.handled.length, handled, 'the committed page replays instead of running again');
  // Pool timeout: nothing ran, nothing was claimed.
  const timedOut = { ...database, transaction: async () => { throw new Error('timeout exceeded when trying to connect'); } };
  const other = page('fault');
  const [pool] = outcomes(await backend(timedOut, seen, { onError: error => reported.push(error) }).loads('alice', batch(other)));
  assert.equal(pool.outcome.status, 'retryable');
  assert.deepEqual(await saved(other.callId), []);
  // A serialization conflict the driver gave up on is retryable too.
  const conflictPool = new Pool({ connectionString: process.env.DATABASE_URL });
  try {
    const noRetries = pg(conflictPool, { retries: 0 });
    const conflicted = createBackend({ config, native, database: noRetries, authenticate: () => 'alice', onError: error => reported.push(error), loaders: loaders(noRetries, seen),
      loads: { async projectTodos() { throw Object.assign(new Error('could not serialize access'), { code: '40001' }); } } });
    const third = page('fault');
    const [serialization] = outcomes(await conflicted.loads('alice', batch(third)));
    assert.equal(serialization.outcome.status, 'retryable');
    assert.equal(serialization.outcome.error.code, 'transaction.conflict');
    assert.deepEqual(await saved(third.callId), []);
  } finally { await conflictPool.end(); }
  // A deterministic host defect is an unsaved failed item, not an endless retry.
  const broken = { ...database, persistence: tx => ({ call: async request => request.op === 'readStamps' ? [] : database.persistence(tx).call(request) }) };
  const fourth = page('fault');
  const [defect] = outcomes(await backend(broken, seen, { onError: error => reported.push(error) }).loads('alice', batch(fourth)));
  assert.equal(defect.outcome.status, 'failed');
  assert.equal(defect.outcome.error.code, 'host.invalid');
  assert.deepEqual(await saved(fourth.callId), []);
  assert.equal(reported.length, 4, 'every fault reaches onError');
});

test('an inconsistent readStamps answer is a deterministic defect, never a retryable fault', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('stale', 'stale', 'alice', 2);
  // The persistence looks each stamp up by key: reordered rows still answer
  // in request order, and a missing row answers `null` in its position.
  const tamper = change => ({ ...database.driver, query: async (tx, sql, params) => {
    const rows = await database.driver.query(tx, sql, params);
    return sql === READ_STAMPS ? change(rows) : rows;
  } });
  const keys = ['stale-1', 'stale-2'].map(id => JSON.stringify({ id }));
  const read = (driver, request) => database.transaction(tx => answer(driver, tx, request));
  assert.deepEqual(await read(tamper(rows => [...rows].reverse()), { op: 'readStamps', model: 'Todo', identityKeys: keys }), [1, 1]);
  assert.deepEqual(await read(tamper(rows => rows.slice(1)), { op: 'readStamps', model: 'Todo', identityKeys: keys }), [null, 1]);
  assert.deepEqual(await read(tamper(rows => rows.map(row => ({ ...row, stamp: 0 }))), { op: 'readStamps', model: 'Todo', identityKeys: keys }), [null, null]);
  assert.deepEqual(await read(database.driver, { op: 'readStamps', model: 'Todo', identityKeys: [keys[0], keys[0]] }), [], 'a request the engine cannot send');
  // End to end, the engine refuses the answer as `host.invalid`: an unsaved
  // failed item the client stops on.
  const missing = tamper(rows => rows.slice(1));
  const broken = { ...database, persistence: tx => ({ call: request => answer(missing, tx, request) }) };
  const reported = [];
  const app = createBackend({ config, native, database: broken, authenticate: () => 'alice', onError: error => reported.push(error), loaders: loaders(database, { handled: [], loaded: [] }),
    loads: { async projectTodos() { return { data: { todos: [{ id: 'stale-1' }, { id: 'stale-2' }] }, next: null }; } } });
  const item = page('stale');
  const [defect] = outcomes(await app.loads('alice', batch(item)));
  assert.equal(defect.outcome.status, 'failed');
  assert.equal(defect.outcome.error.code, 'host.invalid');
  assert.deepEqual(await saved(item.callId), []);
  assert.equal(reported.length, 1);
});

test('at most four item transactions of one request run at once', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  // Each handler holds its transaction long enough for every item the
  // carrier would admit to arrive, so the peak is the carrier's bound.
  let active = 0, peak = 0;
  const app = createBackend({ config, native, database, authenticate: () => 'alice', loaders: loaders(database, { handled: [], loaded: [] }),
    loads: { async projectTodos() {
      active++; peak = Math.max(peak, active);
      await new Promise(resolve => setTimeout(resolve, 250));
      active--;
      return { data: { todos: [] }, next: null };
    } } });
  const items = Array.from({ length: 8 }, () => page('none'));
  const answered = outcomes(await app.loads('alice', batch(...items)));
  assert.equal(peak, 4);
  assert.deepEqual(answered.map(item => item.callId), items.map(item => item.callId), 'answers keep request order');
  assert.ok(answered.every(item => item.outcome.status === 'succeeded'));
});

test('POST /sync/loads authenticates once, refuses a malformed envelope whole and answers items independently', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('http', 'http', 'alice', 1);
  const app = backend(database);
  const listening = await app.listen({ port: 0 });
  const send = (body, authorization = 'Bearer alice') => fetch(`${listening.url}/sync/loads`, { method: 'POST', headers: authorization ? { authorization } : {}, body });
  try {
    assert.equal((await send(batch(page('http')), null)).status, 401);
    const nine = Array.from({ length: 9 }, () => page('http'));
    const duplicate = page('http');
    for (const body of [batch(), batch(...nine), batch(duplicate, { ...page('http'), callId: duplicate.callId }), JSON.stringify({ loads: [page('http')], extra: 1 })]) {
      const refused = await send(body);
      assert.equal(refused.status, 400, body.slice(0, 60));
      assert.deepEqual(await refused.json(), { code: 'request.invalid' });
    }
    const good = page('http'), bad = { ...page('http'), args: { projectId: 7 } }, invalidState = page('http', { continuation: { state: 2 ** 53 } });
    const response = await send(batch(good, bad, invalidState));
    assert.equal(response.status, 200);
    const [ok, invalid, state] = (await response.json()).loads;
    assert.equal(ok.outcome.status, 'succeeded');
    assert.equal(invalid.outcome.error.code, 'load.invalid');
    assert.equal(state.outcome.error.code, 'load.invalid_continuation');
  } finally {
    await listening.close();
  }
});

test('Load registration names every retained version and keeps wrong-kind diagnostics accurate', () => {
  const database = { transaction: body => body({}), persistence: () => ({ call: async () => null }) };
  const base = { config, native, database, authenticate: () => 'alice', loaders: { todo: async () => [] } };
  assert.throws(() => createBackend(base), /Missing load projectTodos for ProjectTodos v1/);
  assert.throws(() => createBackend({ ...base, loads: { projectTodos: { v2: async () => ({}) } } }), /Missing load projectTodos\.v1 for ProjectTodos v1/);
  assert.throws(() => createBackend({ ...base, loads: { projectTodos: async () => ({}), other: async () => ({}) } }), /Unknown load other: no retained load other/);
  assert.throws(() => createBackend({ ...base, loads: { projectTodos: async () => ({}) }, queries: { projectTodos: async () => ({}) } }), /queries\.projectTodos: ProjectTodos v1 \(load\) retains no query version; register it under loads/);
  assert.throws(() => createBackend({ ...base, loads: { projectTodos: async () => ({}) }, handlers: { projectTodos: async () => {} } }), /Handler projectTodos names ProjectTodos v1 \(load\); register each version under mutations, queries or loads by its kind/);
  const mixed = structuredClone(config);
  mixed.schema.actions = [{ name: 'Ping', version: 1, inputs: [], outputs: [] }];
  assert.throws(() => createBackend({ ...base, config: mixed, mutations: { ping: async () => {} }, loads: { projectTodos: async () => ({}), ping: async () => ({}) } }), /loads\.ping: Ping v1 \(mutation\) retains no load version; register it under mutations/);
});
