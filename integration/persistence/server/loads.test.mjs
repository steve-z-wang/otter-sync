// Native Load pages against real PostgreSQL through every shim: each batch
// item runs and commits in its own application transaction, a repeated page
// call ID replays its saved outcome, and transaction faults are retryable items.
// A page's Channel enrollment commits, fails, wakes and serializes with it;
// every durable effect is read back through an independent connection.
import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { Pool } from 'pg';
import { drizzle as drizzleOrm } from 'drizzle-orm/node-postgres';
import { createBackend, devAuth, MutationRejected, WebSocket } from '../../../packages/server/index.mts';
import { prisma, pg, answer } from '../../../packages/postgres/index.mts';
import { drizzle } from '../../../packages/postgres/src/drizzle.mts';
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
const batch = (...items) => JSON.stringify({ capabilities:['channel-membership-v1'],loads: items });

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
  await q(await readFile(new URL('../../../packages/postgres/migration.sql', import.meta.url), 'utf8'));
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
    // The items run in concurrent Serializable transactions, so PostgreSQL may
    // abort one and the driver run it again (#202): judge the runs in the
    // transaction that saved each page (`claim_tx`), and require every other
    // run to be in a transaction that did not commit it.
    const committed = async callId => (await q('SELECT claim_tx::text AS txid FROM axton_call WHERE call_id=$1', [callId]))[0].txid;
    const [goodTx, badTx] = [await committed(good.callId), await committed(bad.callId)];
    const runsIn = (runs, txid) => runs.filter(run => run.txid === txid).length;
    assert.equal(runsIn(seen.handled.filter(call => call.callId === good.callId), goodTx), 1, `${name}: one handler run in the page's committed transaction`);
    assert.equal(runsIn(seen.loaded, goodTx), 1, `${name}: Loader runs in the page's transaction, once`);
    assert.equal(runsIn(seen.handled.filter(call => call.callId === bad.callId), badTx), 1, name);
    assert.notEqual(goodTx, badTx, `${name}: batch items share no transaction`);
    assert.equal(runsIn(seen.loaded, badTx), 0, `${name}: the rejected page read nothing`);
    assert.deepEqual(seen.handled[0].keys, ['callId', 'channel', 'loadId', 'tx', 'userId'], `${name}: a Load context adds to Channels and has no touch`);
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
    for (const body of [batch(), batch(...nine), batch(duplicate, { ...page('http'), callId: duplicate.callId }), JSON.stringify({ capabilities:['channel-membership-v1'],loads: [page('http')], extra: 1 })]) {
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

/** Every Channel a Todo belongs to, sorted. */
const channelsOf = async id => (await q("SELECT m.channel FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id WHERE r.model='Todo' AND r.identity_key=$1 ORDER BY m.channel", [JSON.stringify({ id })])).map(row => row.channel);
/** Records which of `channels` wake, until `stop`. */
const listen = (app, channels) => { const woken = []; const stops = channels.map(channel => app.onCommitted(channel, () => woken.push(channel))); return { woken, stop: () => stops.forEach(stop => stop()) }; };
/** One page over `enroll-*` rows whose handler is `body({ ctx, rows })`. */
const enrolling = (database, body, extra = {}) => createBackend({ config, native, database, authenticate: () => 'alice', onError: () => {}, loaders: loaders(database, { handled: [], loaded: [] }), ...extra,
  loads: { async projectTodos({ ctx, args }) {
    const rows = await database.driver.query(ctx.tx, 'SELECT id FROM load_todo WHERE project=$1 ORDER BY id', [args.projectId]);
    await body({ ctx, rows });
    return { data: { todos: rows.map(row => ({ id: row.id })) }, next: null };
  } } });

test('a Load enrolls the records it declares on every shim, waking the Channel after commit; a replay enrolls nothing', async () => {
  for (const { name, database } of shims) {
    const project = `enroll-${name}`;
    await seed(project, project, 'alice', 3);
    const channel = `project:${project}`;
    let runs = 0;
    const app = enrolling(database, ({ ctx, rows }) => {
      runs++;
      // Only the declared subset: the third returned row is not enrolled.
      ctx.channel(channel).todo.add({ id: rows[0].id });
      ctx.channel(channel).add([{ model: 'Todo', identity: { id: rows[1].id } }, { model: 'Todo', identity: { id: rows[0].id } }]);
    });
    const wakes = listen(app, [channel]);
    const item = page(project);
    const [done] = outcomes(await app.loads('alice', batch(item)));
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(done.outcome.status, 'succeeded', name);
    assert.deepEqual(done.memberships, [1, 2].map((cursor) => ({channel, cursor, model: 'Todo', identity: {id: `${project}-${cursor}`}})), name);
    assert.deepEqual(await channelsOf(`${project}-1`), [channel], name);
    assert.deepEqual(await channelsOf(`${project}-2`), [channel], name);
    assert.deepEqual(await channelsOf(`${project}-3`), [], `${name}: returning a record does not enroll it`);
    assert.deepEqual(wakes.woken, [channel], `${name}: one wake, after commit`);
    const [replayed] = outcomes(await app.loads('alice', batch(item)));
    assert.deepEqual(replayed, done, name);
    assert.equal(runs, 1, `${name}: the replay ran no handler`);
    assert.deepEqual(wakes.woken, [channel], `${name}: and woke nobody`);
    wakes.stop();
  }
});

test('a caught overflow or refused declaration fails the saved page with no enrollment and no wake', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('enroll-fail', 'enroll-fail', 'alice', 1);
  const cases = [
    ['overflow', ({ ctx, rows }) => { try { for (let n = 0; n <= 1000; n++) ctx.channel(`fail-${n}`).todo.add({ id: rows[0].id }); } catch {} }, 'load.page_too_large'],
    ['invalid', ({ ctx, rows }) => { ctx.channel('fail-0').todo.add({ id: rows[0].id }); try { ctx.channel('fail-1').todo.add({}); } catch {} }, 'handler.failed'],
    // A saved handler failure, not a retryable host fault: the bridge could not carry the identity.
    ['lone surrogate identity', ({ ctx, rows }) => { ctx.channel('fail-0').todo.add({ id: rows[0].id }); try { ctx.channel('fail-1').todo.add({ id: `${rows[0].id}\ud800` }); } catch {} }, 'handler.failed'],
  ];
  for (const [label, body, code] of cases) {
    const reported = [];
    const app = enrolling(database, body, { onError: error => reported.push(error) });
    const wakes = listen(app, ['fail-0', 'fail-1']);
    const item = page('enroll-fail');
    const [failed] = outcomes(await app.loads('alice', batch(item)));
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(failed.outcome.status, 'failed', label);
    assert.equal(failed.outcome.error.code, code, label);
    assert.equal((await saved(item.callId))[0].response.outcome.status, 'failed', `${label}: a saved terminal outcome`);
    assert.deepEqual(await channelsOf('enroll-fail-1'), [], `${label}: no partial enrollment`);
    assert.deepEqual(wakes.woken, [], `${label}: no wake`);
    assert.equal(reported.length, 1, `${label}: reported`);
    wakes.stop();
  }
});

test('a transaction retry runs the handler with fresh declarations: only the committed attempt enrolls and wakes', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('enroll-retry', 'enroll-retry', 'alice', 1);
  let attempts = 0;
  const app = enrolling(database, ({ ctx, rows }) => {
    attempts++;
    ctx.channel(`retry-${attempts}`).todo.add({ id: rows[0].id });
    if (attempts === 1) throw Object.assign(new Error('could not serialize access'), { code: '40001' });
  });
  const wakes = listen(app, ['retry-1', 'retry-2']);
  const [done] = outcomes(await app.loads('alice', batch(page('enroll-retry'))));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(done.outcome.status, 'succeeded');
  assert.equal(attempts, 2, 'the driver retried the serialization failure');
  assert.deepEqual(await channelsOf('enroll-retry-1'), ['retry-2'], 'the aborted attempt left nothing behind');
  assert.deepEqual(wakes.woken, ['retry-2']);
  wakes.stop();
});

// ---- Enrollment against real PostgreSQL ------------------------------------

const key = id => JSON.stringify({ id });
const settled = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
/** Every durable effect a page can have on `ids` and `channels`: business rows, stamps, memberships, heads and positions. */
const tables = async (ids, channels) => ({
  rows: await q('SELECT id, title FROM load_todo WHERE id = ANY($1) ORDER BY id', [ids]),
  stamps: await q("SELECT identity_key, stamp::int FROM axton_record WHERE model='Todo' AND identity_key = ANY($1) ORDER BY identity_key", [ids.map(key)]),
  members: await q("SELECT m.channel, r.identity_key FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id WHERE r.model='Todo' AND r.identity_key = ANY($1) ORDER BY m.channel, r.identity_key", [ids.map(key)]),
  heads: await q('SELECT channel, head::int FROM axton_channel WHERE channel = ANY($1) ORDER BY channel', [channels]),
  // Each pair's position with its record's current stamp.
  positions: await q('SELECT l.channel, r.identity_key, l.cursor::int, r.stamp::int FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel = ANY($1) ORDER BY l.channel, r.identity_key', [channels]),
});
/** The claimed call: the transaction that claimed it and its saved outcome, if one committed. */
const claimed = async callId => (await q('SELECT claim_tx::text AS tx, response FROM axton_call WHERE call_id=$1', [callId])).map(row => ({ tx: row.tx, response: row.response && JSON.parse(row.response) }));
/** What a Channel delivers from `from`: identity, stamp and title of each change. */
const delivered = async (app, channel, from = 0) =>
  JSON.parse(await app.pull('alice', JSON.stringify({ capabilities:['channel-membership-v1'],cursors: { [channel]: from }, models: { Todo: 1 } }))).changes.map(change => [change.identity.id, change.stamp, change.state?.title ?? null]);

test('on every shim a page commits its enrollment with its saved outcome; a re-add and an existing membership publish nothing', async () => {
  for (const { name, database } of shims) {
    const project = `commit-${name}`;
    await seed(project, project, 'alice', 2);
    const [old, fresh] = [`${project}-1`, `${project}-2`], ids = [old, fresh];
    const [A, B, C] = ['a', 'b', 'c'].map(suffix => `${project}:${suffix}`), channels = [A, B, C];
    let declare, runs = 0;
    const app = enrolling(database, ({ ctx }) => { runs++; declare(ctx.channel); });
    // An old domain row: stamped 3 by earlier changes and already a member of B.
    await q("INSERT INTO axton_record(model, identity_key, stamp) VALUES('Todo', $1, 3)", [key(old)]);
    await app.transaction(async ({ channel }) => { channel(B).todo.add({ id: old }); });
    const wakes = listen(app, channels);
    const load = async (declaration, item = page(project)) => {
      declare = declaration;
      const [answered] = outcomes(await app.loads('alice', batch(item)));
      await settled();
      assert.equal(answered.outcome.status, 'succeeded', name);
      return answered;
    };

    // Initial add: both rows join A in the page's own transaction.
    const first = page(project);
    const added = await load(channel => channel(A).add(ids.map(id => ({ model: 'Todo', identity: { id } }))), first);
    const [saved] = await claimed(first.callId);
    assert.deepEqual(saved.response, added, `${name}: the saved outcome is the answered page`);
    const enrolled = await tables(ids, channels);
    assert.deepEqual(enrolled.stamps, [{ identity_key: key(old), stamp: 3 }, { identity_key: key(fresh), stamp: 1 }], `${name}: the old row keeps its stamp; one without a stamp starts at 1`);
    assert.deepEqual(enrolled.members, [{ channel: A, identity_key: key(old) }, { channel: A, identity_key: key(fresh) }, { channel: B, identity_key: key(old) }], name);
    assert.deepEqual(enrolled.heads, [{ channel: A, head: 2 }, { channel: B, head: 1 }], `${name}: A gained one position per new member; B, which already held the row, none`);
    const at = (tableRows, channel) => tableRows.positions.filter(row => row.channel === channel);
    assert.deepEqual(at(enrolled, B), [{ channel: B, identity_key: key(old), cursor: 1, stamp: 3 }], `${name}: B was not republished`);
    assert.deepEqual(at(enrolled, A).map(row => row.cursor).sort(), [1, 2], name);
    assert.deepEqual(added.records.map(record => [key(record.identity.id), record.stamp]), at(enrolled, A).map(row => [row.identity_key, row.stamp]), `${name}: each position carries the stamp the page answered`);
    assert.deepEqual(wakes.woken, [A], `${name}: one wake, for the Channel the commit published to`);

    // A fresh page re-adding the same members publishes nothing and wakes nobody.
    const again = page(project);
    await load(channel => { channel(A).todo.add({ id: old }); channel(A).todo.add({ id: fresh }); }, again);
    assert.equal((await claimed(again.callId))[0].response.outcome.status, 'succeeded', name);
    assert.deepEqual(await tables(ids, channels), enrolled, `${name}: an idempotent re-add`);
    assert.deepEqual(wakes.woken, [A], name);

    // A second Channel: new positions in C only, at the unchanged stamps.
    await load(channel => { channel(C).add([{ model: 'Todo', identity: { id: fresh } }, { model: 'Todo', identity: { id: old } }]); channel(A).todo.add({ id: old }); });
    const twice = await tables(ids, channels);
    assert.deepEqual(twice.stamps, enrolled.stamps, `${name}: no stamp advanced`);
    assert.deepEqual(twice.heads, [{ channel: A, head: 2 }, { channel: B, head: 1 }, { channel: C, head: 2 }], name);
    assert.deepEqual(at(twice, C).map(row => [row.identity_key, row.stamp]), [[key(old), 3], [key(fresh), 1]], name);
    assert.deepEqual(wakes.woken, [A, C], name);

    // Replaying the first page runs nothing and changes nothing.
    const [replayed] = outcomes(await app.loads('alice', batch(first)));
    await settled();
    assert.deepEqual(replayed, added, name);
    assert.equal(runs, 3, `${name}: the replay ran no handler`);
    assert.deepEqual(await tables(ids, channels), twice, `${name}: the replay settled nothing`);
    assert.deepEqual(wakes.woken, [A, C], `${name}: and woke nobody`);
    wakes.stop();
  }
});

test('held after its outcome is saved, an enrolling page is invisible to another connection and wakes nobody until COMMIT', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('held', 'held', 'alice', 1);
  const channel = 'held', ids = ['held-1'];
  const saving = deferred(), gate = deferred();
  // The persistence answers saveCall, then keeps the transaction open before COMMIT.
  const holding = { ...database, persistence: tx => {
    const storage = database.persistence(tx);
    return { call: async request => { const answered = await storage.call(request); if (request.op === 'saveCall') { saving.resolve(); await gate.promise; } return answered; } };
  } };
  const app = enrolling(holding, ({ ctx, rows }) => { ctx.channel(channel).todo.add({ id: rows[0].id }); });
  const item = page('held');
  // Each wake reads the database the moment it fires, through another connection.
  const readsAtWake = [];
  const stop = app.onCommitted(channel, () => { readsAtWake.push(Promise.all([claimed(item.callId), tables(ids, [channel])])); });
  const before = await tables(ids, [channel]);
  const answered = app.loads('alice', batch(item));
  await saving.promise;
  assert.deepEqual(await claimed(item.callId), [], 'neither the claim nor the saved success is visible');
  assert.deepEqual(await tables(ids, [channel]), before, 'nor a stamp, membership, head or position');
  await settled();
  assert.equal(readsAtWake.length, 0, 'no wake before COMMIT');
  gate.resolve();
  const [done] = outcomes(await answered);
  await settled();
  assert.equal(done.outcome.status, 'succeeded');
  assert.equal(readsAtWake.length, 1, 'one wake, after COMMIT');
  const [claimsAtWake, atWake] = await readsAtWake[0];
  assert.deepEqual(claimsAtWake.map(claim => claim.response), [done], 'the woken reader sees the saved outcome');
  assert.deepEqual(atWake.members, [{ channel, identity_key: key('held-1') }], 'and the membership');
  assert.deepEqual(atWake.heads, [{ channel, head: 1 }]);
  assert.deepEqual(atWake.positions, [{ channel, identity_key: key('held-1'), cursor: 1, stamp: 1 }]);
  stop();
});

test('a failed unit keeps no enrollment: a saveCall fault, a COMMIT failure and a Loader refusal after the declarations, while a sibling commits and wakes', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  // Two equal keys under a deferred unique constraint: the INSERT succeeds and COMMIT fails.
  await q('CREATE TABLE load_commit_guard(k int, CONSTRAINT load_commit_guard_k UNIQUE(k) DEFERRABLE INITIALLY DEFERRED)');
  const units = ['good', 'save', 'commit', 'refused'];
  for (const unit of units) await seed(`unit-${unit}`, `unit-${unit}`, 'alice', 1);
  const ids = units.map(unit => `unit-${unit}-1`), channels = units.map(unit => `unit:${unit}`);
  const items = Object.fromEntries(units.map(unit => [unit, page(`unit-${unit}`)]));
  const faulty = { ...database, persistence: tx => {
    const storage = database.persistence(tx);
    return { call: async request => {
      if (request.op === 'saveCall' && request.callId === items.save.callId) throw new Error('forced saveCall fault');
      return storage.call(request);
    } };
  } };
  const reported = [];
  const make = db => createBackend({ config, native, database: db, authenticate: () => 'alice', onError: error => reported.push(error),
    loaders: { async todo(call) {
      if (call.ids.some(({ id }) => id === 'unit-refused-1')) throw new MutationRejected('todo.forbidden');
      return loaders(database, { handled: [], loaded: [] }).todo(call);
    } },
    loads: { async projectTodos({ ctx, args }) {
      const rows = await database.driver.query(ctx.tx, 'SELECT id FROM load_todo WHERE project=$1 ORDER BY id', [args.projectId]);
      await database.driver.query(ctx.tx, 'INSERT INTO load_audit(note) VALUES($1)', [`unit:${ctx.callId}`]);
      ctx.channel(`unit:${args.projectId.slice('unit-'.length)}`).todo.add({ id: rows[0].id });
      if (args.projectId === 'unit-commit') await database.driver.query(ctx.tx, 'INSERT INTO load_commit_guard(k) VALUES(1), (1)', []);
      return { data: { todos: rows.map(row => ({ id: row.id })) }, next: null };
    } } });
  const app = make(faulty);
  const wakes = listen(app, channels);
  const failedIds = ids.slice(1), failedChannels = channels.slice(1);
  const before = await tables(failedIds, failedChannels);
  const [good, save, commit, refused] = outcomes(await app.loads('alice', batch(items.good, items.save, items.commit, items.refused)));
  await settled();
  assert.equal(good.outcome.status, 'succeeded');
  const unavailable = { status: 'retryable', error: { code: 'server.unavailable', message: 'the page transaction did not complete; resend the same call ID' } };
  assert.deepEqual(save.outcome, unavailable, 'a saveCall fault rolls the page back');
  assert.deepEqual(commit.outcome, unavailable, 'so does a failed COMMIT');
  assert.equal(refused.outcome.status, 'failed');
  assert.equal(refused.outcome.error.code, 'todo.forbidden', 'a Loader refusal after the declarations is the saved page failure');
  assert.deepEqual(await tables(failedIds, failedChannels), before, 'no stamp, membership, head or position from a failed unit');
  const sibling = await tables(['unit-good-1'], ['unit:good']);
  assert.deepEqual([sibling.members, sibling.heads], [[{ channel: 'unit:good', identity_key: key('unit-good-1') }], [{ channel: 'unit:good', head: 1 }]], 'the sibling enrolled');
  assert.deepEqual(await claimed(items.save.callId), [], 'the saveCall fault kept no claim');
  assert.deepEqual(await claimed(items.commit.callId), [], 'neither did the failed COMMIT');
  assert.equal((await claimed(items.refused.callId))[0].response.outcome.error.code, 'todo.forbidden');
  assert.equal((await claimed(items.good.callId))[0].response.outcome.status, 'succeeded');
  const audit = async item => (await q('SELECT note FROM load_audit WHERE note=$1', [`unit:${item.callId}`])).length;
  assert.deepEqual([await audit(items.good), await audit(items.save), await audit(items.commit), await audit(items.refused)], [1, 0, 0, 0], 'only the sibling kept its handler write');
  assert.deepEqual(await q('SELECT k FROM load_commit_guard'), []);
  assert.deepEqual(wakes.woken, ['unit:good'], 'only the committed unit wakes');
  assert.ok(reported.some(error => /forced saveCall fault/.test(error.message)));
  assert.ok(reported.some(error => error.code === '23505'), 'the COMMIT failure reached onError');
  // Resent under the same call ID once the fault is gone, the page enrolls once.
  const [resent] = outcomes(await make(database).loads('alice', batch(items.save)));
  await settled();
  assert.equal(resent.outcome.status, 'succeeded');
  assert.deepEqual((await tables(['unit-save-1'], ['unit:save'])).heads, [{ channel: 'unit:save', head: 1 }]);
  wakes.stop();
});

test('an initialized live subscription hears a Load enrollment and a later touch without reconnecting; a replay adds no publication', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('live', 'live', 'alice', 1);
  const channel = 'live:load';
  let runs = 0;
  const app = enrolling(database, ({ ctx, rows }) => { runs++; ctx.channel(channel).todo.add({ id: rows[0].id }); });
  const server = await app.listen({ port: 0 });
  const socket = new WebSocket(`${server.url.replace('http', 'ws')}/sync/live`, { headers: { authorization: 'Bearer alice' } });
  const frames = [];
  const until = async (ready, label) => {
    for (const started = Date.now(); !ready(); await new Promise(resolve => setTimeout(resolve, 5)))
      if (Date.now() - started > 2000) throw new Error(`timed out waiting for ${label}`);
  };
  try {
    await new Promise((resolve, reject) => { socket.addEventListener('open', resolve, { once: true }); socket.addEventListener('error', reject, { once: true }); });
    socket.addEventListener('message', event => frames.push(JSON.parse(String(event.data))));
    socket.send(JSON.stringify({ capabilities:['channel-membership-v1'],type: 'subscribe', channels: [channel], models: { Todo: 1 } }));
    await until(() => frames.length >= 1, 'the acknowledgement');
    assert.equal(frames[0].type, 'subscribed');
    assert.equal(frames[0].cursors[channel], 0, 'initialized at an empty Channel');
    const item = page('live');
    const [done] = outcomes(await app.loads('alice', batch(item)));
    assert.equal(done.outcome.status, 'succeeded');
    await until(() => frames.length >= 2, 'the enrollment');
    assert.deepEqual(frames[1].changes.map(change => [change.identity.id, change.stamp, change.state.title]), [['live-1', 1, 'live 1']], 'the loaded record at the page stamp');
    assert.deepEqual(frames[1].cursors[channel], { from: 0, to: 1, head: 1 });
    // The enrolled record's next change arrives over the same socket, with no second add.
    await app.transaction(async ({ tx, touch }) => { await database.driver.query(tx, "UPDATE load_todo SET title='touched' WHERE id='live-1'", []); touch.todo({ id: 'live-1' }); });
    await until(() => frames.some(frame => frame.changes?.some(change => change.stamp === 2)), 'the touch');
    assert.deepEqual(frames.at(-1).changes.map(change => [change.identity.id, change.stamp, change.state.title]), [['live-1', 2, 'touched']]);
    // A replay of the page does no membership or publication work.
    const committed = await tables(['live-1'], [channel]);
    let woken = 0;
    const stop = app.onCommitted(channel, () => { woken++; });
    const [replayed] = outcomes(await app.loads('alice', batch(item)));
    await settled();
    stop();
    assert.deepEqual(replayed, done);
    assert.equal(runs, 1, 'no handler ran');
    assert.deepEqual(await tables(['live-1'], [channel]), committed, 'no stamp, membership or position changed');
    assert.equal(woken, 0, 'no subscriber was woken');
  } finally {
    socket.close();
    await server.close();
  }
});

test('a touch and an enrolling page serialize in either order: the committed page carries the new version or the touch publishes it to the new Channel', async t => {
  const { database } = shims.find(shim => shim.name === 'pg');
  const { driver } = database;
  const trials = new Map();
  // One record per trial, loaded by id; each side notes every attempt's
  // transaction and what it read, and its first attempt waits at its gate
  // once its snapshot is fixed.
  const app = createBackend({ config, native, database, authenticate: () => 'alice', onError: () => {}, loaders: loaders(database, { handled: [], loaded: [] }),
    loads: { async projectTodos({ ctx, args }) {
      const trial = trials.get(args.projectId);
      const [{ txid }] = await driver.query(ctx.tx, 'SELECT txid_current()::text AS txid', []);
      const [{ title }] = await driver.query(ctx.tx, 'SELECT title FROM load_todo WHERE id=$1', [args.projectId]);
      trial.attempts.page.push({ txid, title });
      if (trial.attempts.page.length === 1) { trial.fixed.page.resolve(); await trial.gates.page.promise; }
      ctx.channel(trial.C).todo.add({ id: args.projectId });
      return { data: { todos: [{ id: args.projectId }] }, next: null };
    } } });
  const touching = trial => app.transaction(async ({ tx, touch }) => {
    const [{ txid }] = await driver.query(tx, 'SELECT txid_current()::text AS txid', []);
    const members = (await driver.query(tx, "SELECT m.channel FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id WHERE r.model='Todo' AND r.identity_key=$1 ORDER BY m.channel", [key(trial.id)])).map(row => row.channel);
    trial.attempts.touch.push({ txid, members });
    if (trial.attempts.touch.length === 1) { trial.fixed.touch.resolve(); await trial.gates.touch.promise; }
    await driver.query(tx, "UPDATE load_todo SET title='touched' WHERE id=$1", [trial.id]);
    touch.todo({ id: trial.id });
  });
  const status = async txid => (await q('SELECT pg_xact_status($1::xid8) AS status', [txid]))[0].status;
  /**
   * Runs the page and the touch of `id` with `held` waiting at its gate while
   * the other commits, or with both snapshots fixed before either goes on.
   */
  const race = async (id, held, initial) => {
    await q("INSERT INTO load_todo(id, project, owner_id, title) VALUES($1, $1, 'alice', 'v1')", [id]);
    const B = `${id}:B`, C = `${id}:C`;
    // A stamped record is an old domain row that is already a member of B.
    if (initial === 'stamped') await app.transaction(async ({ channel }) => { channel(B).todo.add({ id }); });
    const trial = { id, C, attempts: { page: [], touch: [] }, fixed: { page: deferred(), touch: deferred() }, gates: { page: deferred(), touch: deferred() } };
    trials.set(id, trial);
    const wakes = listen(app, [C]);
    const item = page(id);
    const loading = () => app.loads('alice', batch(item)).then(outcomes);
    const first = (fixed, running) => Promise.race([fixed, running.then(() => { throw new Error('committed before its snapshot was held'); })]);
    let answered;
    if (held === 'page') {
      trial.gates.touch.resolve();
      const running = loading();
      await first(trial.fixed.page.promise, running);
      await touching(trial);
      trial.gates.page.resolve();
      [answered] = await running;
    } else if (held === 'touch') {
      trial.gates.page.resolve();
      const running = touching(trial);
      await first(trial.fixed.touch.promise, running);
      [answered] = await loading();
      trial.gates.touch.resolve();
      await running;
    } else {
      const running = [loading(), touching(trial)];
      await Promise.all([trial.fixed.page.promise, trial.fixed.touch.promise]);
      trial.gates.page.resolve();
      trial.gates.touch.resolve();
      [[answered]] = await Promise.all(running);
    }
    await settled();
    wakes.stop();
    assert.equal(answered.outcome.status, 'succeeded', `${id}: ${JSON.stringify(answered.outcome)}`);
    // Classify by what committed: the page attempt whose transaction claimed
    // the call, and the one touch attempt PostgreSQL committed. Every other
    // attempt aborted, taking its stamps, positions and wakes with it.
    const [{ tx: claim }] = await claimed(item.callId);
    const committedPage = trial.attempts.page.filter(attempt => attempt.txid === claim);
    assert.equal(committedPage.length, 1, `${id}: one page attempt claimed the call`);
    const statuses = { page: [], touch: [] };
    for (const side of ['page', 'touch']) for (const attempt of trial.attempts[side]) statuses[side].push(await status(attempt.txid));
    assert.equal(statuses.page.filter(s => s === 'committed').length, 1, `${id}: ${statuses.page}`);
    assert.equal(statuses.page[trial.attempts.page.indexOf(committedPage[0])], 'committed', id);
    assert.equal(statuses.touch.filter(s => s === 'committed').length, 1, `${id}: ${statuses.touch}`);
    const committedTouch = trial.attempts.touch[statuses.touch.indexOf('committed')];
    assert.ok([...statuses.page, ...statuses.touch].every(s => s === 'committed' || s === 'aborted'), `${id}: every other attempt aborted`);
    assert.ok(trial.attempts.page.length + trial.attempts.touch.length > 2, `${id}: overlapping snapshots force a retry`);
    const order = committedPage[0].title === 'touched' ? 'touchFirst' : 'pageFirst';
    const got = await tables([id], [B, C]);
    const stamp = got.stamps[0].stamp;
    const [record] = answered.records;
    const position = channel => got.positions.filter(row => row.channel === channel).map(row => [row.cursor, row.stamp]);
    const head = channel => got.heads.find(row => row.channel === channel)?.head ?? 0;
    // The one committed touch advanced the stamp once (or created it at 1).
    assert.equal(stamp, initial === 'stamped' || order === 'pageFirst' ? 2 : 1, `${id}: ${order}`);
    assert.deepEqual(got.members.map(row => row.channel), initial === 'stamped' ? [B, C] : [C], id);
    if (order === 'touchFirst') {
      assert.deepEqual([record.stamp, record.state.title], [stamp, 'touched'], `${id}: the committed page carries the new version`);
      assert.deepEqual([head(C), position(C)], [1, [[1, stamp]]], `${id}: enrolled once at it`);
      assert.ok(!committedTouch.members.includes(C), `${id}: the touch committed before the enrollment`);
      assert.deepEqual(wakes.woken, [C]);
    } else {
      assert.deepEqual([record.stamp, record.state.title], [stamp - 1, 'v1'], `${id}: the committed page carries the old version`);
      assert.deepEqual([head(C), position(C)], [2, [[2, stamp]]], `${id}: and the later touch published the new stamp to C`);
      assert.ok(committedTouch.members.includes(C), `${id}: the touch that committed read the enrollment`);
      assert.deepEqual(wakes.woken, [C, C], `${id}: the enrollment and the touch each woke C once`);
    }
    if (initial === 'stamped') assert.deepEqual([head(B), position(B)], [2, [[2, 2]]], `${id}: B heard the touch once`);
    assert.deepEqual(await delivered(app, C), [[id, stamp, 'touched']], `${id}: C ends at the new version, never permanently stale`);
    return order;
  };
  const orders = [];
  for (const initial of ['stamped', 'unstamped']) {
    // The page holds a snapshot from before the touch: it retries and reads the new version.
    assert.equal(await race(`race-${initial}-page`, 'page', initial), 'touchFirst');
    // The touch holds a snapshot from before the enrollment: it retries and publishes to C.
    assert.equal(await race(`race-${initial}-touch`, 'touch', initial), 'pageFirst');
    for (let trial = 0; trial < 4; trial++) orders.push(`${initial}:${await race(`race-${initial}-${trial}`, 'both', initial)}`);
  }
  t.diagnostic(`released together, committed orders: ${orders.join(' ')}`);
});

test('a Loader denial, an out-of-page enrollment and a forged remove or change answer each save a failed page and change no table', async () => {
  const { database } = shims.find(shim => shim.name === 'pg');
  await seed('deny', 'deny', 'alice', 2);
  const [member, bare] = ['deny-1', 'deny-2'], ids = [member, bare];
  const [A, B] = ['deny:a', 'deny:b'];
  let declare, returned, tamper, refused;
  // A bridge that rewrites the handleLoad answer on its way to the engine, as a
  // broken or malicious host could; every other request passes through.
  const forging = { ...native, processLoad: (configJson, owner, item, callback) => native.processLoad(configJson, owner, item, async request => {
    const answered = await callback(request);
    return JSON.parse(request).op === 'handleLoad' ? JSON.stringify(tamper(JSON.parse(answered))) : answered;
  }) };
  const app = createBackend({ config, native: forging, database, authenticate: () => 'alice', onError: () => {},
    loaders: { async todo(call) {
      if (call.ids.some(({ id }) => id === refused)) throw new MutationRejected('todo.forbidden');
      return loaders(database, { handled: [], loaded: [] }).todo(call);
    } },
    loads: { async projectTodos({ ctx }) {
      await database.driver.query(ctx.tx, 'INSERT INTO load_audit(note) VALUES($1)', [`deny:${ctx.callId}`]);
      declare(ctx.channel);
      return { data: { todos: returned.map(id => ({ id })) }, next: null };
    } } });
  await app.transaction(async ({ channel }) => { channel(B).todo.add({ id: member }); });
  const before = await tables(ids, [A, B]);
  assert.deepEqual([before.stamps, before.members, before.heads, before.positions], [
    [{ identity_key: key(member), stamp: 1 }], [{ channel: B, identity_key: key(member) }], [{ channel: B, head: 1 }], [{ channel: B, identity_key: key(member), cursor: 1, stamp: 1 }],
  ], 'one record is a member of B; the other has no metadata');
  const both = channel => { channel(A).todo.add({ id: member }); channel(A).todo.add({ id: bare }); };
  const cases = [
    // Resolution initializes the bare record's stamp before the Loader refuses it.
    ['a Loader denial after the declarations', () => { refused = bare; declare = both; }, 'todo.forbidden'],
    ['an enrollment outside the page', () => { returned = [member]; declare = both; }, 'handler.invalid'],
    ['a forged remove beside a valid add', () => {
      declare = channel => channel(A).todo.add({ id: member });
      tamper = answered => ({ ...answered, memberships: [...answered.memberships, { kind: 'remove', channel: B, record: { model: 'Todo', identity: { id: member } } }] });
    }, 'handler.invalid'],
    ['a forged tag selector beside a valid add', () => {
      declare = channel => channel(A).todo.add({ id: member });
      tamper = answered => ({ ...answered, memberships: [...answered.memberships, { kind: 'removeTag', channel: A, tag: 'X' }] });
    }, 'handler.invalid'],
    // JavaScript's trim keeps U+0085; the engine calls the tag blank and refuses it.
    ['a tag only the engine calls blank', () => { declare = channel => channel(A).todo.add({ id: member }, { tags: ['\u0085'] }); }, 'handler.invalid'],
    ['a forged change beside a valid add', () => {
      declare = channel => channel(A).todo.add({ id: member });
      tamper = answered => ({ ...answered, changes: [{ model: 'Todo', identity: { id: member } }] });
    }, 'handler.invalid'],
  ];
  for (const [label, arrange, code] of cases) {
    returned = ids; refused = undefined; tamper = answered => answered;
    arrange();
    const wakes = listen(app, [A, B]);
    const item = page('deny');
    const [failed] = outcomes(await app.loads('alice', batch(item)));
    await settled();
    wakes.stop();
    assert.equal(failed.outcome.status, 'failed', label);
    assert.equal(failed.outcome.error.code, code, label);
    assert.deepEqual(failed.records, [], label);
    assert.deepEqual((await claimed(item.callId))[0].response, failed, `${label}: the failure is the saved outcome`);
    assert.deepEqual(await tables(ids, [A, B]), before, `${label}: no business row, stamp, membership, head or position changed`);
    assert.deepEqual(await q('SELECT note FROM load_audit WHERE note=$1', [`deny:${item.callId}`]), [], `${label}: the handler's own write rolled back`);
    assert.deepEqual(wakes.woken, [], `${label}: no wake`);
  }
  // A valid tagged add enrolls the member with its tag.
  returned = ids; tamper = answered => answered;
  declare = channel => channel(A).todo.add({ id: member }, { tags: ['X'] });
  const [done] = outcomes(await app.loads('alice', batch(page('deny'))));
  assert.equal(done.outcome.status, 'succeeded');
  assert.deepEqual(await q("SELECT t.name FROM axton_channel_tag t JOIN axton_channel_member_tag mt ON mt.tag_id=t.id JOIN axton_channel_member m ON m.id=mt.member_id WHERE m.channel=$1", [A]), [{ name: 'X' }]);
});


test('a saved enrolled page keeps its old claim after a later removal on every shim', async () => {
  for (const {name,database} of shims) {
    const project = `claim-retry-${name}`;
    const channel = `claim:${project}`;
    await seed(project, project, 'alice', 1);
    const app = enrolling(database, ({ctx,rows}) => ctx.channel(channel).todo.add({id:rows[0].id}));
    await q('INSERT INTO axton_channel(channel,head) VALUES($1,9)', [channel]);
    const item = page(project);
    const [original] = outcomes(await app.loads('alice',batch(item)));
    assert.deepEqual(original.memberships,[{channel,cursor:10,model:'Todo',identity:{id:`${project}-1`}}]);
    await app.transaction(async ({channel:scope}) => scope(channel).todo.remove({id:`${project}-1`}));
    const before = await q('SELECT l.channel,l.cursor,l.kind,r.model,r.identity FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel=$1',[channel]);
    assert.equal(Number(before[0].cursor),11);
    assert.equal(before[0].kind,'remove');
    const heads = await q('SELECT * FROM axton_channel WHERE channel=$1',[channel]);
    const [replayed] = outcomes(await app.loads('alice',batch(item)));
    assert.deepEqual(replayed,original);
    assert.deepEqual(await channelsOf(`${project}-1`),[]);
    assert.deepEqual(await q('SELECT * FROM axton_channel WHERE channel=$1',[channel]),heads);
    assert.deepEqual(await q('SELECT l.channel,l.cursor,l.kind,r.model,r.identity FROM axton_channel_log l JOIN axton_record r ON r.id=l.record_id WHERE l.channel=$1',[channel]),before);
  }
});

test('a capable retry across cutover replays a saved legacy Load without claims or enrollment on every shim', async () => {
  for (const {name,database} of shims) {
    const project=`legacy-cutover-${name}`,channel=`${project}:room`,record=`${project}-1`;
    await seed(project,project,'alice',1);
    let runs=0;
    const app=enrolling(database,({ctx,rows})=>{runs++;ctx.channel(channel).todo.add({id:rows[0].id});});
    const item=page(project);
    const [first]=outcomes(await app.loads('alice',batch(item)));
    assert.equal(first.memberships.length,1,name);
    await app.transaction(({channel:scope})=>scope(channel).todo.remove({id:record}));
    // The durable pre-capability fixture has no claims. Negotiation in a
    // stored request is nonsemantic, regardless of which writer saved it.
    const [row]=await q('SELECT request,response FROM axton_call WHERE call_id=$1',[item.callId]);
    const request=JSON.parse(row.request);request.capabilities=['channel-membership-v1'];
    const response=JSON.parse(row.response);delete response.memberships;
    await q('UPDATE axton_call SET request=$2,response=$3 WHERE call_id=$1',[item.callId,JSON.stringify(request),JSON.stringify(response)]);
    const before=await tables([record],[channel]);
    const [retry]=outcomes(await app.loads('alice',batch(item)));
    assert.deepEqual(retry,response,name);
    assert.equal(runs,1,`${name}: the saved page never reenrolls`);
    assert.equal(retry.memberships,undefined,`${name}: legacy response fabricates no claim`);
    assert.deepEqual(await tables([record],[channel]),before,`${name}: heads, members and removal positions stay unchanged`);
  }
});
