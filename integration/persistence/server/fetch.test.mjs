// Model Fetch against real PostgreSQL (#153): `POST /sync/fetch` and the
// native `processFetch` run one Loader read in the application transaction,
// claim and save the call in `axton_call`, and change no Scope state.
import test, { before, after } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { Pool } from 'pg';
import { createBackend, devAuth, CallRejected } from '../../../packages/server/index.mts';
import { pg } from '../../../packages/postgres/index.mts';
const require = createRequire(import.meta.url);
const native = require('../../../bindings/node/axton-node.node');
const check = new Pool({ connectionString: process.env.DATABASE_URL });
const q = async (sql, params = []) => (await check.query(sql, params)).rows;
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const database = pg(pool);
const string = name => ({ name, type: { kind: 'scalar', name: 'string' }, nullable: false });
const fields = [string('id'), string('title')];
// A Model and its Loader, and no Mutation or Query.
const config = { schema: { enums: [], actions: [],
  models: [{ name: 'Todo', version: 1, identity: ['id'], fields }],
  resultModels: [{ name: 'Todo', version: 1, identity: ['id'], fields, enums: [] }] },
  mutations: [], loaders: ['Todo'] };
let loads = 0;
const reported = [];
const todo = async ({ tx, ids }) => {
  loads++;
  return Promise.all(ids.map(async ({ id }) => {
    if (id === 'secret') throw new CallRejected('todo.hidden');
    // A failing statement aborts the transaction; the Fetch savepoint recovers it.
    if (id === 'broken') await database.driver.query(tx, 'SELECT * FROM fetch_missing_table', []);
    return (await database.driver.query(tx, 'SELECT id,title FROM fetch_todo WHERE id=$1', [id]))[0] ?? null;
  }));
};
const backend = (db = database) => createBackend({ config, native, database: db, authenticate: devAuth(), onError: error => reported.push(error), loaders: { todo } });
const body = (callId, id, extra = {}) => JSON.stringify({ capabilities:['scope-membership-v1'],callId, model: 'Todo', version: 1, identity: { id }, ...extra });
const stamps = async id => (await q("SELECT stamp FROM axton_record WHERE model='Todo' AND identity_key=$1", [JSON.stringify({ id })])).map(row => Number(row.stamp));
const scopeState = async () => ({
  scopes: await q('SELECT scope, head FROM axton_scope ORDER BY scope'),
  positions: (await q('SELECT count(*)::int AS n FROM axton_scope_log'))[0].n,
  memberships: (await q('SELECT count(*)::int AS n FROM axton_scope_member'))[0].n,
});

before(async () => {
  await q(await readFile(new URL('../../../packages/postgres/migration.sql', import.meta.url), 'utf8'));
  await q('CREATE TABLE fetch_todo(id text PRIMARY KEY, title text NOT NULL)');
  await q("INSERT INTO fetch_todo(id,title) VALUES('f1','A'),('f2','P'),('f3','R'),('secret','S')");
});
after(() => Promise.all([check.end(), pool.end()]));

test('HTTP Fetch authenticates, commits one snapshot, replays it and isolates owners', async () => {
  const listening = await backend().listen({ port: 0 });
  const send = (request, token = 'alice') => fetch(`${listening.url}/sync/fetch`, {
    method: 'POST',
    headers: token ? { authorization: `Bearer ${token}` } : {},
    body: request,
  });
  const callId = '01890f47-1234-7123-8123-1234567f0001';
  const before = await scopeState();
  try {
    assert.equal((await send(body(callId, 'f1'), null)).status, 401);
    assert.equal((await q('SELECT 1 FROM axton_call WHERE call_id=$1', [callId])).length, 0);
    const first = await send(body(callId.toUpperCase(), 'f1'));
    assert.equal(first.status, 200);
    const result = await first.json();
    assert.deepEqual(result, {
      completion: { callId, outcome: { status: 'succeeded', result: { id: 'f1', title: 'A' } } },
      records: [{ model: 'Todo', identity: { id: 'f1' }, stamp: 1, state: { title: 'A' } }],
    });
    const [saved] = await q('SELECT request, response FROM axton_call WHERE owner_id=$1 AND call_id=$2', ['alice', callId]);
    assert.deepEqual(JSON.parse(saved.request), { kind: 'fetch', callId, model: 'Todo', version: 1, identity: { id: 'f1' }, store: true });
    assert.deepEqual(JSON.parse(saved.response), result, 'the committed response is the reply');
    assert.deepEqual(await stamps('f1'), [1], 'stamp evidence without advancing it');
    // A lost response: the retry answers the committed snapshot, not the changed row.
    await q("UPDATE fetch_todo SET title='A2' WHERE id='f1'");
    const loaded = loads;
    const replay = await send(body(callId, 'f1'));
    assert.equal(replay.status, 200);
    assert.deepEqual(await replay.json(), result);
    assert.equal(loads, loaded, 'replay runs no Loader');
    // The same call ID under another principal is that principal's own call.
    const bob = await (await send(body(callId, 'f1'), 'bob')).json();
    assert.deepEqual(bob.completion.outcome.result, { id: 'f1', title: 'A2' });
    assert.equal(loads, loaded + 1);
    assert.equal((await q('SELECT owner_id FROM axton_call WHERE call_id=$1 ORDER BY owner_id', [callId])).map(row => row.owner_id).join(), 'alice,bob');
    // Reusing the call ID for another intent conflicts and saves nothing new.
    const conflict = await (await send(body(callId, 'f2'))).json();
    assert.deepEqual(conflict.completion.outcome, { status: 'failed', code: 'call.identity_conflict', execution: 'rejected' });
    assert.equal(loads, loaded + 1);
    // Envelope and identity errors are HTTP failures before any claim.
    const malformed = await send(JSON.stringify({ capabilities:['scope-membership-v1'],callId: '01890f47-1234-7123-8123-1234567f0002', model: 'Todo', version: 1, identity: { id: 'f1' }, store: 'yes' }));
    assert.equal(malformed.status, 400);
    assert.deepEqual(await malformed.json(), { code: 'request.invalid' });
    const unknownField = await send(body('01890f47-1234-7123-8123-1234567f0003', 'f1', { identity: { id: 'f1', title: 'A' } }));
    assert.equal(unknownField.status, 400);
    assert.equal((await q("SELECT 1 FROM axton_call WHERE call_id IN ('01890f47-1234-7123-8123-1234567f0002','01890f47-1234-7123-8123-1234567f0003')")).length, 0);
    // An unserved read version is the call's own committed rejection.
    const unserved = '01890f47-1234-7123-8123-1234567f0004';
    const refused = await send(JSON.stringify({ capabilities:['scope-membership-v1'],callId: unserved, model: 'Todo', version: 9, identity: { id: 'f1' } }));
    assert.equal(refused.status, 200);
    assert.deepEqual((await refused.json()).completion.outcome, { status: 'failed', code: 'model_version_unsupported', execution: 'rejected' });
    assert.equal((await q('SELECT 1 FROM axton_call WHERE call_id=$1 AND response IS NOT NULL', [unserved])).length, 1);
    assert.deepEqual(await scopeState(), before, 'Fetch changes no Scope, invalidation or membership');
  } finally {
    await listening.close();
  }
});

test('absence commits stamped null authority; store false allocates nothing', async () => {
  const app = backend();
  const before = await scopeState();
  const missing = JSON.parse(await app.fetch('alice', body('01890f47-1234-7123-8123-1234567f0010', 'nobody')));
  assert.deepEqual(missing.completion.outcome, { status: 'succeeded', result: null });
  assert.deepEqual(missing.records, [{ model: 'Todo', identity: { id: 'nobody' }, stamp: 1, state: null }]);
  assert.deepEqual(await stamps('nobody'), [1]);
  const preview = JSON.parse(await app.fetch('alice', body('01890f47-1234-7123-8123-1234567f0011', 'f2', { store: false })));
  assert.deepEqual(preview.completion.outcome.result, { id: 'f2', title: 'P' });
  assert.deepEqual(preview.records, []);
  assert.deepEqual(await stamps('f2'), [], 'no stamp is allocated without storage');
  const absentPreview = JSON.parse(await app.fetch('alice', body('01890f47-1234-7123-8123-1234567f0012', 'ghost', { store: false })));
  assert.deepEqual(absentPreview, { completion: { callId: '01890f47-1234-7123-8123-1234567f0012', outcome: { status: 'succeeded', result: null } }, records: [] });
  assert.deepEqual(await stamps('ghost'), []);
  const [saved] = await q('SELECT request FROM axton_call WHERE call_id=$1', ['01890f47-1234-7123-8123-1234567f0011']);
  assert.equal(JSON.parse(saved.request).store, false, 'the storage policy is part of the call identity');
  assert.deepEqual(await scopeState(), before);
});

test('Loader refusal and failure are committed rejections that replay without reading', async () => {
  const app = backend();
  reported.length = 0;
  for (const [callId, id, code] of [
    ['01890f47-1234-7123-8123-1234567f0020', 'secret', 'todo.hidden'],
    ['01890f47-1234-7123-8123-1234567f0021', 'broken', 'loader.failed'],
  ]) {
    const first = JSON.parse(await app.fetch('alice', body(callId, id)));
    assert.deepEqual(first, { completion: { callId, outcome: { status: 'failed', code, execution: 'rejected' } }, records: [] });
    assert.deepEqual(await stamps(id), [], `${id}: the savepoint rolled back its stamp evidence`);
    const loaded = loads;
    assert.deepEqual(JSON.parse(await app.fetch('alice', body(callId, id))), first);
    assert.equal(loads, loaded);
  }
  assert.equal(reported.length, 1, 'only the thrown Loader failure is reported');
  assert.match(String(reported[0]?.message), /fetch_missing_table/);
});

test('a persistence fault rolls back the claim and stamp so a retry reads again', async () => {
  const faulty = { ...database, persistence: tx => ({ call: async request => {
    if (request.op === 'saveCall') throw new Error('forced saveCall fault');
    return database.persistence(tx).call(request);
  } }) };
  const callId = '01890f47-1234-7123-8123-1234567f0030';
  await assert.rejects(() => backend(faulty).fetch('alice', body(callId, 'f3')), /forced saveCall fault/);
  assert.deepEqual(await q('SELECT 1 FROM axton_call WHERE call_id=$1', [callId]), []);
  assert.deepEqual(await stamps('f3'), [], 'stamp evidence rolled back with the claim');
  await q("UPDATE fetch_todo SET title='R2' WHERE id='f3'");
  const loaded = loads;
  const retry = JSON.parse(await backend().fetch('alice', body(callId, 'f3')));
  assert.deepEqual(retry.completion.outcome.result, { id: 'f3', title: 'R2' }, 'the retry is a fresh read');
  assert.equal(loads, loaded + 1);
  assert.deepEqual(retry.records.map(record => record.stamp), [1]);
});
