import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Client } from '../../../packages/client-js/index.mts';

const schema = JSON.parse(await readFile(new URL('../../../fixtures/schemas/entry.json', import.meta.url), 'utf8'));
const edit = (id, text) => ({ name: 'Edit', operations: [{ model: 'Entry', op: 'update', identity: { id }, values: { text } }] });
async function within(promise, ms) {
 let timeout;
 try {
  return await Promise.race([promise, new Promise(resolve => { timeout = setTimeout(() => resolve('timeout'), ms); })]);
 } finally { clearTimeout(timeout); }
}

test('a captured client mutate rejects promptly inside its transaction callback', async () => {
 const dir = await mkdtemp(join(tmpdir(), 'axton-runtime-'));
 const client = await Client.open({ path: join(dir, 'client.sqlite'), schema });
 let release, entered;
 const gate = new Promise(resolve => { release = resolve; });
 const started = new Promise(resolve => { entered = resolve; });
 let attempt;
 try {
  await client.transaction(tx => tx.direct({ model: 'Entry', op: 'create', identity: { id: 'e' }, values: { text: 'A' } }));
  const capturedMutate = client.mutate.bind(client);
  const transaction = client.transaction(async () => {
   attempt = capturedMutate(edit('e', 'B')).then(() => 'accepted', error => error.message);
   entered();
   await gate;
  });
  try {
   await started;
   assert.equal(await within(attempt, 1000), 'transaction_active');
  } finally {
   release();
   await transaction;
  }
  assert.equal((await client.syncState()).pending, 0);
  assert.equal(await client.mutate(edit('e', 'C')), 1);
  assert.equal((await client.read('Entry', { id: 'e' })).text, 'C');
 } finally { await client.close(); await rm(dir, { recursive: true, force: true }); }
});

test('an unrelated Node async context waits for a transaction then enqueues', async () => {
 const dir = await mkdtemp(join(tmpdir(), 'axton-runtime-'));
 const client = await Client.open({ path: join(dir, 'client.sqlite'), schema });
 let release, entered;
 const gate = new Promise(resolve => { release = resolve; });
 const started = new Promise(resolve => { entered = resolve; });
 try {
  await client.transaction(tx => tx.direct({ model: 'Entry', op: 'create', identity: { id: 'e' }, values: { text: 'A' } }));
  const transaction = client.transaction(async tx => { entered(); assert.equal('mutate' in tx, false); await gate; });
  await started;
  const mutation = client.mutate(edit('e', 'B'));
  assert.equal(await within(mutation.then(() => 'committed'), 50), 'timeout');
  release();
  await transaction;
  assert.equal(await mutation, 1);
  assert.equal((await client.read('Entry', { id: 'e' })).text, 'B');
 } finally { release?.(); await client.close(); await rm(dir, { recursive: true, force: true }); }
});

test('failed standalone enqueue rolls back its queue entry and optimistic record', async () => {
 const dir = await mkdtemp(join(tmpdir(), 'axton-runtime-'));
 const client = await Client.open({ path: join(dir, 'client.sqlite'), schema });
 try {
  await assert.rejects(client.mutate({ name: 'Broken', operations: [
   { model: 'Entry', op: 'create', identity: { id: 'failed' }, values: { text: 'optimistic' } },
   { model: 'Missing', op: 'create', identity: { id: 'missing' }, values: { text: 'invalid' } },
  ] }));
  assert.equal((await client.syncState()).pending, 0);
  assert.equal(await client.read('Entry', { id: 'failed' }), null);
  assert.equal(await client.mutate({ name: 'Create', operations: [
   { model: 'Entry', op: 'create', identity: { id: 'good' }, values: { text: 'committed' } },
  ] }), 1);
 } finally { await client.close(); await rm(dir, { recursive: true, force: true }); }
});

test('standalone queued mutation and optimistic value survive SQLite reopen', async () => {
 const dir = await mkdtemp(join(tmpdir(), 'axton-runtime-'));
 const path = join(dir, 'client.sqlite');
 let client;
 try {
  client = await Client.open({ path, schema });
  assert.equal(await client.mutate({ name: 'Create', operations: [
   { model: 'Entry', op: 'create', identity: { id: 'queued' }, values: { text: 'persistent' } },
  ] }), 1);
  await client.close();
  client = await Client.open({ path, schema });
  assert.equal((await client.syncState()).pending, 1);
  assert.equal((await client.read('Entry', { id: 'queued' })).text, 'persistent');
 } finally { await client?.close(); await rm(dir, { recursive: true, force: true }); }
});

// Local watch ([#134](https://github.com/zanminwang/axton/issues/134)): the
// runtime registers the query, re-runs it after every commit and publishes only
// a result that differs; the SDK delivers what it publishes.
const create = (id, text) => ({ model: 'Entry', op: 'create', identity: { id }, values: { text } });
const update = (id, text) => ({ model: 'Entry', op: 'update', identity: { id }, values: { text } });
async function until(predicate, what) {
 const deadline = Date.now() + 5000;
 while (!predicate()) { if (Date.now() > deadline) throw Error(`${what} timed out`); await new Promise(r => setTimeout(r, 5)); }
}
const settle = () => new Promise(resolve => setTimeout(resolve, 30));

test('watch delivers the committed rows, then only a result that changed, and nothing after stop', async () => {
 const dir = await mkdtemp(join(tmpdir(), 'axton-watch-'));
 const client = await Client.open({ path: join(dir, 'client.sqlite'), schema });
 try {
  const seen = [];
  const errors = [];
  const stop = client.watch('Entry', { id: 'one' }, rows => seen.push(rows.map(row => row.text)), error => errors.push(error));
  await until(() => seen.length === 1, 'the initial rows');
  assert.deepEqual(seen, [[]], 'the current result arrives first, empty or not');
  await client.direct(create('one', 'A'));
  await until(() => seen.length === 2, 'the committed change');
  assert.deepEqual(seen, [[], ['A']]);
  // A commit that leaves this result as it was publishes nothing.
  await client.direct(create('two', 'B'));
  await settle();
  assert.deepEqual(seen, [[], ['A']], 'an equal result is suppressed');
  // Uncommitted writes are invisible: only the committed transaction is seen.
  await client.transaction(async tx => { await tx.direct(update('one', 'draft')); await tx.direct(update('one', 'C')); });
  await until(() => seen.length === 3, 'the committed transaction');
  assert.deepEqual(seen.at(-1), ['C'], 'only the committed state, never the draft');
  stop();
  await client.direct(update('one', 'D'));
  await settle();
  assert.equal(seen.length, 3, 'a stopped watch hears nothing');
  assert.deepEqual(errors, []);
 } finally { await client.close(); await rm(dir, { recursive: true, force: true }); }
});

test('a watch listener that throws is reported to onError and hears every later result', async () => {
 const dir = await mkdtemp(join(tmpdir(), 'axton-watch-'));
 const client = await Client.open({ path: join(dir, 'client.sqlite'), schema });
 try {
  const seen = [];
  const errors = [];
  const stop = client.watch('Entry', {}, rows => { seen.push(rows.length); throw Error(`listener failed at ${rows.length}`); }, error => errors.push(error.message));
  await until(() => seen.length === 1, 'the initial rows');
  await client.direct(create('one', 'A'));
  await client.direct(create('two', 'B'));
  await until(() => seen.length === 3, 'both commits');
  assert.deepEqual(seen, [0, 1, 2]);
  assert.deepEqual(errors, ['listener failed at 0', 'listener failed at 1', 'listener failed at 2']);
  assert.equal((await client.query('Entry')).length, 2, 'the exceptions undid nothing');
  stop();
  // A registration the runtime refuses reaches onError too, and nothing is delivered.
  const refused = [];
  const rows = [];
  client.watch('Missing', {}, value => rows.push(value), error => refused.push(error));
  await until(() => refused.length === 1, 'the refused registration');
  assert.ok(refused[0] instanceof Error);
  assert.deepEqual(rows, []);
 } finally { await client.close(); await rm(dir, { recursive: true, force: true }); }
});

/** A client over a scripted runtime that answers `watch` with observer 3 and records every task. */
async function scriptedWatchClient() {
 const { createClient } = await import('../../../packages/client-js/runtime.mts');
 const { Transaction } = await import('../../../packages/client-js/transaction.mts');
 let wake;
 const outbox = [];
 const tasks = [];
 const later = () => setImmediate(() => wake('1'));
 const carrier = {
  runtimeOpen(request, wakeRuntime) {
   wake = wakeRuntime;
   outbox.push({ type: 'taskCompleted', requestId: JSON.parse(request).requestId, ok: true, value: { clientId: 'c', schema: { rebuilt: false, pending: null, lastRebuild: null } } });
   later();
   return '1';
  },
  runtimeSubmit(runtimeId, message) {
   const input = JSON.parse(message);
   if (input.type === 'close') outbox.push({ type: 'runtimeClosed' });
   if (input.type === 'task') {
    tasks.push(input.command);
    const value = input.command.kind === 'watch' ? { observerId: '3' } : null;
    outbox.push({ type: 'taskCompleted', requestId: input.requestId, ok: true, value });
    if (input.command.kind === 'watch') outbox.push({ type: 'observerChanged', observerId: '3', snapshot: { kind: 'watch', rows: [{ id: 'a' }] } });
   }
   later();
  },
  runtimeDrain: () => JSON.stringify(outbox.splice(0)),
  runtimeDetach() {},
 };
 const Scripted = createClient(carrier, Transaction, () => ({ push: async () => '', open() {} }));
 const client = await Scripted.open({ path: 'unused', schema });
 return { client, tasks, publish(...events) { outbox.push(...events); wake('1'); } };
}

test('stop unregisters the watch by its observer id; a watch stopped before registering is unregistered too', async () => {
 const { client, tasks, publish } = await scriptedWatchClient();
 try {
  const seen = [];
  const stop = client.watch('Entry', { id: 'a' }, rows => seen.push(rows));
  await until(() => seen.length === 1, 'the rows published behind the task');
  assert.deepEqual(tasks, [{ kind: 'watch', model: 'Entry', spec: { filter: { id: 'a' } } }]);
  publish({ type: 'observerChanged', observerId: '3', snapshot: { kind: 'watch', rows: [] } });
  assert.deepEqual(seen, [[{ id: 'a' }], []], 'every published result, as published');
  stop();
  stop();
  assert.deepEqual(tasks.at(-1), { kind: 'unwatch', observerId: '3' });
  assert.equal(tasks.filter(t => t.kind === 'unwatch').length, 1, 'stopping twice unregisters once');
  // Published before the runtime saw the unwatch: delivery already stopped.
  publish({ type: 'observerChanged', observerId: '3', snapshot: { kind: 'watch', rows: [{ id: 'late' }] } });
  assert.equal(seen.length, 2);
  // Stopped before the registration answered: it is claimed and unregistered, never delivered.
  const early = [];
  client.watch('Entry', {}, rows => early.push(rows))();
  await until(() => tasks.filter(t => t.kind === 'unwatch').length === 2, 'the early stop unregisters');
  assert.deepEqual(early, []);
 } finally { await client.close(); }
});

test('inside a callback, every task of the outer client rejects promptly with transaction_active', async () => {
 const dir = await mkdtemp(join(tmpdir(), 'axton-runtime-guard-'));
 const client = await Client.open({ path: join(dir, 'client.sqlite'), schema });
 try {
  await client.direct(create('e', 'A'));
  const outcomes = {};
  const message = (promise) => within(promise.then(() => 'ran', error => error.message), 1000);
  const rows = [];
  let watchError;
  await client.transaction(async tx => {
   // The transaction's own commands are not the outer client's.
   await tx.direct(update('e', 'inside'));
   outcomes.read = await message(client.read('Entry', { id: 'e' }));
   outcomes.transaction = await message(client.transaction(async () => { rows.push('nested body ran'); }));
   outcomes.query = await message(client.query('Entry'));
   outcomes.readSql = await message(client.readSql('SELECT 1 AS one'));
   outcomes.querySpec = await message(client.querySpec('Entry'));
   outcomes.syncState = await message(client.syncState());
   outcomes.recordState = await message(client.syncState('Entry', { id: 'e' }));
   outcomes.subscribe = await message(client.subscribe('scope'));
   outcomes.subscribeScope = await message(client.subscribeScope('scope'));
   outcomes.unsubscribe = await message(client.unsubscribe('scope'));
   outcomes.rebuild = await message(client.rebuild());
   outcomes.pendingTasks = await message(client.pendingTasks());
   outcomes.setReadiness = await message(client.setReadiness('k', 'ready'));
   outcomes.drop = await message(client.drop(1));
   outcomes.dismissRejection = await message(client.dismissRejection(1));
   outcomes.runPrerequisites = await message(client.runPrerequisites({}));
   const stopped = new Promise(resolve => client.watch('Entry', {}, value => rows.push(value), error => resolve(error)));
   watchError = await within(stopped, 1000);
   assert.equal((await tx.read('Entry', { id: 'e' })).text, 'inside');
  });
  for (const [method, outcome] of Object.entries(outcomes))
   assert.equal(outcome, 'transaction_active', method);
  assert.equal(watchError?.message, 'transaction_active', 'watch reports the refusal to onError');
  assert.deepEqual(rows, [], 'no nested body ran and no watch delivered');
  // Nothing was left parked behind the transaction.
  assert.equal((await client.read('Entry', { id: 'e' })).text, 'inside');
  assert.deepEqual((await client.syncState()).channels, []);
 } finally { await client.close(); await rm(dir, { recursive: true, force: true }); }
});
