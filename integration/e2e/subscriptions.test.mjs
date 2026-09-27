// One assembled scenario for persistent Scope subscriptions (#150): a new
// subscription's origin is the first head the server acknowledges, and that
// origin is established once per subscription identity. Real Node client, native
// Rust engine, HTTP and WebSocket against the round-trip backend on PostgreSQL.
//
// What is published before the origin stays on the server unless the explicit
// bootstrap() loads it, which this scenario never calls: everything published
// after the origin arrives, across a disconnect and across closing and
// reopening the local database. The load itself is
// [bootstrap.test.mjs](bootstrap.test.mjs).
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createExample } from './fixtures/round-trip/server.mts';
import { GeneratedClient } from './fixtures/round-trip/generated/client.ts';

const CHANNEL = 'book:demo';
/** The `bootstrap` part of a status snapshot for a registration that never asked for a load. */
const notRequested = { phase: 'not-requested', error: null };

async function wait(predicate, label, timeout = 10000) {
 const deadline = Date.now() + timeout;
 for (;;) {
  if (await predicate()) return;
  if (Date.now() >= deadline) throw Error(`Timed out waiting for ${label}`);
  await new Promise(resolve => setTimeout(resolve, 10));
 }
}

/** Fails if the condition becomes true within `millis`; absence of a delivery, not proof of settlement. */
async function never(predicate, label, millis = 400) {
 const deadline = Date.now() + millis;
 while (Date.now() < deadline) {
  if (await predicate()) throw Error(`Unexpected: ${label}`);
  await new Promise(resolve => setTimeout(resolve, 10));
 }
}

/** The stored ledger row for `CHANNEL`: identity and both boundaries as SQLite holds them. */
async function ledger(client) {
 const rows = await client.readSql(
  'SELECT subscription_id, starting_cursor, cursor FROM axton_subscription WHERE channel = ?',
  [CHANNEL],
 );
 assert.equal(rows.length, 1, 'one subscription row');
 return rows[0];
}

test('a new subscription starts at the acknowledged head and keeps that origin across reconnect and reopen', { timeout: 60000 }, async () => {
 const app = await createExample();
 const directory = await mkdtemp(join(tmpdir(), 'axton-subscriptions-e2e-'));
 const errors = [];
 const path = join(directory, 'reader.sqlite');
 let client;
 let server;
 /** Publish one Entry on the Scope, the way a background job does. */
 const publish = (id, text) => app.backend.transaction(async ({ tx, channel, touch }) => {
  await tx.entry.upsert({ where: { id }, create: { id, text }, update: { text } });
  touch.entry({ id });
  channel(CHANNEL).entry.add({ id });
 });
 const onError = { onError: error => errors.push(error) };
 try {
  await app.initialize();
  server = await app.listen(0);

  // Published before anyone subscribes: the Scope has a history.
  await publish('old-entry', 'published before the subscription');

  // Offline registration: the intent commits without a connection, and its
  // first boundary is not committed yet.
  client = await GeneratedClient.open({ path });
  const subscription = await client.scopes.subscribe(CHANNEL);
  assert.equal(subscription.scope, CHANNEL);
  assert.deepEqual(subscription.status, { active: true, initialization: 'pending', connection: 'offline', bootstrap: notRequested });
  assert.deepEqual(await ledger(client), { subscription_id: 1, starting_cursor: null, cursor: null });
  assert.equal(await client.scopes.subscribe(CHANNEL), subscription, 'a repeated registration answers the same handle');

  // The first handshake establishes the origin S. Nothing rewinds to zero.
  const observed = [];
  const stopWatching = subscription.watch(status => observed.push(status));
  const connection = await client.connect({ url: server.url, token: 'demo-user' }, onError);
  await wait(() => subscription.status.initialization === 'ready', 'first initialization');
  await wait(() => subscription.status.connection === 'live', 'live delivery');
  const origin = await ledger(client);
  const S = origin.starting_cursor;
  assert.ok(Number.isInteger(S) && S > 0, `the origin is the acknowledged head, not zero: ${S}`);
  assert.deepEqual(origin, { subscription_id: 1, starting_cursor: S, cursor: S }, 'both boundaries commit together');
  // Watching delivers the current snapshot first, then the changes.
  assert.equal(observed[0]?.initialization, 'pending', `the first snapshot is the one at the time of watching: ${JSON.stringify(observed)}`);
  assert.ok(
   observed.some(status => status.initialization === 'ready'),
   `the watcher saw the boundary commit: ${JSON.stringify(observed)}`,
  );

  // The default change: what the Scope held before S is not loaded.
  await never(async () => (await client.models.entry.get({ id: 'old-entry' })) !== null, 'history loaded by subscribing');
  assert.equal(await client.models.entry.get({ id: 'entry-1' }), null, 'the seeded record was published before S too');

  // What is published after S arrives on the stream.
  await publish('new-entry', 'published after the subscription');
  await wait(async () => (await client.models.entry.get({ id: 'new-entry' }))?.text === 'published after the subscription', 'live delivery of a later publication');
  assert.equal(await client.models.entry.get({ id: 'old-entry' }), null, 'a later page does not backfill history');
  assert.equal((await ledger(client)).starting_cursor, S, 'delivery advances the cursor, never the origin');

  // Disconnect, publish during the outage, reconnect: catch-up fills the gap
  // from the committed cursor and the origin is still S.
  await connection.pause();
  await wait(() => subscription.status.connection === 'offline', 'the lane reports the outage');
  const beforeOutage = await ledger(client);
  await publish('outage-entry', 'published while the socket was closed');
  await publish('outage-entry-2', 'also published during the outage');
  await never(async () => (await client.models.entry.get({ id: 'outage-entry' })) !== null, 'delivery while paused');
  await connection.resume();
  await wait(async () => (await client.models.entry.get({ id: 'outage-entry-2' }))?.text === 'also published during the outage', 'catch-up after reconnect');
  assert.equal((await client.models.entry.get({ id: 'outage-entry' })).text, 'published while the socket was closed', 'the gap was filled, not skipped');
  const afterOutage = await ledger(client);
  assert.equal(afterOutage.starting_cursor, S, 'reconnect keeps the origin');
  assert.equal(afterOutage.subscription_id, 1, 'reconnect keeps the subscription identity');
  assert.ok(afterOutage.cursor > beforeOutage.cursor, `the cursor moved forward: ${beforeOutage.cursor} -> ${afterOutage.cursor}`);
  assert.equal(await client.models.entry.get({ id: 'old-entry' }), null, 'catch-up starts at the committed cursor, not at zero');

  // Reopen the local database: the initialization is committed state, so the
  // next session resumes from the cursor instead of initializing again.
  await connection.close();
  await client.close();
  client = await GeneratedClient.open({ path });
  assert.deepEqual(await ledger(client), afterOutage, 'the boundaries survive close and reopen');
  const resumed = await client.scopes.subscribe(CHANNEL);
  assert.deepEqual(
   resumed.status,
   { active: true, initialization: 'ready', connection: 'offline', bootstrap: notRequested },
   'the reopened handle reads the committed boundary at once: no second initialization is pending',
  );

  const reconnected = await client.connect({ url: server.url, token: 'demo-user' }, onError);
  try {
   // The second session delivers for the same registration: it reports live
   // without the handle ever returning to `pending`.
   await wait(() => resumed.status.connection === 'live', 'the reopened subscription is live again');
   assert.equal(resumed.status.initialization, 'ready', 'a second session initializes nothing');
   await publish('after-reopen', 'published after the reopen');
   await wait(async () => (await client.models.entry.get({ id: 'after-reopen' }))?.text === 'published after the reopen', 'delivery after the reopen');
   const final = await ledger(client);
   assert.equal(final.starting_cursor, S, 'the second session did not re-initialize');
   assert.equal(final.subscription_id, 1, 'the subscription identity is the same one');
   assert.ok(final.cursor > afterOutage.cursor, 'the cursor resumed from where it was committed');
   assert.equal(await client.models.entry.get({ id: 'old-entry' }), null, 'still no implicit historical load: bootstrap() is explicit and was never called');
  } finally {
   await reconnected.close();
  }
  stopWatching();
  assert.deepEqual(errors, []);
 } finally {
  await client?.close();
  await server?.close();
  await app.close();
  await rm(directory, { recursive: true, force: true });
 }
});
