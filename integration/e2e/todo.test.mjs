// To-do backend scenarios: real Node clients (native Rust engine) over HTTP/WebSocket
// against the To-do example backend on PostgreSQL. Each test opens its own server,
// temporary SQLite databases and distinct task ids; PostgreSQL rows accumulate for
// the run, so ids never collide with the seeds or with other tests.
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createExample } from '../../examples/todo/server.mts';
import { GeneratedClient } from '../../examples/todo/generated/node/client.ts';
import { declaredModels, syncProtocol } from './protocol-fixture.mjs';

const SCOPE = 'todo:demo';

async function wait(predicate, label, timeout = 10000) {
 const deadline = Date.now() + timeout;
 for (;;) {
  if (await predicate()) return;
  if (Date.now() >= deadline) throw Error(`Timed out waiting for ${label}`);
  await new Promise(resolve => setTimeout(resolve, 10));
 }
}

/** Fails if the condition becomes true within `millis`; absence of an event, not proof of settlement. */
async function never(predicate, label, millis = 300) {
 const deadline = Date.now() + millis;
 while (Date.now() < deadline) {
  if (await predicate()) throw Error(`Unexpected: ${label}`);
  await new Promise(resolve => setTimeout(resolve, 10));
 }
}

async function scenario(body) {
 let app = await createExample();
 const directory = await mkdtemp(join(tmpdir(), 'axton-todo-e2e-'));
 const clients = new Set();
 const scopes = new Map();
 const errors = [];
 const fetchOriginal = globalThis.fetch;
 let server;
 const ctx = {
  get app() { return app; },
  directory,
  errors,
  get url() { return server.url; },
  /**
   * Opens a client database with a live connection under `token`; `server: false`
   * opens it offline. A subscription starts at the first head its handshake
   * acknowledges (#150), so the seeds published at startup are not loaded by
   * subscribing: the client asks for them with `bootstrap()` (#151), which is
   * what the example app does. `ready: false` skips that for a client whose
   * handshake is not expected to succeed, and `bootstrap: false` leaves the
   * Scope's history unloaded.
   */
  async open(name, token, options = {}) {
   const client = await GeneratedClient.open({
    path: join(directory, `${name}.sqlite`),
    ...(options.server === false ? {} : { server: { url: server.url, token }, connection: { onError: error => errors.push(error) } }),
   });
   clients.add(client);
   if (options.subscribe !== false) {
    const subscription = await client.scopes.subscribe(SCOPE);
    scopes.set(name, subscription);
    if (options.server !== false && options.ready !== false) {
     await wait(() => subscription.status.initialization === 'ready', `${name}'s subscription is initialized`);
     if (options.bootstrap !== false) await subscription.bootstrap();
    }
   }
   return client;
  },
  /** The subscription handle `open` registered for `name`. */
  scope: name => scopes.get(name),
  async close(client) {
   clients.delete(client);
   await client.close();
  },
  settled: client => wait(async () => (await client.syncState()).pending === 0, `${client.clientId} settled`),
  rejection: (client, code) => wait(async () => (await client.syncState()).rejections.some(r => r.code === code), `rejection ${code}`),
  row: id => app.db.todo.findUnique({ where: { id } }),
  /** Stops the backend process state (HTTP server and Prisma client) and starts a fresh one on the same port and database. */
  async restart() {
   const port = Number(new URL(server.url).port);
   await server.close();
   await app.close();
   app = await createExample();
   await app.initialize();
   server = await app.listen(port);
  },
  /** Delays HTTP pushes made with `token` until the returned release function runs. */
  gate(token) {
   const opened = Promise.withResolvers();
   const entered = Promise.withResolvers();
   let held = false;
   globalThis.fetch = async (url, init) => {
    if (!held && String(url).endsWith('/sync/mutations') && init?.headers?.authorization === `Bearer ${token}`) {
     held = true;
     entered.resolve();
     await opened.promise;
    }
    return fetchOriginal(url, init);
   };
   return { entered: entered.promise, release: () => { opened.resolve(); globalThis.fetch = fetchOriginal; } };
  },
  /** Holds every historical page request made with `token` until the returned release function runs. */
  holdBootstrap(token) {
   const opened = Promise.withResolvers();
   const entered = Promise.withResolvers();
   globalThis.fetch = async (url, init) => {
    if (String(url).endsWith('/sync/pull') && init?.headers?.authorization === `Bearer ${token}`
     && typeof init.body === 'string' && JSON.parse(init.body).mode === 'bootstrap') {
     entered.resolve();
     await opened.promise;
    }
    return fetchOriginal(url, init);
   };
   return { entered: entered.promise, release: () => { opened.resolve(); globalThis.fetch = fetchOriginal; } };
  },
 };
 try {
  await app.initialize();
  server = await app.listen(0);
  await body(ctx);
 } finally {
  globalThis.fetch = fetchOriginal;
  for (const client of clients) await client.close().catch(() => {});
  await server?.close();
  await app.close();
  await rm(directory, { recursive: true, force: true });
 }
}

const addTodo = (client, todo) => client.mutations.addTodo({ todo });
const setDone = (client, id, done) => client.mutations.setTodoDone({ todo: { id, done } });

test('seeds reach both participants and survive a restart without resetting edits', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  await wait(async () => (await alice.models.todo.query()).length >= 3, 'seed catch-up');
  const seeds = await alice.models.todo.query({ orderBy: [{ field: 'id', direction: 'ascending' }] });
  assert.deepEqual(seeds.filter(t => t.id.startsWith('seed-')), [
   { id: 'seed-1', title: 'Buy milk', done: false, createdById: 'alice' },
   { id: 'seed-2', title: 'Book a table', done: false, createdById: 'bob' },
   { id: 'seed-3', title: 'Pick up keys', done: false, createdById: 'alice' },
  ]);
  assert.deepEqual(await alice.models.user.get({ id: 'bob' }), { id: 'bob', name: 'Bob' });
  await setDone(alice, 'seed-3', true);
  await ctx.settled(alice);
  await ctx.app.initialize();
  assert.equal((await ctx.row('seed-3')).done, true, 'initialize keeps edits');
  await setDone(alice, 'seed-3', false);
  await ctx.settled(alice);
  assert.equal((await ctx.row('seed-3')).done, false);
  assert.equal(ctx.errors.length, 0);
 });
});

// A durable Action submitted while the Scope's history is still loading
// ([#151](https://github.com/zanminwang/axton/issues/151)): the two are separate
// work, so the call completes from its receipt (A3) and the load finishes
// afterwards with its own coverage intact.
test('an Action submitted during the historical load completes, and the load still covers the Scope', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice', { bootstrap: false });
  const subscription = ctx.scope('alice');
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'not-requested', error: null });
  assert.equal(await alice.models.todo.get({ id: 'seed-1' }), null, 'subscribing loaded nothing published before the origin');

  // The historical page is held on the wire for the whole of the Action.
  const held = ctx.holdBootstrap('alice');
  const loading = subscription.bootstrap();
  loading.catch(() => {});
  await held.entered;
  await wait(() => subscription.status.bootstrap.phase === 'loading', 'a running load');
  await addTodo(alice, { id: 'during-1', title: 'Added while loading', done: false, createdById: 'alice' });
  await ctx.settled(alice);
  assert.equal((await alice.models.todo.get({ id: 'during-1' })).title, 'Added while loading', 'the receipt applied without waiting for the load');
  assert.deepEqual(await ctx.row('during-1'), { id: 'during-1', title: 'Added while loading', done: false, createdById: 'alice' });
  assert.equal(subscription.status.bootstrap.phase, 'loading', 'the Action disturbed no historical progress');

  held.release();
  await loading;
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null });
  assert.equal((await alice.models.todo.get({ id: 'seed-1' })).title, 'Buy milk', 'the load covered the Scope\'s history');
  assert.deepEqual(await alice.models.user.get({ id: 'bob' }), { id: 'bob', name: 'Bob' });
  assert.equal((await alice.models.todo.get({ id: 'during-1' })).title, 'Added while loading', 'and delivered no older authority over the receipt');
  assert.equal(ctx.errors.length, 0);
 });
});

test('happy path: Alice adds, Bob completes, PostgreSQL and both clients converge', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  const bob = await ctx.open('bob', 'bob');
  const observed = [];
  const unwatch = bob.models.todo.watch({ where: { id: 'happy-1' } }, rows => observed.push(rows));
  await addTodo(alice, { id: 'happy-1', title: '  Buy milk  ', done: false, createdById: 'alice' });
  assert.equal((await alice.models.todo.get({ id: 'happy-1' })).title, '  Buy milk  ', 'local commit is immediate and untrimmed');
  await ctx.settled(alice);
  assert.deepEqual(await ctx.row('happy-1'), { id: 'happy-1', title: 'Buy milk', done: false, createdById: 'alice' });
  await wait(async () => observed.some(rows => rows.length === 1 && rows[0].title === 'Buy milk'), 'Bob watch delivers the trimmed row');
  await wait(async () => (await alice.models.todo.get({ id: 'happy-1' }))?.title === 'Buy milk', 'Alice replays the server value');
  assert.equal((await bob.models.todo.query({ where: { id: 'happy-1' } })).length, 1);
  await setDone(bob, 'happy-1', true);
  assert.equal((await bob.models.todo.get({ id: 'happy-1' })).done, true);
  await ctx.settled(bob);
  await ctx.settled(alice);
  assert.deepEqual(await ctx.row('happy-1'), { id: 'happy-1', title: 'Buy milk', done: true, createdById: 'alice' });
  await wait(async () => (await alice.models.todo.get({ id: 'happy-1' }))?.done === true, 'Alice receives completion');
  assert.deepEqual(await alice.models.todo.get({ id: 'happy-1' }), await ctx.row('happy-1'));
  assert.deepEqual(await bob.models.todo.get({ id: 'happy-1' }), await ctx.row('happy-1'));
  unwatch();
  assert.deepEqual((await alice.syncState()).rejections, []);
  assert.deepEqual((await bob.syncState()).rejections, []);
  assert.equal(ctx.errors.length, 0, String(ctx.errors));
 });
});

for (const [name, todo, code] of [
 ['whitespace-only title', { id: 'reject-title', title: '   ', done: false, createdById: 'alice' }, 'todo.title_empty'],
 ['creator other than the authenticated user', { id: 'reject-creator', title: 'Spoofed', done: false, createdById: 'bob' }, 'todo.creator_invalid'],
 ['initial done=true', { id: 'reject-done', title: 'Already done', done: true, createdById: 'alice' }, 'todo.initial_state_invalid'],
]) {
 test(`${name} is rejected as ${code} and rolled back locally`, async () => {
  await scenario(async ctx => {
   const alice = await ctx.open('alice', 'alice');
   const bob = await ctx.open('bob', 'bob');
   await addTodo(alice, todo);
   assert.deepEqual(await alice.models.todo.get({ id: todo.id }), todo, 'optimistic local row');
   await ctx.rejection(alice, code);
   await ctx.settled(alice);
   assert.equal(await alice.models.todo.get({ id: todo.id }), null, 'local row rolled back');
   assert.equal(await ctx.row(todo.id), null, 'nothing persisted');
   await never(async () => (await bob.models.todo.get({ id: todo.id })) !== null, 'Bob receives a rejected row');
   assert.equal(ctx.errors.length, 0, String(ctx.errors));
  });
 });
}

test('unknown identity token is refused with HTTP 401 and persists nothing', async () => {
 await scenario(async ctx => {
  // Its token is refused, so no handshake ever acknowledges a head: the
  // subscription stays uninitialized and the seeds are never published for it.
  const mallory = await ctx.open('mallory', 'mallory', { ready: false });
  await addTodo(mallory, { id: 'mallory-1', title: 'Intruder', done: false, createdById: 'mallory' });
  await wait(() => ctx.errors.some(error => error?.status === 401), 'connection.onError reports 401');
  assert.equal(await ctx.row('mallory-1'), null);
  assert.equal((await mallory.syncState()).pending, 1, 'the mutation stays queued locally');
  assert.deepEqual((await mallory.syncState()).rejections, []);
  assert.equal((await mallory.models.todo.query()).length, 1, 'no seeds are delivered to an unauthenticated client');
  await never(async () => (await ctx.row('mallory-1')) !== null, 'row persisted for unknown identity');
 });
});

test('setTodoDone on a task the server no longer has is rejected as todo.missing', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  await addTodo(alice, { id: 'missing-1', title: 'Doomed', done: false, createdById: 'alice' });
  await ctx.settled(alice);
  await ctx.app.db.todo.delete({ where: { id: 'missing-1' } });
  await setDone(alice, 'missing-1', true);
  await ctx.rejection(alice, 'todo.missing');
  await ctx.settled(alice);
  assert.equal(await ctx.row('missing-1'), null);
  assert.equal((await alice.models.todo.get({ id: 'missing-1' })).done, false, 'local completion rolled back');
  await setDone(alice, 'seed-1', false);
  await ctx.settled(alice);
  assert.deepEqual((await alice.syncState()).rejections.map(r => r.code), ['todo.missing'], 'the transaction stays usable after a rejection');
  assert.equal(ctx.errors.length, 0, String(ctx.errors));
 });
});

test('a distinct create with an existing id is rejected as todo.id_conflict and never overwrites', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  const bob = await ctx.open('bob', 'bob');
  await wait(async () => (await bob.models.todo.query()).length >= 3, 'Bob catches up');
  await bob.connection.pause();
  await addTodo(alice, { id: 'conflict-1', title: 'Original', done: false, createdById: 'alice' });
  await ctx.settled(alice);
  await addTodo(bob, { id: 'conflict-1', title: 'Impostor', done: false, createdById: 'bob' });
  assert.equal((await bob.models.todo.get({ id: 'conflict-1' })).title, 'Impostor');
  const calls = ctx.app.handlerCalls;
  await bob.connection.resume();
  await ctx.rejection(bob, 'todo.id_conflict');
  await ctx.settled(bob);
  assert.deepEqual(await ctx.row('conflict-1'), { id: 'conflict-1', title: 'Original', done: false, createdById: 'alice' });
  await wait(async () => (await bob.models.todo.get({ id: 'conflict-1' }))?.title === 'Original', 'Bob converges to the existing row');
  assert.equal(ctx.app.handlerCalls, calls + 1, 'one refused handler call');
  await setDone(bob, 'conflict-1', true);
  await ctx.settled(bob);
  assert.equal((await ctx.row('conflict-1')).done, true, 'the connection stays usable after the refusal');
  // If the page carrying Alice's row lands before Bob's receipt, Bob's queued
  // create no longer replays over it and is reported as diverged (D8). Nothing
  // else may reach onError.
  const others = ctx.errors.filter(error => !(error?.kind === 'diverged' && error.identity?.id === 'conflict-1'));
  assert.equal(others.length, 0, String(others));
 });
});

test('a retried frozen request after a lost receipt runs the handler once and stores one row', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice', { server: false, subscribe: false });
  const transport = async (kind, body) => {
   const response = await fetch(`${ctx.url}/sync/${kind === 'push' ? 'mutations' : 'pull'}`, { method: 'POST', headers: { authorization: 'Bearer alice', 'content-type': 'application/json' }, body });
   if (!response.ok) throw Error(`HTTP ${response.status}: ${await response.text()}`);
   return response.text();
  };
  const models = declaredModels(ctx.app.schema);
  // The raw wire fixture pulls from a committed cursor and never from zero, so
  // this client first establishes its origin the way an application does: one
  // live session whose acknowledged head becomes the subscription's first
  // boundary. The seeds are published again after that and the pull delivers them.
  const subscription = await alice.scopes.subscribe(SCOPE);
  const origin = await alice.connect({ url: ctx.url, token: 'alice' }, { onError: error => ctx.errors.push(error) });
  await wait(() => subscription.status.initialization === 'ready', 'the origin is committed');
  await origin.close();
  assert.equal(await alice.models.todo.get({ id: 'seed-1' }), null, 'subscribing loaded nothing published earlier');
  await ctx.app.publishSeeds();
  await syncProtocol(alice.client, transport, models);
  assert.equal((await alice.models.todo.get({ id: 'seed-1' })).title, 'Buy milk');
  await addTodo(alice, { id: 'retry-1', title: 'Once', done: false, createdById: 'alice' });
  const before = ctx.app.handlerCalls;
  let dropped = false;
  await assert.rejects(() => syncProtocol(alice.client, async (kind, body) => {
   const result = await transport(kind, body);
   if (kind === 'push' && !dropped) { dropped = true; throw Error('lost receipt after commit'); }
   return result;
  }, models), /lost receipt/);
  assert.equal(ctx.app.handlerCalls, before + 1, 'the first push committed');
  assert.deepEqual(await ctx.row('retry-1'), { id: 'retry-1', title: 'Once', done: false, createdById: 'alice' });
  assert.equal((await alice.syncState()).pending, 1, 'the request stays frozen until acknowledged');
  await syncProtocol(alice.client, transport, models);
  assert.equal(ctx.app.handlerCalls, before + 1, 'the replayed receipt does not run the handler again');
  assert.equal((await alice.syncState()).pending, 0);
  assert.deepEqual((await alice.syncState()).rejections, []);
  assert.equal(await ctx.app.db.todo.count({ where: { id: 'retry-1' } }), 1);
 });
});

test('offline add-then-done survives relaunch and syncs in order while Bob keeps working', async () => {
 await scenario(async ctx => {
  let alice = await ctx.open('alice', 'alice');
  const bob = await ctx.open('bob', 'bob');
  await wait(async () => (await alice.models.todo.query()).length >= 3, 'Alice catches up');
  await alice.connection.pause();
  await addTodo(alice, { id: 'offline-1', title: 'Offline task', done: false, createdById: 'alice' });
  await setDone(alice, 'offline-1', true);
  assert.equal((await alice.syncState()).pending, 2);
  assert.equal((await alice.models.todo.get({ id: 'offline-1' })).done, true);
  await addTodo(bob, { id: 'offline-2', title: 'Meanwhile', done: false, createdById: 'bob' });
  await ctx.settled(bob);
  assert.equal(await ctx.row('offline-1'), null, 'nothing reaches the server while paused');
  await never(async () => (await alice.models.todo.get({ id: 'offline-2' })) !== null, 'paused Alice receives remote rows');
  const clientId = alice.clientId;
  await ctx.close(alice);
  alice = await ctx.open('alice', 'alice', { server: false, subscribe: false });
  assert.equal(alice.clientId, clientId, 'client identity survives relaunch');
  assert.equal((await alice.syncState()).pending, 2, 'queued work survives relaunch');
  assert.deepEqual(await alice.models.todo.get({ id: 'offline-1' }), { id: 'offline-1', title: 'Offline task', done: true, createdById: 'alice' });
  const connection = await alice.connect({ url: ctx.url, token: 'alice' }, { onError: error => ctx.errors.push(error) });
  try {
   await ctx.settled(alice);
   assert.deepEqual(await ctx.row('offline-1'), { id: 'offline-1', title: 'Offline task', done: true, createdById: 'alice' });
   await wait(async () => (await alice.models.todo.get({ id: 'offline-2' }))?.title === 'Meanwhile', 'Alice receives Bob\'s task');
   await wait(async () => (await bob.models.todo.get({ id: 'offline-1' }))?.done === true, 'Bob receives the completed task');
   assert.equal(await ctx.app.db.todo.count({ where: { id: { in: ['offline-1', 'offline-2'] } } }), 2, 'no duplicates');
   assert.deepEqual((await alice.syncState()).rejections, []);
   assert.equal(ctx.errors.length, 0, String(ctx.errors));
  } finally {
   await connection.close();
  }
 });
});

for (const [order, heldToken, heldValue, freeToken, freeValue] of [
 ['Bob commits false first, then Alice true', 'alice', true, 'bob', false],
 ['Alice commits true first, then Bob false', 'bob', false, 'alice', true],
]) {
 test(`opposing setTodoDone: ${order}; both converge to PostgreSQL`, async () => {
  await scenario(async ctx => {
   const id = `race-${heldToken}`;
   const clients = { alice: await ctx.open('alice', 'alice'), bob: await ctx.open('bob', 'bob') };
   await addTodo(clients.alice, { id, title: 'Contested', done: false, createdById: 'alice' });
   await ctx.settled(clients.alice);
   await wait(async () => (await clients.bob.models.todo.get({ id })) !== null, 'Bob receives the row');
   const held = clients[heldToken];
   const free = clients[freeToken];
   const gate = ctx.gate(heldToken);
   await setDone(held, id, heldValue);
   await gate.entered;
   await setDone(free, id, freeValue);
   await ctx.settled(free);
   assert.equal((await ctx.row(id)).done, freeValue, 'the free client commits first');
   assert.equal((await held.syncState()).pending, 1, 'the held push is still in flight');
   gate.release();
   await ctx.settled(held);
   const final = await ctx.row(id);
   assert.equal(final.done, heldValue, 'the last committed update wins');
   await wait(async () => (await held.models.todo.get({ id }))?.done === final.done, 'held client converges');
   await wait(async () => (await free.models.todo.get({ id }))?.done === final.done, 'free client converges');
   await ctx.settled(free);
   assert.deepEqual((await held.syncState()).rejections, []);
   assert.deepEqual((await free.syncState()).rejections, []);
   assert.equal(ctx.errors.length, 0, String(ctx.errors));
  });
 });
}

test('setting done true twice remains true', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  await addTodo(alice, { id: 'twice-1', title: 'Twice', done: false, createdById: 'alice' });
  await setDone(alice, 'twice-1', true);
  await setDone(alice, 'twice-1', true);
  await ctx.settled(alice);
  assert.equal((await ctx.row('twice-1')).done, true);
  assert.equal((await alice.models.todo.get({ id: 'twice-1' })).done, true);
  assert.deepEqual((await alice.syncState()).rejections, []);
 });
});

test('a backend restart on the same database keeps state and delivers work queued while it was down', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  await wait(async () => (await alice.models.todo.query()).length >= 3, 'Alice catches up');
  await addTodo(alice, { id: 'restart-1', title: 'Before restart', done: false, createdById: 'alice' });
  await ctx.settled(alice);
  await setDone(alice, 'seed-2', true);
  await ctx.settled(alice);
  await ctx.restart();
  assert.equal((await ctx.row('seed-2')).done, true, 'seeding after restart does not reset edits');
  assert.deepEqual(await ctx.row('restart-1'), { id: 'restart-1', title: 'Before restart', done: false, createdById: 'alice' });
  // Bob joins through the restarted backend before the next task is created, so
  // it reaches him on the stream: his subscription starts at the head his
  // handshake acknowledged and loads nothing older (#150).
  const bob = await ctx.open('bob', 'bob');
  await wait(async () => (await bob.models.todo.get({ id: 'seed-2' }))?.done === true, 'the republished seeds carry the edit made before the restart');
  await addTodo(alice, { id: 'restart-2', title: 'After restart', done: false, createdById: 'alice' });
  await ctx.settled(alice);
  assert.equal(ctx.app.handlerCalls, 1, 'only the post-restart mutation ran on the new backend');
  await wait(async () => (await bob.models.todo.get({ id: 'restart-2' }))?.title === 'After restart', 'Bob loads through the restarted backend');
 });
});

test('a completion request without a boolean done is an empty patch: a no-op the handler sees once', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  await wait(async () => (await alice.models.todo.get({ id: 'seed-1' })) !== null, 'Alice catches up');
  const before = ctx.app.handlerCalls;
  await alice.mutations.setTodoDone({ todo: { id: 'seed-1' } });
  await ctx.settled(alice);
  assert.equal(ctx.app.handlerCalls, before + 1, 'the handler runs with an empty patch');
  assert.deepEqual((await alice.syncState()).rejections, []);
  assert.equal((await ctx.row('seed-1')).done, false, 'nothing was written');
 });
});

test('direct result keeps its Loader snapshot while an independent durable edit replays', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  await wait(async () => (await alice.models.todo.get({ id: 'seed-1' })) !== null, 'seed is local');
  const gate = ctx.gate('alice');
  try {
   const pending = await setDone(alice, 'seed-1', true);
   await gate.entered;
   assert.equal((await alice.models.todo.get({ id: 'seed-1' })).done, true, 'durable optimism is visible');
   const result = await alice.mutations.call.setTodoDone({ todo: { id: 'seed-1', done: false } });
   assert.equal(result.todo.done, false, 'direct result is the committed Loader snapshot');
   assert.equal((await alice.models.todo.get({ id: 'seed-1' })).done, true, 'pending optimism replays over direct authority');
   assert.equal((await alice.syncState()).pending, 1, 'direct call does not drain the independent durable queue');
   assert.equal(pending.status, 'pending');
  } finally { gate.release(); }
  await ctx.settled(alice);
  assert.equal((await alice.models.todo.get({ id: 'seed-1' })).done, true);
  assert.equal((await ctx.row('seed-1')).done, true);
  assert.equal(ctx.errors.length, 0, String(ctx.errors));
 });
});

test('a direct retry replays the stored result snapshot after a later update', async () => {
 await scenario(async ctx => {
  const alice = await ctx.open('alice', 'alice');
  await wait(async () => (await alice.models.todo.get({ id: 'seed-1' })) !== null, 'seed is local');
  const originalFetch = globalThis.fetch;
  let firstBody;
  globalThis.fetch = (url, init) => {
   if (String(url).endsWith('/sync/actions') && firstBody === undefined) firstBody = init?.body;
   return originalFetch(url, init);
  };
  try {
   const first = await alice.mutations.call.setTodoDone({ todo: { id: 'seed-1', done: true } });
   assert.equal(first.todo.done, true);
   assert.equal(typeof firstBody, 'string');
   const second = await alice.mutations.call.setTodoDone({ todo: { id: 'seed-1', done: false } });
   assert.equal(second.todo.done, false);
   assert.equal((await ctx.row('seed-1')).done, false);
   const calls = ctx.app.handlerCalls;
   const replay = await originalFetch(`${ctx.url}/sync/actions`, {
    method: 'POST', headers: { authorization: 'Bearer alice', 'content-type': 'application/json' }, body: firstBody,
   });
   assert.equal(replay.status, 200);
   const stored = await replay.json();
   assert.equal(stored.completion.outcome.result.todo.done, true, 'retry returns A, not later B');
   assert.equal(ctx.app.handlerCalls, calls, 'retry does not execute the handler again');
   assert.equal((await ctx.row('seed-1')).done, false, 'retry does not overwrite B');
  } finally { globalThis.fetch = originalFetch; }
 });
});
