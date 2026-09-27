// One assembled scenario set for whole-Scope bootstrap
// ([#151](https://github.com/zanminwang/axton/issues/151)) on top of persistent
// subscriptions ([#150](https://github.com/zanminwang/axton/issues/150)). Real
// Node client, native Rust engine, HTTP and WebSocket against the round-trip
// backend on PostgreSQL.
//
// The contract under test: a subscription's origin *S* is the first head its
// handshake acknowledges, `bootstrap()` walks the historical interval `(0, S]`
// in bounded pages while ordinary delivery continues after *S*, and the run
// completes once the interval is finished and delivery has reached the head
// *H* the final historical page saw. Completion says that interval and that
// barrier were processed - not that a snapshot was taken, and not that every
// record resolved successfully (guarantee D7).
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createExample } from './fixtures/round-trip/server.mts';
import { GeneratedClient } from './fixtures/round-trip/generated/client.ts';

const TOKEN = 'demo-user';

async function wait(predicate, label, timeout = 20000) {
 const deadline = Date.now() + timeout;
 for (;;) {
  if (await predicate()) return;
  if (Date.now() >= deadline) throw Error(`Timed out waiting for ${label}`);
  await new Promise(resolve => setTimeout(resolve, 10));
 }
}

/** Fails if the condition becomes true within `millis`; absence of an event, not proof of settlement. */
async function never(predicate, label, millis = 400) {
 const deadline = Date.now() + millis;
 while (Date.now() < deadline) {
  if (await predicate()) throw Error(`Unexpected: ${label}`);
  await new Promise(resolve => setTimeout(resolve, 10));
 }
}

/**
 * Keeps a background load's rejection attached, so a scenario that fails before
 * it awaits the call reports its own error instead of an unhandled rejection.
 */
function background(loading) { loading.catch(() => {}); return loading; }

/** The phases a load passes through, in the order the ledger commits them. */
const RANK = { 'not-requested': 0, 'waiting-for-initialization': 1, loading: 2, 'catching-up': 3, complete: 4, failed: 4 };
/** Records every phase change of one handle, collapsing repeats. */
function phases(subscription) {
 const seen = [];
 const stop = subscription.watch(status => { if (seen.at(-1) !== status.bootstrap.phase) seen.push(status.bootstrap.phase); });
 return {
  get seen() { return [...seen]; },
  stop,
  /** No phase ever goes backwards, whatever the intermediate states were. */
  assertForward() {
   for (let i = 1; i < seen.length; i++)
    assert.ok(RANK[seen[i]] >= RANK[seen[i - 1]], `phases never move backwards: ${seen.join(' -> ')}`);
  },
 };
}

/**
 * Intercepts `/sync/pull` so a scenario can see every request the client made
 * and hold one in flight. `hold('request', match)` stops a request before it
 * reaches the server; `hold('response', match)` lets the server compute its
 * page and holds the answer, which is how a page resolved at older stamps
 * arrives after newer authority.
 */
function intercept() {
 const original = globalThis.fetch;
 const requests = [];
 const holds = [];
 globalThis.fetch = async (url, init) => {
  if (!String(url).endsWith('/sync/pull') || typeof init?.body !== 'string') return original(url, init);
  const body = JSON.parse(init.body);
  requests.push(body);
  for (const hold of holds)
   if (!hold.released && hold.phase === 'request' && hold.match(body)) { hold.enter.resolve(body); await hold.gate.promise; }
  const response = await original(url, init);
  const after = holds.filter(hold => !hold.released && hold.phase === 'response' && hold.match(body));
  if (after.length === 0) return response;
  const text = await response.text();
  for (const hold of after) { hold.enter.resolve(body); await hold.gate.promise; }
  return new Response(text, { status: response.status, headers: { 'content-type': 'application/json' } });
 };
 return {
  requests,
  /** Bounded historical page requests, in the order the worker issued them. */
  get loads() { return requests.filter(request => request.mode === 'bootstrap'); },
  /** Ordinary catch-up pulls; the live stream does not go through HTTP. */
  get deltas() { return requests.filter(request => request.mode === undefined); },
  hold(phase, match) {
   const hold = { phase, match, released: false, gate: Promise.withResolvers(), enter: Promise.withResolvers() };
   holds.push(hold);
   return { entered: hold.enter.promise, release() { hold.released = true; hold.gate.resolve(); } };
  },
  restore() { globalThis.fetch = original; },
 };
}

/** The stored subscription row for `scope`: the #150 boundaries and the #151 load ledger. */
async function ledger(client, scope) {
 const rows = await client.readSql(
  'SELECT subscription_id, starting_cursor, cursor, bootstrap_state, bootstrap_run, bootstrap_cursor, bootstrap_barrier, bootstrap_error FROM axton_subscription WHERE channel = ?',
  [scope],
 );
 assert.equal(rows.length, 1, `one subscription row for ${scope}`);
 return rows[0];
}

/** The local stamp of one `Entry`, as the client's record metadata holds it. */
async function stampOf(client, id) {
 const rows = await client.readSql('SELECT stamp FROM axton_record WHERE model = ? AND identity = ?', ['Entry', JSON.stringify({ id })]);
 return rows.length === 0 ? null : rows[0].stamp;
}

/** Every named Entry is present locally with the text `publishMany` gave it. */
async function assertLoaded(client, ids) {
 const missing = [];
 for (const id of ids) if ((await client.models.entry.get({ id }))?.text !== `${id} text`) missing.push(id);
 assert.deepEqual(missing, [], `${missing.length} of ${ids.length} historical records are missing or wrong`);
}

async function scenario(body) {
 const app = await createExample();
 const directory = await mkdtemp(join(tmpdir(), 'axton-bootstrap-e2e-'));
 const errors = [];
 const net = intercept();
 const opened = new Set();
 let server;
 const context = {
  app,
  errors,
  net,
  directory,
  get url() { return server.url; },
  /** Reports the client received for records a delivery could not apply (D7/D8). */
  get reports() { return errors.filter(error => error.name === 'AxtonReport'); },
  /** Opens a client database under `name`; `connect: false` leaves it offline. */
  async open(name, options = {}) {
   const client = await GeneratedClient.open({ path: join(directory, `${name}.sqlite`) });
   opened.add(client);
   if (options.connect !== false) await context.connect(client);
   return client;
  },
  connect: client => client.connect({ url: server.url, token: TOKEN }, { onError: error => errors.push(error) }),
  async close(client) { opened.delete(client); await client.close(); },
 };
 try {
  await app.initialize();
  // Every scenario asserts channel heads and cursors, so it starts from an
  // empty invalidation log rather than from what an earlier one published.
  await app.reset();
  server = await app.listen(0);
  await body(context);
 } finally {
  net.restore();
  for (const client of opened) await client.close().catch(() => {});
  await server?.close();
  await app.close();
  await rm(directory, { recursive: true, force: true });
 }
}

// The critical case of the coverage argument: a record published before the
// origin whose latest publication moves above it belongs to the subscription's
// delivery, not to the historical scan, and the completion barrier joins the
// two paths (spec sections 3 and 6).
test('a record that moves above the origin comes from delivery, and the load still covers the interval', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:moving';
  const { app, net } = ctx;
  const before = await app.publishMany(40, { channel: SCOPE, prefix: 'hist', from: 1 });
  await app.publishOne('moving', 'the record that moves', [SCOPE]);
  const after = await app.publishMany(79, { channel: SCOPE, prefix: 'hist', from: 41 });
  const history = [...before, ...after];
  assert.equal(await app.head(SCOPE), 120, 'one cursor per publication');
  const moved = await app.positionOf(SCOPE, 'moving');
  assert.ok(moved > 0 && moved <= 120, `the record starts inside the historical interval at ${moved}`);

  const client = await ctx.open('reader');
  const subscription = await client.scopes.subscribe(SCOPE);
  const observed = phases(subscription);
  await wait(() => subscription.status.initialization === 'ready', 'the committed origin');
  const S = (await ledger(client, SCOPE)).starting_cursor;
  assert.equal(S, 120, 'the origin is the acknowledged head');
  assert.equal(await client.models.entry.get({ id: 'hist-1' }), null, 'subscribing loads no history (D9)');
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'not-requested', error: null });

  // The first historical request is held before it reaches the server, so the
  // record moves above S strictly before any scan could have seen it there.
  const held = net.hold('request', body => body.mode === 'bootstrap');
  const loading = background(subscription.bootstrap());
  await held.entered;
  assert.equal((await ledger(client, SCOPE)).bootstrap_cursor, 0, 'no page has been applied yet');
  // Removed, then added back in a second settlement: an unchanged record takes a
  // new position at the stamp it already has.
  await app.readd(['moving'], SCOPE);
  const republished = await app.positionOf(SCOPE, 'moving');
  assert.equal(republished, 121, 'the re-addition replaced its one retained position');
  assert.ok(republished > S, `its latest publication is above the origin: ${republished} > ${S}`);

  // Normal delivery supplies it while the historical interval is untouched.
  await wait(async () => (await client.models.entry.get({ id: 'moving' }))?.text === 'the record that moves', 'live delivery of the moved record');
  const row = await ledger(client, SCOPE);
  assert.equal(row.bootstrap_cursor, 0, 'the subscription delivered it, not the historical scan');
  assert.equal(row.cursor, 121, 'delivery moved to the republication');
  assert.equal(row.starting_cursor, S, 'the origin never moves');
  const deliveredStamp = await stampOf(client, 'moving');
  assert.equal(await client.models.entry.get({ id: 'hist-1' }), null, 'a later page does not backfill history');

  held.release();
  await loading;
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null });
  observed.assertForward();
  assert.deepEqual(observed.seen.at(0), 'not-requested');
  assert.deepEqual(observed.seen.at(-1), 'complete');
  observed.stop();

  // Bounded pages with a fixed upper bound, and the compacted row the move left
  // behind: position 41 is now a hole, so the first page's fifty rows
  // span fifty-one positions and the next page continues from 51. A compacted
  // interval is expected, not a defect.
  assert.deepEqual(net.loads.map(load => load.after), [0, 51, 101], 'each page continued from committed progress');
  assert.deepEqual([...new Set(net.loads.map(load => load.until))], [S], 'every page is bounded by the origin, never by a moving head');
  const final = await ledger(client, SCOPE);
  assert.equal(final.bootstrap_cursor, S, 'the interval finished at the origin');
  assert.equal(final.bootstrap_barrier, 121, 'the barrier is the head the final page saw');
  assert.equal(final.starting_cursor, S);
  await assertLoaded(client, history);
  assert.equal((await client.models.entry.get({ id: 'moving' })).text, 'the record that moves');
  assert.equal(await stampOf(client, 'moving'), deliveredStamp, 'the historical scan never rewrote what delivery had already applied');
  assert.deepEqual(ctx.reports, [], 'nothing failed to apply');
 });
});

// Record stamps order authority across both paths: a historical page resolved
// before newer authority landed cannot regress it, and cannot resurrect a
// deletion that arrived first (guarantees D2 and D5, spec section 6).
test('an older historical page never regresses newer content or resurrects a newer deletion', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:stamps';
  const { app, net } = ctx;
  const kept = await app.publishMany(8, { channel: SCOPE, prefix: 'kept' });
  await app.publishOne('updated', 'the original text', [SCOPE]);
  await app.publishOne('removed', 'present at first', [SCOPE]);
  const S = await app.head(SCOPE);
  assert.equal(S, 10);

  const client = await ctx.open('reader');
  const subscription = await client.scopes.subscribe(SCOPE);
  await wait(() => subscription.status.initialization === 'ready', 'the committed origin');
  assert.equal((await ledger(client, SCOPE)).starting_cursor, S);

  // The server resolves the whole interval at the stamps it reads now; the
  // answer is held on the wire.
  const held = net.hold('response', body => body.mode === 'bootstrap');
  const loading = background(subscription.bootstrap());
  await held.entered;

  // Newer authority arrives first, by ordinary delivery: one update and one
  // authoritative deletion.
  await app.publishOne('updated', 'the newer text', [SCOPE]);
  await app.tombstone('removed');
  const head = await app.head(SCOPE);
  // The deleted record was never local, so its absence proves nothing until
  // the delivery that carries the deletion has been applied.
  await wait(async () => (await ledger(client, SCOPE)).cursor >= head, 'the newer authority was delivered');
  assert.equal((await client.models.entry.get({ id: 'updated' }))?.text, 'the newer text', 'the live update');
  assert.equal(await client.models.entry.get({ id: 'removed' }), null, 'the live deletion');
  const updatedStamp = await stampOf(client, 'updated');
  const removedStamp = await stampOf(client, 'removed');
  assert.ok(removedStamp > 0, 'the stamp of a deleted record is retained as evidence (D5)');

  held.release();
  await loading;
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null }, 'older authority is idempotent, not a failure');
  assert.equal((await client.models.entry.get({ id: 'updated' })).text, 'the newer text', 'the older page did not regress the update');
  assert.equal(await client.models.entry.get({ id: 'removed' }), null, 'the older page did not resurrect the deletion');
  assert.equal(await stampOf(client, 'updated'), updatedStamp, 'no stamp moved backwards');
  assert.equal(await stampOf(client, 'removed'), removedStamp);
  await assertLoaded(client, kept);
  assert.equal((await ledger(client, SCOPE)).bootstrap_cursor, S);
  assert.deepEqual(ctx.reports, [], 'an older record on a page is applied by stamp, with nothing to report');
 });
});

// Pagination has a fixed upper bound and monotone progress, so it terminates
// however much is published while it runs; the boundary cases are an interval
// that is an exact page and one that is empty (spec sections 3 and 4).
test('exact and empty intervals terminate, and so does one under continuous publication', { timeout: 180000 }, async () => {
 await scenario(async ctx => {
  const { app, net } = ctx;
  const client = await ctx.open('reader');

  // Exactly 50 records: one page whose last row is the origin, so it is
  // terminal and nothing asks for a second.
  const EXACT = 'bootstrap:exact';
  const fifty = await app.publishMany(50, { channel: EXACT, prefix: 'exact' });
  const exact = await client.scopes.subscribe(EXACT);
  await wait(() => exact.status.initialization === 'ready', 'the origin of the exact Scope');
  assert.equal((await ledger(client, EXACT)).starting_cursor, 50);
  await exact.bootstrap();
  assert.deepEqual({ ...exact.status.bootstrap }, { phase: 'complete', error: null });
  const exactLoads = net.loads.filter(load => load.channel === EXACT);
  assert.deepEqual(exactLoads.map(load => [load.after, load.until]), [[0, 50]], 'an exact page is terminal: one request, no empty follow-up');
  await assertLoaded(client, fifty);
  assert.equal((await ledger(client, EXACT)).bootstrap_cursor, 50);

  // A Scope nobody published to: the origin is head zero and the one empty
  // page completes the run.
  const EMPTY = 'bootstrap:empty';
  const empty = await client.scopes.subscribe(EMPTY);
  await wait(() => empty.status.initialization === 'ready', 'the origin of the empty Scope');
  assert.deepEqual(await ledger(client, EMPTY), {
   subscription_id: 2, starting_cursor: 0, cursor: 0,
   bootstrap_state: 'not_requested', bootstrap_run: 0, bootstrap_cursor: 0, bootstrap_barrier: null, bootstrap_error: null,
  });
  await empty.bootstrap();
  assert.deepEqual({ ...empty.status.bootstrap }, { phase: 'complete', error: null }, 'a zero-head Scope completes normally');
  const emptyRow = await ledger(client, EMPTY);
  assert.equal(emptyRow.bootstrap_cursor, 0);
  assert.equal(emptyRow.bootstrap_barrier, 0);

  // 130 historical records while new ones keep being published: the pages are
  // bounded by the origin, so the interval finishes even though the head does
  // not stop moving.
  const BUSY = 'bootstrap:busy';
  const history = await app.publishMany(130, { channel: BUSY, prefix: 'busy' });
  const busy = await client.scopes.subscribe(BUSY);
  await wait(() => busy.status.initialization === 'ready', 'the origin of the busy Scope');
  const S = (await ledger(client, BUSY)).starting_cursor;
  assert.equal(S, 130);
  let settled = false;
  const later = [];
  // Hold the second page, so the publications below are demonstrably made
  // while the historical interval is being paged rather than before or after.
  const paging = net.hold('request', body => body.mode === 'bootstrap' && body.channel === BUSY && body.after === 50);
  const loading = background(busy.bootstrap()).then(() => { settled = true; });
  await paging.entered;
  for (let round = 1; round <= 5; round++)
   later.push(...await app.publishMany(4, { channel: BUSY, prefix: 'later', from: round * 4 - 3 }));
  assert.equal((await ledger(client, BUSY)).bootstrap_cursor, 50, 'the interval is half loaded while the head keeps moving');
  paging.release();
  for (let round = 6; !settled && round <= 40; round++) {
   later.push(...await app.publishMany(4, { channel: BUSY, prefix: 'later', from: round * 4 - 3 }));
   await new Promise(resolve => setTimeout(resolve, 10));
  }
  await loading;
  assert.deepEqual({ ...busy.status.bootstrap }, { phase: 'complete', error: null });
  const busyLoads = net.loads.filter(load => load.channel === BUSY);
  assert.deepEqual(busyLoads.map(load => load.after), [0, 50, 100], 'three bounded pages, whatever was published beside them');
  assert.deepEqual([...new Set(busyLoads.map(load => load.until))], [S], 'the upper bound is the origin, not the head');
  assert.ok(later.length >= 20, `the scenario published while the interval was being loaded: ${later.length}`);

  // Once writes stop, every record of both paths is local and delivery has
  // reached the head.
  const head = await app.head(BUSY);
  await wait(async () => (await ledger(client, BUSY)).cursor === head, 'delivery caught up with the head');
  await assertLoaded(client, [...history, ...later]);
  assert.deepEqual(ctx.reports, []);
 });
});

// Completion is the historical interval plus the fixed live barrier: H is the
// head the final page observed and it is not refreshed while the run waits
// (spec sections 3 and 5).
test('the barrier is the head the final page saw, is fixed, and delivery must reach it', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:barrier';
  const { app, net } = ctx;
  const history = await app.publishMany(5, { channel: SCOPE, prefix: 'past' });
  const client = await ctx.open('reader', { connect: false });
  const connection = await ctx.connect(client);
  const subscription = await client.scopes.subscribe(SCOPE);
  await wait(() => subscription.status.initialization === 'ready', 'the committed origin');
  const S = (await ledger(client, SCOPE)).starting_cursor;
  assert.equal(S, 5);

  // Ordinary delivery is stalled on purpose: the catch-up pull is held, so L
  // stays at S while the historical pages run on their own request slot.
  await connection.pause();
  await wait(() => subscription.status.connection === 'offline', 'the paused lane');
  await app.publishOne('after-origin', 'published after the origin', [SCOPE]);
  assert.equal(await app.head(SCOPE), 6);
  const stalled = net.hold('request', body => body.mode === undefined);
  const observed = phases(subscription);
  const loading = background(subscription.bootstrap());
  await connection.resume();
  await stalled.entered;

  await wait(() => subscription.status.bootstrap.phase === 'catching-up', 'the fixed barrier');
  const waiting = await ledger(client, SCOPE);
  assert.equal(waiting.bootstrap_cursor, S, 'the interval is finished');
  assert.equal(waiting.bootstrap_barrier, 6, 'the barrier is the head the final page observed');
  assert.equal(waiting.cursor, S, 'delivery has not reached it');

  // Writes continue while the run waits: the barrier is fixed, so it does not
  // chase them, and the run does not complete.
  await app.publishMany(3, { channel: SCOPE, prefix: 'later' });
  await never(async () => (await ledger(client, SCOPE)).bootstrap_barrier !== 6, 'the barrier followed the head');
  await never(async () => subscription.status.bootstrap.phase === 'complete', 'completion without delivery');

  stalled.release();
  await loading;
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null });
  assert.deepEqual(observed.seen, ['not-requested', 'loading', 'catching-up', 'complete'], 'the committed transitions, in order');
  observed.stop();
  const done = await ledger(client, SCOPE);
  assert.equal(done.bootstrap_barrier, 6, 'the committed barrier is what it always was');
  assert.ok(done.cursor >= 6, `delivery reached the barrier: ${done.cursor}`);
  await assertLoaded(client, history);
  assert.equal((await client.models.entry.get({ id: 'after-origin' })).text, 'published after the origin');
 });
});

// A record several Scopes provide is one record at one stamp (D4); each
// subscription has its own origin, progress and barrier, and the worker
// rotates between them.
test('overlapping Scopes bootstrap independently and share the record they both provide', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const FIRST = 'bootstrap:overlap-a';
  const SECOND = 'bootstrap:overlap-b';
  const { app, net } = ctx;
  const onlyFirst = await app.publishMany(3, { channel: FIRST, prefix: 'a' });
  const onlySecond = await app.publishMany(4, { channel: SECOND, prefix: 'b' });
  await app.publishOne('shared', 'provided by both Scopes', [FIRST, SECOND]);
  assert.equal(await app.head(FIRST), 4);
  assert.equal(await app.head(SECOND), 5);

  const client = await ctx.open('reader');
  const first = await client.scopes.subscribe(FIRST);
  const second = await client.scopes.subscribe(SECOND);
  await wait(() => first.status.initialization === 'ready' && second.status.initialization === 'ready', 'both origins');
  await Promise.all([first.bootstrap(), second.bootstrap()]);
  assert.deepEqual({ ...first.status.bootstrap }, { phase: 'complete', error: null });
  assert.deepEqual({ ...second.status.bootstrap }, { phase: 'complete', error: null });
  await assertLoaded(client, [...onlyFirst, ...onlySecond]);
  assert.equal((await client.models.entry.get({ id: 'shared' })).text, 'provided by both Scopes');
  assert.deepEqual(
   [...new Set(net.loads.map(load => load.channel))].sort(),
   [FIRST, SECOND],
   'both Scopes were loaded through the one request slot',
  );
  for (const [scope, origin] of [[FIRST, 4], [SECOND, 5]]) {
   const row = await ledger(client, scope);
   assert.equal(row.starting_cursor, origin, `${scope} kept its own origin`);
   assert.equal(row.bootstrap_cursor, origin, `${scope} finished its own interval`);
   assert.equal(row.bootstrap_barrier, origin, `${scope} fixed its own barrier`);
   assert.deepEqual(
    [...new Set(net.loads.filter(load => load.channel === scope).map(load => load.until))],
    [origin],
    `${scope}'s pages are bounded by its own origin`,
   );
  }
 });
});

// The load is background work: local writes keep completing from their
// receipts while it runs, and neither disturbs the other.
test('a queued local write settles while a large interval loads, and the load still completes', { timeout: 180000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:foreground';
  const { app, net } = ctx;
  const history = await app.publishMany(130, { channel: SCOPE, prefix: 'bulk' });
  const client = await ctx.open('reader');
  const subscription = await client.scopes.subscribe(SCOPE);
  await wait(() => subscription.status.initialization === 'ready', 'the committed origin');
  const S = (await ledger(client, SCOPE)).starting_cursor;
  assert.equal(S, 130);
  // One record the subscription itself delivered, so the foreground has
  // something to edit while the historical interval is still loading.
  await app.publishOne('foreground', 'delivered by the subscription', [SCOPE]);
  await wait(async () => (await client.models.entry.get({ id: 'foreground' })) !== null, 'the live record');

  // Hold every page after the first: the run is demonstrably mid-interval for
  // the whole of the foreground write.
  const held = net.hold('request', body => body.mode === 'bootstrap' && body.after > 0);
  const loading = background(subscription.bootstrap());
  await held.entered;
  assert.equal(subscription.status.bootstrap.phase, 'loading');
  assert.equal((await ledger(client, SCOPE)).bootstrap_cursor, 50, 'one page has committed and the next is in flight');

  // The mutation completes from its receipt alone (A3): no channel is awaited,
  // and the load's request slot is not the push lane.
  await client.mutate.edit({ entry: { identity: { id: 'foreground' }, values: { text: '  edited while loading  ' } } });
  await wait(async () => (await client.syncState()).pending === 0, 'the queued write settled');
  assert.equal((await client.models.entry.get({ id: 'foreground' })).text, 'edited while loading', 'the receipt applied the normalized authority');
  assert.equal((await ledger(client, SCOPE)).bootstrap_cursor, 50, 'the foreground write moved no historical progress');
  assert.equal(subscription.status.bootstrap.phase, 'loading', 'and did not disturb the run');

  held.release();
  await loading;
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null });
  assert.equal((await ledger(client, SCOPE)).bootstrap_cursor, S);
  await assertLoaded(client, history);
  assert.equal((await client.models.entry.get({ id: 'foreground' })).text, 'edited while loading', 'the load delivered no older authority over the receipt');
 });
});

// Registration is a local transaction with no connection, and the task is
// durable: neither waiting for connectivity nor closing the client is failure
// (spec sections 2 and 7).
test('a load registered offline completes once connected', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:offline';
  const { app } = ctx;
  const history = await app.publishMany(6, { channel: SCOPE, prefix: 'off' });
  const client = await ctx.open('reader', { connect: false });
  const subscription = await client.scopes.subscribe(SCOPE);
  const observed = phases(subscription);
  const loading = background(subscription.bootstrap());
  await wait(() => subscription.status.bootstrap.phase === 'waiting-for-initialization', 'a registered load with no origin yet');
  assert.equal(subscription.status.connection, 'offline', 'waiting for connectivity is not failure');
  const registered = await ledger(client, SCOPE);
  assert.deepEqual(
   [registered.starting_cursor, registered.bootstrap_state, registered.bootstrap_run],
   [null, 'requested', 1],
   'the registration committed without a boundary',
  );
  await never(() => subscription.status.bootstrap.phase === 'failed', 'a failure while offline');

  const connection = await ctx.connect(client);
  await loading;
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null });
  observed.assertForward();
  assert.deepEqual(observed.seen.at(0), 'not-requested');
  assert.ok(observed.seen.includes('waiting-for-initialization'), `the run waited for #150 initialization: ${observed.seen.join(' -> ')}`);
  assert.deepEqual(observed.seen.at(-1), 'complete');
  observed.stop();
  await assertLoaded(client, history);
  await connection.close();
 });
});

test('a load interrupted by closing the client resumes on the next one without a new call', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:restart';
  const { app, net } = ctx;
  const history = await app.publishMany(120, { channel: SCOPE, prefix: 'again' });
  const path = join(ctx.directory, 'resumed.sqlite');
  let client = await GeneratedClient.open({ path });
  try {
   let connection = await ctx.connect(client);
   let subscription = await client.scopes.subscribe(SCOPE);
   await wait(() => subscription.status.initialization === 'ready', 'the committed origin');
   // Hold the second page: the first one has committed, so the reopened client
   // must continue from 50 rather than start over.
   const held = net.hold('request', body => body.mode === 'bootstrap' && body.after === 50);
   const interrupted = subscription.bootstrap().then(() => null, error => error);
   await held.entered;
   await wait(async () => (await ledger(client, SCOPE)).bootstrap_cursor === 50, 'the first committed page');
   await client.close();
   assert.equal((await interrupted)?.code, 'client_closed', 'this process stopped waiting; the task did not fail');
   held.release();

   client = await GeneratedClient.open({ path });
   const stored = await ledger(client, SCOPE);
   assert.equal(stored.bootstrap_state, 'loading', 'the task survived the close');
   assert.equal(stored.bootstrap_cursor, 50);
   subscription = await client.scopes.subscribe(SCOPE);
   assert.equal(subscription.status.initialization, 'ready');
   connection = await ctx.connect(client);
   // No second bootstrap() call: the reopened client resumes the run it found.
   await wait(() => subscription.status.bootstrap.phase === 'complete', 'the resumed run completes on its own');
   const resumed = await ledger(client, SCOPE);
   assert.equal(resumed.bootstrap_run, 1, 'resuming is not a new run');
   assert.equal(resumed.bootstrap_cursor, 120);
   await assertLoaded(client, history);
   // A call after a valid completion resolves locally, with no request at all.
   const loads = net.loads.length;
   await subscription.bootstrap();
   assert.equal(net.loads.length, loads, 'a completed run asks for nothing more');
   await connection.close();
  } finally {
   await client.close().catch(() => {});
  }
 });
});

// D7 explicitly: a live read failure is reported for that record, the cursor
// still advances, and the load completing does not rewrite it as success.
test('a live read failure stays visible and reported when the load completes', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:live-failure';
  const { app } = ctx;
  const history = await app.publishMany(4, { channel: SCOPE, prefix: 'readable' });
  const client = await ctx.open('reader');
  const subscription = await client.scopes.subscribe(SCOPE);
  await wait(() => subscription.status.initialization === 'ready', 'the committed origin');
  const S = (await ledger(client, SCOPE)).starting_cursor;
  assert.equal(S, 4);

  // One identity the Loader refuses, published after the origin: its live page
  // reports it and the cursor still moves (D7).
  app.failLoads('unreadable');
  await app.publishOne('unreadable', 'never delivered', [SCOPE]);
  await app.publishOne('readable-after', 'delivered beside the failure', [SCOPE]);
  await wait(
   () => ctx.reports.some(report => report.kind === 'readFailed' && report.identity.id === 'unreadable'),
   'the reported read failure',
  );
  const failure = ctx.reports.find(report => report.identity.id === 'unreadable');
  assert.equal(failure.code, 'loader.failed');
  await wait(async () => (await ledger(client, SCOPE)).cursor >= 6, 'the cursor advanced past the failure');
  assert.equal(await client.models.entry.get({ id: 'unreadable' }), null, 'a failed read is not authority');
  assert.equal((await client.models.entry.get({ id: 'readable-after' })).text, 'delivered beside the failure', 'unrelated reads proceeded');

  // The run completes: the interval and the barrier were processed. That is
  // not a claim that this record loaded successfully.
  await subscription.bootstrap();
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null });
  assert.equal(await client.models.entry.get({ id: 'unreadable' }), null, 'completion did not invent authority for the failed read');
  assert.equal(
   ctx.reports.filter(report => report.identity.id === 'unreadable').length,
   1,
   'the failure was reported once and nothing withdrew it',
  );
  await assertLoaded(client, history);

  // It is corrected the next time it is delivered, as D7 requires.
  app.allowLoads('unreadable');
  await app.readd(['unreadable'], SCOPE);
  await wait(async () => (await client.models.entry.get({ id: 'unreadable' }))?.text === 'never delivered', 'the corrected record');
 });
});

// D7/D8 on a historical page: successful authority of that page stays, the
// interval does not advance, the call rejects with the stored failure, and an
// explicit retry revisits the same page (spec section 5).
test('a failed historical page rejects the run, keeps its other records, and retries the same interval', { timeout: 120000 }, async () => {
 await scenario(async ctx => {
  const SCOPE = 'bootstrap:page-failure';
  const { app, net } = ctx;
  const history = await app.publishMany(6, { channel: SCOPE, prefix: 'page' });
  const client = await ctx.open('reader');
  const subscription = await client.scopes.subscribe(SCOPE);
  const observed = phases(subscription);
  await wait(() => subscription.status.initialization === 'ready', 'the committed origin');
  const S = (await ledger(client, SCOPE)).starting_cursor;
  assert.equal(S, 6);

  app.failLoads('page-3');
  const rejected = await subscription.bootstrap().then(() => null, error => error);
  assert.equal(rejected?.code, 'bootstrap.records_failed', 'the background caller was rejected');
  assert.match(rejected.message, /could not be applied/);
  assert.deepEqual(
   { ...subscription.status.bootstrap },
   { phase: 'failed', error: { code: 'bootstrap.records_failed', message: rejected.message } },
   'status observers receive the stored failure',
  );
  assert.ok(
   ctx.reports.some(report => report.kind === 'readFailed' && report.identity.id === 'page-3' && report.code === 'loader.failed'),
   'the record failure is the application\'s too',
  );

  // The page's successful authority stayed; the continuation marker did not move.
  await assertLoaded(client, history.filter(id => id !== 'page-3'));
  assert.equal(await client.models.entry.get({ id: 'page-3' }), null, 'a failed read is not a deletion');
  const failed = await ledger(client, SCOPE);
  assert.equal(failed.bootstrap_cursor, 0, 'the interval did not advance');
  assert.equal(failed.bootstrap_barrier, null);
  assert.equal(failed.bootstrap_run, 1);
  assert.equal(failed.starting_cursor, S, 'the origin is untouched');

  // An explicit retry is a new run over the same, unchanged progress.
  const issued = net.loads.length;
  const again = await subscription.bootstrap().then(() => null, error => error);
  assert.equal(again?.code, 'bootstrap.records_failed');
  assert.deepEqual(net.loads.slice(issued).map(load => [load.after, load.until]), [[0, S]], 'the retry re-requested the same page');
  assert.equal((await ledger(client, SCOPE)).bootstrap_run, 2, 'the retry is another run');
  assert.equal((await ledger(client, SCOPE)).bootstrap_cursor, 0);

  // With the read repaired the same page completes; what had already applied is idempotent.
  app.allowLoads('page-3');
  await subscription.bootstrap();
  assert.deepEqual({ ...subscription.status.bootstrap }, { phase: 'complete', error: null });
  // A failure and its explicit retry are the one place the public phase moves
  // back: within a run it never does, but a retry is another run.
  assert.deepEqual(observed.seen, ['not-requested', 'loading', 'failed', 'loading', 'failed', 'loading', 'complete']);
  observed.stop();
  await assertLoaded(client, history);
  const done = await ledger(client, SCOPE);
  assert.equal(done.bootstrap_cursor, S);
  assert.equal(done.bootstrap_error, null);
  assert.equal(done.bootstrap_run, 3);
 });
});
