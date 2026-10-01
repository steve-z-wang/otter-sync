import test, { after, before } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { GeneratedClient } from "./client.ts";
import type { StoreHooks } from "./client.ts";
import { createFixture } from "./backend-fixture.ts";
import { GeneratedClient as EvolvedClient } from "./evolved/client.ts";
import { createBackend as createEvolvedBackend, devAuth, type Mutations as EvolvedMutations, type Queries as EvolvedQueries, type Loaders as EvolvedLoaders } from "./evolved/backend.ts";
import { pg, type PgClient } from "../../packages/postgres/index.mts";

const fixture = await createFixture();
let url: string;
before(async () => { await fixture.initialize(); url = (await fixture.listen()).url; });
after(async () => { await fixture.close(); });
const server = () => ({ url, token: "alice" });
const execFileAsync = promisify(execFile);
const wait = async (predicate: () => Promise<boolean>, label: string) => {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw Error(`Timed out waiting for ${label}`);
};
/** Records the sync paths the client requests while `body` runs: which delivery path a call took. */
const requestedPaths = async (body: () => Promise<void>) => {
  const original = globalThis.fetch;
  const paths: string[] = [];
  globalThis.fetch = ((input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
    if (path.startsWith("/sync/")) paths.push(path);
    return original(input, init);
  }) as typeof fetch;
  try { await body(); } finally { globalThis.fetch = original; }
  return paths;
};
/** The record's server stamp, or null before any settlement touched it. */
const serverStamp = async (id: string, model = "Todo"): Promise<number | null> => {
  const row = (await fixture.pool.query("SELECT stamp FROM axton_record WHERE model=$1 AND identity_key=$2", [model, JSON.stringify({ id })])).rows[0];
  return row ? Number(row.stamp) : null;
};
/** The Scope's head: the last publication position it allocated. */
const scopeHead = async (scope: string) =>
  Number((await fixture.pool.query("SELECT head FROM axton_stream WHERE stream=$1", [scope])).rows[0]?.head ?? 0);
const post = async (kind: "mutations" | "pull" | "actions", body: string, target = url) => {
  const response = await fetch(`${target}/sync/${kind}`, { method: "POST", headers: { authorization: "Bearer alice", "content-type": "application/json" }, body });
  if (response.status !== 200) throw Error(`HTTP ${response.status}: ${await response.text()}`);
  return response.json();
};

test("generated Mutations and the Search Query cross native SQLite and PostgreSQL", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-generated-"));
  const path = join(directory, "client.sqlite");
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path });
    const initial = await client.mutations.addTodo({ todo: { id: "main", title: "  first  " } });
    assert.equal(initial.status, "pending");
    assert.equal((await client.models.todo.get({ id: "main" }))?.title, "  first  ");
    assert.equal((await client.syncState()).pending, 1);
    await client.close();
    client = await GeneratedClient.open({ path, server: server() });
    await wait(async () => (await client!.syncState()).pending === 0, "offline AddTodo after SQLite reopen");
    assert.equal((await client.models.todo.get({ id: "main" }))?.title, "first");
    assert.deepEqual((await fixture.pool.query("SELECT id,title FROM action_e2e_todo WHERE id='main'")).rows, [{ id: "main", title: "first" }]);

    const search = await client.queries.searchTodos({ query: null });
    assert.equal(search.count, 1);
    assert.deepEqual(search.labels, ["main"]);
    assert.equal(search.hint, null);
    assert.equal(search.todos[0]?.title, "first");
    assert.equal(search.first?.title, "first");
    await client.connection!.pause();
    await client.mutations.sendEmail({ to: "test@example.invalid", subject: "Queued", body: "Offline" });
    assert.equal((await client.syncState()).pending, 1, "ordinary-only Mutation enqueues offline without a Model target");
    await client.close();
    client = await GeneratedClient.open({ path });
    assert.equal((await client.syncState()).pending, 1, "ordinary-only Mutation survived SQLite reopen");
    await client.connect(server());
    await wait(async () => (await client!.syncState()).pending === 0, "offline SendEmail settlement");
    const directMail = await client.mutations.call.sendEmail({ to: "test@example.invalid", subject: "Direct", body: "Immediate" });
    assert.match(directMail.messageId, /^[0-9]+$/);
    const durableMail = await client.mutations.sendEmail({ to: "test@example.invalid", subject: "Durable", body: "Awaited" });
    const durableOutcome = await durableMail.wait();
    assert.equal(durableOutcome.error, null);
    assert.match(durableOutcome.result!.messageId, /^[0-9]+$/);
    assert.notEqual(durableOutcome.result!.messageId, directMail.messageId);
    await client.connection!.pause();
    const lostMail = await client.mutations.sendEmail({ to: "test@example.invalid", subject: "Replay", body: "Once" });
    const frozenMail = await client.client.freeze();
    assert.ok(frozenMail);
    const firstMail = await post("mutations", frozenMail);
    const handledMail = fixture.handlerCalls;
    const replayedMail = await post("mutations", frozenMail);
    assert.deepEqual(replayedMail.completions, firstMail.completions, "cached ordinary output retains messageId");
    assert.equal(fixture.handlerCalls, handledMail, "ordinary side effect was not repeated");
    await client.client.acknowledge(JSON.parse(frozenMail).batchSequence, replayedMail);
    const replayedOutcome = await lostMail.wait();
    assert.equal(replayedOutcome.error, null);
    assert.equal(replayedOutcome.result!.messageId, firstMail.completions[0].outcome.result.messageId);
    await client.connection!.resume();
    assert.deepEqual((await fixture.pool.query("SELECT recipient,subject,body FROM action_e2e_outbox ORDER BY id")).rows, [
      { recipient: "test@example.invalid", subject: "Queued", body: "Offline" },
      { recipient: "test@example.invalid", subject: "Direct", body: "Immediate" },
      { recipient: "test@example.invalid", subject: "Durable", body: "Awaited" },
      { recipient: "test@example.invalid", subject: "Replay", body: "Once" },
    ]);
    const update = await client.mutations.updateTodo({ todo: { id: "main", title: "  revised  " } });
    assert.equal((await update.wait()).error, null);
    assert.equal((await client.models.todo.get({ id: "main" }))?.title, "revised");
    const deleted = await client.mutations.deleteTodo({ todo: { id: "main" } });
    assert.equal((await deleted.wait()).error, null);
    assert.equal(await client.models.todo.get({ id: "main" }), null);
    assert.deepEqual((await fixture.pool.query("SELECT id FROM action_e2e_todo WHERE id='main'")).rows, []);
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("direct result retains Loader snapshot while independent durable optimism replays", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-direct-"));
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    // The subscription's origin is the first head its handshake acknowledges
    // (#150), so it is registered and initialized before the changes it must
    // receive; a null boundary is never read as zero and the pull below starts at
    // that committed cursor.
    const subscription = await client.streams.subscribe("todos:demo");
    await wait(async () => subscription.status.initialization === "ready", "first initialization");
    const created = await client.mutations.addTodo({ todo: { id: "direct", title: "A" } });
    assert.equal((await created.wait()).error, null);
    await client.connection!.pause();
    // EditAndShow returns the record it edits here, so each result is that record's snapshot.
    const pending = await client.mutations.editAndShow({ todo: { id: "direct", title: "B" }, shown: "direct" });
    assert.equal(pending.status, "pending");
    assert.equal((await client.models.todo.get({ id: "direct" }))?.title, "B");
    const result = await client.queries.searchTodos({ query: "A" });
    assert.equal(result.todos[0]?.title, "A", "result is the committed Loader snapshot");
    assert.equal((await client.models.todo.get({ id: "direct" }))?.title, "B", "direct authority replays the independent pending edit");
    assert.equal((await client.syncState()).pending, 1, "direct call did not drain the durable queue");
    const later = await client.mutations.editAndShow({ todo: { id: "direct", title: "C" }, shown: "direct" });
    const frozen = await client.client.freeze();
    assert.ok(frozen);
    assert.equal(JSON.parse(frozen).mutations.length, 2, "both invocations are frozen in one batch");
    const receipt = await post("mutations", frozen);
    await client.client.acknowledge(JSON.parse(frozen).batchSequence, receipt);
    const firstOutcome = await pending.wait();
    const secondOutcome = await later.wait();
    assert.equal(firstOutcome.error, null);
    assert.equal(secondOutcome.error, null);
    assert.equal(firstOutcome.result.todo.title, "B", "first invocation keeps its own Loader snapshot");
    assert.equal(secondOutcome.result.todo.title, "C", "second invocation keeps its later Loader snapshot");
    assert.equal((await client.models.todo.get({ id: "direct" }))?.title, "C");
    const beforePage = await client.syncState();
    assert.equal(typeof beforePage.cursors["todos:demo"], "number", "the subscription has a committed delivery position");
    const page = await post("pull", JSON.stringify({ capabilities: ["stream-membership-v1"], cursors: { "todos:demo": beforePage.cursors["todos:demo"] }, models: { Todo: 1 } }));
    await client.client.applyPull(page);
    const afterPage = await client.syncState();
    assert.ok(afterPage.cursors["todos:demo"] > beforePage.cursors["todos:demo"], "page advances the cursor after the receipt");
    assert.equal((await client.models.todo.get({ id: "direct" }))?.title, "C", "the later page cannot regress receipt authority");
    assert.equal(result.todos[0]?.title, "A", "result does not change after later settlement");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("store selects which Search Query outputs update local Models on both routes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-store-"));
  let client: GeneratedClient | undefined;
  const local = (id: string) => client!.models.todo.get({ id });
  const localStamp = async (id: string) =>
    (await client!.readSql("SELECT stamp FROM axton_record WHERE model='Todo' AND identity LIKE ?", [`%${id}%`]))[0]?.stamp ?? null;
  const serverStamps = async (id: string) =>
    (await fixture.pool.query("SELECT stamp FROM axton_record WHERE model='Todo' AND identity_key LIKE $1", [`%${id}%`])).rows.length;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('store-a','storeq a'),('store-b','storeq b')");
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    let notifications = 0;
    const stop = client.models.todo.watch({}, () => { notifications++; });
    await new Promise((resolve) => setTimeout(resolve, 30));
    const settled = notifications;

    // Direct store:false returns full Loader snapshots and stores nothing.
    const unstored = await client.queries.searchTodos({ query: "storeq" }, { store: false });
    assert.deepEqual(unstored.todos.map((todo) => todo.title), ["storeq a", "storeq b"]);
    assert.equal(unstored.first?.title, "storeq a");
    assert.equal(await local("store-a"), null);
    assert.equal(await localStamp("store-a"), null);
    assert.equal(await serverStamps("store-a"), 0, "no output-only stamp allocation");
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(notifications, settled, "no Model notification for a disabled-only read");

    // Mixed: first is stored, a record only in todos is not.
    const mixed = await client.queries.searchTodos({ query: "storeq" }, { store: { todos: false } });
    assert.equal(mixed.todos.length, 2);
    assert.equal((await local("store-a"))?.title, "storeq a", "enabled overlapping output stores its record");
    assert.equal(await local("store-b"), null, "disabled-only record is not stored");
    const stampA = await localStamp("store-a");
    assert.ok(stampA);

    // A cached row stays unchanged when a disabled read returns newer content.
    // Another writer changes the record and advances its stamp.
    await fixture.pool.query("UPDATE action_e2e_todo SET title='storeq a2' WHERE id='store-a'");
    await fixture.pool.query("UPDATE axton_record SET stamp=stamp+1 WHERE model='Todo' AND identity_key LIKE '%store-a%'");
    const newer = await client.queries.searchTodos({ query: "storeq" }, { store: false });
    assert.equal(newer.first?.title, "storeq a2", "result is this invocation's Loader snapshot");
    assert.equal((await local("store-a"))?.title, "storeq a", "cached row unchanged");
    assert.equal(await localStamp("store-a"), stampA);

    // Durable store:false behaves the same, through the queue and receipt.
    const durable = await client.queries.enqueue.searchTodos({ query: "storeq" }, { store: false });
    const outcome = await durable.wait();
    assert.equal(outcome.error, null);
    assert.equal(outcome.result!.todos[1]?.title, "storeq b");
    assert.equal(await local("store-b"), null);
    assert.equal((await local("store-a"))?.title, "storeq a");
    // The default stores every eligible output.
    const stored = await client.queries.enqueue.searchTodos({ query: "storeq" });
    assert.equal((await stored.wait()).error, null);
    assert.equal((await local("store-a"))?.title, "storeq a2");
    assert.equal((await local("store-b"))?.title, "storeq b");

    // Required mutation reconciliation is never disabled.
    await client.connection!.pause();
    const edit = await client.mutations.updateTodo({ todo: { id: "store-a", title: "  edited  " } }, { store: false });
    assert.equal((await local("store-a"))?.title, "  edited  ", "optimistic edit");
    const snapshot = await client.queries.searchTodos({ query: "storeq" }, { store: false });
    assert.equal(snapshot.first?.title, "storeq a2", "snapshot A while pending edit B stays local");
    assert.equal((await local("store-a"))?.title, "  edited  ");
    await client.connection!.resume();
    assert.equal((await edit.wait()).error, null);
    assert.equal((await local("store-a"))?.title, "edited", "required authority reconciled the write");
    assert.equal(snapshot.first?.title, "storeq a2", "returned snapshot does not change later");
    stop();
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("committed response loss replays frozen intent and stored result after a scope page arrives first", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-replay-"));
  const path = join(directory, "client.sqlite");
  let client: GeneratedClient | undefined;
  let upgraded: EvolvedClient | undefined;
  let evolvedListener: Awaited<ReturnType<ReturnType<typeof createEvolvedBackend<PgClient>>["listen"]>> | undefined;
  try {
    client = await GeneratedClient.open({ path, server: server() });
    // One session establishes the subscription's origin, then the socket is
    // paused so the Action is pushed by hand: the page below is pulled from that
    // committed cursor, never from zero (#150).
    const subscription = await client.streams.subscribe("todos:demo");
    await wait(async () => subscription.status.initialization === "ready", "first initialization");
    await client.connection!.pause();
    const origin = (await client.syncState()).cursors["todos:demo"];
    assert.equal(typeof origin, "number");
    await client.mutations.addTodo({ todo: { id: "replay", title: "  saved  " } });
    const frozen = await client.client.freeze();
    assert.ok(frozen);
    const first = await post("mutations", frozen);
    const page = await post("pull", JSON.stringify({ capabilities: ["stream-membership-v1"], cursors: { "todos:demo": origin }, models: { Todo: 1 } }));
    await client.client.applyPull(page);
    const handled = fixture.handlerCalls;
    const loaded = fixture.loaderCalls;
    assert.equal((await client.models.todo.get({ id: "replay" }))?.title, "saved", "the page delivered the published record");
    assert.equal((await client.syncState()).pending, 1, "page authority alone does not complete the Action");
    await client.close();
    await fixture.pool.query("ALTER TABLE action_e2e_todo ADD COLUMN note text");
    let unexpected = 0;
    const forbidden = async (): Promise<never> => { unexpected++; throw Error("cached Action reexecuted"); };
    const mutations: EvolvedMutations<PgClient> = {
      addTodo: { v1: forbidden, v2: forbidden },
      updateTodo: { v1: forbidden, v2: forbidden },
      deleteTodo: forbidden,
      sendEmail: forbidden,
      searchTodos: forbidden,
      retitleTodos: { v1: forbidden, v2: forbidden },
    };
    // SearchTodos retains a Mutation v1 and Query v2/v3: each registers under its own kind.
    const queries: EvolvedQueries<PgClient> = { searchTodos: { v2: forbidden, v3: forbidden } };
    const loaders: EvolvedLoaders<PgClient> = { todo: { v1: forbidden, v2: forbidden } };
    const evolvedBackend = createEvolvedBackend<PgClient>({ database: pg(fixture.pool), authenticate: devAuth(), mutations, queries, loaders });
    evolvedListener = await evolvedBackend.listen({ port: 0 });
    upgraded = await EvolvedClient.open({ path });
    assert.equal(await upgraded.client.freeze(), frozen, "SQLite retained the exact frozen request bytes through nullable schema evolution");
    const replay = await post("mutations", frozen, evolvedListener.url);
    assert.deepEqual(replay.completions, first.completions);
    assert.deepEqual(replay.records, first.records, "frozen v1 read intent keeps its original authority encoding");
    assert.equal(fixture.handlerCalls, handled, "handler was not rerun");
    assert.equal(fixture.loaderCalls, loaded, "Loader was not rerun");
    assert.equal(unexpected, 0, "upgraded handlers and Loaders were never invoked");
    await upgraded.client.acknowledge(JSON.parse(frozen).batchSequence, replay);
    assert.equal((await upgraded.syncState()).pending, 0);
    assert.deepEqual(await upgraded.models.todo.get({ id: "replay" }), { id: "replay", title: "saved", note: null });
  } finally { await client?.close(); await upgraded?.close(); await evolvedListener?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("Queries read fresh on the direct route and at execution time when enqueued", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-query-fresh-"));
  const path = join(directory, "client.sqlite");
  let client: GeneratedClient | undefined;
  const queued = async () => (await client!.readSql("SELECT count(*) AS n FROM axton_mutation"))[0]!.n;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('fresh-a','freshq one')");
    client = await GeneratedClient.open({ path, server: server() });
    const calls = fixture.queryCalls;
    let first: Awaited<ReturnType<typeof client.queries.searchTodos>> | undefined;
    let second: typeof first;
    const paths = await requestedPaths(async () => {
      first = await client!.queries.searchTodos({ query: "freshq" });
      await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('fresh-b','freshq two')");
      second = await client!.queries.searchTodos({ query: "freshq" });
    });
    assert.deepEqual(paths, ["/sync/actions", "/sync/actions"], "a default Query takes the direct path, never the queue");
    assert.deepEqual(first!.labels, ["fresh-a"]);
    assert.deepEqual(second!.labels, ["fresh-a", "fresh-b"], "each direct invocation reads again");
    assert.equal(await queued(), 0, "a direct Query writes no queue metadata");
    assert.equal((await client.syncState()).pending, 0);
    assert.equal(fixture.queryCalls, calls + 2);

    // Enqueued while paused: a durable intent that reads when it executes.
    await client.connection!.pause();
    const later = await client.queries.enqueue.searchTodos({ query: "freshq" });
    assert.equal(later.status, "pending");
    assert.equal(await queued(), 1);
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('fresh-c','freshq three')");
    await client.connection!.resume();
    const outcome = await later.wait();
    assert.equal(outcome.error, null);
    assert.deepEqual(outcome.result!.labels, ["fresh-a", "fresh-b", "fresh-c"], "read at execution, not at enqueue");

    // A queued Query survives reopen and derives no local optimism.
    await client.connection!.pause();
    await client.queries.enqueue.searchTodos({ query: "freshq" });
    await client.close();
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('fresh-d','freshq four')");
    client = await GeneratedClient.open({ path });
    assert.equal((await client.syncState()).pending, 1, "the queued Query survived SQLite reopen");
    const intents = await client.readSql("SELECT m.name, m.version, count(o.ordinal) AS operations FROM axton_mutation m LEFT JOIN axton_mutation_operation o ON o.ordinal = m.ordinal GROUP BY m.ordinal");
    assert.deepEqual(intents.map((row) => [row.name, row.version, row.operations]), [["SearchTodos", 2, 0]], "a queued Query derives no local Model operations");
    await client.connect(server());
    await wait(async () => (await client!.syncState()).pending === 0, "reopened queued Query settlement");
    assert.equal((await client.models.todo.get({ id: "fresh-d" }))?.title, "freshq four", "its default store:true outputs updated local Models");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a direct Query retry with the same call ID replays its saved result", async () => {
  await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('replay-q','replayq one')");
  const call = (args: object) => JSON.stringify({ capabilities: ["stream-membership-v1"], call: { callId: "01890f47-1234-7123-8123-1234567890aa", name: "SearchTodos", version: 2, args }, models: { Todo: 1 } });
  const first = await post("actions", call({ query: "replayq" }));
  assert.deepEqual(first.completion.outcome.result.labels, ["replay-q"]);
  const handled = fixture.handlerCalls;
  const loaded = fixture.loaderCalls;
  await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('replay-r','replayq two')");
  const retried = await post("actions", call({ query: "replayq" }));
  assert.deepEqual(retried, first, "the saved result is replayed, not a fresh read");
  assert.equal(fixture.handlerCalls, handled, "the Query handler did not run again");
  assert.equal(fixture.loaderCalls, loaded, "no Loader ran again");
  const conflict = await post("actions", call({ query: "other" }));
  assert.equal(conflict.completion.outcome.code, "call.identity_conflict");
  // The Query's retained kind comes from the backend: v1 is the Mutation contract.
  const retained = await post("actions", JSON.stringify({ capabilities: ["stream-membership-v1"], call: { callId: "01890f47-1234-7123-8123-1234567890ab", name: "SearchTodos", version: 1, args: { query: "replayq" } }, models: { Todo: 1 } }));
  assert.equal(retained.completion.outcome.code, "search.v1_retired", "the retained Mutation v1 runs its own handler");
});

test("a direct Mutation resolves after the backend commits and its authority applies", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-mutation-direct-"));
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    let done: Awaited<ReturnType<typeof client.mutations.call.addTodo>> | undefined;
    const paths = await requestedPaths(async () => { done = await client!.mutations.call.addTodo({ todo: { id: "direct-m", title: "  direct  " } }); });
    assert.deepEqual(paths, ["/sync/actions"], "a direct Mutation never enters the queue");
    assert.equal(done, undefined, "AddTodo declares no output: there is no result, and no input fills one");
    assert.deepEqual((await fixture.pool.query("SELECT title FROM action_e2e_todo WHERE id='direct-m'")).rows, [{ title: "direct" }]);
    assert.equal((await client.models.todo.get({ id: "direct-m" }))?.title, "direct", "authority applied before resolving");
    assert.equal((await client.readSql("SELECT count(*) AS n FROM axton_mutation"))[0]!.n, 0, "no queue row and no optimism");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a Mutation whose serialization conflicts outlast the retries is a retryable failure: it stays queued and the resend commits it", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-mutation-conflict-"));
  let client: GeneratedClient | undefined;
  const original = globalThis.fetch;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    assert.equal((await (await client.mutations.addTodo({ todo: { id: "conflicted", title: "before" } })).wait()).error, null);
    // Answers of /sync/mutations in order. After the first 500 the next push
    // waits for `resend`, so the test can look at the queue in between.
    const statuses: number[] = [];
    let failed!: () => void, resend!: () => void;
    const firstFailure = new Promise<void>((resolve) => { failed = resolve; });
    const resendGate = new Promise<void>((resolve) => { resend = resolve; });
    globalThis.fetch = (async (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
      const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
      if (path !== "/sync/mutations") return original(input, init);
      if (statuses.includes(500)) await resendGate;
      const response = await original(input, init);
      statuses.push(response.status);
      if (response.status === 500) failed();
      return response;
    }) as typeof fetch;
    const calls = await fixture.pool.query("SELECT count(*)::int AS n FROM axton_call");
    const handled = fixture.handlerCalls;
    fixture.conflictUpdates = 4; // the first attempt and the pg shim's three retries
    const update = await client.mutations.updateTodo({ todo: { id: "conflicted", title: "  after  " } });
    await firstFailure;
    assert.equal(fixture.handlerCalls - handled, 4, "one delivery ran the transaction four times, every attempt a serialization failure");
    assert.equal((await client.syncState()).pending, 1, "the Mutation is still queued");
    assert.deepEqual((await fixture.pool.query("SELECT count(*)::int AS n FROM axton_call")).rows, calls.rows, "no outcome was saved for it");
    assert.deepEqual((await fixture.pool.query("SELECT title FROM action_e2e_todo WHERE id='conflicted'")).rows, [{ title: "before" }]);
    resend();
    const outcome = await update.wait();
    assert.equal(outcome.error, null, "never a rejection: the resend was accepted");
    assert.deepEqual(statuses, [500, 200], "exhausted retries answer a server failure, and the client sends the same batch again");
    assert.equal(fixture.handlerCalls - handled, 5, "the resend ran the handler once more and committed");
    assert.equal(fixture.conflictUpdates, 0);
    assert.deepEqual((await fixture.pool.query("SELECT title FROM action_e2e_todo WHERE id='conflicted'")).rows, [{ title: "after" }]);
    assert.equal((await client.models.todo.get({ id: "conflicted" }))?.title, "after");
    assert.equal((await client.syncState()).pending, 0);
  } finally { globalThis.fetch = original; fixture.conflictUpdates = 0; await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("explicit extra touches are stamped once and are not caller authority; outputs follow store", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-mutation-store-"));
  let client: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('retitle-a','retitleq a'),('retitle-b','retitleq b')");
    const hooked: string[] = [];
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server(), onStore: { todo: (_tx, changes) => { hooked.push(...changes.map(change => change.identity.id)); } } });
    const direct = await client.mutations.call.retitleTodos({ query: "retitleq", title: "retitleq direct" }, { store: false });
    assert.deepEqual(hooked, [], "extra touch without stored caller authority invokes no hook");
    assert.deepEqual(direct.todos.map((todo) => todo.title), ["retitleq direct", "retitleq direct"], "the result is the Loader snapshot");
    assert.equal(await client.models.todo.get({ id: "retitle-a" }), null, "a touch alone is not caller authority, and store:false stores no output");
    assert.equal(await serverStamp("retitle-a"), 1, "the touch stamped the record once");
    const durable = await client.mutations.retitleTodos({ query: "retitleq", title: "retitleq durable" });
    const outcome = await durable.wait();
    assert.equal(outcome.error, null);
    assert.equal(outcome.result!.first?.title, "retitleq durable");
    assert.equal((await client.models.todo.get({ id: "retitle-b" }))?.title, "retitleq durable", "the default store policy stores the explicit outputs");
    assert.equal(await serverStamp("retitle-b"), 2, "one stamp per settlement");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("an edit of A that explicitly returns B: A is reconciled, the result is B, and only A is stamped", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-a-b-"));
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    for (const [id, title] of [["ab-a", "A"], ["ab-b", "B"]]) assert.equal((await (await client.mutations.addTodo({ todo: { id, title } })).wait()).error, null);
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('ab-c','C only on the server')");
    const stampA = await serverStamp("ab-a");
    const stampB = await serverStamp("ab-b");
    const head = await scopeHead("todos:demo");

    // Direct: authority for input A is applied before the call resolves.
    const direct = await client.mutations.call.editAndShow({ todo: { id: "ab-a", title: "  A1  " }, shown: "ab-b" });
    assert.deepEqual(direct, { todo: { id: "ab-b", title: "B" } }, "result.todo is B's Loader snapshot, not the input A");
    assert.equal((await client.models.todo.get({ id: "ab-a" }))?.title, "A1", "local A holds the server's normalized authority");
    assert.equal(await serverStamp("ab-a"), stampA + 1, "A advanced exactly one stamp");
    assert.equal(await serverStamp("ab-b"), stampB, "an output-only read does not stamp B");
    assert.equal(await scopeHead("todos:demo"), head + 1, "one position for A on its Scope, none for B");

    // Durable: the optimistic A is replaced by the committed authority on completion.
    await client.connection!.pause();
    const call = await client.mutations.editAndShow({ todo: { id: "ab-a", title: "  A2  " }, shown: "ab-b" });
    assert.equal((await client.models.todo.get({ id: "ab-a" }))?.title, "  A2  ", "optimistic A");
    await client.connection!.resume();
    const outcome = await call.wait();
    assert.equal(outcome.error, null);
    assert.deepEqual(outcome.result, { todo: { id: "ab-b", title: "B" } });
    assert.equal((await client.models.todo.get({ id: "ab-a" }))?.title, "A2", "local A corrected once the call completed");
    assert.equal(await serverStamp("ab-a"), stampA + 2);
    assert.equal(await serverStamp("ab-b"), stampB);

    // store:false suppresses storing the output, never the input's authority.
    const unstored = await client.mutations.call.editAndShow({ todo: { id: "ab-a", title: " A3 " }, shown: "ab-c" }, { store: false });
    assert.equal(unstored.todo.title, "C only on the server");
    assert.equal(await client.models.todo.get({ id: "ab-c" }), null, "the unstored output did not reach the local Model");
    assert.equal(await serverStamp("ab-c"), null, "reading an output allocates no stamp");
    assert.equal((await client.models.todo.get({ id: "ab-a" }))?.title, "A3", "input authority is mandatory");
    const unstoredDurable = await (await client.mutations.editAndShow({ todo: { id: "ab-a", title: " A4 " }, shown: "ab-c" }, { store: false })).wait();
    assert.equal(unstoredDurable.result!.todo.title, "C only on the server");
    assert.equal(await client.models.todo.get({ id: "ab-c" }), null);
    assert.equal((await client.models.todo.get({ id: "ab-a" }))?.title, "A4");
    const stored = await client.mutations.call.editAndShow({ todo: { id: "ab-a", title: "A5" }, shown: "ab-c" });
    assert.equal((await client.models.todo.get({ id: "ab-c" }))?.title, stored.todo.title, "the default policy stores the output");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("generated store hook commits derived rows before direct, queued and Query success", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-store-hook-"));
  const path = join(directory, "client.sqlite");
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path, server: server() });
    for (const [id, title] of [["hook-a", "A"], ["hook-b", "B"]]) {
      await client.mutations.call.addTodo({ todo: { id, title } });
    }
    await client.close();
    const observed: Array<{ kind: string; id: string; row?: string; before?: string }> = [];
    const hooks: StoreHooks = {
      async todo(tx, changes) {
        for (const change of changes) {
          const before = await tx.models.todo.get(change.identity);
          observed.push({ kind: change.kind, id: change.identity.id, row: change.kind === "upsert" ? change.row.title : undefined, before: before?.title });
          if (change.kind === "upsert" && (change.identity.id === "hook-a" || change.identity.id === "hook-query")) {
            await tx.models.todo.update({ id: "hook-b" }, { title: `derived:${change.row.title}` });
            await tx.streams.subscribe("hook-derived");
          }
        }
      },
    };
    client = await GeneratedClient.open({ path, server: server(), onStore: hooks });
    Object.assign(hooks, { todo: async () => { throw Error("registration was not snapshotted"); } });
    const watched: string[][] = [];
    const stop = client.models.todo.watch({}, rows => watched.push(rows.map(row => `${row.id}:${row.title}`).sort()));
    try {
      const result = await client.mutations.call.editAndShow({ todo: { id: "hook-a", title: " A1 " }, shown: "hook-b" }, { store: false });
      assert.deepEqual(result, { todo: { id: "hook-b", title: "B" } }, "result remains the Loader snapshot");
      assert.deepEqual(observed, [{ kind: "upsert", id: "hook-a", row: "A1", before: "A" }], "only mandatory input authority calls the hook");
      assert.equal((await client.models.todo.get({ id: "hook-a" }))?.title, "A1");
      assert.equal((await client.models.todo.get({ id: "hook-b" }))?.title, "derived:A1");
      await wait(async () => watched.some(rows => rows.includes("hook-a:A1") && rows.includes("hook-b:derived:A1")), "watcher after hook commit");
      assert.equal(watched.some(rows => rows.includes("hook-a:A1") !== rows.includes("hook-b:derived:A1")), false, "watchers see no partial A/B commit");
      assert.deepEqual(await client.readSql("SELECT stream FROM axton_subscription WHERE stream = ?", ["hook-derived"]), [{ stream: "hook-derived" }], "hook subscription intent committed");
    } finally { stop(); }
    const pending = await client.mutations.editAndShow({ todo: { id: "hook-a", title: " A2 " }, shown: "hook-b" }, { store: false });
    const outcome = await pending.wait();
    assert.equal(outcome.error, null);
    assert.deepEqual(outcome.result, { todo: { id: "hook-b", title: "B" } }, "queued result remains B's Loader snapshot");
    assert.equal((await client.models.todo.get({ id: "hook-a" }))?.title, "A2", "Call.wait follows authoritative A commit");
    assert.equal((await client.models.todo.get({ id: "hook-b" }))?.title, "derived:A2", "Call.wait follows derived B commit");
    assert.deepEqual(observed[1], { kind: "upsert", id: "hook-a", row: "A2", before: " A2 " }, "queued hook sees the optimistic pre-store view and incoming server row");
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('hook-query','query source')");
    const queried = await client.queries.searchTodos({ query: "query source" });
    assert.deepEqual(queried.todos, [{ id: "hook-query", title: "query source" }], "direct Query returns its Loader snapshot");
    assert.equal((await client.models.todo.get({ id: "hook-query" }))?.title, "query source", "Query resolves after authoritative row commit");
    assert.equal((await client.models.todo.get({ id: "hook-b" }))?.title, "derived:query source", "Query resolves after hook derived row commit");
    assert.deepEqual(observed[2], { kind: "upsert", id: "hook-query", row: "query source", before: undefined });
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("Dart generated application hook sees mandatory A and keeps B's Loader snapshot", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-dart-hook-"));
  try {
    const root = process.cwd();
    const { stdout } = await execFileAsync("dart", [
      "run", "action_e2e_hook.dart", url, join(directory, "client.sqlite"),
      join(root, `target/debug/libaxton_dart.${process.platform === "darwin" ? "dylib" : "so"}`),
    ], { cwd: join(root, "integration/action-runtime-dart"), timeout: 20_000 });
    assert.match(stdout, /Dart generated A\/B store hook: passed/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test("a retried A-returns-B call replays its saved result without new stamps or positions on both routes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-a-b-retry-"));
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    for (const [id, title] of [["abr-a", "A"], ["abr-b", "B"]]) assert.equal((await (await client.mutations.addTodo({ todo: { id, title } })).wait()).error, null);
    const direct = JSON.stringify({ capabilities: ["stream-membership-v1"], call: { callId: "01890f47-1234-7123-8123-1234567890c1", name: "EditAndShow", version: 1, args: { todo: { id: "abr-a", title: "A1" }, shown: "abr-b" } }, models: { Todo: 1, Note: 1 } });
    const first = await post("actions", direct);
    assert.deepEqual(first.completion.outcome.result, { todo: { id: "abr-b", title: "B" } });
    assert.deepEqual(first.records.map((record: { identity: object }) => record.identity), [{ id: "abr-a" }, { id: "abr-b" }], "caller authority: input A, and output B under the default store policy");
    await fixture.pool.query("UPDATE action_e2e_todo SET title='B later' WHERE id='abr-b'");
    const stamp = await serverStamp("abr-a");
    const handled = fixture.handlerCalls;
    const retried = await post("actions", direct);
    assert.deepEqual(retried, first, "the saved result still names B as it was");
    assert.equal(fixture.handlerCalls, handled, "the handler did not run again");
    assert.equal(await serverStamp("abr-a"), stamp, "the retry allocated no stamp");

    // Durable: the frozen batch replays its receipt.
    await client.connection!.pause();
    const call = await client.mutations.editAndShow({ todo: { id: "abr-a", title: "A2" }, shown: "abr-b" });
    const frozen = await client.client.freeze();
    assert.ok(frozen);
    const receipt = await post("mutations", frozen);
    const stamped = await serverStamp("abr-a");
    const heads = (await fixture.pool.query("SELECT stream, head FROM axton_stream ORDER BY stream")).rows;
    await fixture.pool.query("UPDATE action_e2e_todo SET title='B latest' WHERE id='abr-b'");
    const replayed = await post("mutations", frozen);
    assert.deepEqual(replayed, receipt, "the stored receipt, including result B, is replayed");
    assert.equal(fixture.handlerCalls, handled + 1, "the durable handler ran once");
    assert.equal(await serverStamp("abr-a"), stamped, "no stamp on replay");
    assert.deepEqual((await fixture.pool.query("SELECT stream, head FROM axton_stream ORDER BY stream")).rows, heads, "no position on replay");
    await client.client.acknowledge(JSON.parse(frozen).batchSequence, replayed);
    const outcome = await call.wait();
    assert.deepEqual(outcome.result, { todo: { id: "abr-b", title: "B later" } });
    assert.equal((await client.models.todo.get({ id: "abr-a" }))?.title, "A2");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a no-output edit completes on both routes once local A holds its reconciled authority", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-no-output-"));
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    assert.equal((await (await client.mutations.addTodo({ todo: { id: "plain-a", title: "A" } })).wait()).error, null);
    const stamp = await serverStamp("plain-a");
    const direct = await client.mutations.call.updateTodo({ todo: { id: "plain-a", title: "  direct  " } });
    assert.equal(direct, undefined, "no declared output, no result");
    assert.equal((await client.models.todo.get({ id: "plain-a" }))?.title, "direct", "the server's trimmed title is local when the call resolves");
    await client.connection!.pause();
    const call = await client.mutations.updateTodo({ todo: { id: "plain-a", title: "  durable  " } });
    await client.connection!.resume();
    const outcome = await call.wait();
    assert.deepEqual(outcome, { result: undefined, error: null });
    assert.equal((await client.models.todo.get({ id: "plain-a" }))?.title, "durable", "completion follows the local application of A's authority");
    assert.equal(await serverStamp("plain-a"), stamp + 2, "one stamp per edit");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a touch of a Model the caller never declared commits and reaches a different subscriber on both routes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-extra-"));
  const note = "0b6f4bd4-5a1d-4c8e-9a51-3f1f0e7d2c11";
  let reader: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('extra-a','A')");
    await fixture.pool.query("INSERT INTO action_e2e_note(id,body,mood,created_at,tag) VALUES($1,'before','calm','2026-01-01T00:00:00.000Z',NULL)", [note]);
    reader = await GeneratedClient.open({ path: join(directory, "reader.sqlite"), server: server() });
    const subscription = await reader.streams.subscribe("notes:demo");
    await wait(async () => subscription.status.initialization === "ready", "the reader's origin");
    // Enrollment outside any handler: adding an absent member delivers its current state.
    await fixture.backend.transaction(async ({ stream: scope }) => { scope("notes:demo").track.note({ id: note }); });
    await wait(async () => (await reader!.models.note.get({ id: note }))?.body === "before", "the enrolled Note");
    const noteStamp = await serverStamp(note, "Note");

    // The initiating caller declares only Todo: its descriptor has no Note.
    const args = (body: string) => ({ todo: { id: "extra-a", title: ` ${body} ` }, note, body });
    const direct = await post("actions", JSON.stringify({ capabilities: ["stream-membership-v1"], call: { callId: "01890f47-1234-7123-8123-1234567890d1", name: "AnnotateTodo", version: 1, args: args("direct") }, models: { Todo: 1 } }));
    assert.equal(direct.completion.outcome.error ?? null, null, JSON.stringify(direct.completion));
    assert.deepEqual(direct.records.map((record: { model: string; identity: object; state: object }) => [record.model, record.identity, record.state]), [["Todo", { id: "extra-a" }, { title: "direct" }]], "only the input is caller authority");
    await wait(async () => (await reader!.models.note.get({ id: note }))?.body === "direct", "the touched Note on the reader's Scope");
    assert.equal(await serverStamp(note, "Note"), noteStamp + 1);

    const push = JSON.stringify({ capabilities: ["stream-membership-v1"], clientId: "extra-touch-client", batchSequence: 1, models: { Todo: 1 }, mutations: [{ ordinal: 1, callId: "01890f47-1234-7123-8123-1234567890d2", name: "AnnotateTodo", version: 1, args: args("durable") }] });
    const receipt = await post("mutations", push);
    assert.deepEqual(receipt.rejections ?? [], []);
    assert.deepEqual(receipt.records.map((record: { model: string }) => record.model), ["Todo"], "the receipt carries no Note");
    await wait(async () => (await reader!.models.note.get({ id: note }))?.body === "durable", "the durable touch on the reader's Scope");
    assert.equal(await serverStamp(note, "Note"), noteStamp + 2);
    assert.deepEqual(await post("mutations", push), receipt, "a retried batch replays its receipt");
    assert.equal(await serverStamp(note, "Note"), noteStamp + 2, "and touches nothing again");
  } finally { await reader?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("create defaults are generated once by the client and reach both routes unchanged", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-defaults-"));
  const path = join(directory, "client.sqlite");
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
  const wire = (note: { id: string; body: string; mood: string; createdAt: Date; tag: string | null }) => ({ ...note, createdAt: note.createdAt.toISOString() });
  const stored = async (id: string) => (await fixture.pool.query("SELECT id,body,mood,created_at AS \"createdAt\",tag FROM action_e2e_note WHERE id=$1", [id])).rows[0];
  let client: GeneratedClient | undefined;
  try {
    // Durable, offline: the optimistic row, the persisted intent and the frozen
    // batch share the values generated at submission.
    client = await GeneratedClient.open({ path });
    await client.mutations.addNote({ note: { body: "queued" } });
    const [optimistic] = await client.models.note.query();
    assert.ok(optimistic);
    assert.match(optimistic.id, uuid);
    assert.ok(Math.abs(optimistic.createdAt.getTime() - Date.now()) < 60_000, "client wall clock");
    assert.deepEqual({ body: optimistic.body, mood: optimistic.mood, tag: optimistic.tag }, { body: "queued", mood: "calm", tag: "inbox" });
    const frozen = await client.client.freeze();
    assert.ok(frozen);
    assert.deepEqual(JSON.parse(frozen).mutations[0].args.note, wire(optimistic), "frozen intent carries the optimistic values");
    assert.match(JSON.parse(frozen).mutations[0].args.note.createdAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/, "UTC at millisecond precision");
    await client.close();
    client = await GeneratedClient.open({ path });
    assert.deepEqual(await client.models.note.query(), [optimistic], "reopen regenerates nothing");
    assert.equal(await client.client.freeze(), frozen, "the frozen batch is byte-identical after reopen");
    const handled = fixture.handlerCalls;
    const first = await post("mutations", frozen);
    const replayed = await post("mutations", frozen);
    assert.deepEqual(replayed.completions, first.completions, "a retried batch replays the stored outcome");
    assert.equal(fixture.handlerCalls, handled + 1, "the handler ran once");
    await client.client.acknowledge(JSON.parse(frozen).batchSequence, replayed);
    assert.deepEqual(fixture.notes.at(-1), optimistic, "the durable handler received the client's values");
    assert.deepEqual(await stored(optimistic.id), wire(optimistic));
    assert.deepEqual(await client.models.note.get({ id: optimistic.id }), optimistic, "settled authority agrees");

    // Direct: the request carries the generated values; explicit null wins.
    await client.connect(server());
    const direct = await client.mutations.call.addNote({ note: { tag: null, mood: "busy" } });
    assert.match(direct.saved.id, uuid);
    assert.notEqual(direct.saved.id, optimistic.id, "each fresh create generates its own id");
    assert.deepEqual({ body: direct.saved.body, mood: direct.saved.mood, tag: direct.saved.tag }, { body: "", mood: "busy", tag: null });
    assert.deepEqual(fixture.notes.at(-1), direct.saved, "the direct handler received exactly what the Loader returned");
    assert.deepEqual(await stored(direct.saved.id), wire(direct.saved));
    assert.deepEqual(await client.models.note.get({ id: direct.saved.id }), direct.saved, "direct authority applied locally");

    // Durable online: the Loader snapshot equals the optimistic row it settles.
    const call = await client.mutations.addNote({ note: {} });
    const before = (await client.models.note.query()).find((note) => note.id !== optimistic.id && note.id !== direct.saved.id);
    assert.ok(before);
    const outcome = await call.wait();
    assert.equal(outcome.error, null);
    assert.deepEqual(outcome.result!.saved, before, "returned snapshot agrees with the optimistic create");

    // Local-only create fills defaults too and never reaches the backend.
    const handledLocal = fixture.handlerCalls;
    await client.models.note.create({ body: "local" });
    const localNote = (await client.models.note.query({ where: { body: "local" } }))[0];
    assert.match(localNote!.id, uuid);
    assert.equal(fixture.handlerCalls, handledLocal);
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

// ---- Query once (#158): complete result snapshots over a real backend ----

test("once reuses the complete Query snapshot; default calls stay fresh; refresh replaces it", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-query-once-"));
  let client: GeneratedClient | undefined;
  let other: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('once-a','onceq A')");
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    const calls = () => fixture.onceCalls.todoPage;
    const start = calls();
    const first = await client.queries.todoPage({ query: "onceq" }, { once: true });
    assert.equal(calls(), start + 1, "a miss executes the handler");
    assert.deepEqual(first.todos, [{ id: "once-a", title: "onceq A" }]);
    assert.equal(first.count, 1);
    assert.equal(first.next, "after:once-a");
    assert.ok(first.asOf instanceof Date);
    let second: typeof first | undefined;
    const paths = await requestedPaths(async () => { second = await client!.queries.todoPage({ query: "onceq" }, { once: true }); });
    assert.deepEqual(paths, [], "a hit issues no request");
    assert.equal(calls(), start + 1);
    assert.deepEqual(second, first, "the same complete typed result");
    assert.notStrictEqual(second, first);
    // Mutating a returned result, its lists or Dates never reaches the snapshot.
    (first.todos as { id: string; title: string }[]).push({ id: "x", title: "x" });
    first.asOf.setUTCFullYear(1999);
    const third = await client.queries.todoPage({ query: "onceq" }, { once: true });
    assert.equal(third.todos.length, 1);
    assert.equal(third.asOf.getUTCFullYear(), 2026);
    // An ordinary call is an independent request and replaces nothing.
    const fresh = await client.queries.todoPage({ query: "onceq" });
    assert.equal(calls(), start + 2);
    assert.notEqual(fresh.asOf.getTime(), third.asOf.getTime());
    assert.equal((await client.queries.todoPage({ query: "onceq" }, { once: true })).asOf.getTime(), third.asOf.getTime());
    // Refresh always requests and replaces the snapshot on success.
    const refreshed = await client.queries.todoPage({ query: "onceq" }, { once: true, refresh: true });
    assert.equal(calls(), start + 3);
    assert.notEqual(refreshed.asOf.getTime(), third.asOf.getTime());
    assert.equal((await client.queries.todoPage({ query: "onceq" }, { once: true })).asOf.getTime(), refreshed.asOf.getTime());
    assert.equal(calls(), start + 3);
    // A parameterless scalar-only Query.
    const counted = fixture.onceCalls.countTodos;
    const count = await client.queries.countTodos({}, { once: true });
    assert.equal(typeof count.count, "number");
    assert.deepEqual(await client.queries.countTodos({}, { once: true }), count);
    assert.equal(fixture.onceCalls.countTodos, counted + 1);
    // Concurrent callers of one miss share one request.
    const shared = await Promise.all([1, 2, 3].map(() => client!.queries.todoPage({ query: "onceq-none" }, { once: true })));
    assert.equal(calls(), start + 4);
    assert.deepEqual(shared[0], shared[2]);
    // Another local database is another cache: nothing is shared.
    other = await GeneratedClient.open({ path: join(directory, "other.sqlite"), server: server() });
    await other.queries.todoPage({ query: "onceq" }, { once: true });
    assert.equal(calls(), start + 5);
    assert.equal((await client.syncState()).pending, 0, "once never enqueues");
  } finally {
    await other?.close();
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("a hit returns the old snapshot while local Models move on, and writes or wakes nothing", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-query-once-models-"));
  let client: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('snap-a','snapq A')");
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    const cached = await client.queries.todoPage({ query: "snapq" }, { once: true });
    assert.equal(cached.todos[0]?.title, "snapq A");
    assert.equal((await client.models.todo.get({ id: "snap-a" }))?.title, "snapq A", "the miss stored its Model authority");
    const update = await client.mutations.updateTodo({ todo: { id: "snap-a", title: "snapq B" } });
    assert.equal((await update.wait()).error, null);
    assert.equal((await client.models.todo.get({ id: "snap-a" }))?.title, "snapq B");
    const seen: unknown[] = [];
    const stop = client.models.todo.watch({ id: "snap-a" }, (rows) => seen.push(rows));
    await wait(async () => seen.length === 1, "initial watch");
    const hit = await client.queries.todoPage({ query: "snapq" }, { once: true });
    assert.equal(hit.todos[0]?.title, "snapq A", "the snapshot is the earlier request's result");
    assert.equal((await client.models.todo.get({ id: "snap-a" }))?.title, "snapq B", "the hit reapplied no old authority");
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(seen.length, 1, "no Model watcher woke");
    stop();
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("once snapshots survive offline reopen; misses, refresh failures and invalidation behave", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-query-once-offline-"));
  const path = join(directory, "client.sqlite");
  let client: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('off-a','offq A')");
    client = await GeneratedClient.open({ path, server: server() });
    const calls = () => fixture.onceCalls.todoPage;
    const saved = await client.queries.todoPage({ query: "offq" }, { once: true });
    await client.close();
    const start = calls();
    client = await GeneratedClient.open({ path });
    assert.deepEqual(await client.queries.todoPage({ query: "offq" }, { once: true }), saved, "offline after reopen");
    await assert.rejects(client.queries.todoPage({ query: "offq-miss" }, { once: true }), (error: { code?: string }) => error.code === "action.unavailable");
    await assert.rejects(client.queries.todoPage({ query: "offq" }, { once: true, refresh: true }), (error: { code?: string }) => error.code === "action.unavailable");
    assert.equal((await client.syncState()).pending, 0, "never enqueued implicitly");
    assert.equal(calls(), start);
    await client.connect(server());
    // A failed refresh keeps the previous snapshot.
    fixture.failQueries = true;
    try {
      await assert.rejects(client.queries.todoPage({ query: "offq" }, { once: true, refresh: true }), (error: { code?: string; execution?: string }) => error.code === "query.down" && error.execution === "rejected");
    } finally { fixture.failQueries = false; }
    assert.equal(calls(), start + 1);
    assert.deepEqual(await client.queries.todoPage({ query: "offq" }, { once: true }), saved);
    // Explicit invalidation needs no network and causes the next miss.
    await client.connection!.pause();
    await client.queries.invalidate.todoPage({ query: "offq" });
    await client.connection!.resume();
    const again = await client.queries.todoPage({ query: "offq" }, { once: true });
    assert.equal(calls(), start + 2);
    assert.notEqual(again.asOf.getTime(), saved.asOf.getTime());
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("store variants are separate snapshots and store:false still persists the result", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-query-once-store-"));
  const path = join(directory, "client.sqlite");
  let client: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('ostore-a','ostoreq A')");
    client = await GeneratedClient.open({ path, server: server() });
    const calls = () => fixture.onceCalls.todoPage;
    const start = calls();
    const unstored = await client.queries.todoPage({ query: "ostoreq" }, { once: true, store: false });
    assert.equal(unstored.todos[0]?.title, "ostoreq A");
    assert.equal(await client.models.todo.get({ id: "ostore-a" }), null, "store:false materialized no Model");
    await client.close();
    client = await GeneratedClient.open({ path, server: server() });
    assert.deepEqual(await client.queries.todoPage({ query: "ostoreq" }, { once: true, store: { todos: false } }), unstored, "the equivalent store:false policy hits after reopen");
    assert.equal(calls(), start + 1);
    // Asking for Models is a different key; the old snapshot is never replayed into Models.
    await client.queries.todoPage({ query: "ostoreq" }, { once: true });
    assert.equal(calls(), start + 2);
    assert.equal((await client.models.todo.get({ id: "ostore-a" }))?.title, "ostoreq A");
    await client.queries.invalidate.todoPage({ query: "ostoreq" });
    await client.queries.todoPage({ query: "ostoreq" }, { once: true, store: false });
    await client.queries.todoPage({ query: "ostoreq" }, { once: true });
    assert.equal(calls(), start + 4, "invalidation cleared every store variant");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("Model Fetch reads through the real Loader: stored by default, shared only while in flight, never publishing", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-model-fetch-"));
  let client: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('fetch-a','Fetched A'),('fetch-b','Preview B')");
    const note = "0190c3a1-0000-7000-8000-00000000f153";
    await fixture.pool.query("INSERT INTO action_e2e_note(id,body,mood,created_at,tag) VALUES($1,'noted','busy','2026-03-04T05:06:07.000Z',NULL)", [note]);
    const invalidations = async () => Number((await fixture.pool.query("SELECT count(*)::int AS n FROM axton_stream_log")).rows[0].n);
    const memberships = async () => Number((await fixture.pool.query("SELECT count(*)::int AS n FROM axton_stream_member")).rows[0].n);
    const published = await invalidations();
    const members = await memberships();
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    const loads = fixture.loaderCalls;
    let paths: string[] = [];
    let todo: Awaited<ReturnType<typeof client.fetch.todo>> = null;
    paths = await requestedPaths(async () => { todo = await client!.fetch.todo({ id: "fetch-a" }); });
    assert.deepEqual(paths, ["/sync/fetch"]);
    assert.deepEqual(todo, { id: "fetch-a", title: "Fetched A" });
    assert.equal(fixture.loaderCalls, loads + 1);
    assert.deepEqual(await client.models.todo.get({ id: "fetch-a" }), { id: "fetch-a", title: "Fetched A" }, "stored by default");
    assert.equal(await serverStamp("fetch-a"), 1, "stamp evidence, not an advance");
    // A present local row does not satisfy the next call: it reads again.
    // Business writes go through the framework so the record gets a new stamp.
    await fixture.backend.transaction(async ({ tx, invalidate: touch }) => {
      await tx.query("UPDATE action_e2e_todo SET title='Fetched A2' WHERE id='fetch-a'");
      touch.todo({ id: "fetch-a" });
    });
    assert.equal((await client.fetch.todo({ id: "fetch-a" }))?.title, "Fetched A2");
    assert.equal(fixture.loaderCalls, loads + 2);
    assert.equal((await client.models.todo.get({ id: "fetch-a" }))?.title, "Fetched A2");
    // Overlapping identical callers share one request and one Loader call.
    const shared = await Promise.all([1, 2, 3].map(() => client!.fetch.todo({ id: "fetch-a" })));
    assert.equal(fixture.loaderCalls, loads + 3);
    assert.deepEqual(shared[0], shared[2]);
    assert.notStrictEqual(shared[0], shared[2]);
    // store:false returns the snapshot and neither stores nor stamps.
    assert.deepEqual(await client.fetch.todo({ id: "fetch-b" }, { store: false }), { id: "fetch-b", title: "Preview B" });
    assert.equal(await client.models.todo.get({ id: "fetch-b" }), null);
    assert.equal(await serverStamp("fetch-b"), null);
    // Typed codecs: a UUID identity, a DateTime and an enum.
    const fetchedNote = await client.fetch.note({ id: note }, { store: false });
    assert.ok(fetchedNote?.createdAt instanceof Date);
    assert.equal(fetchedNote.createdAt.toISOString(), "2026-03-04T05:06:07.000Z");
    assert.equal(fetchedNote.mood, "busy");
    assert.equal(fetchedNote.tag, null);
    // The Loader's null is absence: stored as a deletion of the local row.
    await fixture.backend.transaction(async ({ tx, invalidate: touch }) => {
      await tx.query("DELETE FROM action_e2e_todo WHERE id='fetch-a'");
      touch.todo({ id: "fetch-a" });
    });
    assert.equal(await client.fetch.todo({ id: "fetch-a" }), null);
    assert.equal(await client.models.todo.get({ id: "fetch-a" }), null);
    // The touched records belong to no Scope, so any publication is Fetch's.
    assert.equal(await invalidations(), published, "Fetch publishes nothing");
    assert.equal(await memberships(), members, "Fetch changes no membership");
    assert.equal((await client.syncState()).pending, 0, "Fetch never enqueues");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("the PublishEntry backend fixture stores Entry, media and placement together, or rejects them all while switched", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-publish-fixture-"));
  let client: GeneratedClient | undefined;
  const input = (id: string) => ({
    entry: { id, title: `title ${id}`, body: "body" },
    media: [{ id: `${id}-m1`, entryId: id, url: "one.jpg" }, { id: `${id}-m2`, entryId: id, url: "two.jpg" }],
    placement: { id: `${id}-p`, entryId: id, journal: "daily", position: 1 },
  });
  const stored = async (id: string) => ({
    entries: Number((await fixture.pool.query("SELECT count(*)::int AS n FROM action_e2e_entry WHERE id=$1", [id])).rows[0].n),
    media: Number((await fixture.pool.query("SELECT count(*)::int AS n FROM action_e2e_media WHERE entry_id=$1", [id])).rows[0].n),
    placements: Number((await fixture.pool.query("SELECT count(*)::int AS n FROM action_e2e_placement WHERE entry_id=$1", [id])).rows[0].n),
  });
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    const accepted = await (await client.mutations.publishEntry(input("publish-ok"))).wait();
    assert.equal(accepted.error, null);
    assert.deepEqual(fixture.publishes.at(-1), input("publish-ok"), "the handler receives exactly the business input");
    assert.deepEqual(await stored("publish-ok"), { entries: 1, media: 2, placements: 1 });
    assert.equal((await client.models.entry.get({ id: "publish-ok" }))?.title, "title publish-ok");
    assert.equal((await client.models.media.get({ id: "publish-ok-m2" }))?.url, "two.jpg");
    assert.equal((await client.models.placement.get({ id: "publish-ok-p" }))?.journal, "daily");

    fixture.rejectPublish = true;
    try {
      const rejected = await (await client.mutations.publishEntry(input("publish-no"))).wait();
      assert.equal(rejected.error?.code, "publish.rejected");
      assert.equal(rejected.error?.execution, "rejected");
    } finally { fixture.rejectPublish = false; }
    assert.deepEqual(fixture.publishes.at(-1), input("publish-no"));
    assert.deepEqual(await stored("publish-no"), { entries: 0, media: 0, placements: 0 }, "the rejection rolled back the Entry and media it had inserted");
    assert.equal(await client.models.entry.get({ id: "publish-no" }), null, "rejection removes the optimism");
    assert.equal(await client.models.media.get({ id: "publish-no-m1" }), null);
    assert.equal(await client.models.placement.get({ id: "publish-no-p" }), null);
    assert.equal((await client.syncState()).pending, 0);
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});

test("a device-only Composition (no Loader): plain local writes and reads work and never reach the wire", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-device-only-"));
  let client: GeneratedClient | undefined;
  const original = globalThis.fetch;
  /** Every request the client makes, with its body minus the read-contract `models` map, which names every Model. */
  const requests: { path: string; body: string }[] = [];
  globalThis.fetch = ((input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
    let body = typeof init?.body === "string" ? init.body : "";
    try { const { models, ...rest } = JSON.parse(body); body = JSON.stringify(rest); } catch { /* not JSON */ }
    requests.push({ path, body });
    return original(input, init);
  }) as typeof fetch;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    await client.models.composition.create({ id: "device-a", title: "draft a", body: "local only" });
    await client.models.composition.create({ id: "device-b", title: "draft b", body: "local only" });
    await client.models.composition.update({ id: "device-a" }, { body: "edited locally" });
    await client.models.composition.delete({ id: "device-b" });
    assert.deepEqual(await client.models.composition.get({ id: "device-a" }), { id: "device-a", title: "draft a", body: "edited locally" });
    assert.equal(await client.models.composition.get({ id: "device-b" }), null);
    assert.equal((await client.syncState()).pending, 0, "a local write queues nothing");
    assert.deepEqual(requests.filter(({ path }) => path === "/sync/mutations"), [], "nothing was pushed for the local writes");
    // A later business call is pushed on its own and names no Composition.
    const added = await (await client.mutations.addTodo({ todo: { id: "device-only-todo", title: "after local drafts" } })).wait();
    assert.equal(added.error, null);
    assert.ok(requests.some(({ path }) => path === "/sync/mutations"), "the business call was pushed");
    for (const { path, body } of requests) assert.doesNotMatch(body, /composition|device-a|device-b/i, `${path} carries no Composition`);
    assert.equal(Number((await fixture.pool.query("SELECT count(*)::int AS n FROM axton_record WHERE model='Composition'")).rows[0].n), 0, "the backend stamped no Composition");
    // A Fetch reaches a backend with no Loader to read one: a saved rejection that leaves the local row alone.
    await assert.rejects(client.fetch.composition({ id: "device-a" }), (error: { code?: string }) => error.code === "loader.unregistered");
    assert.deepEqual(await client.models.composition.get({ id: "device-a" }), { id: "device-a", title: "draft a", body: "edited locally" });
  } finally {
    globalThis.fetch = original;
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

/** PublishEntry business input for Entry `id`, built from a Composition's content. */
const publishInput = (id: string, composition: { title: string; body: string }) => ({
  entry: { id, title: composition.title, body: composition.body },
  media: [{ id: `${id}-m1`, entryId: id, url: "one.jpg" }, { id: `${id}-m2`, entryId: id, url: "two.jpg" }],
  placement: { id: `${id}-p`, entryId: id, journal: "daily", position: 1 },
});
/** Parses every push body the client sends from now until `stop()`. */
const capturePushes = () => {
  const original = globalThis.fetch;
  const bodies: { batchSequence: number; mutations: { callId: string; name: string; version: number; args: unknown }[]; [key: string]: unknown }[] = [];
  globalThis.fetch = ((input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
    if (path === "/sync/mutations") bodies.push(JSON.parse(String(init?.body)));
    return original(input, init);
  }) as typeof fetch;
  return { bodies, stop: () => { globalThis.fetch = original; } };
};
/**
 * Whether a pushed request carries companion data: a Mutation field other
 * than its business call, a Composition anywhere but the read-contract
 * `models` map, or one of the companions' identities. The business input
 * itself is built from Composition content, so content is not a signal.
 */
const leaksComposition = (body: { mutations: object[]; models?: unknown; [key: string]: unknown }, identities: string[]) => {
  const { models, ...rest } = body;
  const text = JSON.stringify(rest);
  return /composition/i.test(text) || identities.some((id) => text.includes(`"id":"${id}"`))
    || body.mutations.some((mutation) => Object.keys(mutation).sort().join() !== "args,callId,name,ordinal,version")
    || Object.values(models as Record<string, unknown>).some((version) => typeof version !== "number");
};

test("tx.mutations.publishEntry deletes its Composition as a local companion: acceptance keeps it deleted, rejection restores editing", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-publish-companion-"));
  let client: GeneratedClient | undefined;
  const capture = capturePushes();
  const publishedBefore = fixture.publishes.length;
  const draft = (id: string) => ({ id, title: `draft ${id}`, body: `body ${id}` });
  let localRuns = 0;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    await client.connection!.pause();
    for (const id of ["comp-ok", "comp-ok-re", "comp-no", "comp-no-re", "comp-side"]) await client.models.composition.create(draft(id));
    fixture.rejectedEntries.add("entry-no");
    const calls = await client.transaction(async (tx) => {
      const accepted = await tx.models.composition.get({ id: "comp-ok" });
      const rejected = await tx.models.composition.get({ id: "comp-no" });
      assert.ok(accepted && rejected);
      const ok = await tx.mutations.publishEntry(publishInput("entry-ok", accepted), {
        local: async (local) => {
          localRuns++;
          assert.equal((await local.models.entry.get({ id: "entry-ok" }))?.title, accepted.title, "the callback reads its own call's optimism");
          await local.models.composition.delete({ id: "comp-ok" });
          await local.models.composition.delete({ id: "comp-ok-re" });
        },
      });
      await assert.rejects(ok.wait(), { name: "CallError", code: "transaction_uncommitted" }, "a wait before commit fails at once");
      const no = await tx.mutations.publishEntry(publishInput("entry-no", rejected), {
        local: async (local) => {
          localRuns++;
          await local.models.composition.delete({ id: "comp-no" });
          await local.models.composition.delete({ id: "comp-no-re" });
        },
      });
      // An ordinary transaction write: independent of both calls.
      await tx.models.composition.update({ id: "comp-side" }, { title: "edited in the transaction" });
      assert.equal(await tx.models.composition.get({ id: "comp-ok" }), null, "later transaction reads see the companion delete");
      return { ok, no };
    });
    assert.equal(localRuns, 2);
    assert.equal((await client.syncState()).pending, 2, "both calls committed together");
    for (const id of ["comp-ok", "comp-ok-re", "comp-no", "comp-no-re"]) assert.equal(await client.models.composition.get({ id }), null);
    assert.equal((await client.models.entry.get({ id: "entry-no" }))?.title, "draft comp-no", "optimism is committed");
    // Later independent writes: recreate two deleted identities and edit the side row again.
    await client.models.composition.create({ id: "comp-ok-re", title: "next ok", body: "fresh" });
    await client.models.composition.create({ id: "comp-no-re", title: "next no", body: "fresh" });
    await client.models.composition.update({ id: "comp-side" }, { body: "edited after commit" });
    assert.equal(capture.bodies.length, 0, "nothing was sent while paused");

    await client.connection!.resume();
    const ok = await calls.ok.wait();
    assert.equal(ok.error, null);
    const no = await calls.no.wait();
    assert.equal(no.error?.code, "publish.rejected");
    assert.equal(no.error?.execution, "rejected");
    assert.equal(localRuns, 2, "settlement never runs a callback");

    assert.equal(await client.models.composition.get({ id: "comp-ok" }), null, "acceptance keeps the companion delete");
    assert.deepEqual(await client.models.composition.get({ id: "comp-no" }), draft("comp-no"), "rejection restores the Composition");
    assert.deepEqual(await client.models.composition.get({ id: "comp-ok-re" }), { id: "comp-ok-re", title: "next ok", body: "fresh" }, "acceptance does not delete a later recreate");
    assert.deepEqual(await client.models.composition.get({ id: "comp-no-re" }), { id: "comp-no-re", title: "next no", body: "fresh" }, "rejection does not restore old content over a later recreate");
    assert.deepEqual(await client.models.composition.get({ id: "comp-side" }), { id: "comp-side", title: "edited in the transaction", body: "edited after commit" }, "independent writes survive both outcomes");
    await client.models.composition.update({ id: "comp-no" }, { title: "editing again" });
    assert.equal((await client.models.composition.get({ id: "comp-no" }))?.title, "editing again", "the restored Composition is editable");

    assert.equal((await client.models.entry.get({ id: "entry-ok" }))?.title, "draft comp-ok");
    assert.equal(await client.models.entry.get({ id: "entry-no" }), null, "rejection removes the call's optimism");
    assert.deepEqual((await fixture.pool.query("SELECT id,title,body FROM action_e2e_entry WHERE id IN ('entry-ok','entry-no') ORDER BY id")).rows, [{ id: "entry-ok", title: "draft comp-ok", body: "body comp-ok" }]);
    assert.equal((await client.syncState()).pending, 0);

    const pushed = capture.bodies.flatMap((body) => body.mutations);
    assert.deepEqual(pushed.map((call) => [call.name, call.args]), [["PublishEntry", publishInput("entry-ok", draft("comp-ok"))], ["PublishEntry", publishInput("entry-no", draft("comp-no"))]], "only the business calls are pushed");
    for (const body of capture.bodies) assert.equal(leaksComposition(body, ["comp-ok", "comp-ok-re", "comp-no", "comp-no-re", "comp-side"]), false, `push carries no companion: ${JSON.stringify(body)}`);
    assert.deepEqual(fixture.publishes.slice(publishedBefore), [publishInput("entry-ok", draft("comp-ok")), publishInput("entry-no", draft("comp-no"))], "handlers receive exactly the business input");
  } finally {
    capture.stop();
    fixture.rejectedEntries.delete("entry-no");
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("queued transactional PublishEntry survives offline reopen and settles without re-running local", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-publish-reopen-"));
  const path = join(directory, "client.sqlite");
  let client: GeneratedClient | undefined;
  const runs = { kept: 0, restored: 0 };
  const draft = (id: string) => ({ id, title: `draft ${id}`, body: `body ${id}` });
  const capture = capturePushes();
  try {
    client = await GeneratedClient.open({ path });
    for (const id of ["reopen-kept", "reopen-restored"]) await client.models.composition.create(draft(id));
    for (const [key, id, entry] of [["kept", "reopen-kept", "reopen-entry-ok"], ["restored", "reopen-restored", "reopen-entry-no"]] as const) {
      const call = await client.transaction(async (tx) => {
        const composition = await tx.models.composition.get({ id });
        assert.ok(composition);
        return tx.mutations.publishEntry(publishInput(entry, composition), {
          local: async (local) => { runs[key]++; await local.models.composition.delete({ id }); },
        });
      });
      assert.equal(call.status, "pending");
    }
    assert.deepEqual(runs, { kept: 1, restored: 1 });
    await client.close();

    client = await GeneratedClient.open({ path });
    assert.equal((await client.syncState()).pending, 2, "the queued calls survived reopen");
    assert.equal(await client.models.composition.get({ id: "reopen-kept" }), null, "the companion delete survived reopen");
    assert.equal(await client.models.composition.get({ id: "reopen-restored" }), null);
    assert.equal((await client.models.entry.get({ id: "reopen-entry-no" }))?.title, "draft reopen-restored");
    await client.close();

    fixture.rejectedEntries.add("reopen-entry-no");
    client = await GeneratedClient.open({ path, server: server() });
    await wait(async () => (await client!.syncState()).pending === 0, "reopened transactional calls settle");
    assert.deepEqual(runs, { kept: 1, restored: 1 }, "reopen, retry and settlement never re-run local");
    assert.equal(await client.models.composition.get({ id: "reopen-kept" }), null, "acceptance keeps the deletion after reopen");
    assert.deepEqual(await client.models.composition.get({ id: "reopen-restored" }), draft("reopen-restored"), "rejection restores the Composition after reopen");
    assert.equal((await client.models.entry.get({ id: "reopen-entry-ok" }))?.title, "draft reopen-kept");
    assert.equal(await client.models.entry.get({ id: "reopen-entry-no" }), null);
    assert.deepEqual((await fixture.pool.query("SELECT id FROM action_e2e_entry WHERE id LIKE 'reopen-%' ORDER BY id")).rows, [{ id: "reopen-entry-ok" }]);
    const pushed = capture.bodies.flatMap((body) => body.mutations.map((call) => (call.args as { entry: { id: string } }).entry.id));
    assert.deepEqual([...new Set(pushed)].sort(), ["reopen-entry-no", "reopen-entry-ok"]);
    for (const body of capture.bodies) assert.equal(leaksComposition(body, ["reopen-kept", "reopen-restored"]), false, `push carries no companion: ${JSON.stringify(body)}`);
  } finally {
    capture.stop();
    fixture.rejectedEntries.delete("reopen-entry-no");
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("Dart tx.mutations.publishEntry keeps or restores its companion delete across reopen, and the backend sees only business input", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-dart-publish-"));
  const publishedBefore = fixture.publishes.length;
  for (const id of ["dart-entry-no", "dart-entry-live-no"]) fixture.rejectedEntries.add(id);
  try {
    const root = process.cwd();
    const { stdout, stderr } = await execFileAsync("dart", [
      "run", "action_e2e_publish.dart", url, join(directory, "client.sqlite"),
      join(root, `target/debug/libaxton_dart.${process.platform === "darwin" ? "dylib" : "so"}`),
    ], { cwd: join(root, "integration/action-runtime-dart"), timeout: 30_000 });
    assert.match(stdout, /Dart transactional PublishEntry companion: passed/, stderr);
  } finally {
    for (const id of ["dart-entry-no", "dart-entry-live-no"]) fixture.rejectedEntries.delete(id);
    await rm(directory, { recursive: true, force: true });
  }
  const draft = (id: string) => ({ title: `draft ${id}`, body: `body ${id}` });
  assert.deepEqual(fixture.publishes.slice(publishedBefore), [
    publishInput("dart-entry-ok", draft("dart-comp-ok")),
    publishInput("dart-entry-no", draft("dart-comp-no")),
    publishInput("dart-entry-live-no", draft("dart-comp-live-no")),
  ], "handlers receive exactly the business input, once each");
  assert.deepEqual((await fixture.pool.query("SELECT id FROM action_e2e_entry WHERE id LIKE 'dart-entry-%' ORDER BY id")).rows, [{ id: "dart-entry-ok" }]);
});

// ---- DateTime precision and omitted optional operands (#189) ----

test("Dart microsecond DateTimes cross the backend at UTC millisecond precision, and an omitted operand is null", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-dart-datetime-"));
  const notesBefore = fixture.notes.length;
  const restampsBefore = fixture.restamps.length;
  let expected: { note: string; created: string; moved: string; at: string };
  try {
    const root = process.cwd();
    const { stdout, stderr } = await execFileAsync("dart", [
      "run", "action_e2e_datetime.dart", url, join(directory, "client.sqlite"),
      join(root, `target/debug/libaxton_dart.${process.platform === "darwin" ? "dylib" : "so"}`),
    ], { cwd: join(root, "integration/action-runtime-dart"), timeout: 30_000 });
    assert.match(stdout, /Dart DateTime precision and omitted operand: passed/, stderr);
    expected = JSON.parse(stdout.trim().split("\n").at(-1)!);
  } finally { await rm(directory, { recursive: true, force: true }); }
  assert.equal(expected.created, "2026-09-28T12:34:56.789Z");
  assert.equal(expected.at, "2026-09-28T16:00:00.002Z");
  const created = fixture.notes.slice(notesBefore);
  assert.equal(created.length, 1);
  assert.equal(created[0]!.createdAt.toISOString(), expected.created, "the AddNote handler received the truncated instant");
  const restamps = fixture.restamps.slice(restampsBefore);
  assert.deepEqual(restamps.map(({ note, at }) => ({ note: note && { id: note.id, createdAt: note.createdAt?.toISOString() }, at: at.toISOString() })), [
    { note: null, at: expected.at },
    { note: null, at: expected.at },
    { note: { id: expected.note, createdAt: expected.moved }, at: expected.at },
    { note: { id: expected.note, createdAt: expected.created }, at: expected.at },
    { note: null, at: expected.at },
  ], "queued calls in order, then the direct and the online durable call");
  assert.deepEqual(restamps[0], restamps[1], "an omitted operand and an explicit null reach the handler identically");
  assert.deepEqual((await fixture.pool.query("SELECT created_at FROM action_e2e_note WHERE id=$1", [expected.note])).rows, [{ created_at: expected.created }]);
});

test("TypeScript: an omitted optional operand records and reaches the handler exactly as null", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-omitted-"));
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite") });
    const at = new Date("2026-09-28T17:00:00.003Z");
    await client.mutations.restamp({ at });
    await client.mutations.restamp({ note: null, at });
    const queued = await client.readSql("SELECT args FROM axton_mutation ORDER BY ordinal");
    assert.equal(queued.length, 2);
    assert.equal(queued[0]!.args, queued[1]!.args, "omitted and null record identical args");
    assert.deepEqual(JSON.parse(String(queued[0]!.args)), { note: null, at: at.toISOString() });
    const before = fixture.restamps.length;
    await client.connect(server());
    await wait(async () => (await client!.syncState()).pending === 0, "Restamp calls drained");
    assert.deepEqual(fixture.restamps.slice(before), [{ note: null, at }, { note: null, at }], "the handler receives null both times");
  } finally { await client?.close(); await rm(directory, { recursive: true, force: true }); }
});
