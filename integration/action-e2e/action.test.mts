import test, { after, before } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GeneratedClient } from "./client.ts";
import { createFixture } from "./backend-fixture.ts";
import { GeneratedClient as EvolvedClient } from "./evolved/client.ts";
import { createBackend as createEvolvedBackend, devAuth, type Mutations as EvolvedMutations, type Queries as EvolvedQueries, type Loaders as EvolvedLoaders } from "./evolved/backend.ts";
import { pg, type PgClient } from "../../packages/postgres/index.mts";

const fixture = await createFixture();
let url: string;
before(async () => { await fixture.initialize(); url = (await fixture.listen()).url; });
after(async () => { await fixture.close(); });
const server = () => ({ url, token: "alice" });
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
/** The Channel's head: the last publication position it allocated. */
const channelHead = async (channel: string) =>
  Number((await fixture.pool.query("SELECT head FROM axton_channel WHERE channel=$1", [channel])).rows[0]?.head ?? 0);
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
    const subscription = await client.scopes.subscribe("todos:demo");
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
    const page = await post("pull", JSON.stringify({ cursors: { "todos:demo": beforePage.cursors["todos:demo"] }, models: { Todo: 1 } }));
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

test("committed response loss replays frozen intent and stored result after a channel page arrives first", async () => {
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
    const subscription = await client.scopes.subscribe("todos:demo");
    await wait(async () => subscription.status.initialization === "ready", "first initialization");
    await client.connection!.pause();
    const origin = (await client.syncState()).cursors["todos:demo"];
    assert.equal(typeof origin, "number");
    await client.mutations.addTodo({ todo: { id: "replay", title: "  saved  " } });
    const frozen = await client.client.freeze();
    assert.ok(frozen);
    const first = await post("mutations", frozen);
    const page = await post("pull", JSON.stringify({ cursors: { "todos:demo": origin }, models: { Todo: 1 } }));
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
  const call = (args: object) => JSON.stringify({ call: { callId: "01890f47-1234-7123-8123-1234567890aa", name: "SearchTodos", version: 2, args }, models: { Todo: 1 } });
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
  const retained = await post("actions", JSON.stringify({ call: { callId: "01890f47-1234-7123-8123-1234567890ab", name: "SearchTodos", version: 1, args: { query: "replayq" } }, models: { Todo: 1 } }));
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

test("explicit extra touches are stamped once and are not caller authority; outputs follow store", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-mutation-store-"));
  let client: GeneratedClient | undefined;
  try {
    await fixture.pool.query("INSERT INTO action_e2e_todo(id,title) VALUES('retitle-a','retitleq a'),('retitle-b','retitleq b')");
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    const direct = await client.mutations.call.retitleTodos({ query: "retitleq", title: "retitleq direct" }, { store: false });
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
    const head = await channelHead("todos:demo");

    // Direct: authority for input A is applied before the call resolves.
    const direct = await client.mutations.call.editAndShow({ todo: { id: "ab-a", title: "  A1  " }, shown: "ab-b" });
    assert.deepEqual(direct, { todo: { id: "ab-b", title: "B" } }, "result.todo is B's Loader snapshot, not the input A");
    assert.equal((await client.models.todo.get({ id: "ab-a" }))?.title, "A1", "local A holds the server's normalized authority");
    assert.equal(await serverStamp("ab-a"), stampA + 1, "A advanced exactly one stamp");
    assert.equal(await serverStamp("ab-b"), stampB, "an output-only read does not stamp B");
    assert.equal(await channelHead("todos:demo"), head + 1, "one position for A on its Channel, none for B");

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

test("a retried A-returns-B call replays its saved result without new stamps or positions on both routes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-action-a-b-retry-"));
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({ path: join(directory, "client.sqlite"), server: server() });
    for (const [id, title] of [["abr-a", "A"], ["abr-b", "B"]]) assert.equal((await (await client.mutations.addTodo({ todo: { id, title } })).wait()).error, null);
    const direct = JSON.stringify({ call: { callId: "01890f47-1234-7123-8123-1234567890c1", name: "EditAndShow", version: 1, args: { todo: { id: "abr-a", title: "A1" }, shown: "abr-b" } }, models: { Todo: 1, Note: 1 } });
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
    const heads = (await fixture.pool.query("SELECT channel, head FROM axton_channel ORDER BY channel")).rows;
    await fixture.pool.query("UPDATE action_e2e_todo SET title='B latest' WHERE id='abr-b'");
    const replayed = await post("mutations", frozen);
    assert.deepEqual(replayed, receipt, "the stored receipt, including result B, is replayed");
    assert.equal(fixture.handlerCalls, handled + 1, "the durable handler ran once");
    assert.equal(await serverStamp("abr-a"), stamped, "no stamp on replay");
    assert.deepEqual((await fixture.pool.query("SELECT channel, head FROM axton_channel ORDER BY channel")).rows, heads, "no position on replay");
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
    const subscription = await reader.scopes.subscribe("notes:demo");
    await wait(async () => subscription.status.initialization === "ready", "the reader's origin");
    // Enrollment outside any handler: adding an absent member delivers its current state.
    await fixture.backend.transaction(async ({ channel }) => { channel("notes:demo").note.add({ id: note }); });
    await wait(async () => (await reader!.models.note.get({ id: note }))?.body === "before", "the enrolled Note");
    const noteStamp = await serverStamp(note, "Note");

    // The initiating caller declares only Todo: its descriptor has no Note.
    const args = (body: string) => ({ todo: { id: "extra-a", title: ` ${body} ` }, note, body });
    const direct = await post("actions", JSON.stringify({ call: { callId: "01890f47-1234-7123-8123-1234567890d1", name: "AnnotateTodo", version: 1, args: args("direct") }, models: { Todo: 1 } }));
    assert.equal(direct.completion.outcome.error ?? null, null, JSON.stringify(direct.completion));
    assert.deepEqual(direct.records.map((record: { model: string; identity: object; state: object }) => [record.model, record.identity, record.state]), [["Todo", { id: "extra-a" }, { title: "direct" }]], "only the input is caller authority");
    await wait(async () => (await reader!.models.note.get({ id: note }))?.body === "direct", "the touched Note on the reader's Channel");
    assert.equal(await serverStamp(note, "Note"), noteStamp + 1);

    const push = JSON.stringify({ clientId: "extra-touch-client", batchSequence: 1, models: { Todo: 1 }, mutations: [{ ordinal: 1, callId: "01890f47-1234-7123-8123-1234567890d2", name: "AnnotateTodo", version: 1, args: args("durable") }] });
    const receipt = await post("mutations", push);
    assert.deepEqual(receipt.rejections ?? [], []);
    assert.deepEqual(receipt.records.map((record: { model: string }) => record.model), ["Todo"], "the receipt carries no Note");
    await wait(async () => (await reader!.models.note.get({ id: note }))?.body === "durable", "the durable touch on the reader's Channel");
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
