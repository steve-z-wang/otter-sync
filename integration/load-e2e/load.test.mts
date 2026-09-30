// Native Loads (#173) end to end: generated TypeScript and Dart clients on
// native SQLite, over real HTTP, against the generated backend on disposable
// PostgreSQL. Forced exits kill a real client process and restart it on the
// same SQLite file.
import test, { after, before } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";
import { promisify } from "node:util";
import { GeneratedClient, type GeneratedTransaction, type Item, type ItemIdentity, type StoreChange } from "./client.ts";
import { countSeen } from "./child.mts";
import { createFixture, type LoadRequestItem, type LoadResponseItem, type Proxy } from "./server.mts";

const fixture = await createFixture();
let proxy: Proxy;
before(async () => { await fixture.initialize(); proxy = await fixture.listen(); });
after(async () => { await fixture.close(); });
const server = () => ({ url: proxy.url, token: "alice" });
const here = new URL(".", import.meta.url).pathname;
const execFileAsync = promisify(execFile);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const wait = async (predicate: () => boolean | Promise<boolean>, label: string, timeout = 20_000) => {
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(10);
  }
  throw Error(`Timed out waiting for ${label}`);
};
const scratch = async (name: string) => {
  const directory = await mkdtemp(join(tmpdir(), `axton-load-e2e-${name}-`));
  return { directory, path: join(directory, "client.sqlite"), cleanup: () => rm(directory, { recursive: true, force: true }) };
};
const hooks = { item: countSeen };
/** Each Seen row as `id → committed hook runs`. */
const seen = async (client: GeneratedClient) =>
  Object.fromEntries((await client.models.seen.query()).map((row) => [row.id, row.hits]));
const titles = async (client: GeneratedClient, project: string) =>
  (await client.models.item.query({ where: { project }, orderBy: [{ field: "id", direction: "ascending" }] })).map((item) => `${item.id}:${item.title}`);
/** The runtime's projected status of one job. */
const statusOf = async (client: GeneratedClient, id: string) =>
  (await client.loads.list({ limit: 100 })).find((status) => status.id === id);
/** Every page request of one job through the proxy, in order, with its answer when one was received by the proxy. */
const pagesOf = (loadId: string) =>
  proxy.loads().flatMap((exchange) => exchange.items.filter((item) => item.loadId === loadId).map((item) => ({
    request: item,
    answer: exchange.answers.find((answer) => answer.callId === item.callId),
    dropped: exchange.dropped,
  })));
const handledCalls = (loadId: string) => fixture.handled.filter((page) => page.loadId === loadId).map((page) => page.callId);
/**
 * The continuation each page call of one job handed its handler, one entry per
 * call ID in order. Items of one batch run in concurrent Serializable
 * transactions, so PostgreSQL may abort one and the driver run its handler
 * again (#202). A retry is told apart from a double run by its transaction:
 * exactly one run of each call happened in the transaction that saved it
 * (`axton_call.claim_tx`), and every run received the same continuation.
 */
const handledContinuations = async (loadId: string) => {
  const runs = fixture.handled.filter((handled) => handled.loadId === loadId);
  const callIds = [...new Set(runs.map((run) => run.callId))];
  const saved = new Map((await fixture.pool.query("SELECT call_id, claim_tx::text AS xact FROM axton_call WHERE call_id = ANY($1)", [callIds])).rows.map((row) => [String(row.call_id), String(row.xact)]));
  return callIds.map((callId) => {
    const ofCall = runs.filter((run) => run.callId === callId);
    assert.equal(ofCall.filter((run) => run.xact === saved.get(callId)).length, 1, "exactly one run in the transaction that saved the page; any other was a rolled-back retry");
    for (const run of ofCall) assert.deepEqual(run.continuation, ofCall[0]!.continuation, "a retried run received the same continuation");
    return ofCall[0]!.continuation;
  });
};
const itemIds = (answer: LoadResponseItem | undefined) =>
  answer?.outcome.status === "succeeded" ? answer.outcome.data.items!.map((item) => item.id) : undefined;
const rejectsWith = (code: string) => (error: { code?: string }) => { assert.equal(error.code, code); return true; };
const post = async (body: unknown) => {
  const response = await fetch(`${proxy.url}/sync/loads`, { method: "POST", headers: { authorization: "Bearer alice", "content-type": "application/json" }, body: JSON.stringify(body) });
  assert.equal(response.status, 200);
  return (await response.json()) as { loads: LoadResponseItem[] };
};

// ---- Backend over HTTP: no client SDK involved ----

test("the backend pages both Loads over HTTP: structured state, a final empty page, one failing item and exact replay", async () => {
  await fixture.seed("raw", 5);
  await fixture.seedTags("raw", ["red", "blue"]);
  await fixture.seed("shelf:raw", 5, "raw-shelf");
  fixture.failing.add("raw-closed");
  const id = (n: number) => `01890f47-1234-7123-8123-${n.toString(16).padStart(12, "0")}`;
  let n = 0x5000;
  const page = (name: string, args: Record<string, unknown>, loadId: string, continuation: LoadRequestItem["continuation"]): LoadRequestItem =>
    ({ loadId, callId: id(n++), name, version: 1, args, continuation, models: { Item: 1, Tag: 1 } });
  const project = id(n++), catalog = id(n++), closed = id(n++);
  const first = [page("ProjectItems", { project: "raw" }, project, null), page("Catalog", { shelf: "raw" }, catalog, null), page("ProjectItems", { project: "raw-closed" }, closed, null)];
  const answered = (await post({ loads: first })).loads;
  const by = (loadId: string) => answered.find((answer) => answer.loadId === loadId)!;
  assert.equal(by(closed).outcome.status, "failed");
  assert.deepEqual(by(closed), { loadId: closed, callId: first[2]!.callId, outcome: { status: "failed", error: { code: "project.closed", message: "project.closed" } }, records: [] }, "the failing item fails alone");
  assert.deepEqual(by(project).outcome, {
    status: "succeeded",
    data: { items: [{ id: "raw-1" }, { id: "raw-2" }], tags: [{ id: "raw-tag-1" }, { id: "raw-tag-2" }] },
    next: { state: { after: "raw-2", page: 1, trail: ["raw-1", "raw-2"], meta: { size: 2, nested: { flags: [true, false, null], label: "p1" } } } },
  });
  assert.deepEqual(by(project).records.map((record) => [record.model, record.identity.id, record.state]), [
    ["Item", "raw-1", { project: "raw", title: "raw-1 title" }], ["Item", "raw-2", { project: "raw", title: "raw-2 title" }],
    ["Tag", "raw-tag-1", { label: "red" }], ["Tag", "raw-tag-2", { label: "blue" }],
  ]);
  assert.deepEqual(by(catalog).outcome, { status: "succeeded", data: { items: [{ id: "raw-shelf-1" }, { id: "raw-shelf-2" }] }, next: { state: null } }, "a null state is not the end");

  // Follow each job to its end, one page per request, exactly as answered.
  const follow = async (name: string, args: Record<string, unknown>, loadId: string, next: LoadRequestItem["continuation"]) => {
    const pages: { items: string[]; next: LoadRequestItem["continuation"] }[] = [];
    while (next !== null) {
      const [answer] = (await post({ loads: [page(name, args, loadId, next)] })).loads;
      assert.equal(answer!.outcome.status, "succeeded", JSON.stringify(answer));
      if (answer!.outcome.status !== "succeeded") break;
      next = answer!.outcome.next;
      pages.push({ items: answer!.outcome.data.items!.map((item) => item.id), next });
    }
    return pages;
  };
  const projectPages = await follow("ProjectItems", { project: "raw" }, project, (by(project).outcome as { next: LoadRequestItem["continuation"] }).next);
  assert.deepEqual(projectPages.map((p) => p.items), [["raw-3", "raw-4"], ["raw-5"], []], "the final page is empty and completes");
  assert.equal(projectPages.at(-1)!.next, null);
  assert.deepEqual(await handledContinuations(project), [
    null,
    { state: { after: "raw-2", page: 1, trail: ["raw-1", "raw-2"], meta: { size: 2, nested: { flags: [true, false, null], label: "p1" } } } },
    { state: { after: "raw-4", page: 2, trail: ["raw-1", "raw-2", "raw-3", "raw-4"], meta: { size: 2, nested: { flags: [true, false, null], label: "p2" } } } },
    { state: { after: "raw-5", page: 3, trail: ["raw-1", "raw-2", "raw-3", "raw-4", "raw-5"], meta: { size: 2, nested: { flags: [true, false, null], label: "p3" } } } },
  ], "the handler received each structured state exactly as it answered it");
  const catalogPages = await follow("Catalog", { shelf: "raw" }, catalog, { state: null });
  assert.deepEqual(catalogPages.map((p) => p.items), [["raw-shelf-3", "raw-shelf-4"], ["raw-shelf-5"]]);
  assert.deepEqual(await handledContinuations(catalog), [null, { state: null }, { state: { offset: [4, "items", { big: "9007199254740993" }] } }]);

  // The same page call IDs again: the saved outcomes, with no handler run.
  const handled = fixture.handled.length;
  await fixture.pool.query("UPDATE load_e2e_item SET title='changed' WHERE project='raw'");
  const replayed = (await post({ loads: first })).loads;
  assert.deepEqual(replayed, answered, "each item replays its saved outcome and authority");
  assert.equal(fixture.handled.length, handled, "no handler ran again");
  fixture.failing.delete("raw-closed");
});

// ---- Generated TypeScript client ----

test("two independent multi-page Loads share HTTP batches, store and publish page by page, and one failing job fails alone", async () => {
  const { path, cleanup } = await scratch("batch");
  let client: GeneratedClient | undefined;
  const stops: (() => void)[] = [];
  try {
    const alphaIds = await fixture.seed("alpha", 5);
    await fixture.seedTags("alpha", ["urgent", "later"]);
    await fixture.seed("shelf:beta", 5, "beta");
    fixture.failing.add("gamma");
    // Offline, so all three are ready together when the connection starts.
    client = await GeneratedClient.open({ path, onStore: hooks });
    const alpha = await client.loads.projectItems({ project: "alpha" });
    const beta = await client.loads.catalog({ shelf: "beta" });
    const gamma = await client.loads.projectItems({ project: "gamma" });
    assert.equal(new Set([alpha.id, beta.id, gamma.id]).size, 3, "ordinary starts are independent jobs");
    assert.deepEqual([alpha.status.phase, alpha.status.pages, alpha.status.name, alpha.status.version, alpha.status.error], ["waiting", 0, "ProjectItems", 1, null], "accepted durably offline");
    const statuses: { phase: string; pages: number }[] = [];
    stops.push(alpha.watch((status) => statuses.push({ phase: status.phase, pages: status.pages })));
    const counts: number[] = [];
    stops.push(client.models.item.watch({ where: { project: "alpha" } }, (rows) => counts.push(rows.length)));
    // Page 2 of alpha waits in its handler: page 1 must already be local.
    const second = fixture.holdHandler((page) => page.loadId === alpha.id && page.continuation !== null);
    const exchanges = proxy.loads().length;
    await client.connect(server());

    const held = await second.arrived;
    await wait(() => counts.at(-1) === 2, "the Model watch sees page 1");
    assert.deepEqual(await titles(client, "alpha"), ["alpha-1:alpha-1 title", "alpha-2:alpha-2 title"], "page 1 committed on its own");
    assert.equal((await statusOf(client, alpha.id))?.pages, 1);
    assert.equal((await client.models.tag.get({ id: "alpha-tag-1" }))?.label, "urgent", "the page's second output list stored too");
    assert.deepEqual(held.continuation, { state: { after: "alpha-2", page: 1, trail: ["alpha-1", "alpha-2"], meta: { size: 2, nested: { flags: [true, false, null], label: "p1" } } } });
    second.release();

    await alpha.wait();
    await beta.wait();
    await assert.rejects(gamma.wait(), rejectsWith("project.closed"));
    const [batch] = proxy.loads().slice(exchanges);
    assert.deepEqual(batch!.items.map((item) => item.loadId).sort(), [alpha.id, beta.id, gamma.id].sort(), "one HTTP request carried every ready job");
    assert.deepEqual(batch!.answers.map((answer) => [answer.loadId, answer.outcome.status]).sort(), [[alpha.id, "succeeded"], [beta.id, "succeeded"], [gamma.id, "failed"]].sort(), "its failing item failed alone");
    assert.equal(proxy.loads().slice(exchanges).flatMap((exchange) => exchange.items).length, 4 + 3 + 1, "every page was requested once");

    assert.deepEqual(await statusOf(client, alpha.id), { id: alpha.id, name: "ProjectItems", version: 1, phase: "complete", pages: 4, error: null }, "four pages, the last one empty");
    assert.deepEqual(await statusOf(client, beta.id), { id: beta.id, name: "Catalog", version: 1, phase: "complete", pages: 3, error: null });
    const failed = await statusOf(client, gamma.id);
    assert.equal(failed?.phase, "failed");
    assert.equal(failed?.pages, 0);
    assert.equal(failed?.error?.code, "project.closed");
    assert.deepEqual(itemIds(pagesOf(alpha.id).at(-1)?.answer), [], "the final page named nothing");
    assert.equal((pagesOf(alpha.id).at(-1)?.answer?.outcome as { next: unknown }).next, null);

    assert.deepEqual(await titles(client, "alpha"), alphaIds.map((id) => `${id}:${id} title`));
    assert.equal((await client.models.item.query({ where: { project: "shelf:beta" } })).length, 5);
    assert.deepEqual(await seen(client), Object.fromEntries([...alphaIds, "beta-1", "beta-2", "beta-3", "beta-4", "beta-5"].map((id) => [id, 1])), "onStore ran once per stored Item");
    // Page by page: every committed page published its own snapshot, in order.
    await wait(() => statuses.at(-1)?.phase === "complete", "the final status");
    const pages = statuses.map((status) => status.pages);
    assert.deepEqual([...new Set(pages)], [0, 1, 2, 3, 4], JSON.stringify(statuses));
    assert.ok(pages.every((value, index) => index === 0 || value >= pages[index - 1]!), "pages never go back");
    assert.equal(statuses.findIndex((status) => status.phase === "complete"), statuses.length - 1, "complete only after the last page");
    assert.ok(counts.every((value, index) => index === 0 || value >= counts[index - 1]!), "rows only arrive");
    assert.equal(counts.at(-1), 5);
    assert.deepEqual((await client.loads.list()).slice(0, 3).map((status) => status.id).sort(), [alpha.id, beta.id, gamma.id].sort(), "newest first");
  } finally {
    fixture.failing.delete("gamma");
    for (const stop of stops) stop();
    await client?.close();
    await cleanup();
  }
});

test("a newer live update that arrives before an older Load page keeps the newer row", async () => {
  const reader = await scratch("live-reader");
  const writerPath = await scratch("live-writer");
  let client: GeneratedClient | undefined;
  let writer: GeneratedClient | undefined;
  try {
    // The subscription's origin is its first acknowledged head: it is ready
    // before the writes it must receive live.
    client = await GeneratedClient.open({ path: reader.path, server: server(), onStore: hooks });
    const subscription = await client.scopes.subscribe("items:live");
    await wait(() => subscription.status.initialization === "ready", "the live subscription");
    writer = await GeneratedClient.open({ path: writerPath.path, server: server() });
    await writer.mutations.call.addItem({ item: { id: "live-1", project: "live", title: "v1" } });
    await writer.mutations.call.addItem({ item: { id: "live-2", project: "live", title: "v1" } });
    await wait(async () => (await client!.models.item.get({ id: "live-1" }))?.title === "v1", "the subscribed row");

    // The page's Loader has read live-1 at v1 and waits before answering.
    const loader = fixture.holdLoader((ids) => ids.includes("live-1"));
    const load = await client.loads.projectItems({ project: "live" });
    await loader.arrived;
    await writer.mutations.call.renameItem({ item: { id: "live-1", title: "v2" } });
    await wait(async () => (await client!.models.item.get({ id: "live-1" }))?.title === "v2", "the live update");
    loader.release();
    await load.wait();

    const [first] = pagesOf(load.id);
    const older = first!.answer!.records.find((record) => record.identity.id === "live-1")!;
    assert.equal(older.state.title, "v1", "the page carried the older row");
    const [{ stamp }] = (await fixture.pool.query("SELECT stamp FROM axton_record WHERE model='Item' AND identity_key=$1", [JSON.stringify({ id: "live-1" })])).rows;
    assert.ok(older.stamp < Number(stamp), `the page stamp ${older.stamp} is older than ${stamp}`);
    assert.equal((await client.models.item.get({ id: "live-1" }))?.title, "v2", "the older page did not regress the live row");
    assert.deepEqual(await statusOf(client, load.id), { id: load.id, name: "ProjectItems", version: 1, phase: "complete", pages: 2, error: null }, "the older row was a no-op, not a failure");
  } finally {
    await client?.close();
    await writer?.close();
    await reader.cleanup();
    await writerPath.cleanup();
  }
});

test("an unrelated Mutation settles while a Load batch is delayed", async () => {
  const { path, cleanup } = await scratch("mutation");
  let client: GeneratedClient | undefined;
  try {
    await fixture.seed("slow", 3);
    client = await GeneratedClient.open({ path, server: server(), onStore: hooks });
    const hold = fixture.holdHandler((page) => page.key === "slow");
    const load = await client.loads.projectItems({ project: "slow" });
    await hold.arrived;
    const ping = await client.mutations.ping({ note: "while a Load waits" });
    assert.equal((await ping.wait()).error, null);
    assert.ok(fixture.pings.includes("while a Load waits"));
    assert.equal(pagesOf(load.id)[0]?.answer, undefined, "the Load request is still unanswered");
    assert.equal((await statusOf(client, load.id))?.phase, "loading");
    assert.equal((await statusOf(client, load.id))?.pages, 0);
    hold.release();
    await load.wait();
    assert.equal((await statusOf(client, load.id))?.pages, 3);
    assert.deepEqual(await titles(client, "slow"), ["slow-1:slow-1 title", "slow-2:slow-2 title", "slow-3:slow-3 title"]);
  } finally {
    await client?.close();
    await cleanup();
  }
});

// ---- Forced exit and restart on the same SQLite file ----

type Child = { event(name: string): Promise<Record<string, any>>; exit: Promise<{ code: number | null; signal: NodeJS.Signals | null }>; kill(): void; stderr: string[] };
const child = (...args: string[]): Child => {
  const process_ = spawn(process.execPath, ["--experimental-strip-types", "--no-warnings", join(here, "child.mts"), ...args], { stdio: ["ignore", "pipe", "pipe"] });
  const events: Record<string, any>[] = [];
  const waiting = new Set<() => void>();
  const stderr: string[] = [];
  createInterface({ input: process_.stdout! }).on("line", (line) => { events.push(JSON.parse(line)); for (const wake of [...waiting]) wake(); });
  process_.stderr!.on("data", (chunk) => stderr.push(String(chunk)));
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => process_.on("exit", (code, signal) => { for (const wake of [...waiting]) wake(); resolve({ code, signal }); }));
  let exited = false;
  void exit.then(() => { exited = true; });
  return {
    stderr,
    exit,
    kill: () => { process_.kill("SIGKILL"); },
    event: (name) => new Promise((resolve, reject) => {
      const check = () => {
        const found = events.find((event) => event.event === name);
        const failed = events.find((event) => event.event === "error");
        if (found) { waiting.delete(check); resolve(found); } else if (failed || exited) { waiting.delete(check); reject(Error(`child ended without ${name}: ${JSON.stringify(events)} ${stderr.join("")}`)); }
      };
      waiting.add(check);
      check();
    }),
  };
};
const stampsOf = async (ids: string[]) =>
  (await fixture.pool.query("SELECT identity_key, stamp FROM axton_record WHERE model='Item' AND identity_key = ANY($1::text[]) ORDER BY identity_key", [ids.map((id) => JSON.stringify({ id }))])).rows;

/** After a forced exit and a restart: the page was replayed exactly, and nothing advanced twice. */
const assertReplayed = async (options: { path: string; loadId: string; lost: { request: LoadRequestItem; answer: LoadResponseItem }; ids: string[]; pages: number; since: number }) => {
  const { lost, loadId } = options;
  const resent = proxy.loads().slice(options.since).flatMap((exchange) => exchange.items.map((item) => ({ item, answer: exchange.answers.find((answer) => answer.callId === item.callId) }))).filter(({ item }) => item.loadId === loadId);
  assert.deepEqual(resent[0]?.item, lost.request, "the restarted client resent the frozen page: same call ID, continuation and contracts");
  assert.deepEqual(resent[0]?.answer, lost.answer, "and the backend replayed its exact saved outcome");
  const calls = handledCalls(loadId);
  assert.equal(calls.length, options.pages, "each page ran its handler once");
  assert.equal(new Set(calls).size, options.pages);
  assert.equal(calls.filter((call) => call === lost.request.callId).length, 1, "the replayed page did not run again");
  const reopened = await GeneratedClient.open({ path: options.path });
  try {
    assert.deepEqual(await statusOf(reopened, loadId), { id: loadId, name: "ProjectItems", version: 1, phase: "complete", pages: options.pages, error: null }, "progress counted each page once");
    assert.deepEqual(await seen(reopened), Object.fromEntries(options.ids.map((id) => [id, 1])), "each onStore write committed once");
    assert.equal((await reopened.models.item.query({ where: { project: lost.request.args.project as string } })).length, options.ids.length);
  } finally { await reopened.close(); }
};

test("a client killed while applying a page the backend committed replays that exact page after restart", async () => {
  const { path, cleanup } = await scratch("killed-applying");
  try {
    const ids = await fixture.seed("crash", 5);
    const first = child("start", proxy.url, path, "crash", "crash-3");
    const { loadId } = await first.event("started");
    const applying = await first.event("applying");
    assert.deepEqual(applying.ids, ["crash-3", "crash-4"], "killed inside page 2's onStore, after its Seen writes");
    assert.equal((await first.exit).signal, "SIGKILL");
    const lost = pagesOf(loadId).find((page) => itemIds(page.answer)?.includes("crash-3"))!;
    assert.equal(lost.answer!.outcome.status, "succeeded", "the backend committed page 2 and the client received it");
    // Pages 1 and 2 are stamped; page 3 is not requested yet.
    const stamps = await stampsOf(ids.slice(0, 4));
    const since = proxy.loads().length;

    const second = child("resume", proxy.url, path, loadId);
    const resumed = await second.event("resumed");
    assert.equal(resumed.status.pages, 1, "only page 1 had committed locally");
    const done = await second.event("complete");
    assert.equal(done.status.phase, "complete");
    assert.equal((await second.exit).code, 0, second.stderr.join(""));
    await assertReplayed({ path, loadId, lost: { request: lost.request, answer: lost.answer! }, ids, pages: 4, since });
    assert.deepEqual(await stampsOf(ids.slice(0, 4)), stamps, "the replay allocated no stamp");
  } finally { await cleanup(); }
});

test("a client killed after the backend committed a page but before its response arrived replays it after restart", async () => {
  const { path, cleanup } = await scratch("killed-in-flight");
  try {
    const ids = await fixture.seed("inflight", 3);
    const held = proxy.holdResponse((exchange) => exchange.path === "/sync/loads" && (JSON.parse(exchange.body).loads as LoadRequestItem[]).some((item) => item.args.project === "inflight" && item.continuation !== null));
    const first = child("start", proxy.url, path, "inflight");
    const { loadId } = await first.event("started");
    const exchange = await held.arrived;
    const answer = (JSON.parse(exchange.response!).loads as LoadResponseItem[]).find((item) => item.loadId === loadId)!;
    assert.equal(answer.outcome.status, "succeeded", "the backend committed page 2");
    first.kill();
    assert.equal((await first.exit).signal, "SIGKILL");
    await held.clientGone;
    held.release();
    await proxy.until(() => exchange.dropped === "response", "the undelivered response");
    const request = (JSON.parse(exchange.body).loads as LoadRequestItem[]).find((item) => item.loadId === loadId)!;
    const since = proxy.loads().length;

    const second = child("resume", proxy.url, path, loadId);
    assert.equal((await second.event("resumed")).status.pages, 1);
    assert.equal((await second.event("complete")).status.phase, "complete");
    assert.equal((await second.exit).code, 0, second.stderr.join(""));
    await assertReplayed({ path, loadId, lost: { request, answer }, ids, pages: 3, since });
  } finally { await cleanup(); }
});

test("a hook failure rolls the whole page back; an explicit retry reads again from the committed continuation with a new call ID", async () => {
  const { path, cleanup } = await scratch("hook-retry");
  let client: GeneratedClient | undefined;
  try {
    const ids = await fixture.seed("hooked", 5);
    let refuse = true;
    client = await GeneratedClient.open({
      path, server: server(),
      onStore: {
        async item(tx: GeneratedTransaction, changes: readonly StoreChange<ItemIdentity, Item>[]) {
          await countSeen(tx, changes);
          if (refuse && changes.some((change) => change.identity.id === "hooked-3")) throw Error("hooked-3 refused");
        },
      },
    });
    const load = await client.loads.projectItems({ project: "hooked" });
    await assert.rejects(load.wait(), rejectsWith("load.hook_failed"));
    const failed = await statusOf(client, load.id);
    assert.equal(failed?.phase, "failed");
    assert.equal(failed?.pages, 1, "page 1 stays committed");
    assert.equal(failed?.error?.code, "load.hook_failed");
    assert.deepEqual(await titles(client, "hooked"), ["hooked-1:hooked-1 title", "hooked-2:hooked-2 title"], "no row of page 2 was kept");
    assert.deepEqual(await seen(client), { "hooked-1": 1, "hooked-2": 1 }, "page 2's hook writes rolled back with it");
    const refused = pagesOf(load.id)[1]!;
    assert.equal(refused.answer?.outcome.status, "succeeded", "the backend had answered page 2");

    refuse = false;
    const requests = pagesOf(load.id).length;
    await load.retry();
    await load.wait();
    const retried = pagesOf(load.id).slice(requests);
    assert.notEqual(retried[0]!.request.callId, refused.request.callId, "the retry is a new page call");
    assert.deepEqual(retried[0]!.request.continuation, refused.request.continuation, "read again from the committed continuation");
    assert.equal(pagesOf(load.id).filter((page) => page.request.callId === refused.request.callId).length, 1, "the refused page ID was never resent");
    assert.equal(handledCalls(load.id).filter((call) => call === retried[0]!.request.callId).length, 1, "the backend ran the new call");
    assert.deepEqual(await statusOf(client, load.id), { id: load.id, name: "ProjectItems", version: 1, phase: "complete", pages: 4, error: null }, "committed pages were kept and counted once");
    assert.deepEqual(await seen(client), Object.fromEntries(ids.map((id) => [id, 1])));
  } finally {
    await client?.close();
    await cleanup();
  }
});

test("a network cut loses a committed page's response; after reconnect the same call ID replays it", async () => {
  const { path, cleanup } = await scratch("network");
  let client: GeneratedClient | undefined;
  try {
    const ids = await fixture.seed("net", 3);
    client = await GeneratedClient.open({ path, server: server(), onStore: hooks });
    proxy.dropResponseAndGoDown((exchange) => exchange.path === "/sync/loads" && (JSON.parse(exchange.body).loads as LoadRequestItem[]).some((item) => item.args.project === "net" && item.continuation !== null));
    const load = await client.loads.projectItems({ project: "net" });
    const lost = () => pagesOf(load.id).find((page) => page.dropped === "response");
    await proxy.until(() => lost() !== undefined, "the lost response");
    const { request } = lost()!;
    await wait(async () => (await statusOf(client!, load.id))?.phase === "waiting", "waiting while offline");
    // A resend while the network is still down reaches no backend.
    await proxy.until(() => pagesOf(load.id).some((page) => page.dropped === "request"), "a resend during the outage", 10_000);
    proxy.up();
    await load.wait();
    const attempts = pagesOf(load.id).filter((page) => page.request.callId === request.callId);
    assert.deepEqual(attempts.map((page) => page.dropped ?? "delivered"), ["response", "request", "delivered"]);
    for (const attempt of attempts) assert.deepEqual(attempt.request, request, "every attempt resent the same frozen page");
    const lostResponse = proxy.loads().find((exchange) => exchange.dropped === "response" && exchange.items.some((item) => item.callId === request.callId))!.answers.find((answer) => answer.callId === request.callId);
    assert.deepEqual(attempts.at(-1)!.answer, lostResponse, "the delivered page is the saved one");
    assert.equal(handledCalls(load.id).filter((call) => call === request.callId).length, 1, "the handler ran once for it");
    assert.deepEqual(await statusOf(client, load.id), { id: load.id, name: "ProjectItems", version: 1, phase: "complete", pages: 3, error: null });
    assert.deepEqual(await seen(client), Object.fromEntries(ids.map((id) => [id, 1])));
  } finally {
    proxy.up();
    await client?.close();
    await cleanup();
  }
});

// ---- Once reuse, refresh and invalidation ----

test("a completed once Load is reused offline after reopen with no request, Model change or onStore", async () => {
  const { path, cleanup } = await scratch("once-offline");
  let client: GeneratedClient | undefined;
  try {
    const ids = await fixture.seed("once", 3);
    let hookRuns = 0;
    const counting = { async item(tx: GeneratedTransaction, changes: readonly StoreChange<ItemIdentity, Item>[]) { hookRuns++; await countSeen(tx, changes); } };
    client = await GeneratedClient.open({ path, server: server(), onStore: counting });
    const first = await client.loads.projectItems({ project: "once" }, { once: true });
    const joined = await client.loads.projectItems({ project: "once" }, { once: true });
    assert.equal(joined.id, first.id, "a second once start shares the job");
    await first.wait();
    await joined.wait();
    const ordinary = await client.loads.projectItems({ project: "once" });
    assert.notEqual(ordinary.id, first.id, "an ordinary start is always a new job");
    await ordinary.wait();
    const again = await client.loads.projectItems({ project: "once" }, { once: true });
    assert.equal(again.id, first.id, "an ordinary run never replaces the once mapping");
    await client.close();

    client = await GeneratedClient.open({ path, onStore: counting });
    const before = { requests: proxy.exchanges.length, hooks: hookRuns, seen: await seen(client), rows: await titles(client, "once") };
    assert.deepEqual(before.seen, Object.fromEntries(ids.map((id) => [id, 1])));
    const watched: number[] = [];
    const stop = client.models.item.watch({ where: { project: "once" } }, (rows) => watched.push(rows.length));
    try {
      await wait(() => watched.length === 1, "the initial Model snapshot");
      const hit = await client.loads.projectItems({ project: "once" }, { once: true });
      assert.equal(hit.id, first.id, "the completed job, offline");
      assert.equal(hit.status.phase, "complete");
      assert.equal(hit.status.pages, 3);
      await hit.wait();
      await sleep(100);
      assert.equal(proxy.exchanges.length, before.requests, "no request");
      assert.equal(hookRuns, before.hooks, "onStore did not run");
      assert.deepEqual(await seen(client), before.seen);
      assert.deepEqual(await titles(client, "once"), before.rows);
      assert.deepEqual(watched, [3], "no Model watcher woke");
      assert.deepEqual(await statusOf(client, first.id), { id: first.id, name: "ProjectItems", version: 1, phase: "complete", pages: 3, error: null }, "the hit added no page");
    } finally { stop(); }
  } finally {
    await client?.close();
    await cleanup();
  }
});

test("invalidating during a blocked once request: a new once job and reverse completion leave the newer mapping", async () => {
  const { path, cleanup } = await scratch("once-invalidate");
  let client: GeneratedClient | undefined;
  try {
    await fixture.seed("inv", 3);
    client = await GeneratedClient.open({ path, server: server(), onStore: hooks });
    const older = fixture.holdHandler((page) => page.key === "inv");
    const first = await client.loads.projectItems({ project: "inv" }, { once: true });
    assert.equal((await older.arrived).loadId, first.id);
    const requests = proxy.exchanges.length;
    await client.loads.invalidate.projectItems({ project: "inv" });
    assert.equal(proxy.exchanges.length, requests, "invalidation is local");
    assert.equal((await statusOf(client, first.id))?.phase, "loading", "and cancels nothing");

    const newer = fixture.holdHandler((page) => page.key === "inv" && page.loadId !== first.id);
    const second = await client.loads.projectItems({ project: "inv" }, { once: true });
    assert.notEqual(second.id, first.id, "a once start after invalidation creates a new job");
    assert.equal((await newer.arrived).loadId, second.id);
    // Finish the newer job first, then the older one.
    newer.release();
    await second.wait();
    assert.equal((await statusOf(client, first.id))?.pages, 0, "the older job is still blocked");
    older.release();
    await first.wait();
    assert.equal((await statusOf(client, first.id))?.phase, "complete", "the invalidated job still completed and stored");

    const before = proxy.exchanges.length;
    const reused = await client.loads.projectItems({ project: "inv" }, { once: true });
    assert.equal(reused.id, second.id, "the older completion did not restore its mapping");
    assert.equal(reused.status.phase, "complete");
    assert.equal(proxy.exchanges.length, before);
  } finally {
    await client?.close();
    await cleanup();
  }
});

test("a failed refresh stays failed for once callers, never the earlier completion, until retried", async () => {
  const { path, cleanup } = await scratch("once-refresh");
  let client: GeneratedClient | undefined;
  try {
    const ids = await fixture.seed("rf", 3);
    let hookRuns = 0;
    client = await GeneratedClient.open({ path, server: server(), onStore: { async item(tx: GeneratedTransaction, changes: readonly StoreChange<ItemIdentity, Item>[]) { hookRuns++; await countSeen(tx, changes); } } });
    await assert.rejects(async () => client!.loads.projectItems({ project: "rf" }, { refresh: true }), rejectsWith("load.invalid_options"));
    const done = await client.loads.projectItems({ project: "rf" }, { once: true });
    await done.wait();
    fixture.failing.add("rf");
    const refreshed = await client.loads.projectItems({ project: "rf" }, { once: true, refresh: true });
    assert.notEqual(refreshed.id, done.id, "refresh of a complete job starts a new one");
    await assert.rejects(refreshed.wait(), rejectsWith("project.closed"));

    const requests = proxy.exchanges.length;
    const hooksBefore = hookRuns;
    const reused = await client.loads.projectItems({ project: "rf" }, { once: true });
    assert.equal(reused.id, refreshed.id, "once now names the failed refresh");
    assert.equal(reused.status.phase, "failed");
    assert.equal(reused.status.error?.code, "project.closed");
    await assert.rejects(reused.wait(), rejectsWith("project.closed"));
    assert.equal(proxy.exchanges.length, requests, "a failed hit sends no hidden retry");
    assert.equal(hookRuns, hooksBefore);
    assert.equal((await statusOf(client, done.id))?.phase, "complete", "the earlier job keeps its own completion");
    const reattached = await client.loads.get(done.id);
    assert.equal(reattached?.status.phase, "complete");
    reattached?.dispose();

    fixture.failing.delete("rf");
    await reused.retry();
    await reused.wait();
    const hit = await client.loads.projectItems({ project: "rf" }, { once: true });
    assert.equal(hit.id, refreshed.id);
    assert.equal(hit.status.phase, "complete");
    assert.deepEqual(await seen(client), Object.fromEntries(ids.map((id) => [id, 1])), "equal-stamp rows of the refresh ran no hook");
  } finally {
    fixture.failing.delete("rf");
    await client?.close();
    await cleanup();
  }
});

// ---- Channel enrollment from a Load ----
// Items seeded here bypass every Mutation, so they belong to no Channel until
// a Load that `fixture.enrolling` names adds the records it returns to
// `items:${project}`. Each reader subscribes first and waits for the
// subscription's persisted initialization before loading: `subscribe()` alone
// is local intent, and the first handshake is the gap-free boundary.

/** A client subscribed to `channel`, once its first handshake is persisted. */
const subscribed = async (name: string, channel: string) => {
  const directory = await scratch(name);
  const client = await GeneratedClient.open({ path: directory.path, server: server(), onStore: hooks });
  const subscription = await client.scopes.subscribe(channel);
  await wait(() => subscription.status.initialization === "ready", `${channel} initialized`);
  return { client, cleanup: async () => { await client.close(); await directory.cleanup(); } };
};
const titleOf = async (client: GeneratedClient, id: string) => (await client.models.item.get({ id }))?.title;
/** Exchanges that could carry records other than a Channel's: Loads and Fetches. */
const reads = () => proxy.exchanges.filter((exchange) => exchange.path === "/sync/loads" || exchange.path === "/sync/fetch").length;

test("a Load enrolls the records it returns; a later touch or Mutation reaches the subscribed client through its Channel with no second add", async () => {
  const ids = await fixture.seed("enr", 3);
  const [tag] = await fixture.seedTags("enr", ["red"]);
  fixture.enrolling.add("enr");
  const reader = await subscribed("enroll", "items:enr");
  const writerPath = await scratch("enroll-writer");
  let writer: GeneratedClient | undefined;
  const snapshots: string[][] = [];
  let stop = () => {};
  try {
    const { client } = reader;
    stop = client.models.item.watch({ where: { project: "enr" } }, (rows) => snapshots.push(rows.map((row) => `${row.id}:${row.title}`).sort()));
    const load = await client.loads.projectItems({ project: "enr" });
    await load.wait();
    assert.deepEqual(await titles(client, "enr"), ids.map((id) => `${id}:${id} title`));
    const before = reads();

    await fixture.retitle("enr-2", "touched");
    await wait(() => snapshots.at(-1)?.includes("enr-2:touched") === true, "the touch through the Channel and the Model watch");
    writer = await GeneratedClient.open({ path: writerPath.path, server: server() });
    await writer.mutations.call.renameItem({ item: { id: "enr-3", title: "renamed" } });
    await wait(() => snapshots.at(-1)?.includes("enr-3:renamed") === true, "the Mutation's inferred change through the Channel");
    await fixture.relabel(tag!, "blue");
    await wait(async () => (await client.models.tag.get({ id: tag! }))?.label === "blue", "the Tag the mixed-list add enrolled");
    assert.equal(reads(), before, "no Load or Fetch carried them: only the Channel");
    assert.deepEqual(snapshots.at(-1), ["enr-1:enr-1 title", "enr-2:touched", "enr-3:renamed"]);
    assert.deepEqual(await seen(client), { "enr-1": 1, "enr-2": 2, "enr-3": 2 }, "a record the page and its enrollment both delivered was stored once; each later change once more");
  } finally {
    fixture.enrolling.delete("enr");
    stop();
    await writer?.close();
    await reader.cleanup();
    await writerPath.cleanup();
  }
});

test("native Load enrollment releases live content durably and a second Channel hold prevents eviction", async () => {
  const directory = await scratch("load-release");
  await fixture.seed("release", 2);
  fixture.enrolling.add("release");
  let client = await GeneratedClient.open({ path: directory.path, server: server() });
  try {
    const first = await client.scopes.subscribe("items:release");
    const second = await client.scopes.subscribe("items:release-other");
    await wait(() => first.status.initialization === "ready" && second.status.initialization === "ready", "both Channels initialized");
    await (await client.loads.projectItems({ project: "release" })).wait();
    await fixture.membership("release-2", "items:release-other", true);
    await wait(async () => (await client.readSql("SELECT present FROM axton_channel_member WHERE channel=? AND model='Item'", ["items:release-other"]))?.length === 1, "the second hold persisted");
    await fixture.membership("release-1", "items:release", false);
    await fixture.membership("release-2", "items:release", false);
    await wait(async () => (await client.models.item.get({ id: "release-1" })) === null, "live release evicts without an application hook");
    assert.ok(await client.models.item.get({ id: "release-2" }), "second Channel keeps content");
    await client.close();
    client = await GeneratedClient.open({ path: directory.path });
    assert.equal(await client.models.item.get({ id: "release-1" }), null, "release persists across offline reopen");
    assert.ok(await client.models.item.get({ id: "release-2" }), "second hold persists offline");
  } finally {
    fixture.enrolling.delete("release");
    await client.close();
    await directory.cleanup();
  }
});

test("a newer Channel update of an enrolled record arrives before its held Load page: no regression, duplicates are harmless and a pending edit stays", async () => {
  const ids = await fixture.seed("gate", 2);
  fixture.enrolling.add("gate");
  const reader = await subscribed("gate", "items:gate");
  const writerPath = await scratch("gate-writer");
  let writer: GeneratedClient | undefined;
  try {
    const { client } = reader;
    // The backend commits page 1 and its enrollment; the client does not get it yet.
    const held = proxy.holdResponse((exchange) => exchange.path === "/sync/loads" && (JSON.parse(exchange.body).loads as LoadRequestItem[]).some((item) => item.args.project === "gate" && item.continuation === null));
    const load = await client.loads.projectItems({ project: "gate" });
    const exchange = await held.arrived;
    writer = await GeneratedClient.open({ path: writerPath.path, server: server() });
    await writer.mutations.call.renameItem({ item: { id: "gate-1", title: "v2" } });
    await wait(async () => (await titleOf(client, "gate-1")) === "v2", "the newer update through the Channel, before the page");
    assert.equal((await statusOf(client, load.id))?.pages, 0, "the page is still held");

    // A pending edit of the other record, its Mutation request held before the backend sees it.
    const pushed = proxy.holdRequest((candidate) => candidate.path === "/sync/mutations" && candidate.body.includes('"mine"'));
    const mine = await client.mutations.renameItem({ item: { id: "gate-2", title: "mine" } });
    await pushed.arrived;
    await wait(async () => (await titleOf(client, "gate-2")) === "mine", "the optimistic edit");

    held.release();
    await load.wait();
    const answer = (JSON.parse(exchange.response!).loads as LoadResponseItem[]).find((item) => item.loadId === load.id)!;
    const older = answer.records.find((record) => record.identity.id === "gate-1")!;
    assert.equal(older.state.title, "gate-1 title", "the page carried the older row");
    const [current] = await stampsOf(["gate-1"]);
    assert.ok(older.stamp < Number(current.stamp), `the page stamp ${older.stamp} is older than ${current.stamp}`);
    assert.equal(await titleOf(client, "gate-1"), "v2", "the older page did not regress the Channel's newer row");
    assert.equal(await titleOf(client, "gate-2"), "mine", "the pending edit is still replayed over the page and the Channel");
    assert.equal((await seen(client))["gate-2"], 1, "the page and the Channel delivered gate-2 at one stamp: stored once");

    pushed.release();
    assert.equal((await mine.wait()).error, null);
    assert.equal(await titleOf(client, "gate-2"), "mine", "and the backend accepted it");

    // A fresh traversal adds the same members again: no stamp moves and nothing is published.
    const stamps = await stampsOf(ids);
    const head = await fixture.head("items:gate");
    const again = await client.loads.projectItems({ project: "gate" });
    await again.wait();
    assert.notEqual(again.id, load.id);
    assert.ok(handledCalls(again.id).length > 0, "its pages ran the handler, which added both records again");
    assert.deepEqual(await stampsOf(ids), stamps, "re-adding existing members advanced no stamp");
    assert.equal(await fixture.head("items:gate"), head, "and published nothing");
    assert.deepEqual(await titles(client, "gate"), ["gate-1:v2", "gate-2:mine"]);
  } finally {
    fixture.enrolling.delete("gate");
    await writer?.close();
    await reader.cleanup();
    await writerPath.cleanup();
  }
});

test("a reused once Load enrolls nothing; a fresh traversal establishes the membership it missed", async () => {
  await fixture.seed("old", 2);
  const reader = await subscribed("once-enroll", "items:old");
  try {
    const { client } = reader;
    // Completed by a handler that did not enroll yet.
    const first = await client.loads.projectItems({ project: "old" }, { once: true });
    await first.wait();
    fixture.enrolling.add("old");
    const requests = proxy.exchanges.length;
    const runs = fixture.handled.length;
    const hit = await client.loads.projectItems({ project: "old" }, { once: true });
    assert.equal(hit.id, first.id, "the completed job");
    await hit.wait();
    assert.equal(proxy.exchanges.length, requests, "no request");
    assert.equal(fixture.handled.length, runs, "no handler ran, so nothing was enrolled");

    // A change to old-1, then a new Channel member: once the member arrives,
    // old-1's change would have too had old-1 been a member.
    await fixture.retitle("old-1", "unseen");
    await fixture.create("old-marker", "old", "items:old");
    await wait(async () => (await titleOf(client, "old-marker")) === "old-marker title", "the later Channel member");
    assert.equal(await titleOf(client, "old-1"), "old-1 title", "the change of a record no Load enrolled did not reach the Channel");

    const refreshed = await client.loads.projectItems({ project: "old" }, { once: true, refresh: true });
    assert.notEqual(refreshed.id, first.id, "refresh is a fresh traversal");
    await refreshed.wait();
    assert.equal(await titleOf(client, "old-1"), "unseen", "its page carried the current row");
    await fixture.retitle("old-1", "followed");
    await wait(async () => (await titleOf(client, "old-1")) === "followed", "a change after the fresh traversal enrolled old-1");
  } finally {
    fixture.enrolling.delete("old");
    await reader.cleanup();
  }
});

test("cancelling and forgetting a Load keep its committed page's membership; records it never returned need their own enrollment", async () => {
  await fixture.seed("cx", 3);
  fixture.enrolling.add("cx");
  const reader = await subscribed("cancel-enroll", "items:cx");
  try {
    const { client } = reader;
    const second = fixture.holdHandler((page) => page.key === "cx" && page.continuation !== null);
    const load = await client.loads.projectItems({ project: "cx" });
    await second.arrived;
    await wait(async () => (await statusOf(client, load.id))?.pages === 1, "page 1 committed on both sides");
    const cancelling = load.cancel();
    // The page 2 request in flight fails, so it enrolls nothing.
    fixture.failing.add("cx");
    second.release();
    await cancelling;
    assert.equal((await statusOf(client, load.id))?.phase, "cancelled");
    await load.forget();
    assert.equal(await client.loads.get(load.id), null);

    await fixture.retitle("cx-3", "unseen");
    await fixture.create("cx-4", "cx");
    await fixture.retitle("cx-1", "after cancel");
    await wait(async () => (await titleOf(client, "cx-1")) === "after cancel", "a member the cancelled Load's committed page enrolled");
    assert.equal(await client.models.item.get({ id: "cx-3" }), null, "no committed page returned cx-3, so it was never enrolled");
    assert.equal(await client.models.item.get({ id: "cx-4" }), null, "a record created later with no add is not enrolled");

    fixture.failing.delete("cx");
    const fresh = await client.loads.projectItems({ project: "cx" });
    await fresh.wait();
    await fixture.retitle("cx-3", "followed");
    await fixture.retitle("cx-4", "followed");
    await wait(async () => (await titleOf(client, "cx-3")) === "followed" && (await titleOf(client, "cx-4")) === "followed", "changes after a fresh Load returned them");
  } finally {
    fixture.failing.delete("cx");
    fixture.enrolling.delete("cx");
    await reader.cleanup();
  }
});

// ---- Generated Dart client ----

test("the generated Dart client pages, reuses a once Load offline and invalidates it", async () => {
  const { path, cleanup } = await scratch("dart");
  try {
    await fixture.seed("dart", 3);
    const root = join(here, "../..");
    const { stdout } = await execFileAsync("dart", [
      "run", "client.dart", proxy.url, path,
      join(root, `target/debug/libaxton_dart.${process.platform === "darwin" ? "dylib" : "so"}`),
    ], { cwd: here, timeout: 60_000 });
    assert.match(stdout, /Dart generated Loads: passed/);
    assert.ok(fixture.handled.some((page) => page.key === "dart"), "the Dart client reached the backend");
  } finally { await cleanup(); }
});

test("the generated Dart client receives a later change to a record its Load enrolled, through the Channel", async () => {
  const { path, cleanup } = await scratch("dart-enroll");
  try {
    await fixture.seed("dart-enr", 3);
    fixture.enrolling.add("dart-enr");
    const root = join(here, "../..");
    const { stdout } = await execFileAsync("dart", [
      "run", "client.dart", proxy.url, path,
      join(root, `target/debug/libaxton_dart.${process.platform === "darwin" ? "dylib" : "so"}`), "enroll",
    ], { cwd: here, timeout: 60_000 });
    assert.match(stdout, /Dart Load enrollment: passed/);
    assert.ok(fixture.handled.some((page) => page.key === "dart-enr"), "the Dart client's Load reached the backend");
  } finally {
    fixture.enrolling.delete("dart-enr");
    await cleanup();
  }
});


test("the generated Dart client releases Load enrollment and reopens offline with a second hold retained", async () => {
  const { path, cleanup } = await scratch("dart-release");
  await fixture.seed("dart-release", 2);
  fixture.enrolling.add("dart-release");
  const root = join(here, "../..");
  const child = spawn("dart", ["run", "client.dart", proxy.url, path,
    join(root, `target/debug/libaxton_dart.${process.platform === "darwin" ? "dylib" : "so"}`), "remove"], { cwd: here });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => { stdout += String(chunk); });
  child.stderr.on("data", (chunk) => { stderr += String(chunk); });
  const exited = new Promise<number | null>((resolve, reject) => { child.once("exit", resolve); child.once("error", reject); });
  try {
    await wait(() => stdout.includes("Dart release: loaded"), "Dart Load stored its enrolled rows");
    await fixture.membership("dart-release-2", "items:dart-release-other", true);
    await wait(() => stdout.includes("Dart release: second held"), "Dart persisted the second hold");
    await fixture.membership("dart-release-1", "items:dart-release", false);
    await fixture.membership("dart-release-2", "items:dart-release", false);
    await wait(() => stdout.includes("Dart Load removal: passed"), `Dart offline removal: ${stderr}`);
    assert.equal(await exited, 0, stderr);
  } finally {
    child.kill();
    fixture.enrolling.delete("dart-release");
    await cleanup();
  }
});
