// Finite history acquisition and named reads on the actual bound native actor.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setImmediate } from "node:timers/promises";
import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import { GeneratedClient } from "./client.ts";
import { createFixture, type Proxy } from "./server.mts";
const fixture = await createFixture();
let proxy: Proxy;
before(async () => {
  await fixture.initialize();
  proxy = await fixture.listen();
});
after(async () => {
  await fixture.close();
});
const connection = (viewer: string) => ({
  url: proxy.url,
  token: viewer,
  identity: { backend: "read-e2e", viewer, contract: "read-v04" },
});
async function wait(
  predicate: () => boolean | Promise<boolean>,
  label: string,
) {
  const end = Date.now() + 20000;
  while (Date.now() < end) {
    if (await predicate()) return;
    await setImmediate();
  }
  throw Error(`Timed out: ${label}`);
}
async function session(viewer: string) {
  const dir = await mkdtemp(join(tmpdir(), "axton-read-"));
  const path = join(dir, "db");
  const open = () =>
    GeneratedClient.open({
      path,
      stream: `User:${viewer}`,
      connection: connection(viewer),
    });
  let client = await open();
  return {
    get client() {
      return client;
    },
    path,
    async reopen() {
      await client.close();
      client = await open();
      return client;
    },
    async close() {
      await client.close();
      await rm(dir, { recursive: true, force: true });
    },
  };
}
const action = (name: string) => (x: { path: string; body: string }) =>
  x.path === "/sync/actions" && JSON.parse(x.body).name === name;

test("Query/Fetch true and false, multi-Model output, absence and no implicit enrollment", async () => {
  const ids = await fixture.seed("reads", 5);
  await fixture.seedTags("reads", ["visible"]);
  const s = await session("read-viewer");
  try {
    const result = await s.client.queries.projectItems(
      { project: "reads" },
      { store: false },
    );
    assert.equal(result.items.length, 5);
    assert.equal(result.tags.length, 1);
    assert.equal(await s.client.models.item.get({ id: ids[0]! }), null);
    assert.deepEqual(await fixture.tracked("User:read-viewer"), []);
    const stored = await s.client.queries.projectItems({ project: "reads" });
    assert.deepEqual(stored, result);
    assert.equal((await s.client.models.item.query()).length, 5);
    assert.equal((await s.client.models.tag.query()).length, 1);
    assert.deepEqual(await fixture.tracked("User:read-viewer"), []);
    await fixture.retitle(ids[0]!, "fresh");
    assert.equal(
      (await s.client.fetch.item({ id: ids[0]! }, { store: false }))?.title,
      "fresh",
    );
    assert.equal(
      (await s.client.models.item.get({ id: ids[0]! }))?.title,
      `${ids[0]} title`,
    );
    await s.client.fetch.item({ id: ids[0]! });
    assert.equal(
      (await s.client.models.item.get({ id: ids[0]! }))?.title,
      "fresh",
    );
    assert.equal(await s.client.fetch.item({ id: "missing" }), null);
  } finally {
    await s.close();
  }
});

test("once Query survives offline reopen; refresh is explicit and a refused refresh cannot return stale success", async () => {
  const ids = await fixture.seed("once", 2);
  const s = await session("once");
  try {
    const first = await s.client.queries.projectItems(
      { project: "once" },
      { store: false, once: true },
    );
    await fixture.retitle(ids[0]!, "changed");
    await s.reopen();
    await s.client.connection!.pause();
    const count = proxy.exchanges.length;
    assert.deepEqual(
      await s.client.queries.projectItems(
        { project: "once" },
        { store: false, once: true },
      ),
      first,
    );
    assert.equal(proxy.exchanges.length, count);
    await s.client.connection!.resume();
    const fresh = await s.client.queries.projectItems(
      { project: "once" },
      { store: false, once: true, refresh: true },
    );
    assert.equal(fresh.items[0]!.title, "changed");
    fixture.failing.add("once");
    await assert.rejects(
      s.client.queries.projectItems(
        { project: "once" },
        { once: true, refresh: true },
      ),
    );
    await assert.rejects(
      s.client.queries.projectItems({ project: "once" }, { once: true }),
    );
    fixture.failing.delete("once");
    assert.equal(
      (
        await s.client.queries.projectItems(
          { project: "once" },
          { once: true, refresh: true },
        )
      ).items.length,
      2,
    );
  } finally {
    fixture.failing.delete("once");
    await s.close();
  }
});

test("finite Bootstrap tracks marked rows; Queries never enroll and later explicit publication reaches a held file", async () => {
  const ids = await fixture.seed("history", 140);
  const s = await session("history");
  try {
    await s.client.bootstrap();
    assert.equal((await s.client.models.item.query()).length, 140);
    assert.equal((await fixture.tracked("User:history")).length, 140);
    await fixture.retitle(ids[0]!, "live");
    await wait(
      async () =>
        (await s.client.models.item.get({ id: ids[0]! }))?.title === "live",
      "post-Bootstrap delta",
    );
    const before = await s.client.syncState();
    await s.reopen();
    assert.deepEqual((await s.client.syncState()).cursors, before.cursors);
    assert.equal((await s.client.models.item.query()).length, 140);
  } finally {
    await s.close();
  }
});

test("delayed ordinary Query snapshot cannot replace installed Stream authority; unrelated Mutation settles", async () => {
  const ids = await fixture.seed("ordering", 2);
  const s = await session("ordering");
  try {
    await s.client.bootstrap();
    const hold = proxy.holdResponse(action("ProjectItems"));
    const reading = s.client.queries.projectItems({ project: "ordering" });
    reading.catch(() => {});
    await hold.arrived;
    const ping = await s.client.mutations.ping({ note: "during-read" });
    assert.equal((await ping.wait()).error, null);
    await fixture.retitle(ids[0]!, "newer authority");
    await wait(
      async () =>
        (await s.client.models.item.get({ id: ids[0]! }))?.title ===
        "newer authority",
      "live authority while read held",
    );
    hold.release();
    assert.equal((await reading).items[0]!.title, `${ids[0]} title`);
    assert.equal(
      (await s.client.models.item.get({ id: ids[0]! }))?.title,
      "newer authority",
    );
  } finally {
    proxy.releaseAll();
    await s.close();
  }
});

test("two bound files independently hold one identity; selected absence is isolated and global authority reaches both", async () => {
  const ids = await fixture.seed("shared", 1);
  const a = await session("shared-a");
  const b = await session("shared-b");
  try {
    await Promise.all([a.client.bootstrap(), b.client.bootstrap()]);
    await fixture.publish(ids, ["User:shared-a", "User:shared-b"]);
    await wait(
      async () =>
        (await a.client.models.item.get({ id: ids[0]! })) !== null &&
        (await b.client.models.item.get({ id: ids[0]! })) !== null,
      "both files",
    );
    fixture.absent.add(ids[0]!);
    await fixture.invalidate(ids, ["User:shared-a"]);
    await wait(
      async () => (await a.client.models.item.get({ id: ids[0]! })) === null,
      "selected absence",
    );
    assert.ok(await b.client.models.item.get({ id: ids[0]! }));
    fixture.absent.delete(ids[0]!);
    await fixture.retitle(ids[0]!, "global");
    await wait(
      async () =>
        (await a.client.models.item.get({ id: ids[0]! }))?.title === "global" &&
        (await b.client.models.item.get({ id: ids[0]! }))?.title === "global",
      "global restoration",
    );
    assert.equal((await fixture.tracked("User:shared-a")).length, 1);
    assert.equal((await fixture.tracked("User:shared-b")).length, 1);
  } finally {
    fixture.absent.delete(ids[0]!);
    await a.close();
    await b.close();
  }
});

test("lost accepted Mutation response retries the durable intent after a real process restart", async () => {
  await fixture.seed("crash", 1);
  const dir = await mkdtemp(join(tmpdir(), "axton-read-crash-"));
  const path = join(dir, "db");
  let child: ReturnType<typeof spawn> | undefined;
  try {
    const hold = proxy.holdResponse(action("RenameItem"));
    child = spawn(
      process.execPath,
      [
        "--experimental-strip-types",
        new URL("./child.mts", import.meta.url).pathname,
        proxy.url,
        path,
        "crash",
      ],
      { stdio: ["ignore", "pipe", "pipe"] },
    );
    let errors = "";
    child.stderr!.on("data", (chunk) => (errors += chunk));
    const exited = new Promise((resolve) => child!.once("exit", resolve));
    await hold.arrived;
    child.kill("SIGKILL");
    await exited;
    hold.release();
    const calls = proxy
      .requests("/sync/actions")
      .filter((x) => x.request.name === "RenameItem");
    assert.equal(calls.length, 1);
    const client = await GeneratedClient.open({
      path,
      stream: "User:crash",
      connection: connection("crash"),
    });
    try {
      await wait(
        async () => (await client.syncState()).pending === 0,
        "reopened durable call",
      );
      assert.equal(
        (await client.models.item.get({ id: "crash-1" }))?.title,
        "restarted",
      );
      const replay = proxy
        .requests("/sync/actions")
        .filter((x) => x.request.name === "RenameItem");
      assert.ok(replay.length >= 2, errors);
      assert.equal(replay[0]!.request.callId, replay.at(-1)!.request.callId);
      assert.deepEqual(replay[0]!.request, replay.at(-1)!.request);
    } finally {
      await client.close();
    }
  } finally {
    proxy.releaseAll();
    if (child?.exitCode === null && child.signalCode === null)
      child.kill("SIGKILL");
    await rm(dir, { recursive: true, force: true });
  }
});

test("generated Dart named reads preserve once output and Store policy over the real host", async () => {
  await fixture.seed("dart", 2);
  await fixture.seedTags("dart", ["read-only tag"]);
  const result = await promisify(execFile)(
    process.env.AXTON_DART ?? "dart",
    ["run", "client.dart", proxy.url],
    {
      cwd: new URL(".", import.meta.url).pathname,
      timeout: 30000,
      env: process.env,
    },
  );
  assert.match(result.stdout, /Dart bound reads: PASS/);
});

test("device-only callback companions share the named Call fate, while a failed local transaction sends nothing", async () => {
  const s = await session("atomic");
  try {
    await s.client.transaction(async (tx) => {
      await tx.models.seen.create({ id: "companion", hits: 1 });
    });
    const requests = proxy
      .requests("/sync/actions")
      .filter((x) => x.request.name === "Ping").length;
    await assert.rejects(
      s.client.transaction(async (tx) => {
        await tx.models.seen.update({ id: "companion" }, { hits: 9 });
        throw Error("abort local");
      }),
      /abort local/,
    );
    assert.equal(
      (await s.client.models.seen.get({ id: "companion" }))?.hits,
      1,
    );
    assert.equal(
      proxy.requests("/sync/actions").filter((x) => x.request.name === "Ping")
        .length,
      requests,
    );
    const ok = await s.client.mutations.ping(async (tx) => {
      await tx.models.seen.update({ id: "companion" }, { hits: 2 });
      return { note: "accept companion" };
    });
    assert.equal((await ok.wait()).error, null);
    assert.equal(
      (await s.client.models.seen.get({ id: "companion" }))?.hits,
      2,
    );
    const no = await s.client.mutations.ping(async (tx) => {
      await tx.models.seen.update({ id: "companion" }, { hits: 3 });
      return { note: "reject" };
    });
    assert.equal((await no.wait()).error?.code, "ping.denied");
    assert.equal(
      (await s.client.models.seen.get({ id: "companion" }))?.hits,
      2,
    );
    assert.equal((await s.client.syncState()).pending, 0);
  } finally {
    await s.close();
  }
});

test("explicit once invalidation fences a delayed prior result and Catalog preserves nullable arguments", async () => {
  const ids = await fixture.seed("generation", 1);
  await fixture.seed("shelf:all", 3);
  const s = await session("generation-viewer");
  try {
    const gate = proxy.holdResponse(action("ProjectItems"));
    const old = s.client.queries.projectItems(
      { project: "generation" },
      { store: false, once: true },
    );
    old.catch(() => {});
    await gate.arrived;
    await fixture.retitle(ids[0]!, "new generation");
    await s.client.queries.invalidate.projectItems({ project: "generation" });
    const fresh = await s.client.queries.projectItems(
      { project: "generation" },
      { store: false, once: true },
    );
    assert.equal(fresh.items[0]!.title, "new generation");
    gate.release();
    assert.equal((await old).items[0]!.title, `${ids[0]} title`);
    assert.equal(
      (
        await s.client.queries.projectItems(
          { project: "generation" },
          { store: false, once: true },
        )
      ).items[0]!.title,
      "new generation",
    );
    assert.equal(
      (await s.client.queries.catalog({ shelf: null }, { store: false })).items
        .length,
      3,
    );
    assert.equal((await s.client.models.item.query()).length, 0);
  } finally {
    proxy.releaseAll();
    await s.close();
  }
});

test("Loader failure refuses an entire ordinary read without caching a partial multi-Model result", async () => {
  const ids = await fixture.seed("loader-failure", 2);
  await fixture.seedTags("loader-failure", ["tag"]);
  const s = await session("loader-failure-viewer");
  try {
    fixture.loaderErrors.add(ids[0]!);
    await assert.rejects(
      s.client.queries.projectItems({ project: "loader-failure" }),
    );
    assert.deepEqual(await s.client.models.item.query(), []);
    assert.deepEqual(await s.client.models.tag.query(), []);
    fixture.loaderErrors.delete(ids[0]!);
    assert.equal(
      (await s.client.queries.projectItems({ project: "loader-failure" })).items
        .length,
      2,
    );
    assert.equal((await s.client.models.item.query()).length, 2);
  } finally {
    fixture.loaderErrors.delete(ids[0]!);
    await s.close();
  }
});
