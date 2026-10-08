import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createExample } from "./fixtures/round-trip/server.mts";
import { GeneratedClient } from "./fixtures/round-trip/generated/client.ts";
import { createProxy, connection, wait, intent } from "./protocol-fixture.mjs";
async function fixture() {
  const app = await createExample();
  await app.initialize();
  await app.reset();
  const server = await app.listen(0);
  const proxy = await createProxy(server.url);
  const dir = await mkdtemp(join(tmpdir(), "axton-bootstrap-"));
  let client;
  const path = join(dir, "db");
  const open = () =>
    GeneratedClient.open({
      path,
      stream: "User:demo-user",
      connection: connection(proxy.url),
    });
  return {
    app,
    proxy,
    dir,
    path,
    open,
    get client() {
      return client;
    },
    async start() {
      client = await open();
      return client;
    },
    async close() {
      await client?.close();
      proxy.releaseAll();
      await proxy.close();
      await app.close();
      await rm(dir, { recursive: true, force: true });
    },
  };
}
const page = (x) => x.path === "/sync/pull" && intent(x).bootstrap;

test(
  "Bootstrap range covers empty and multi-unit histories",
  { timeout: 90000 },
  async () => {
    const f = await fixture();
    try {
      await f.start();
      await f.client.bootstrap();
      assert.equal((await f.client.models.entry.query()).length, 0);
      await f.client.close();
      const ids = await f.app.publishMany(256, {
        scope: "User:demo-user",
        prefix: "history",
      });
      const other = await GeneratedClient.open({
        path: join(f.dir, "other"),
        stream: "User:demo-user",
        connection: connection(f.proxy.url),
      });
      try {
        await other.bootstrap();
        assert.deepEqual(
          new Set((await other.models.entry.query()).map((x) => x.id)),
          new Set(ids),
        );
        const coverage = await other.readSql(
          "SELECT start_cursor,bootstrap_cursor FROM axton_store",
        );
        assert.equal(coverage[0].bootstrap_cursor, coverage[0].start_cursor);
        assert.equal((await other.syncState()).pending, 0);
      } finally {
        await other.close();
      }
    } finally {
      await f.close();
    }
  },
);

test(
  "moving identity and newer tombstone do not regress behind a held historical page",
  { timeout: 90000 },
  async () => {
    const f = await fixture();
    try {
      await f.app.publishMany(140, {
        scope: "User:demo-user",
        prefix: "moving",
      });
      await f.start();
      const gate = f.proxy.holdResponse(page);
      const bootstrap = f.client.bootstrap();
      bootstrap.catch(() => {});
      await gate.arrived;
      await f.app.publishOne("moving-1", "newer", ["User:demo-user"]);
      await f.app.tombstone("moving-2");
      gate.release();
      await bootstrap;
      // Bootstrap covers its fixed historical start; the later authority
      // arrives through live delivery and must then replace that snapshot.
      await wait(
        async () =>
          (await f.client.models.entry.get({ id: "moving-1" }))?.text === "newer" &&
          (await f.client.models.entry.get({ id: "moving-2" })) === null,
        "newer live state and tombstone after captured Bootstrap",
      );
      assert.equal(
        (await f.client.models.entry.get({ id: "moving-1" })).text,
        "newer",
      );
      assert.equal(await f.client.models.entry.get({ id: "moving-2" }), null);
      assert.equal((await f.client.models.entry.query()).length, 139);
    } finally {
      await f.close();
    }
  },
);

test(
  "interrupted Bootstrap resumes its saved range after reopen and pending named edit completes",
  { timeout: 90000 },
  async () => {
    const f = await fixture();
    let reopened;
    try {
      await f.app.publishMany(140, {
        scope: "User:demo-user",
        prefix: "resume",
      });
      await f.start();
      const gate = f.proxy.holdResponse(page);
      const original = f.client.bootstrap();
      original.catch(() => {});
      await gate.arrived;
      await f.client.close();
      gate.release();
      reopened = await f.open();
      await reopened.bootstrap();
      assert.equal((await reopened.models.entry.query()).length, 140);
      const call = await reopened.mutations.editEntry({
        entry: { id: "resume-1", text: " edited " },
      });
      assert.equal((await call.wait()).error, null);
      assert.equal(
        (await reopened.models.entry.get({ id: "resume-1" })).text,
        "edited",
      );
      const pulls = f.proxy
        .requests("/sync/pull")
        .filter((x) => x.request.bootstrap);
      assert.ok(pulls.length >= 2);
      assert.equal(
        new Set(pulls.map((x) => x.request.through)).size,
        1,
        "reopen retains the original Bootstrap range",
      );
    } finally {
      await reopened?.close();
      await f.close();
    }
  },
);

test(
  "Loader failure leaves Bootstrap coverage incomplete and retry completes its range",
  { timeout: 90000 },
  async () => {
    const f = await fixture();
    try {
      await f.app.publishOne("a-healthy", "a-healthy", ["User:demo-user"]);
      await f.app.publishOne("z-broken", "z-broken", ["User:demo-user"]);
      f.app.failLoads("z-broken");
      await f.start();
      const running = f.client.bootstrap();
      running.catch(() => {});
      await wait(
        () =>
          f.proxy
            .requests("/sync/pull")
            .some((x) => x.request.bootstrap && x.status !== 200),
        "failed Bootstrap range",
      );
      const [state] = await f.client.readSql(
        "SELECT start_cursor,bootstrap_cursor FROM axton_store",
      );
      assert.ok(
        (state.bootstrap_cursor ?? 0) < state.start_cursor,
        "failed authority cannot complete coverage",
      );
      f.app.allowLoads("z-broken");
      await running;
      assert.equal((await f.client.models.entry.query()).length, 2);
    } finally {
      f.app.allowLoads("z-broken");
      await f.close();
    }
  },
);

test(
  "captured Bootstrap start stays fixed under later publications and independent viewer files cover their ranges",
  { timeout: 90000 },
  async () => {
    const f = await fixture();
    let other;
    let hold;
    try {
      await f.app.publishOne("tail-row", "initial", ["User:demo-user"]);
      hold = f.proxy.holdResponse(
        (x) => x.path === "/sync/pull" && intent(x).bootstrap,
      );
      await f.start();
      const running = f.client.bootstrap();
      running.catch(() => {});
      const exchange = await hold.arrived;
      const H = intent(exchange).through;
      assert.ok(Number.isSafeInteger(H));
      await f.app.publishOne("tail-row", "after-tail", ["User:demo-user"]);
      hold.release();
      await running;
      const saved = await f.client.readSql(
        "SELECT start_cursor,bootstrap_cursor FROM axton_store",
      );
      assert.equal(saved[0].start_cursor, H);
      assert.equal(saved[0].bootstrap_cursor, H);
      assert.ok((await f.client.syncState()).cursors["User:demo-user"] >= H);
      await wait(
        async () =>
          (await f.client.models.entry.get({ id: "tail-row" })).text ===
          "after-tail",
        "later live publication",
      );
      other = await GeneratedClient.open({
        path: join(f.dir, "other-viewer"),
        stream: "User:another",
        connection: connection(f.proxy.url, "another"),
      });
      await other.bootstrap();
      assert.equal(
        (await other.models.entry.get({ id: "tail-row" })).text,
        "after-tail",
      );
      assert.deepEqual((await other.syncState()).streams, ["User:another"]);
      assert.equal((await f.client.syncState()).streams.length, 1);
    } finally {
      hold?.release?.();
      await other?.close();
      await f.close();
    }
  },
);
