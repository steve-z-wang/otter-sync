import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import { createExample } from "./fixtures/round-trip/server.mts";
import { GeneratedClient } from "./fixtures/round-trip/generated/client.ts";
import {
  createProxy,
  connection,
  wait,
  mutation,
  intent,
} from "./protocol-fixture.mjs";

async function fixture() {
  const app = await createExample();
  await app.initialize();
  const server = await app.listen(0);
  const proxy = await createProxy(server.url);
  const dir = await mkdtemp(join(tmpdir(), "axton-round-"));
  const path = join(dir, "db");
  let client = await GeneratedClient.open({
    path,
    stream: "User:demo-user",
    connection: connection(proxy.url),
  });
  return {
    app,
    proxy,
    dir,
    path,
    get client() {
      return client;
    },
    async reopen() {
      await client.close();
      client = await GeneratedClient.open({
        path,
        stream: "User:demo-user",
        connection: connection(proxy.url),
      });
    },
    async close() {
      await client.close();
      await proxy.close();
      await app.close();
      await rm(dir, { recursive: true, force: true });
    },
  };
}

test(
  "Prisma round trip: offline optimism, durable accepted retry, rejection and local writes during HTTP wait",
  { timeout: 90000 },
  async () => {
    const f = await fixture();
    try {
      await f.client.bootstrap();
      assert.equal(
        (await f.client.models.entry.get({ id: "entry-1" })).text,
        "Hello from the server",
      );
      await f.client.connection.pause();
      await f.client.mutations.editEntry({
        entry: { id: "entry-1", text: " offline edit " },
      });
      assert.equal(
        (await f.client.models.entry.get({ id: "entry-1" })).text,
        " offline edit ",
      );
      assert.equal((await f.client.syncState()).pending, 1);
      // Freeze is owned by the real sender. Lose the accepted HTTP response, then
      // close/reopen the same file and prove exact durable intent replay.
      const held = f.proxy.holdResponse(mutation);
      await f.reopen();
      await held.arrived;
      const first = f.proxy
        .requests("/sync/actions")
        .find((x) => x.request.name === "EditEntry").request;
      const calls = f.app.handlerCalls;
      f.proxy.down();
      held.release();
      await f.client.close();
      f.proxy.up();
      await f.reopen();
      await wait(
        async () => (await f.client.syncState()).pending === 0,
        "accepted retry",
      );
      const requests = f.proxy
        .requests("/sync/actions")
        .filter((x) => x.request.name === "EditEntry");
      assert.deepEqual(requests.at(-1).request, first);
      assert.equal(f.app.handlerCalls, calls);
      assert.equal(
        (await f.client.models.entry.get({ id: "entry-1" })).text,
        "offline edit",
      );
      const refusal = await f.client.mutations.editEntry({
        entry: { id: "entry-1", text: "reject" },
      });
      assert.equal((await refusal.wait()).error.code, "entry.denied");
      assert.equal(
        (await f.client.models.entry.get({ id: "entry-1" })).text,
        "offline edit",
      );
      const gate = f.proxy.holdResponse(mutation);
      const one = await f.client.mutations.editEntry({
        entry: { id: "entry-1", text: " first " },
      });
      await gate.arrived;
      const two = await f.client.mutations.editEntry(async (tx) => {
        await tx.models.entry.create({
          id: "companion",
          text: "device companion",
          note: null,
        });
        return { entry: { id: "entry-1", text: " final " } };
      });
      assert.equal(
        (await f.client.models.entry.get({ id: "entry-1" })).text,
        " final ",
      );
      gate.release();
      assert.equal((await one.wait()).error, null);
      assert.equal((await two.wait()).error, null);
      assert.equal(
        (await f.client.models.entry.get({ id: "entry-1" })).text,
        "final",
      );
      assert.equal(
        (await f.client.models.entry.get({ id: "companion" })).text,
        "device companion",
      );
    } finally {
      f.proxy.releaseAll();
      await f.close();
    }
  },
);

test(
  "live catch-up spans bounded pages, commits watchers, preserves offline dependent edits and Dart delivery",
  { timeout: 90000 },
  async () => {
    const f = await fixture();
    let reader;
    try {
      await f.client.bootstrap();
      reader = await GeneratedClient.open({
        path: join(f.dir, "reader"),
        stream: "User:demo-user",
        connection: connection(f.proxy.url),
      });
      await reader.bootstrap();
      let views = [];
      const stop = reader.models.entry.watch({}, (rows) => views.push(rows));
      await reader.connection.pause();
      const ids = await f.app.publishMany(270, {
        scope: "User:demo-user",
        prefix: "bulk",
        batch: 20,
      });
      await reader.connection.resume();
      await wait(
        async () => (await reader.models.entry.query()).length === 271,
        "all catch-up pages",
      );
      assert.ok(
        views.some((rows) => rows.some((row) => row.id === ids.at(-1))),
      );
      stop();
      await f.client.connection.pause();
      const a = await f.client.mutations.editEntry({
        entry: { id: "entry-1", text: " offline first " },
      });
      const b = await f.client.mutations.editEntry({
        entry: { id: "entry-1", text: " offline last " },
      });
      await f.client.connection.resume();
      assert.equal((await a.wait()).error, null);
      assert.equal((await b.wait()).error, null);
      await wait(
        async () =>
          (await reader.models.entry.get({ id: "entry-1" })).text ===
          "offline last",
        "other-file authority",
      );
      const result = await promisify(execFile)(
        "dart",
        [
          `--packages=${new URL("../../packages/dart/.dart_tool/package_config.json", import.meta.url).pathname}`,
          new URL("./dart_client.dart", import.meta.url).pathname,
          f.proxy.url,
          f.dir,
        ],
        {
          cwd: new URL("../../packages/dart/", import.meta.url).pathname,
          timeout: 30000,
          env: process.env,
        },
      );
      assert.match(result.stdout, /Dart.*passed/);
      assert.equal(
        (await f.app.db.entry.findUnique({ where: { id: "entry-1" } })).text,
        "from Dart",
      );
    } finally {
      await reader?.close();
      await f.close();
    }
  },
);

test(
  "documented CLI uses the bound generated facade and keeps paused edits local",
  { timeout: 60000 },
  async () => {
    const f = await fixture();
    let child;
    let exited;
    let output = "";
    try {
      child = spawn(
        process.execPath,
        [new URL("./fixtures/round-trip/client.mts", import.meta.url).pathname],
        {
          env: {
            ...process.env,
            AXTON_URL: f.proxy.url,
            AXTON_DATABASE: join(f.dir, "cli"),
          },
          stdio: ["pipe", "pipe", "pipe"],
        },
      );
      exited = new Promise((resolve) => child.once("exit", resolve));
      child.stdout.on("data", (chunk) => (output += chunk));
      child.stderr.on("data", (chunk) => (output += chunk));
      await wait(() => output.includes("Commands:"), "CLI ready");
      child.stdin.write("offline\n");
      await wait(() => output.includes("Sync paused"), "pause admitted");
      child.stdin.write("edit  from CLI \nstatus\n");
      await wait(
        () => output.includes("from CLI") && output.includes('\"pending\":1'),
        "offline projection",
      );
      assert.equal(
        (await f.app.db.entry.findUnique({ where: { id: "entry-1" } })).text,
        "Hello from the server",
      );
      child.stdin.write("online\n");
      await wait(
        async () =>
          (await f.app.db.entry.findUnique({ where: { id: "entry-1" } }))
            .text === "from CLI",
        "CLI canonical settlement",
      );
      child.stdin.write("quit\n");
      assert.equal(await exited, 0, output);
    } finally {
      if (child?.exitCode === null) {
        child.kill("SIGKILL");
        await exited;
      }
      await f.close();
    }
  },
);
