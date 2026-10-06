import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createExample } from "./fixtures/round-trip/server.mts";
import { GeneratedClient } from "./fixtures/round-trip/generated/client.ts";
import { connection, wait } from "./protocol-fixture.mjs";

test(
  "one bound Stream keeps proven coverage across reconnect and reopen; another binding cannot reuse its file",
  { timeout: 60000 },
  async () => {
    const app = await createExample();
    const dir = await mkdtemp(join(tmpdir(), "axton-binding-"));
    let client;
    try {
      await app.initialize();
      const server = await app.listen(0);
      const path = join(dir, "db");
      const open = () =>
        GeneratedClient.open({
          path,
          stream: "User:demo-user",
          connection: connection(server.url),
        });
      client = await open();
      await client.bootstrap();
      const origin = (await client.syncState()).cursors["User:demo-user"];
      assert.ok(origin > 0);
      await client.connection.pause();
      await app.publishOne("outage", "offline", ["User:demo-user"]);
      assert.equal(await client.models.entry.get({ id: "outage" }), null);
      await client.connection.resume();
      await wait(
        async () =>
          (await client.models.entry.get({ id: "outage" }))?.text === "offline",
        "gap filled",
      );
      const coverage = (await client.syncState()).cursors["User:demo-user"];
      assert.ok(coverage > origin);
      await client.close();
      await assert.rejects(
        GeneratedClient.open({
          path,
          stream: "User:other",
          connection: connection(server.url, "other"),
        }),
        /binding|mismatch/,
      );
      client = await open();
      assert.equal(
        (await client.syncState()).cursors["User:demo-user"],
        coverage,
      );
      await app.publishOne("reopen", "live", ["User:demo-user"]);
      await wait(
        async () =>
          (await client.models.entry.get({ id: "reopen" }))?.text === "live",
        "reopen delivery",
      );
      assert.deepEqual((await client.syncState()).streams, ["User:demo-user"]);
    } finally {
      await client?.close();
      await app.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
);
