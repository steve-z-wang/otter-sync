import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createExample } from "./fixtures/round-trip/server.mts";
import { GeneratedClient } from "./fixtures/round-trip/generated/client.ts";
import { connection } from "./protocol-fixture.mjs";

test(
  "Node and Dart agree on typed optimism, acceptance, rejection and device-only content",
  { timeout: 60000 },
  async () => {
    const app = await createExample();
    const dir = await mkdtemp(join(tmpdir(), "axton-parity-"));
    let client;
    try {
      await app.initialize();
      const server = await app.listen(0);
      client = await GeneratedClient.open({
        path: join(dir, "node"),
        stream: "User:demo-user",
        connection: connection(server.url),
      });
      await client.bootstrap();
      const ok = await client.mutations.editEntry({
        entry: { id: "entry-1", text: " parity " },
      });
      assert.equal((await ok.wait()).error, null);
      const no = await client.mutations.editEntry({
        entry: { id: "entry-1", text: "reject" },
      });
      assert.equal((await no.wait()).error.code, "entry.denied");
      await client.transaction(async (tx) => {
        await tx.models.entry.create({
          id: "local",
          text: "device-only",
          note: null,
        });
      });
      const expected = (await client.models.entry.query())
        .map(({ id, text, note }) => ({ id, text, note }))
        .sort((a, b) => a.id.localeCompare(b.id));
      const result = await promisify(execFile)(
        "dart",
        [
          `--packages=${new URL("../../packages/frontend/dart/.dart_tool/package_config.json", import.meta.url).pathname}`,
          new URL("./parity_client.dart", import.meta.url).pathname,
          server.url,
          dir,
        ],
        {
          cwd: new URL("../../packages/frontend/dart/", import.meta.url).pathname,
          timeout: 30000,
          env: process.env,
        },
      );
      const output = result.stdout
        .split("\n")
        .find((x) => x.startsWith("PARITY "));
      assert.ok(output, result.stdout);
      assert.deepEqual(JSON.parse(output.slice(7)), expected);
      assert.equal(
        await app.db.entry.findUnique({ where: { id: "local" } }),
        null,
        "direct local writes never reach backend",
      );
    } finally {
      await client?.close();
      await app.close();
      await rm(dir, { recursive: true, force: true });
    }
  },
);
