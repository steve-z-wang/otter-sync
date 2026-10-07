import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Pool } from "pg";
import { pg, type PgClient } from "../../packages/postgres/index.mts";
import { createBackend, devAuth } from "../action-runtime-dart/backend.ts";

test(
  "generated Dart scalar DateTime/enum/null Mutation and fresh Query codecs survive actual v5 HTTP settlement and reopen",
  { timeout: 120000 },
  async () => {
    const dart = process.env.AXTON_DART;
    assert.ok(dart, "Dart must participate in the maintained e2e gate");
    const admin = new Pool({ connectionString: process.env.DATABASE_URL });
    await admin.query("CREATE SCHEMA dart_codec05");
    await admin.end();
    const pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      options: "-c search_path=dart_codec05",
    });
    await pool.query(
      await readFile(
        new URL("../../packages/postgres/migration.sql", import.meta.url),
        "utf8",
      ),
    );
    const backend = createBackend<PgClient>({
      database: pg(pool),
      authenticate: devAuth(),
      protocol5: {
        authorizeStream: (owner, stream) => stream === `User:${owner}`,
      },
      mutations: {
        echo: async ({ args }) => ({
          result: args.at,
          moods: args.moods,
          maybe: args.maybe,
        }),
        touch: async ({ args }) => ({ stamp: args.note.at }),
        ping: async () => {},
      },
      queries: {
        now: async ({ args }) => ({ at: args.at }),
        notesSince: async () => ({ notes: [], pinned: [] }),
      },
      loaders: { note: async ({ ids }) => ids.map(() => null) },
      bootstrap: async () => {},
    });
    const listener = await backend.listen({ port: 0 });
    try {
      const result = await promisify(execFile)(
        dart,
        ["run", "host.dart", listener.url],
        {
          cwd: new URL("../action-runtime-dart/", import.meta.url).pathname,
          timeout: 60000,
        },
      );
      assert.match(
        result.stdout,
        /Dart generated DateTime\/enum real host: PASS/,
      );
    } finally {
      await listener.close();
      await pool.end();
    }
  },
);
