// ESM consumers: every public entry point, without any optional peer installed.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import * as server from "@axtonjs/server";
import * as client from "@axtonjs/client";
import * as postgres from "@axtonjs/postgres";
import { pg } from "@axtonjs/postgres/pg";
import { prisma } from "@axtonjs/postgres/prisma";
import native from "@axtonjs/native";

assert.equal(typeof server.createBackend, "function");
assert.equal(typeof server.isRetryableTransactionError, "function");
assert.equal(typeof client.Client.open, "function");
assert.equal(typeof postgres.persistence, "function");
assert.equal(typeof postgres.pg, "function");
assert.equal(typeof postgres.prisma, "function");
assert.equal(typeof pg, "function");
assert.equal(typeof prisma, "function");
assert.equal(typeof native.validateConfig, "function");
const migration = readFileSync(createRequire(import.meta.url).resolve("@axtonjs/postgres/migration.sql"), "utf8");
assert.match(migration, /CREATE TABLE/i);
// The drizzle shim is its own entry point: without drizzle-orm it names what is missing.
await assert.rejects(import("@axtonjs/postgres/drizzle"), (error) => error.code === "ERR_MODULE_NOT_FOUND" && /drizzle-orm/.test(error.message));
console.log("import: server, client, postgres, postgres/{pg,prisma,migration.sql}, native; drizzle needs drizzle-orm");
