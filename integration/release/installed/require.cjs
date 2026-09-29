// CommonJS consumers load the ESM packages through Node's require(esm).
const assert = require("node:assert/strict");
const server = require("@axtonjs/server");
const client = require("@axtonjs/client");
const postgres = require("@axtonjs/postgres");
assert.equal(typeof server.createBackend, "function");
assert.equal(typeof server.devAuth, "function");
assert.equal(typeof client.Client.open, "function");
assert.equal(typeof postgres.persistence, "function");
assert.equal(typeof require("@axtonjs/postgres/pg").pg, "function");
assert.equal(typeof require("@axtonjs/postgres/prisma").prisma, "function");
assert.equal(typeof require("@axtonjs/native").validateConfig, "function");
console.log("require(esm): server, client, postgres, postgres/pg, postgres/prisma, native");
