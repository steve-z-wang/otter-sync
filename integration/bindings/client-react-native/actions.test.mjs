// The RN scope and HTTP carrier over the real native runtime. Business
// settlement/publication is checked by the real-backend Action E2E suite.
import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { createServerConnection } from "../../../packages/client-react-native/live.mts";
import {
  openStore,
  offlineNetwork,
  emptyPull as encodeEmptyPull,
  read05,
} from "../client-js/store-fixture.mjs";
import {
  fetchModels,
  makeMutations,
  makeQueries,
  liveModels,
  schema as generatedSchema,
} from "../../action-runtime-ts/generated.ts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const emptyPull = (body) => JSON.parse(encodeEmptyPull(body));
const readReply = read05;

test("throwing mobile diagnostic listener does not stop later native Query completions", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-reports-"));
  let failed = false,
    requests = 0;
  const Client = createClient(native, Transaction, () => ({
    open() {},
    async push(kind, text) {
      const body = JSON.parse(text);
      if (kind === "pull" || kind === "handshake") {
        if (!failed) {
          failed = true;
          throw Error("temporary transport failure");
        }
        return JSON.stringify(emptyPull(body));
      }
      assert.equal(kind, "action");
      requests++;
      return JSON.stringify(readReply(body, null));
    },
  }));
  const schema = {
    models: [],
    enums: [],
    actions: [
      { name: "Ping", version: 1, kind: "query", inputs: [], outputs: [] },
    ],
  };
  const previous = globalThis.reportError,
    diagnostic = Error("mobile listener failed"),
    observed = [],
    reports = [];
  let client;
  globalThis.reportError = (error) => observed.push(error);
  try {
    // Bound open activates transport: register diagnostics before its first pull.
    client = await openStore(Client, {
      path: join(directory, "store"),
      schema,
      connection: {
        url: "http://fixture",
        token: "mobile",
        options: {
          onError: (report) => {
            reports.push(report);
            throw diagnostic;
          },
        },
      },
    });
    await client.bootstrap();
    for (let i = 0; i < 3; i++)
      assert.equal(
        await client.invokeQuery("Ping", 1, {}, () => undefined),
        undefined,
      );
    assert.equal(requests, 3);
    assert.equal(reports.length, 1);
    assert.match(reports[0].message, /temporary transport failure/);
    assert.deepEqual(observed, [diagnostic]);
    assert.equal((await client.syncState()).pending, 0);
  } finally {
    globalThis.reportError = previous;
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("generated mobile Mutation has one durable entry and dropping it settles its Call", async () => {
  const Client = createClient(native, Transaction, offlineNetwork),
    directory = await mkdtemp(join(tmpdir(), "axton-rn-generated-"));
  const client = await openStore(Client, {
    path: join(directory, "store"),
    schema: generatedSchema,
  });
  try {
    const models = liveModels(client);
    await models.todo.create({
      id: "one",
      title: "local",
      at: new Date("2026-01-01T00:00:00Z"),
      status: "open",
      note: null,
    });
    assert.equal((await client.syncState()).pending, 0);
    const mutations = makeMutations(client),
      queries = makeQueries(client);
    assert.equal("call" in mutations, false);
    assert.equal("enqueue" in queries, false);
    const call = await mutations.ping({});
    await client.drop(1);
    assert.equal((await call.wait()).error?.code, "dropped");
    assert.equal(call.status, "failed");
    assert.equal((await models.todo.get({ id: "one" })).title, "local");
    await assert.rejects(
      queries.find({ at: new Date("2026-01-01T00:00:00Z") }),
      { code: "action.unavailable" },
    );
    assert.equal(
      (await client.syncState()).pending,
      0,
      "direct Query does not enter the Mutation queue",
    );
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("generated Fetch and Query use RN HTTP, decode Model identities and choose Store per call", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-read-")),
    seen = [],
    at = new Date("2026-03-04T05:06:07.000Z");
  const server = createServer(async (request, response) => {
    let text = "";
    for await (const chunk of request) text += chunk;
    const body = JSON.parse(text);
    seen.push({
      url: request.url,
      authorization: request.headers.authorization,
      body,
    });
    if (request.url === "/sync/pull" || request.url === "/sync/handshake") {
      response.end(JSON.stringify(emptyPull(body)));
      return;
    }
    const identity = body.invocation.key?.identity ?? { id: "queried" },
      model = body.invocation.key?.model ?? "Todo",
      state =
        model === "Todo"
          ? {
              title: body.invocation.kind === "query" ? "queried" : "remote",
              at: "2026-02-03T04:05:06.000Z",
              status: "closed",
              note: null,
            }
          : { label: "pinned" };
    response.end(
      JSON.stringify(
        readReply(
          body,
          body.invocation.kind === "query"
            ? { todo: { ...identity, ...state } }
            : { ...identity, ...state },
          [{ key: { model, identity }, cursor: null, state }],
        ),
      ),
    );
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  class NoSocket {
    constructor() {
      throw Error("fixture does not provide live sockets");
    }
  }
  const Client = createClient(native, Transaction, (options) =>
    createServerConnection(options, NoSocket),
  );
  let client;
  try {
    client = await openStore(Client, {
      path: join(directory, "store"),
      schema: generatedSchema,
      connection: {
        url: `http://127.0.0.1:${server.address().port}`,
        token: "mobile",
      },
    });
    const fetch = fetchModels(client),
      models = liveModels(client),
      queries = makeQueries(client);
    const todo = await fetch.todo({ id: "one" });
    assert.ok(todo.at instanceof Date);
    assert.equal(todo.status, "closed");
    assert.equal((await models.todo.get({ id: "one" })).title, "remote");
    const pin = await fetch.pin({ todo: "one", at }, { store: false });
    assert.equal(pin.at.getTime(), at.getTime());
    assert.equal(pin.label, "pinned");
    assert.equal(await models.pin.get({ todo: "one", at }), null);
    const snapshot = await queries.find({ at }, { store: false });
    assert.ok(snapshot.todo.at instanceof Date);
    assert.equal(await models.todo.get({ id: "queried" }), null);
    await queries.find({ at });
    assert.equal((await models.todo.get({ id: "queried" })).title, "queried");
    const reads = seen.filter(
      (request) =>
        request.url === "/sync/fetch" || request.url === "/sync/actions",
    );
    assert.deepEqual(
      reads.map(({ url, authorization, body }) => [
        url,
        authorization,
        body.invocation.key?.model ?? body.invocation.name,
        body.invocation.version,
        body.store,
      ]),
      [
        ["/sync/fetch", "Bearer mobile", "Todo", 1, true],
        ["/sync/fetch", "Bearer mobile", "Pin", 1, false],
        ["/sync/actions", "Bearer mobile", "Find", 2, false],
        ["/sync/actions", "Bearer mobile", "Find", 2, true],
      ],
    );
    assert.ok(reads.every((request) => request.body.stream === "User:viewer"));
    assert.equal((await client.syncState()).pending, 0);
  } finally {
    await client?.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});
