import { openStore, emptyPull, emptyHandshake, read05 } from "./store-fixture.mjs";
// Model Fetch through the real native runtime (#153): Rust validates, joins
// or starts the request, stores the reply and completes every caller; this
// host only executes the `fetch` HTTP effect and decodes each caller's own
// result. The network here is a fake transport so each request is visible.
import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { WebSocketServer } from "ws";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CallError } from "../../../packages/client-js/actions.mts";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-js/transaction.mts";
import { createServerConnection } from "../../../packages/client-js/live.mts";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const scalar = (name) => ({ kind: "scalar", name });
const schema = {
  enums: [{ name: "Status", values: ["open", "closed"] }],
  models: [
    {
      name: "Entry",
      version: 2,
      identity: ["id"],
      fields: [
        { name: "id", type: scalar("string"), nullable: false },
        { name: "title", type: scalar("string"), nullable: false },
        { name: "at", type: scalar("dateTime"), nullable: false },
        {
          name: "status",
          type: { kind: "enum", name: "Status" },
          nullable: false,
        },
        {
          name: "tags",
          type: { kind: "list", element: scalar("string") },
          nullable: false,
        },
      ],
    },
  ],
  actions: [],
};
const state = (title) => ({
  title,
  at: "2026-01-02T03:04:05.000Z",
  status: "open",
  tags: [title],
});
/** Keeps the raw object: independence must come from the host, not a decoder. */
const raw = (row) => row;

function deferred() {
  let resolve;
  const promise = new Promise((done) => (resolve = done));
  return { promise, resolve };
}

/**
 * A client whose transport answers `fetch` requests with `reply(request, n)`,
 * the n-th request's completion outcome and authority. `gates` holds the n-th
 * request until released.
 */
async function harness(body) {
  const directory = await mkdtemp(join(tmpdir(), "axton-fetch-"));
  const net = {
    requests: [],
    gates: new Map(),
    reply: (request, n) => ({
      result: { id: request.invocation.key.identity.id, ...state(`v${n}`) },
    }),
  };
  const FetchClient = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, text) => {
      if (kind === "handshake" || kind === "pull") return emptyPull(text);
      assert.equal(kind, "fetch");
      const request = JSON.parse(text);
      net.requests.push(request);
      const n = net.requests.length;
      await net.gates.get(n)?.promise;
      const answer = net.reply(request, n);
      if (answer instanceof Error) throw answer;
      const outcome = answer.failure
        ? { status: "failed", code: answer.failure, execution: "rejected" }
        : { status: "succeeded", result: answer.result };
      const records = answer.failure
        ? []
        : [
            {
              key: request.invocation.key,
              cursor: null,
              state:
                answer.result === null
                  ? null
                  : Object.fromEntries(
                      Object.entries(answer.result).filter(
                        ([key]) => key !== "id",
                      ),
                    ),
            },
          ];
      return JSON.stringify(
        answer.failure
          ? {
              protocol: request.protocol,
              storeId: request.storeId,
              stream: request.stream,
              materialization: request.materialization,
              requestId: request.requestId,
              outcome: { kind: "failed", code: answer.failure, message: null },
              records: [],
            }
          : read05(request, answer.result, records),
      );
    },
  }));
  const client = await openStore(FetchClient, {
    path: join(directory, "db"),
    schema,
  });
  try {
    await body({ client, net });
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
}
const connect = (client) =>
  client.connect({ url: "http://unused", token: "token" });
const fetchEntry = (client, id, options) =>
  client.fetchModel("Entry", 2, { id }, raw, options);

test("default storage commits permitted null-cursor cache before resolving", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    assert.deepEqual(await fetchEntry(client, "a"), {
      id: "a",
      ...state("v1"),
    });
    assert.deepEqual(net.requests[0].invocation.key.identity, { id: "a" });
    assert.equal(net.requests[0].invocation.version, 2);
    assert.equal(net.requests[0].store, true);
    assert.equal((await client.read("Entry", { id: "a" })).title, "v1");
    await fetchEntry(client, "a", { store: true });
    assert.equal(net.requests.length, 2);
    assert.equal((await client.read("Entry", { id: "a" })).title, "v2");
  });
});
test("store false returns snapshot without cache writes", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    assert.deepEqual(await fetchEntry(client, "a", { store: false }), {
      id: "a",
      ...state("v1"),
    });
    assert.equal(net.requests[0].store, false);
    assert.equal(await client.read("Entry", { id: "a" }), null);
  });
});
test("ordinary null returns absence without deleting cached content", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    await fetchEntry(client, "a");
    net.reply = () => ({ result: null });
    assert.equal(await fetchEntry(client, "a"), null);
    assert.equal((await client.read("Entry", { id: "a" })).title, "v1");
    assert.equal(await fetchEntry(client, "b", { store: false }), null);
  });
});
test("malformed cache state rejects without a partial write", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    net.reply = () => ({ result: { id: "a", title: "missing fields" } });
    await assert.rejects(fetchEntry(client, "a"), CallError);
    assert.equal(await client.read("Entry", { id: "a" }), null);
  });
});

test("each invocation has its own request and independent snapshot objects", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    await client.bootstrap();
    const gate = deferred();
    net.gates.set(1, gate);
    const first = fetchEntry(client, "a");
    const second = fetchEntry(client, "a", { store: true });
    const preview = fetchEntry(client, "a", { store: false });
    await new Promise((resolve) => setTimeout(resolve, 20));
    gate.resolve();
    const [a, b, c] = await Promise.all([first, second, preview]);
    assert.equal(
      net.requests.length,
      3,
      "each invocation requests a fresh snapshot",
    );
    assert.equal(
      new Set(net.requests.map((request) => request.requestId)).size,
      3,
    );
    assert.deepEqual(a, { id: "a", ...state("v1") });
    assert.deepEqual(b, { id: "a", ...state("v2") });
    assert.notEqual(a, b, "each caller decodes its own object");
    assert.notEqual(a.tags, b.tags);
    a.tags.push("mutated");
    assert.deepEqual(b.tags, [b.title]);
    assert.equal(c.id, "a");
  });
});

test("a backend refusal rejects with its typed code", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    net.reply = () => ({ failure: "loader.failed" });
    await assert.rejects(
      fetchEntry(client, "a"),
      (error) =>
        error instanceof CallError &&
        error.code === "loader.failed" &&
        error.execution === "rejected",
    );
    assert.equal(await client.read("Entry", { id: "a" }), null);
  });
});

test("local failures keep their fetch codes and transport details", async () => {
  await harness(async ({ client, net }) => {
    // Not connected: no request is made.
    await assert.rejects(
      fetchEntry(client, "a"),
      (error) =>
        error instanceof CallError && error.code === "fetch.unavailable",
    );
    await connect(client);
    net.reply = () =>
      Object.assign(Error("fetch failed: 503 down"), { status: 503 });
    await assert.rejects(fetchEntry(client, "a"), (error) => {
      assert.ok(error instanceof CallError);
      assert.equal(error.code, "fetch.transport_failed");
      assert.equal(error.cause.status, 503);
      assert.match(error.cause.message, /503/);
      return true;
    });
    net.reply = () => ({ result: { id: "other", ...state("x") } });
    await assert.rejects(
      fetchEntry(client, "a"),
      (error) => {
        assert.ok(error instanceof CallError);
        assert.equal(error.code, "fetch.invalid_response");
        assert.match(error.cause?.message ?? "", /identity mismatch/);
        return true;
      },
    );
  });
});

test("invalid options and identities are refused before any request", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    const invalid = (error) =>
      error instanceof CallError &&
      error.code === "fetch.invalid_options" &&
      error.execution === "rejected";
    // Rust is the authority for storage options and identities.
    await assert.rejects(
      fetchEntry(client, "a", { store: { entry: false } }),
      invalid,
    );
    await assert.rejects(fetchEntry(client, "a", { store: null }), invalid);
    await assert.rejects(client.fetchModel("Entry", 2, {}, raw), invalid);
    await assert.rejects(
      client.fetchModel("Entry", 2, { id: 1 }, raw),
      invalid,
    );
    await assert.rejects(
      client.fetchModel("Entry", 1, { id: "a" }, raw),
      invalid,
    );
    await assert.rejects(
      client.fetchModel("Missing", 1, { id: "a" }, raw),
      invalid,
    );
    // The runtime ignores members it does not know, so the SDK refuses them.
    await assert.rejects(fetchEntry(client, "a", { once: true }), invalid);
    await assert.rejects(fetchEntry(client, "a", { refresh: true }), invalid);
    await client.transaction(async () => {
      await assert.rejects(
        fetchEntry(client, "a"),
        (error) =>
          error instanceof CallError && error.code === "transaction_active",
      );
    });
    assert.equal(net.requests.length, 0);
  });
});

test("the default connection posts Fetch to /sync/fetch with its credentials", async () => {
  const seen = [];
  const server = createServer(async (request, response) => {
    if (request.url === "/sync/live") { response.statusCode = 404; response.end(); return; }
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks).toString());
    if (request.url === "/sync/pull" || request.url === "/sync/handshake") {
      response.end(emptyPull(body));
      return;
    }
    seen.push({
      url: request.url,
      authorization: request.headers.authorization,
    });
    if (seen.length === 1) {
      response.statusCode = 401;
      return response.end("expired");
    }
    response.end(
      JSON.stringify(
        read05(body, { id: "a", ...state("http") }, [
          { key: body.invocation.key, cursor: null, state: state("http") },
        ]),
      ),
    );
  });
  const sockets = new WebSocketServer({server});
  sockets.on("connection", socket => socket.on("message", text => socket.send(JSON.stringify(emptyHandshake(JSON.parse(text.toString()))))));
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const directory = await mkdtemp(join(tmpdir(), "axton-fetch-http-"));
  const HttpClient = createClient(native, Transaction, createServerConnection);
  const client = await openStore(HttpClient, {
    path: join(directory, "db"),
    schema,
  });
  let token = "first";
  try {
    await client.connect(
      { url: `http://127.0.0.1:${server.address().port}`, token: () => token },
      { refreshAuth: async () => void (token = "second") },
    );
    const result = await fetchEntry(client, "a", { store: false });
    assert.equal(result.title, "http");
    assert.deepEqual(seen, [
      { url: "/sync/fetch", authorization: "Bearer first" },
      { url: "/sync/fetch", authorization: "Bearer second" },
    ]);
  } finally {
    await client.close();
    for (const socket of sockets.clients) socket.terminate();
    await new Promise(resolve => sockets.close(resolve));
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});

test("a Fetch on a closed client rethrows the admission error", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    await client.close();
    await assert.rejects(fetchEntry(client, "a"), (error) => {
      // The raw admission error every task of a closed client gets; unlike a
      // direct call it is not mapped to a CallError.
      assert.equal(error instanceof CallError, false);
      assert.equal(error.message, "client_closed");
      return true;
    });
    assert.equal(net.requests.length, 0);
  });
});

test("close while a Fetch waits on the network rejects it unavailable and stores nothing late", async () => {
  await harness(async ({ client, net }) => {
    await connect(client);
    const gate = deferred();
    net.gates.set(1, gate);
    net.gates.set(2, gate);
    const pending = fetchEntry(client, "a");
    const joined = fetchEntry(client, "a");
    while (net.requests.length === 0)
      await new Promise((resolve) => setTimeout(resolve, 5));
    const outcomes = Promise.allSettled([pending, joined]);
    await client.close();
    // The HTTP effect is still outstanding when the runtime closes.
    gate.resolve();
    for (const outcome of await outcomes) {
      assert.equal(outcome.status, "rejected");
      assert.ok(outcome.reason instanceof CallError, String(outcome.reason));
      assert.equal(outcome.reason.code, "fetch.unavailable");
      assert.equal(outcome.reason.execution, "unknown");
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(net.requests.length, 2);
  });
});
