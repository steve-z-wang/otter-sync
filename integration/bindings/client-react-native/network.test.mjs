import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import WebSocket, { WebSocketServer } from "ws";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { delivery05, emptyHandshake } from "../client-js/store-fixture.mjs";
import { createServerConnection } from "../../../packages/client-react-native/live.mts";
// Node adapts RN's socket API here; actual device loading is a separate gate.
class NativeSocket extends WebSocket {
  constructor(url, protocols, options) {
    super(url, protocols, options);
    this.on("error", () => {});
  }
}
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const Client = createClient(native, Transaction, (options) =>
  createServerConnection(options, NativeSocket),
);
async function until(probe) {
  const end = Date.now() + 5000;
  while (!(await probe())) {
    assert.ok(Date.now() < end, "mobile transport timed out");
    await new Promise((r) => setTimeout(r, 5));
  }
}
const page = (body, text) =>
  delivery05(
    body,
    body.after === body.through
      ? []
      : [
          {
            kind: "record",
            cursor: body.through,
            key: { model: "Entry", identity: { id: "one" } },
            state: { text, note: null },
          },
        ],
  );

test("bound mobile transport authenticates HTTP/WS, catches up from committed C and deduplicates live pages", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-network-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  const requests = [],
    errors = [];
  let head = 0,
    peer,
    context,
    authorization;
  const http = createServer(async (request, response) => {
    let text = "";
    for await (const chunk of request) text += chunk;
    const body = JSON.parse(text);
    requests.push({
      authorization: request.headers.authorization,
      url: request.url,
      body,
    });
    context = body;
    response.setHeader("content-type", "application/json");
    response.end(
      JSON.stringify(
        request.url === "/sync/handshake"
          ? { ...emptyHandshake(body), head }
          : page(body, "catch-up"),
      ),
    );
  });
  const sockets = new WebSocketServer({ server: http });
  sockets.on("connection", (socket, request) => {
    peer = socket;
    authorization = request.headers.authorization;
    socket.on("message", (text) => {
      const body = JSON.parse(text.toString());
      socket.send(JSON.stringify({ ...emptyHandshake(body), head }));
    });
  });
  http.listen(0, "127.0.0.1");
  await once(http, "listening");
  let client;
  try {
    client = await Client.open({
      path: join(directory, "store"),
      schema,
      stream: "scope",
      connection: {
        url: `http://127.0.0.1:${http.address().port}`,
        token: "alice",
        options: { onError: (error) => errors.push(error) },
      },
    });
    await client.bootstrap();
    await until(() => peer && authorization);
    assert.equal((await client.syncState()).cursors.scope, 0);
    assert.equal(authorization, "Bearer alice");
    await client.connection.pause();
    await until(() => sockets.clients.size === 0);
    head = 1;
    await client.connection.resume();
    await until(async () => (await client.syncState()).cursors.scope === 1);
    const pulls = requests.filter(
      (request) => request.url === "/sync/pull" && !request.body.bootstrap,
    );
    assert.equal(pulls.length, 1);
    assert.equal(pulls[0].url, "/sync/pull");
    assert.equal(pulls[0].body.after, 0);
    assert.equal((await client.read("Entry", { id: "one" })).text, "catch-up");
    assert.ok(
      requests.every(
        (request) =>
          request.authorization === "Bearer alice" &&
          request.body.stream === "scope",
      ),
    );
    assert.equal(authorization, "Bearer alice");
    const delivered = page(
      { ...context, after: 1, through: 2, bootstrap: false },
      "live",
    );
    peer.send(JSON.stringify(delivered));
    await until(
      async () => (await client.read("Entry", { id: "one" }))?.text === "live",
    );
    peer.send(JSON.stringify(delivered));
    await new Promise((resolve) => setTimeout(resolve, 60));
    assert.equal(
      requests.filter(
        (request) => request.url === "/sync/pull" && !request.body.bootstrap,
      ).length,
      1,
    );
    assert.equal((await client.syncState()).cursors.scope, 2);
    assert.deepEqual(errors, []);
    await client.close();
    const before = requests.length;
    await new Promise((resolve) => setTimeout(resolve, 30));
    assert.equal(requests.length, before);
  } finally {
    await client?.close();
    for (const socket of sockets.clients) socket.terminate();
    await new Promise((resolve) => sockets.close(resolve));
    await new Promise((resolve) => http.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});
