import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "../../../packages/client-js/runtime.mts";
import {
  openStore,
  offlineNetwork,
  emptyHandshake,
  delivery05,
} from "./store-fixture.mjs";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const schema = {
  models: [],
  enums: [],
  actions: [
    { name: "Ping", kind: "query", version: 1, inputs: [], outputs: [] },
  ],
};
const until = async (probe) => {
  const deadline = Date.now() + 5000;
  while (!probe()) {
    assert.ok(Date.now() < deadline, "Bootstrap fixture timed out");
    await new Promise((r) => setTimeout(r, 2));
  }
};
const starts = (requests) =>
  requests.filter((x) => x.route === "handshake").map((x) => x.body);
const ranges = (requests) =>
  requests
    .filter((x) => x.route === "pull" && x.body.bootstrap)
    .map((x) => x.body);
export function bootstrapSuite(test, Transaction) {
  async function harness(body, loseStart = false) {
    const directory = await mkdtemp(join(tmpdir(), "axton-bootstrap-"));
    const requests = [];
    let release,
      held = false;
    const gate = new Promise((resolve) => (release = resolve));
    const Client = createClient(native, Transaction, (options) => ({
      open() {},
      async push(route, text) {
        if (options.url !== "http://fixture")
          return offlineNetwork().push(route, text);
        const request = JSON.parse(text);
        requests.push({ route, body: request });
        assert.equal(request.protocol, 5);
        if (route === "handshake") {
          if (loseStart) {
            loseStart = false;
            throw Error("controlled lost Handshake response");
          }
          return JSON.stringify(emptyHandshake(request));
        }
        assert.equal(route, "pull");
        if (request.bootstrap && held) await gate;
        return JSON.stringify(delivery05(request));
      },
    }));
    let client = await openStore(Client, {
      path: join(directory, "db"),
      schema,
    });
    try {
      await body({
        get client() {
          return client;
        },
        requests,
        hold: () => (held = true),
        release: () => {
          held = false;
          release();
        },
        until,
        online: () =>
          client.connect({ url: "http://fixture", token: "viewer" }),
        reopen: async () => {
          await client.close();
          client = await openStore(Client, {
            path: join(directory, "db"),
            schema,
          });
          return client;
        },
      });
    } finally {
      release();
      await client.close();
      await rm(directory, { recursive: true, force: true });
    }
  }
  test("Bootstrap first await covers durable start and complete local range", () =>
    harness(async (f) => {
      f.hold();
      await f.online();
      let done = false;
      const pending = f.client.bootstrap().then(() => (done = true));
      await until(() => ranges(f.requests).length > 0);
      assert.equal(done, false);
      f.release();
      await pending;
      assert.equal(done, true);
      assert.equal(ranges(f.requests)[0].after, 0);
      assert.equal(ranges(f.requests)[0].through, 0);
    }));
  test("offline Bootstrap registers durable work and resumes on connection", () =>
    harness(async (f) => {
      let done = false;
      const pending = f.client.bootstrap().then(() => (done = true));
      await new Promise(setImmediate);
      assert.equal(done, false);
      assert.deepEqual(f.requests, []);
      await f.online();
      await pending;
      assert.equal(starts(f.requests).length, 1);
      assert.ok(ranges(f.requests).length > 0);
    }));
  test("lost Handshake response retries the exact Store and Stream start request", () =>
    harness(async (f) => {
      await f.online();
      await f.client.bootstrap();
      const requests = starts(f.requests);
      assert.ok(requests.length >= 2);
      for (const request of requests) assert.deepEqual(request, requests[0]);
    }, true));
  test("close ends Bootstrap waiter and reopen resumes the saved range context", () =>
    harness(async (f) => {
      f.hold();
      await f.online();
      const outcome = f.client.bootstrap().then(
        () => null,
        (error) => error,
      );
      await until(() => ranges(f.requests).length > 0);
      const first = ranges(f.requests)[0];
      const startsBefore = starts(f.requests).length;
      await f.client.close();
      assert.match((await outcome).message, /client_closed/);
      f.release();
      await f.reopen();
      await f.online();
      await f.client.bootstrap();
      assert.equal(starts(f.requests).length, startsBefore + 1);
      assert.deepEqual(ranges(f.requests).at(-1), first);
    }));
  test("Bootstrap from local transaction rejects promptly without registering", () =>
    harness(async (f) => {
      await f.online();
      await f.client.bootstrap();
      const before = ranges(f.requests).length;
      await f.client.transaction(async () => {
        await assert.rejects(f.client.bootstrap(), /transaction_active/);
      });
      assert.equal(ranges(f.requests).length, before);
    }));
}
