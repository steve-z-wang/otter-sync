import test from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { createServerConnection } from "../../../packages/client-react-native/live.mts";

test("mobile durable diagnostics do not stop later native Actions", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-reports-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../../fixtures/schemas/entry.json", import.meta.url),
      "utf8",
    ),
  );
  schema.actions = [{ name: "Ping", version: 1, inputs: [], outputs: [] }];
  let requests = 0;
  const Client = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, bodyText) => {
      assert.equal(kind, "push");
      const body = JSON.parse(bodyText);
      requests++;
      return JSON.stringify({
        clientId: body.clientId,
        batchSequence: body.batchSequence,
        rejections: [],
        completions: body.mutations.map((mutation) => ({
          callId: mutation.callId,
          outcome: { status: "succeeded", result: null },
        })),
        records:
          requests === 3
            ? []
            : ["a", "b"].map((id) => ({
                model: "Entry",
                identity: { id },
                stamp: 1,
                state: {
                  text: requests === 1 ? "first" : "second",
                  note: null,
                },
              })),
      });
    },
  }));
  const client = await Client.open({ path: join(directory, "db"), schema });
  const previous = globalThis.reportError;
  const diagnostic = Error("mobile diagnostic failed");
  const observed = [];
  const reports = [];
  globalThis.reportError = (error) => observed.push(error);
  try {
    const connection = await client.connect(
      { url: "http://unused", token: "token" },
      {
        onError: (report) => {
          reports.push(report);
          throw diagnostic;
        },
      },
    );
    for (let i = 0; i < 3; i++) {
      const call = await client.invokeAction("Ping", 1, {}, () => undefined);
      const outcome = await Promise.race([
        call.wait(),
        new Promise((_, reject) =>
          setTimeout(() => reject(Error("pump stalled")), 1000),
        ),
      ]);
      assert.equal(outcome.error, null);
    }
    assert.equal(requests, 3);
    assert.equal(reports.length, 2);
    assert.ok(reports.every((report) => report.message.includes("conflict")));
    assert.deepEqual(observed, [diagnostic, diagnostic]);
    assert.equal((await client.syncState()).pending, 0);
    await connection.close();
  } finally {
    globalThis.reportError = previous;
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});
import {
  fetchModels,
  makeMutations,
  makeQueries,
  liveModels,
  schema as generatedSchema,
} from "../../action-runtime-ts/generated.ts";

test("mobile host runtime settles Actions and standalone local writes", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const Client = createClient(native, Transaction, () => {
    throw Error("network not configured");
  });
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-actions-"));
  const schema = {
    enums: [],
    models: [
      {
        name: "Entry",
        identity: ["id"],
        fields: [
          {
            name: "id",
            type: { kind: "scalar", name: "string" },
            nullable: false,
          },
          {
            name: "text",
            type: { kind: "scalar", name: "string" },
            nullable: false,
          },
        ],
      },
    ],
    actions: [{ name: "Ping", version: 1, inputs: [], outputs: [] }],
  };
  const client = await Client.open({ path: join(directory, "db"), schema });
  try {
    await client.direct({
      model: "Entry",
      op: "create",
      identity: { id: "one" },
      values: { text: "local" },
    });
    assert.equal((await client.read("Entry", { id: "one" })).text, "local");
    assert.equal((await client.syncState()).pending, 0);
    const call = await client.invokeAction("Ping", 1, {}, () => undefined);
    const waiting = call.wait();
    await client.drop(1);
    assert.equal((await waiting).error.code, "dropped");
    assert.equal(call.status, "failed");
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("generated Mutation, Query and Model bindings run through the mobile host adapter", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const Client = createClient(native, Transaction, () => {
    throw Error("network not configured");
  });
  const directory = await mkdtemp(
    join(tmpdir(), "axton-rn-generated-actions-"),
  );
  const client = await Client.open({
    path: join(directory, "db"),
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
    assert.equal((await models.todo.get({ id: "one" }))?.title, "local");
    assert.equal((await client.syncState()).pending, 0);
    const call = await makeMutations(client).ping({});
    await client.drop(1);
    assert.equal((await call.wait()).error?.code, "dropped");
    const at = new Date("2026-01-01T00:00:00Z");
    // Queued Queries persist their store policy with zero local operations.
    await makeQueries(client).enqueue.find({ at }, { store: { todo: false } });
    await makeQueries(client).enqueue.find({ at }, { store: false });
    const stored = await client.readSql(
      "SELECT name, version, store FROM axton_mutation ORDER BY ordinal",
    );
    assert.deepEqual(
      stored.map((row) => [row.name, row.version, row.store]),
      [
        ["Find", 2, '{"todo":false}'],
        ["Find", 2, "false"],
      ],
    );
    assert.equal((await models.todo.get({ id: "one" }))?.title, "local");
    // Direct routes never fall back to the queue without a connection.
    await assert.rejects(makeQueries(client).find({ at }), {
      code: "action.unavailable",
    });
    await assert.rejects(makeMutations(client).call.ping({}), {
      code: "action.unavailable",
    });
    assert.equal(
      (await client.readSql("SELECT count(*) AS n FROM axton_mutation"))[0].n,
      2,
    );
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});

// Model Fetch ([#153](https://github.com/zanminwang/axton/issues/153)) on the
// mobile host adapter: the React Native connection posts Rust's `fetch` effect
// to `/sync/fetch` with its token, and the generated facade decodes the
// result. This runs the shared JS Bridge over the Node carrier; it does not
// prove a device run.
test("generated Fetch routes through the mobile host connection to /sync/fetch", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const seen = [];
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = JSON.parse(Buffer.concat(chunks).toString());
    seen.push({
      url: request.url,
      authorization: request.headers.authorization,
      body,
    });
    const state =
      body.model === "Todo"
        ? {
            title: "remote",
            at: "2026-02-03T04:05:06.000Z",
            status: "closed",
            note: null,
          }
        : { label: "pinned" };
    response.end(
      JSON.stringify({
        completion: {
          callId: body.callId,
          outcome: {
            status: "succeeded",
            result: { ...body.identity, ...state },
          },
        },
        records:
          body.store === false
            ? []
            : [
                {
                  model: body.model,
                  identity: body.identity,
                  stamp: 1,
                  state,
                },
              ],
      }),
    );
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  // No Scope is followed, so no socket is opened; one would fail the test.
  class NoSocket {
    constructor() {
      throw Error("no socket expected");
    }
  }
  const Client = createClient(native, Transaction, (options) =>
    createServerConnection(options, NoSocket),
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-fetch-"));
  const client = await Client.open({
    path: join(directory, "db"),
    schema: generatedSchema,
  });
  try {
    await client.connect({
      url: `http://127.0.0.1:${server.address().port}`,
      token: "mobile",
    });
    const fetch = fetchModels(client);
    const todo = await fetch.todo({ id: "one" });
    assert.ok(todo.at instanceof Date);
    assert.equal(todo.status, "closed");
    assert.equal(
      (await liveModels(client).todo.get({ id: "one" }))?.title,
      "remote",
    );
    const at = new Date("2026-03-04T05:06:07.000Z");
    const pin = await fetch.pin({ todo: "one", at }, { store: false });
    assert.equal(pin.at.getTime(), at.getTime());
    assert.equal(pin.label, "pinned");
    assert.equal(await liveModels(client).pin.get({ todo: "one", at }), null);
    assert.deepEqual(
      seen.map(({ url, authorization, body }) => [
        url,
        authorization,
        body.model,
        body.version,
        body.store,
      ]),
      [
        ["/sync/fetch", "Bearer mobile", "Todo", 1, undefined],
        ["/sync/fetch", "Bearer mobile", "Pin", 1, false],
      ],
    );
  } finally {
    await client.close();
    await new Promise((resolve) => server.close(resolve));
    await rm(directory, { recursive: true, force: true });
  }
});
