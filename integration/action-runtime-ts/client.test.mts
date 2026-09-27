import assert from "node:assert/strict";
import test from "node:test";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "../../packages/client-js/runtime.mts";
import {
  Client,
  Transaction,
  type Call,
  type CallOutcome,
} from "../../packages/client-js/index.mts";
import { GeneratedClient } from "./client.ts";
import { makeMutations, makeQueries } from "./generated.ts";

test("generated operation codecs retain null, lists, omitted patches and DateTime identity", async () => {
  const calls: {
    name: string;
    version: number;
    args: Record<string, unknown>;
  }[] = [];
  const port = {
    async invokeAction(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => unknown,
    ) {
      calls.push({ name, version, args: args as Record<string, unknown> });
      return {
        status: "succeeded" as const,
        async wait() {
          return { result: decode(null), error: null };
        },
      };
    },
    async invokeDirectAction(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => unknown,
    ) {
      calls.push({ name, version, args: args as Record<string, unknown> });
      return decode(
        name === "RemoveMoment"
          ? { at: "2026-01-01T00:00:00.000Z" }
          : name === "Put"
            ? {
                todo: {
                  id: "one",
                  title: "server",
                  at: "2026-01-01T00:00:00.000Z",
                  status: "closed",
                  note: null,
                },
                echoed: "2026-01-01T00:00:00.000Z",
                status: "closed",
              }
            : { todo: null, echoed: "2026-01-01T00:00:00.000Z" },
      );
    },
    async invokeQuery(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => unknown,
    ) {
      calls.push({ name, version, args: args as Record<string, unknown> });
      return decode({ todo: null });
    },
    async invalidateQuery(name: string, version: number, args: object) {
      calls.push({ name, version, args: args as Record<string, unknown> });
    },
  };
  const mutations = makeMutations(port);
  const queries = makeQueries(port);
  const at = new Date("2026-01-01T00:00:00.000Z");
  await mutations.put({
    todo: { id: "one", title: "A", at, status: "open", note: null },
    when: at,
    statuses: ["open", "closed"],
    note: null,
  });
  assert.equal(calls[0]?.name, "Put");
  assert.equal(calls[0]?.version, 2);
  assert.deepEqual(calls[0]?.args, {
    todo: {
      id: "one",
      title: "A",
      at: at.toISOString(),
      status: "open",
      note: null,
    },
    when: at.toISOString(),
    statuses: ["open", "closed"],
    note: null,
  });
  await mutations.change({ at });
  assert.deepEqual(calls[1]?.args, { todo: null, at: at.toISOString() });
  await mutations.change({ todo: { id: "one", title: "B" }, at });
  assert.deepEqual(calls[2]?.args, {
    todo: { id: "one", title: "B" },
    at: at.toISOString(),
  });
  await mutations.clear({ todo: [{ id: "one" }] });
  assert.deepEqual(calls[3]?.args, { todo: [{ id: "one" }] });
  await mutations.mark({ moment: { at, title: "X" } });
  assert.deepEqual(calls[4]?.args, {
    moment: { at: at.toISOString(), title: "X" },
  });
  const result = await mutations.call.change({ at });
  assert.ok(result.echoed instanceof Date);
  assert.equal(result.todo, null);
  const removed = await mutations.call.removeMoment({ moment: { at } });
  assert.ok(removed.at instanceof Date);
  assert.equal(removed.at.toISOString(), at.toISOString());
  const put = await mutations.call.put({
    todo: { id: "one", title: "A", at, status: "open", note: null },
    when: at,
    statuses: ["open"],
    note: null,
  });
  assert.equal(put.status, "closed");
  assert.ok(put.todo.at instanceof Date);
  assert.ok(put.echoed instanceof Date);
  // Find is a Query at v2: direct by default, durable under enqueue.
  const found = await queries.find({ at });
  assert.equal(found.todo, null);
  await queries.enqueue.find({ at });
  assert.deepEqual(
    calls.slice(-2).map(({ name, version }) => ({ name, version })),
    [
      { name: "Find", version: 2 },
      { name: "Find", version: 2 },
    ],
  );
  assert.equal("find" in mutations, false);
  assert.equal("find" in mutations.call, false);
  assert.equal("put" in queries, false);
});

test("generated routes resolve through the native pump and direct transport while Models stay local", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-generated-actions-"));
  const native = createRequire(import.meta.url)(
    "../../bindings/node/axton-node.node",
  );
  const row = {
    id: "one",
    title: "server",
    at: "2026-01-01T00:00:00.000Z",
    status: "open",
    note: null,
  };
  let pushes = 0;
  let directCalls = 0;
  const InjectedClient = createClient(native, Transaction, () => ({
    open() {},
    async push(kind: string, bodyText: string) {
      const body = JSON.parse(bodyText);
      if (kind === "action") {
        directCalls++;
        return JSON.stringify({
          completion: {
            callId: body.call.callId,
            outcome: { status: "succeeded", result: { todo: row } },
          },
          records: [],
        });
      }
      assert.equal(kind, "push");
      pushes++;
      return JSON.stringify({
        clientId: body.clientId,
        batchSequence: body.batchSequence,
        rejections: [],
        records: [],
        completions: body.mutations.map((mutation: { callId: string }) => ({
          callId: mutation.callId,
          outcome: { status: "succeeded", result: null },
        })),
      });
    },
  }));
  const originalOpen = Client.open;
  Client.open = ((options: Parameters<typeof Client.open>[0]) =>
    InjectedClient.open(options)) as typeof Client.open;
  let client: GeneratedClient | undefined;
  try {
    client = await GeneratedClient.open({
      path: join(directory, "state.sqlite"),
    });
    const local = { ...row, title: "local", at: new Date(row.at) };
    await client.models.todo.create(local);
    assert.equal((await client.models.todo.get({ id: "one" }))?.title, "local");
    await client.models.todo.update({ id: "one" }, { title: "local edited" });
    assert.equal(
      (await client.models.todo.get({ id: "one" }))?.title,
      "local edited",
    );
    assert.equal((await client.syncState()).pending, 0);
    const seen: string[] = [];
    const stop = client.models.todo.watch({}, (rows) =>
      seen.push(rows[0]?.title ?? "empty"),
    );
    await client.models.todo.delete({ id: "one" });
    await client.models.todo.create(local);
    await new Promise((resolve) => setTimeout(resolve, 15));
    stop();
    assert.ok(seen.includes("empty") && seen.includes("local"));
    assert.equal((await client.syncState()).pending, 0);

    const connection = await client.connect({
      url: "http://unused",
      token: "alice",
    });
    const call: Call<void> = await client.mutations.ping({});
    const settled: CallOutcome<void> = await call.wait();
    assert.equal(settled.error, null);
    assert.equal(call.status, "succeeded");
    assert.deepEqual(await call.wait(), settled);
    assert.equal(pushes, 1);
    // The default Query route is direct: no queue row, a final result.
    const found = await client.queries.find({ at: new Date(row.at) });
    assert.equal(found.todo?.title, "server");
    assert.ok(found.todo?.at instanceof Date);
    assert.equal(pushes, 1);
    assert.equal((await client.syncState()).pending, 0);
    await connection.close();
    // Offline, direct routes fail instead of silently enqueueing.
    for (const direct of [
      () => client!.queries.find({ at: new Date(row.at) }),
      () => client!.mutations.call.ping({}),
    ])
      await assert.rejects(direct, (error: { code?: string }) => {
        assert.equal(error.code, "action.unavailable");
        return true;
      });
    assert.equal((await client.syncState()).pending, 0);
    // A queued Query is a durable intent with no local optimism.
    const before = await client.models.todo.get({ id: "one" });
    const queued: Call<{ todo: unknown }> = await client.queries.enqueue.find({
      at: new Date(row.at),
    });
    assert.equal(queued.status, "pending");
    assert.equal((await client.syncState()).pending, 1);
    assert.deepEqual(await client.models.todo.get({ id: "one" }), before);
    const pending = await client.mutations.change({
      todo: { id: "one", title: "optimistic" },
      at: new Date(row.at),
    });
    assert.equal(pending.status, "pending");
    assert.equal(
      (await client.models.todo.get({ id: "one" }))?.title,
      "optimistic",
    );
    assert.equal(
      found.todo?.title,
      "server",
      "the returned Loader snapshot does not track local optimism",
    );
    assert.equal(directCalls, 1);
    assert.equal(pushes, 1);
    assert.equal((await client.syncState()).pending, 2);
    await client.close();
    assert.equal((await pending.wait()).error?.code, "client.closed");
    assert.equal((await queued.wait()).error?.code, "client.closed");
  } finally {
    Client.open = originalOpen;
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("every generated route forwards store options beside encoded args", async () => {
  const seen: { name: string; args: unknown; options: unknown }[] = [];
  const port = {
    async invokeAction(
      name: string,
      _version: number,
      args: object,
      decode: (value: unknown) => unknown,
      options?: unknown,
    ) {
      seen.push({ name, args, options });
      return {
        status: "succeeded" as const,
        async wait() {
          return { result: decode(null), error: null };
        },
      };
    },
    async invokeDirectAction(
      name: string,
      _version: number,
      args: object,
      decode: (value: unknown) => unknown,
      options?: unknown,
    ) {
      seen.push({ name, args, options });
      return decode({ todo: null });
    },
    async invokeQuery(
      name: string,
      _version: number,
      args: object,
      decode: (value: unknown) => unknown,
      options?: unknown,
    ) {
      seen.push({ name, args, options });
      return decode({ todo: null });
    },
    async invalidateQuery(name: string, _version: number, args: object) {
      seen.push({ name, args, options: "invalidate" });
    },
  };
  const mutations = makeMutations(port);
  const queries = makeQueries(port);
  const at = new Date("2026-01-01T00:00:00.000Z");
  await mutations.ping({}, { store: false });
  await mutations.call.ping({}, { store: false });
  const found = await queries.find({ at }, { store: { todo: false } });
  assert.equal(found.todo, null);
  await queries.enqueue.find({ at }, { store: { todo: true } });
  await queries.find({ at });
  assert.deepEqual(seen[0], { name: "Ping", args: {}, options: { store: false } });
  assert.deepEqual(seen[1], { name: "Ping", args: {}, options: { store: false } });
  assert.deepEqual(seen[2], {
    name: "Find",
    args: { at: at.toISOString() },
    options: { store: { todo: false } },
  });
  assert.deepEqual(seen[3]?.options, { store: { todo: true } });
  assert.equal(seen[4]?.options, undefined);
  // once controls reach only the direct Query route; invalidation carries
  // the same encoded business args and nothing else.
  await queries.find({ at }, { once: true, refresh: true, store: false });
  assert.deepEqual(seen[5], {
    name: "Find",
    args: { at: at.toISOString() },
    options: { once: true, refresh: true, store: false },
  });
  assert.equal(await queries.invalidate.find({ at }), undefined);
  assert.deepEqual(seen[6], {
    name: "Find",
    args: { at: at.toISOString() },
    options: "invalidate",
  });
});
