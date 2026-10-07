import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  type Call,
  type QueryOptions,
} from "../../packages/client-js/index.mts";
import { GeneratedClient } from "./client.ts";
import { makeMutations, makeQueries, type Todo } from "./generated.ts";

test("generated operation codecs retain null, lists, omitted patches and DateTime identity", async () => {
  const calls: {
    name: string;
    version: number;
    args: Record<string, unknown>;
  }[] = [];
  const port = {
    async submitMutation<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
    ) {
      calls.push({ name, version, args: args as Record<string, unknown> });
      // This port fixture checks codecs only; real settlement is exercised by v05-sdk/run-host.sh.
      const value =
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
            : { todo: null, echoed: "2026-01-01T00:00:00.000Z" };
      return {
        status: "succeeded" as const,
        async wait() {
          return { result: decode(value), error: null };
        },
      };
    },
    async invokeQuery<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
    ) {
      calls.push({ name, version, args: args as Record<string, unknown> });
      return decode({ todo: null });
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
  const result = (await (await mutations.change({ at })).wait()).result!;
  assert.ok(result.echoed instanceof Date);
  assert.equal(result.todo, null);
  const removed = (
    await (await mutations.removeMoment({ moment: { at } })).wait()
  ).result!;
  assert.ok(removed.at instanceof Date);
  assert.equal(removed.at.toISOString(), at.toISOString());
  const put = (
    await (
      await mutations.put({
        todo: { id: "one", title: "A", at, status: "open", note: null },
        when: at,
        statuses: ["open"],
        note: null,
      })
    ).wait()
  ).result!;
  assert.equal(put.status, "closed");
  assert.ok(put.todo.at instanceof Date);
  assert.ok(put.echoed instanceof Date);
  // Find is a request-level Query at v2.
  const found = await queries.find({ at });
  assert.equal(found.todo, null);
  await queries.find({ at }, { store: false });
  assert.deepEqual(
    calls.slice(-2).map(({ name, version }) => ({ name, version })),
    [
      { name: "Find", version: 2 },
      { name: "Find", version: 2 },
    ],
  );
  assert.equal("find" in mutations, false);
  assert.equal("call" in mutations, false);
  assert.equal("enqueue" in queries, false);
  assert.equal("put" in queries, false);
});

test("generated bound native Store keeps local CRUD and named optimism durable offline", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-generated-actions-"));
  let client: GeneratedClient | undefined;
  const at = new Date("2026-01-01T00:00:00.000Z");
  const row = {
    id: "one",
    title: "local",
    at,
    status: "open" as const,
    note: null,
  };
  try {
    client = await GeneratedClient.open({
      path: join(directory, "state.sqlite"),
      stream: "User:alice",
      connection: { url: "http://127.0.0.1:1", token: "offline" },
    });
    await client.models.todo.create(row);
    await client.models.todo.update({ id: "one" }, { title: "edited" });
    assert.equal(
      (await client.models.todo.get({ id: "one" }))?.title,
      "edited",
    );
    assert.equal((await client.syncState()).pending, 0);
    const seen: string[] = [];
    const stop = client.models.todo.watch({}, (rows) =>
      seen.push(rows[0]?.title ?? "empty"),
    );
    await client.models.todo.delete({ id: "one" });
    await client.models.todo.create(row);
    await new Promise((resolve) => setTimeout(resolve, 15));
    stop();
    assert.ok(seen.includes("empty") && seen.includes("local"));
    const pending: Call<{ todo: Todo | null; echoed: Date }> =
      await client.mutations.change(async (tx) => {
        assert.equal((await tx.models.todo.get({ id: "one" }))?.title, "local");
        await tx.models.pin.create({ todo: "one", at, label: "companion" });
        return { todo: { id: "one", title: "optimistic" }, at };
      });
    assert.equal(pending.status, "pending");
    assert.equal(
      (await client.models.todo.get({ id: "one" }))?.title,
      "optimistic",
    );
    assert.equal(
      (await client.models.pin.get({ todo: "one", at }))?.label,
      "companion",
    );
    assert.equal((await client.syncState()).pending, 1);
    await client.close();
    assert.equal((await pending.wait()).error?.code, "client.closed");
    client = await GeneratedClient.open({
      path: join(directory, "state.sqlite"),
      stream: "User:alice",
      connection: { url: "http://127.0.0.1:1", token: "offline" },
    });
    assert.equal((await client.syncState()).pending, 1);
    assert.equal(
      (await client.models.todo.get({ id: "one" }))?.title,
      "optimistic",
    );
    assert.equal(
      (await client.models.pin.get({ todo: "one", at }))?.label,
      "companion",
    );
  } finally {
    await client?.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("generated Query store boolean and DateTime arguments stay request-local", async () => {
  const seen: unknown[] = [];
  const queries = makeQueries({
    async invokeQuery<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
      options?: QueryOptions,
    ) {
      seen.push({ name, version, args, options });
      return decode({ todo: null });
    },
  });
  const at = new Date("2026-01-01T00:00:00.000Z");
  await queries.find({ at }, { store: false });
  await queries.find({ at }, { store: true });
  assert.deepEqual(seen, [
    {
      name: "Find",
      version: 2,
      args: { at: at.toISOString() },
      options: { store: false },
    },
    {
      name: "Find",
      version: 2,
      args: { at: at.toISOString() },
      options: { store: true },
    },
  ]);
  assert.equal("enqueue" in queries, false);
});
