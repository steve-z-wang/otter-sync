// Unsent work (#186, #205, #204): refused and failed acts as change streams,
// their resolutions on the client and inside a transaction, over the native
// runtime and SQLite. Shared by the Node and React Native suites: each passes
// its own transaction adapter and server connection. Not a test file itself.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "../../../packages/client-js/runtime.mts";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const field = (name, nullable = false) => ({
  name,
  type: { kind: "scalar", name: "string" },
  nullable,
});
/**
 * `Write` edits a Note and its `blob` requires `RemoteBlob(key: self)`; a
 * later `Write` of the same Note follows an earlier one. `Create` makes one.
 */
const schema = {
  enums: [],
  models: [
    {
      name: "Note",
      version: 1,
      identity: ["id"],
      fields: [field("id"), field("text"), field("blob", true)],
    },
  ],
  prerequisites: [
    { name: "RemoteBlob", fields: [{ name: "key", type: "String" }] },
  ],
  actions: [
    {
      name: "Write",
      version: 1,
      outputs: [],
      inputs: [
        {
          kind: "model",
          name: "note",
          model: "Note",
          operation: "update",
          cardinality: "single",
        },
      ],
      requirements: [
        {
          model: "Note",
          field: "blob",
          name: "RemoteBlob",
          arguments: { key: "self" },
        },
      ],
      sequence: { after: [{ name: "Write", arguments: { note: "note" } }] },
    },
    {
      name: "Create",
      version: 1,
      outputs: [],
      inputs: [
        {
          kind: "model",
          name: "note",
          model: "Note",
          operation: "create",
          cardinality: "single",
        },
      ],
    },
  ],
};
const blob = (key) =>
  JSON.stringify({ arguments: { key }, name: "RemoteBlob" });
const write = (text, blobKey = null) => ({
  note: { id: "n", text, blob: blobKey },
});
const identity = (value) => value;

/** Poll `probe` until it answers truthy; fail after `ms`. */
async function until(probe, ms = 5000) {
  const deadline = Date.now() + ms;
  for (;;) {
    const value = await probe();
    if (value) return value;
    assert.ok(Date.now() < deadline, "condition not reached in time");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

/** Every value a stream delivered, and a wait for the one that matches. */
function recorder() {
  const values = [];
  const waiters = [];
  return {
    values,
    push(value) {
      values.push(value);
      for (const waiter of [...waiters])
        if (waiter.match(value)) {
          waiters.splice(waiters.indexOf(waiter), 1);
          clearTimeout(waiter.timer);
          waiter.resolve(value);
        }
    },
    next(match) {
      const found = values.find(match);
      if (found !== undefined) return Promise.resolve(found);
      return new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => reject(Error(`no match in ${JSON.stringify(values)}`)),
          5000,
        );
        waiters.push({ match, resolve, timer });
      });
    },
  };
}

/** Register the scenarios with `test`, for a host's adapters. */
export function unsentSuite(test, { Transaction, createServerConnection }) {
  const Client = createClient(native, Transaction, createServerConnection);
  async function harness(body, prerequisites = {}) {
    const directory = await mkdtemp(join(tmpdir(), "axton-unsent-"));
    const client = await Client.open({
      path: join(directory, "db"),
      schema,
      prerequisites,
    });
    try {
      await client.direct({
        model: "Note",
        op: "create",
        identity: { id: "n" },
        values: { text: "base", blob: null },
      });
      await body(client);
    } finally {
      await client.close();
      await rm(directory, { recursive: true, force: true });
    }
  }
  /**
   * Freeze the queue and answer it: `refuse` maps an ordinal to its code,
   * and `records` is the authority of the accepted ones.
   */
  async function settle(client, sequence, refuse = {}, records = []) {
    const request = JSON.parse(await client.freeze());
    const rejections = [];
    const completions = [];
    for (const call of request.mutations) {
      const code = refuse[call.ordinal];
      if (code) {
        rejections.push({ ordinal: call.ordinal, code });
        completions.push({
          callId: call.callId,
          outcome: { status: "failed", code, execution: "rejected" },
        });
      } else {
        completions.push({
          callId: call.callId,
          outcome: { status: "succeeded", result: null },
        });
      }
    }
    await client.acknowledge(sequence, {
      clientId: client.clientId,
      batchSequence: sequence,
      rejections,
      completions,
      records,
    });
  }
  const text = async (client) => (await client.read("Note", { id: "n" })).text;

  test("a refusal keeps the act as submitted until it is dismissed", () =>
    harness(async (client) => {
      const refused = recorder();
      const pending = recorder();
      const stopRefused = client.rejections.watch((items) =>
        refused.push(items),
      );
      const stopPending = client.outbound.watchPending((count) =>
        pending.push(count),
      );
      await refused.next((items) => items.length === 0);
      await pending.next((count) => count === 0);
      const call = await client.invokeAction(
        "Write",
        1,
        write("the author's words"),
        identity,
      );
      await pending.next((count) => count === 1);
      await settle(client, 1, { 1: "note.denied" });
      const [item] = await refused.next((items) => items.length === 1);
      assert.deepEqual(item, {
        id: 1,
        name: "Write",
        version: 1,
        code: "note.denied",
        act: {
          args: write("the author's words"),
          operations: [
            {
              model: "Note",
              op: "update",
              identity: { id: "n" },
              values: { text: "the author's words", blob: null },
            },
          ],
        },
      });
      // The author's words come back from the retained act.
      assert.equal(item.act.args.note.text, "the author's words");
      assert.equal(await text(client), "base");
      assert.equal((await call.wait()).error.code, "note.denied");
      assert.deepEqual(await client.rejections.get(1), item);
      assert.equal(await client.rejections.get(2), null);
      await pending.next((count) => count === 0);
      assert.deepEqual(pending.values, [0, 1, 0], "distinct values only");
      await client.rejections.dismiss(1);
      await refused.next(
        (items) => items.length === 0 && refused.values.length > 2,
      );
      assert.equal(await client.rejections.get(1), null);
      stopRefused();
      stopPending();
    }));

  test("a terminal handler failure lists the act; a retry runs the handler again", () => {
    let calls = 0;
    return harness(
      async (client) => {
        const failures = recorder();
        const stop = client.failures.watch((items) => failures.push(items));
        await failures.next((items) => items.length === 0);
        await client.submitAction("Write", 1, write("photo", "X"));
        const [act] = await failures.next((items) => items.length === 1);
        assert.deepEqual(act, {
          ordinal: 1,
          name: "Write",
          version: 1,
          act: {
            args: write("photo", "X"),
            operations: [
              {
                model: "Note",
                op: "update",
                identity: { id: "n" },
                values: { text: "photo", blob: "X" },
              },
            ],
          },
          tasks: [
            {
              key: blob("X"),
              name: "RemoteBlob",
              arguments: { key: "X" },
              error: "file is gone",
            },
          ],
        });
        assert.equal(calls, 1);
        await client.failures.retry([blob("X")]);
        await failures.next(
          (items) => items.length === 0 && failures.values.length > 2,
        );
        await until(async () => (await client.pendingTasks()).length === 0);
        assert.equal(calls, 2);
        stop();
      },
      {
        RemoteBlob: async () => {
          if (++calls === 1) throw Error("file is gone");
        },
      },
    );
  });

  test("a new requirement on a failed task is listed at once and one retry unblocks both (#204)", () => {
    let calls = 0;
    return harness(
      async (client) => {
        const failures = recorder();
        const stop = client.failures.watch((items) => failures.push(items));
        await client.submitAction("Write", 1, write("one", "X"));
        await failures.next((items) => items.length === 1);
        await client.submitAction("Write", 1, write("two", "X"));
        const both = await failures.next((items) => items.length === 2);
        assert.deepEqual(
          both.map((act) => [act.ordinal, act.tasks[0].error]),
          [
            [1, "upload refused"],
            [2, "upload refused"],
          ],
        );
        assert.equal(calls, 1, "the failed task is not reset");
        await client.failures.retry([blob("X")]);
        await until(async () => (await client.pendingTasks()).length === 0);
        assert.equal(calls, 2, "one handler run covers both acts");
        const request = JSON.parse(await client.freeze());
        assert.deepEqual(
          request.mutations.map((call) => call.ordinal),
          [1, 2],
        );
        stop();
      },
      {
        RemoteBlob: async () => {
          if (++calls === 1) throw Error("upload refused");
        },
      },
    );
  });

  test("drop removes the act without a refusal and refuses a dependent", () =>
    harness(async (client) => {
      const created = await client.invokeAction(
        "Create",
        1,
        { note: { id: "m", text: "new", blob: null } },
        identity,
      );
      const edited = await client.invokeAction(
        "Write",
        1,
        { note: { id: "m", text: "edited", blob: null } },
        identity,
      );
      await client.failures.drop(1);
      assert.equal((await created.wait()).error.code, "dropped");
      assert.equal((await edited.wait()).error.code, "dependency.rejected");
      assert.equal(await client.read("Note", { id: "m" }), null);
      const refused = recorder();
      const stop = client.rejections.watch((items) => refused.push(items));
      const [item] = await refused.next((items) => items.length === 1);
      assert.deepEqual([item.id, item.code], [2, "dependency.rejected"]);
      // The existing drop still records its refusal.
      await client.submitAction("Write", 1, write("kept"));
      await client.drop(3);
      await refused.next((items) => items.length === 2);
      stop();
    }));

  test("a replacement and the drop of the failed original commit as one (#205)", () => {
    let calls = 0;
    return harness(
      async (client) => {
        const original = await client.invokeAction(
          "Write",
          1,
          write("draft", "X"),
          identity,
        );
        await until(async () =>
          (await client.pendingTasks()).some((t) => t.state === "failed"),
        );
        assert.equal(await text(client), "draft");
        let replacement;
        const seen = await client.transaction(async (tx) => {
          await tx.failures.drop(1);
          const row = await tx.read("Note", { id: "n" });
          replacement = await tx.submitMutation(
            "Write",
            1,
            write("fixed"),
            identity,
          );
          return row.text;
        });
        assert.equal(seen, "base", "planned without the original's optimism");
        assert.equal((await original.wait()).error.code, "dropped");
        assert.equal(await text(client), "fixed");
        assert.deepEqual(
          await client.readSql(
            "SELECT COUNT(*) AS n FROM axton_mutation_dependency",
          ),
          [{ n: 0 }],
          "not sequenced after the original",
        );
        await settle(client, 1, {}, [
          {
            model: "Note",
            identity: { id: "n" },
            stamp: 1,
            state: { text: "fixed", blob: null },
          },
        ]);
        assert.equal((await replacement.wait()).error, null, "accepted");
        assert.equal((await client.syncState()).pending, 0);
        assert.equal(calls, 1);
      },
      {
        RemoteBlob: async () => {
          ++calls;
          throw Error("upload refused");
        },
      },
    );
  });

  test("a throw after the drop rolls both back and the original is intact (#205)", () =>
    harness(
      async (client) => {
        await client.submitAction("Write", 1, write("draft", "X"));
        await until(async () =>
          (await client.pendingTasks()).some((t) => t.state === "failed"),
        );
        const failures = recorder();
        const stop = client.failures.watch((items) => failures.push(items));
        await failures.next((items) => items.length === 1);
        await assert.rejects(
          client.transaction(async (tx) => {
            await tx.failures.drop(1);
            await tx.submitMutation("Write", 1, write("fixed"), identity);
            await tx.failures.retry([blob("X")]);
            await tx.rejections.dismiss(1);
            throw Error("the author cancelled");
          }),
          /the author cancelled/,
        );
        assert.equal(await text(client), "draft");
        assert.deepEqual(
          (await client.pendingTasks()).map((t) => t.state),
          ["failed"],
        );
        assert.equal((await client.syncState()).pending, 1);
        assert.deepEqual(await client.rejections.get(1), null);
        assert.equal(failures.values.length, 1, "nothing changed");
        stop();
      },
      {
        RemoteBlob: async () => {
          throw Error("upload refused");
        },
      },
    ));

  test("a stream delivers the first result, then distinct results; stop and close end it", () =>
    harness(async (client) => {
      const counts = [];
      const stop = client.outbound.watchPending((count) => counts.push(count));
      await until(() => counts.length === 1);
      await client.direct({
        model: "Note",
        op: "update",
        identity: { id: "n" },
        values: { text: "local" },
      });
      await client.submitAction("Write", 1, write("one"));
      await until(() => counts.length === 2);
      stop();
      await client.submitAction("Write", 1, write("two"));
      assert.deepEqual(counts, [0, 1], "no value after stop");
      // A listener's exception reaches onError; the stream stays.
      const errors = [];
      const seen = [];
      const stopThrowing = client.failures.watch(
        (items) => {
          seen.push(items);
          throw Error("listener");
        },
        (error) => errors.push(error.message),
      );
      await until(() => errors.length === 1);
      assert.deepEqual(seen, [[]]);
      stopThrowing();
      // After close a registration fails through onError.
      const last = [];
      client.rejections.watch(
        (items) => last.push(items),
        (error) => last.push(error),
      );
      await until(() => last.length === 1);
      await client.close();
      const refused = [];
      client.rejections.watch(
        () => {},
        (error) => refused.push(error),
      );
      await until(() => refused.length === 1);
      assert.deepEqual(last, [[]], "close ends a stream with nothing more");
    }));
}
