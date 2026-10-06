import {openStore} from './store-fixture.mjs';
// Prerequisite handlers registered at open (#185), run by the native runtime
// whenever a task becomes pending. Shared by the Node and React Native
// suites: each passes its own transaction adapter and server connection. Not
// a test file itself.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { PrerequisiteRetry } from "../../../packages/client-js/connection.mts";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const schema = JSON.parse(
  await readFile(
    new URL("../../../fixtures/schemas/entry.json", import.meta.url),
    "utf8",
  ),
);
schema.prerequisites = [
  { name: "RemoteBlob", fields: [{ name: "key", type: "String" }] },
];
schema.requirements = [
  {
    model: "Entry",
    field: "note",
    name: "RemoteBlob",
    arguments: { key: "self" },
  },
];

schema.actions=[{name:'Edit',version:1,kind:'mutation',inputs:[{kind:'model',name:'entry',model:'Entry',operation:'update',cardinality:'single',allowedFields:['text','note']}],outputs:[],requirements:schema.requirements}];

/**
 * The native carrier, except that every `timer` effect's delay is recorded in
 * `delays` and shortened to 1 ms: the backoff is the runtime's decision, and
 * the test observes it without waiting it out.
 */
function recording(delays) {
  return {
    runtimeOpen: (request, wake) => native.runtimeOpen(request, wake),
    runtimeSubmit: (id, message) => native.runtimeSubmit(id, message),
    runtimeDetach: (id) => native.runtimeDetach(id),
    runtimeDrain(id) {
      const events = JSON.parse(native.runtimeDrain(id));
      for (const event of events)
        if (event.type === "effect" && event.operation.kind === "timer") {
          delays.push(event.operation.millis);
          event.operation.millis = 1;
        }
      return JSON.stringify(events);
    },
  };
}

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
const deferred = () => {
  let resolve;
  const promise = new Promise((r) => (resolve = r));
  return { promise, resolve };
};

/** Register the scenarios with `test`, for a host's adapters. */
export function prerequisiteSuite(
  test,
  { Transaction, createServerConnection },
) {
  async function harness(body) {
    const directory = await mkdtemp(join(tmpdir(), "axton-prerequisite-"));
    const delays = [];
    const Client = createClient(
      recording(delays),
      Transaction,
      createServerConnection,
    );
    const open = (prerequisites) =>
      openStore(Client,{ path: join(directory, "db"), schema, prerequisites });
    const attach = async (client, key) => {
      await client.transaction((tx) =>
        tx.direct({
          model: "Entry",
          op: "create",
          identity: { id: key },
          values: { text: "A" },
        }),
      );
      await client.submitMutation('Edit',1,{entry:{id:key,note:key}},value=>value);
    };
    try {
      await body({ open, attach, delays });
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  }

  test("a commit that queues a task runs its handler with no host call", () =>
    harness(async ({ open, attach }) => {
      const calls = [];
      const client = await open({
        RemoteBlob: async (args, signal) => {
          assert.ok(signal instanceof AbortSignal);
          calls.push(args);
        },
      });
      try {
        await attach(client, "asset");
        await until(async () => (await client.pendingTasks()).length === 0);
        assert.deepEqual(calls, [{ key: "asset" }]);
        const status = await client.syncState("Entry", { id: "asset" });
        assert.deepEqual(status.pending[0].prerequisites, []);
      } finally {
        await client.close();
      }
    }));

  test("a task pending at restart runs after reopen", () =>
    harness(async ({ open, attach }) => {
      const started = deferred();
      const first = await open({
        RemoteBlob: () => {
          started.resolve();
          return new Promise(() => {});
        },
      });
      await attach(first, "asset");
      await started.promise;
      await first.close();
      const calls = [];
      const second = await open({
        RemoteBlob: async (args) => {
          calls.push(args);
        },
      });
      try {
        await until(async () => (await second.pendingTasks()).length === 0);
        assert.deepEqual(calls, [{ key: "asset" }]);
      } finally {
        await second.close();
      }
    }));

  test("a transient failure retries with a growing backoff", () =>
    harness(async ({ open, attach, delays }) => {
      let calls = 0;
      const client = await open({
        RemoteBlob: async () => {
          if (++calls <= 3) throw new PrerequisiteRetry("offline");
        },
      });
      try {
        await attach(client, "asset");
        await until(async () => (await client.pendingTasks()).length === 0);
        assert.equal(calls, 4);
        // 1 s doubling per retry, each within the runtime's ±20 % jitter.
        assert.equal(delays.length, 3, `${delays}`);
        delays.forEach((delay, i) => {
          const base = 1000 * 2 ** i;
          assert.ok(delay >= base * 0.8 && delay <= base * 1.2, `${delays}`);
        });
      } finally {
        await client.close();
      }
    }));

  test("a terminal failure stays failed and visible until a reset", () =>
    harness(async ({ open, attach, delays }) => {
      let calls = 0;
      const client = await open({
        RemoteBlob: async () => {
          if (++calls === 1) throw Error("file is gone");
        },
      });
      try {
        await attach(client, "asset");
        const [task] = await until(async () => {
          const tasks = await client.pendingTasks();
          return tasks[0]?.state === "failed" && tasks;
        });
        assert.equal(task.error, "file is gone");
        assert.equal(task.name, "RemoteBlob");
        await new Promise((resolve) => setTimeout(resolve, 50));
        assert.equal(calls, 1, "not retried");
        assert.deepEqual(delays, [], "no backoff");
        assert.equal((await client.pendingTasks()).filter(task=>task.state==='failed').length,1,"failed prerequisite still blocks delivery");
        await client.setReadiness(task.key, "pending");
        await until(async () => (await client.pendingTasks()).length === 0);
        assert.equal(calls, 2);
      } finally {
        await client.close();
      }
    }));

  test("close during a run aborts it, does not hang and reports nothing", () =>
    harness(async ({ open, attach }) => {
      const errors = [];
      const record = (error) => errors.push(error);
      process.on("uncaughtException", record);
      process.on("unhandledRejection", record);
      try {
        const started = deferred();
        let aborted = false;
        const client = await open({
          RemoteBlob: (_args, signal) =>
            new Promise((_resolve, reject) => {
              started.resolve();
              signal.addEventListener("abort", () => {
                aborted = true;
                reject(signal.reason);
              });
            }),
        });
        await attach(client, "asset");
        await started.promise;
        await client.close();
        assert.ok(aborted, "the handler's signal aborted");
        await new Promise((resolve) => setTimeout(resolve, 20));
        assert.deepEqual(errors, []);
      } finally {
        process.off("uncaughtException", record);
        process.off("unhandledRejection", record);
      }
    }));

  test("a handler for a prerequisite the schema does not declare fails open", () =>
    harness(async ({ open }) => {
      await assert.rejects(open({ Upload: async () => {} }), {
        message: /invalid prerequisite handler Upload/,
      });
    }));
}
