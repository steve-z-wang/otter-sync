// Mutations submitted inside a local transaction, with and without a `local`
// callback, through the real native runtime. Shared by the Node and React
// Native suites, which pass their own Transaction adapter; not a test file.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "../../../packages/client-js/runtime.mts";
import {
  schema as loadSchema,
  args as loadArgs,
  until,
} from "./loads-harness.mjs";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);

/** Entry and its Loads, plus `Publish` (creates an Entry), `Ping` and the `Find` Query. */
const schema = {
  ...loadSchema,
  actions: [
    {
      name: "Publish",
      version: 1,
      inputs: [
        {
          kind: "model",
          name: "entry",
          model: "Entry",
          operation: "create",
          cardinality: "single",
        },
      ],
      outputs: [],
    },
    { name: "Ping", version: 1, inputs: [], outputs: [] },
    { name: "Find", version: 1, kind: "query", inputs: [], outputs: [] },
  ],
};

const CAPABILITY = /invalid transaction capability/;
const code = (expected) => (error) => {
  assert.equal(error?.code, expected, String(error));
  return true;
};
const create = (id, text) => ({
  model: "Entry",
  op: "create",
  identity: { id },
  values: { text, note: null },
});
const remove = (id) => ({ model: "Entry", op: "delete", identity: { id } });
const publish = (id) => ({ entry: { id, text: "published", note: null } });
const decode = () => "decoded";
const deferred = () => {
  let resolve;
  const promise = new Promise((r) => (resolve = r));
  return { promise, resolve };
};

/**
 * A client over a fresh file whose backend accepts every pushed call and
 * answers each created Entry's authority. `pushes` records each push body;
 * `reopen` closes and opens the same file.
 */
async function withClient(Transaction, body, { onStore } = {}) {
  const directory = await mkdtemp(join(tmpdir(), "axton-tx-mutations-"));
  const path = join(directory, "db");
  const pushes = [];
  const Client = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, text) => {
      assert.equal(kind, "push");
      const batch = JSON.parse(text);
      pushes.push(batch);
      return JSON.stringify({
        clientId: batch.clientId,
        batchSequence: batch.batchSequence,
        rejections: [],
        completions: batch.mutations.map(({ callId }) => ({
          callId,
          outcome: { status: "succeeded", result: null },
        })),
        records: batch.mutations
          .filter(({ args }) => args.entry)
          .map(({ args: { entry } }) => ({
            model: "Entry",
            identity: { id: entry.id },
            stamp: pushes.length,
            state: { text: entry.text, note: entry.note },
          })),
      });
    },
  }));
  const open = () =>
    Client.open({ path, schema, ...(onStore ? { onStore } : {}) });
  let client = await open();
  try {
    await body({
      client,
      pushes,
      connect: () => client.connect({ url: "http://unused", token: "token" }),
      reopen: async () => {
        await client.close();
        return (client = await open());
      },
    });
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
}

/** The durable queue and the two Entries the Publish scenarios touch. */
async function state(client) {
  return {
    pending: (await client.syncState()).pending,
    draft: (await client.read("Entry", { id: "draft" }))?.text ?? null,
    published: (await client.read("Entry", { id: "p" }))?.text ?? null,
  };
}
const untouched = { pending: 0, draft: "local", published: null };

/**
 * The tests every host shares. `savepoints` marks a Transaction adapter with
 * savepoints; `exactGuard` one whose callback guard knows the callback's own
 * async context (every captured client task is refused, not only writes).
 */
export function mutationTests(test, Transaction, { savepoints, exactGuard }) {
  const run = (body, options) => withClient(Transaction, body, options);

  test("a local callback runs inside its submission and its Call is provisional until the commit", async () => {
    await run(async ({ client, pushes, connect }) => {
      await client.direct(create("draft", "local"));
      const order = [];
      let committed = false;
      const running = client.transaction(async (tx) => {
        const call = await tx.submitMutation(
          "Publish",
          1,
          publish("p"),
          decode,
          {
            local: async (local) => {
              order.push("local");
              // The callback reads the call's optimism and earlier writes.
              assert.equal(
                (await local.read("Entry", { id: "p" })).text,
                "published",
              );
              assert.equal((await local.query("Entry", {})).length, 2);
              await local.direct(remove("draft"));
            },
          },
        );
        order.push("submitted");
        assert.equal(committed, false);
        assert.equal(call.status, "pending");
        // An early wait is an observation error only: caught, the call stays usable.
        await assert.rejects(call.wait(), code("transaction_uncommitted"));
        assert.equal(call.status, "pending");
        assert.equal(await tx.read("Entry", { id: "draft" }), null);
        return { call, value: 7 };
      });
      void running.then(() => (committed = true));
      const { call, value } = await running;
      assert.equal(value, 7);
      assert.deepEqual(order, ["local", "submitted"]);
      assert.deepEqual(await state(client), {
        pending: 1,
        draft: null,
        published: "published",
      });
      await connect();
      assert.deepEqual(await call.wait(), { result: "decoded", error: null });
      assert.equal(call.status, "succeeded");
      // The companion delete is never sent.
      assert.equal(pushes.length, 1);
      assert.deepEqual(
        pushes[0].mutations.map(({ name }) => name),
        ["Publish"],
      );
      assert.equal(JSON.stringify(pushes[0]).includes("draft"), false);
      assert.equal(await client.read("Entry", { id: "draft" }), null);
    });
  });

  test("several Calls commit together and complete separately; the transaction returns any value", async () => {
    await run(async ({ client, pushes, connect }) => {
      await connect();
      const calls = await client.transaction(async (tx) => ({
        first: await tx.submitMutation("Ping", 1, {}, decode),
        second: await tx.submitMutation("Ping", 1, {}, () => "second", {
          store: false,
        }),
      }));
      // Completion may arrive before anybody waits: the handle was routed at
      // submission and keeps the outcome.
      await until(() => pushes.length > 0, "the push");
      assert.deepEqual(await calls.first.wait(), {
        result: "decoded",
        error: null,
      });
      assert.deepEqual(await calls.second.wait(), {
        result: "second",
        error: null,
      });
      assert.notEqual(
        pushes[0].mutations[0].callId,
        pushes[0].mutations[1].callId,
      );
      assert.equal(await client.transaction(async () => "plain"), "plain");
    });
  });

  test("a Call leaked from a failed transaction is rolled back and never sent", async () => {
    await run(async ({ client }) => {
      await client.direct(create("draft", "local"));
      const thrown = Error("rollback");
      let leaked;
      await assert.rejects(
        client.transaction(async (tx) => {
          leaked = await tx.submitMutation("Publish", 1, publish("p"), decode, {
            local: (local) => local.direct(remove("draft")),
          });
          throw thrown;
        }),
        (error) => error === thrown,
      );
      assert.equal(leaked.status, "failed");
      await assert.rejects(leaked.wait(), code("transaction_rolled_back"));
      await assert.rejects(leaked.wait(), code("transaction_rolled_back"));
      assert.deepEqual(await state(client), untouched);
      // An uncaught early wait fails the transaction like any thrown error.
      await assert.rejects(
        client.transaction(async (tx) => {
          const call = await tx.submitMutation("Ping", 1, {}, decode);
          await call.wait();
        }),
        code("transaction_uncommitted"),
      );
      assert.deepEqual(await state(client), untouched);
    });
  });

  test("a failed local callback fails its submission and the transaction", async () => {
    await run(async ({ client }) => {
      await client.direct(create("draft", "local"));
      const attempt = async (local, check) => {
        await assert.rejects(
          client.transaction(async (tx) => {
            await assert.rejects(
              tx.submitMutation("Publish", 1, publish("p"), decode, { local }),
              check,
            );
          }),
          check,
        );
        assert.deepEqual(await state(client), untouched);
      };
      // Thrown: the submission and the transaction reject with that value.
      const thrown = Error("boom");
      await attempt(
        async (local) => {
          await local.direct(remove("draft"));
          throw thrown;
        },
        (error) => error === thrown,
      );
      // A caught failed command still fails it, with that command's error.
      let failure;
      await attempt(
        async (local) => {
          await local.direct(remove("draft"));
          await local.direct(create("p", "twice")).catch((error) => {
            failure = error;
          });
        },
        (error) => error === failure && failure instanceof Error,
      );
      // Unawaited work fails it.
      await attempt(async (local) => {
        void local.direct(remove("draft"));
      }, /unawaited transaction operation/);
      // A refused submission never runs its callback.
      let ran = false;
      await assert.rejects(
        client.transaction(async (tx) => {
          await tx.submitMutation("Find", 1, {}, decode, {
            local: () => void (ran = true),
          });
        }),
        /cannot be submitted in a transaction/,
      );
      // Invalid options are refused before any command.
      await client.transaction(async (tx) => {
        for (const options of [{ local: "yes" }, { once: true }])
          await assert.rejects(
            tx.submitMutation("Ping", 1, {}, decode, options),
            code("action.invalid_options"),
          );
      });
      assert.equal(ran, false);
      assert.deepEqual(await state(client), untouched);
    });
  });

  test("the local adapter exposes only local reads and writes and expires with its callback", async () => {
    await run(async ({ client }) => {
      await client.direct(create("draft", "local"));
      let captured;
      await client.transaction(async (tx) => {
        await tx.submitMutation("Publish", 1, publish("p"), decode, {
          local: async (local) => {
            captured = local;
            for (const member of [
              "submitMutation",
              "channels",
              "savepoint",
              "watch",
              "mutate",
              "finish",
              "runCallback",
            ])
              assert.equal(member in local, false, member);
            assert.equal(local instanceof Transaction, false);
            assert.deepEqual(await local.readSql("SELECT 1 AS one"), [
              { one: 1 },
            ]);
            assert.equal(
              (await local.querySpec("Entry", { filter: {} })).length,
              2,
            );
            await local.direct(remove("draft"));
          },
        });
        // An expired handle is refused; nothing reaches the runtime.
        await assert.rejects(
          captured.direct(create("late", "late")),
          /transaction_closed/,
        );
      });
      assert.deepEqual(await state(client), {
        pending: 1,
        draft: null,
        published: "published",
      });
      assert.equal(await client.read("Entry", { id: "late" }), null);
      await assert.rejects(
        captured.read("Entry", { id: "p" }),
        /transaction_closed/,
      );
    });
  });

  test("outer transaction commands are refused while a local callback is unfinished", async () => {
    await run(async ({ client }) => {
      await client.direct(create("draft", "local"));
      // A captured parent handle inside the callback.
      await assert.rejects(
        client.transaction(async (tx) => {
          await tx.submitMutation("Publish", 1, publish("p"), decode, {
            local: async (local) => {
              await assert.rejects(tx.direct(create("x", "x")), CAPABILITY);
              await assert.rejects(
                tx.submitMutation("Ping", 1, {}, decode),
                CAPABILITY,
              );
              await assert.rejects(tx.channels.subscribe("book"), CAPABILITY);
              if (savepoints)
                await assert.rejects(
                  tx.savepoint(async () => {}),
                  CAPABILITY,
                );
              await local.direct(remove("draft"));
            },
          });
        }),
        CAPABILITY,
      );
      assert.deepEqual(await state(client), untouched);
      // Pipelined behind an unawaited submission: refused whatever the timing.
      await assert.rejects(
        client.transaction(async (tx) => {
          const submission = tx.submitMutation(
            "Publish",
            1,
            publish("p"),
            decode,
            {
              local: (local) => local.direct(remove("draft")),
            },
          );
          await assert.rejects(tx.read("Entry", { id: "draft" }), CAPABILITY);
          await submission;
          // Once the submission settled the parent handle works again.
          assert.equal(await tx.read("Entry", { id: "draft" }), null);
        }),
        CAPABILITY,
      );
      assert.deepEqual(await state(client), untouched);
    });
  });

  test("client calls, Loads and Fetches stay refused inside a local callback", async () => {
    await run(async ({ client }) => {
      await client.direct(create("draft", "local"));
      const job = await client.startLoad("Entries", 1, loadArgs);
      const active = code("transaction_active");
      await client.transaction(async (tx) => {
        await tx.submitMutation("Publish", 1, publish("p"), decode, {
          local: async (local) => {
            await assert.rejects(
              client.startLoad("Entries", 1, loadArgs),
              active,
            );
            await assert.rejects(job.wait(), active);
            await assert.rejects(
              client.invalidateLoad("Entries", loadArgs),
              active,
            );
            await assert.rejects(
              client.fetchModel("Entry", 1, { id: "draft" }, decode),
              active,
            );
            await assert.rejects(
              client.invokeQuery("Find", 1, {}, decode),
              active,
            );
            await assert.rejects(
              client.invokeAction("Ping", 1, {}, decode),
              active,
            );
            await assert.rejects(
              client.mutate({ name: "Ping", operations: [] }),
              /transaction_active/,
            );
            if (exactGuard)
              await assert.rejects(
                client.read("Entry", { id: "p" }),
                /transaction_active/,
              );
            await local.direct(remove("draft"));
          },
        });
      });
      assert.deepEqual(await state(client), {
        pending: 1,
        draft: null,
        published: "published",
      });
    });
  });

  test("close during a local callback releases its submission and commits nothing", async () => {
    await run(async ({ client, reopen }) => {
      await client.direct(create("draft", "local"));
      const entered = deferred();
      const gate = deferred();
      let submission;
      const running = client.transaction(async (tx) => {
        submission = tx.submitMutation("Publish", 1, publish("p"), decode, {
          local: async (local) => {
            entered.resolve();
            await gate.promise;
            await local.direct(remove("draft")).catch(() => {});
          },
        });
        await submission;
      });
      await entered.promise;
      const closing = client.close();
      gate.resolve();
      await assert.rejects(running);
      await assert.rejects(submission);
      await closing;
      const reopened = await reopen();
      assert.deepEqual(await state(reopened), untouched);
      // A provisional Call the close rolled back.
      const inside = deferred();
      const hold = deferred();
      let call;
      const holding = reopened.transaction(async (tx) => {
        call = await tx.submitMutation("Ping", 1, {}, decode);
        inside.resolve();
        await hold.promise;
      });
      await inside.promise;
      const closed = reopened.close();
      hold.resolve();
      await assert.rejects(holding);
      await closed;
      assert.equal(call.status, "failed");
      await assert.rejects(call.wait(), code("transaction_rolled_back"));
      assert.deepEqual(await state(await reopen()), untouched);
    });
  });

  test("an onStore callback cannot submit a Mutation or run a local callback", async () => {
    let refused = 0;
    let ran = false;
    await run(
      async ({ client }) => {
        await client.invokeAction("Ping", 1, {}, decode);
        const batch = JSON.parse(await client.freeze());
        const completions = batch.mutations.map(({ callId }) => ({
          callId,
          outcome: { status: "succeeded", result: null },
        }));
        await assert.rejects(
          client.acknowledge(batch.batchSequence, {
            clientId: batch.clientId,
            batchSequence: batch.batchSequence,
            rejections: [],
            completions,
            records: [
              {
                model: "Entry",
                identity: { id: "s" },
                stamp: 1,
                state: { text: "server", note: null },
              },
            ],
          }),
          /store hook|submit a Mutation/,
        );
        assert.equal(refused, 1);
        assert.equal(ran, false);
        assert.equal(await client.read("Entry", { id: "s" }), null);
      },
      {
        onStore: {
          Entry: async (tx) => {
            await assert.rejects(
              tx.submitMutation("Ping", 1, {}, decode, {
                local: () => void (ran = true),
              }),
              /store hook cannot submit a Mutation/,
            );
            refused++;
          },
        },
      },
    );
  });

  if (savepoints)
    test("a savepoint rollback ends only the Calls of its scope", async () => {
      await run(async ({ client, connect }) => {
        await client.direct(create("draft", "local"));
        const { kept, discarded } = await client.transaction(async (tx) => {
          const kept = await tx.submitMutation("Ping", 1, {}, decode);
          let discarded;
          await tx
            .savepoint(async () => {
              discarded = await tx.submitMutation(
                "Publish",
                1,
                publish("p"),
                decode,
                {
                  local: (local) => local.direct(remove("draft")),
                },
              );
              throw Error("undo");
            })
            .catch(() => {});
          assert.equal(discarded.status, "failed");
          await assert.rejects(
            discarded.wait(),
            code("transaction_rolled_back"),
          );
          await assert.rejects(kept.wait(), code("transaction_uncommitted"));
          return { kept, discarded };
        });
        assert.deepEqual(await state(client), {
          pending: 1,
          draft: "local",
          published: null,
        });
        await connect();
        assert.equal((await kept.wait()).error, null);
        await assert.rejects(discarded.wait(), code("transaction_rolled_back"));
      });
    });
}
