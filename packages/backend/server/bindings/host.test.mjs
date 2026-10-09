import test from "node:test";
import assert from "node:assert/strict";
import { createHost } from "./host.mts";
import { Session } from "./session.mts";
import { effectsFor, loadEffectsFor, bootstrapEffectsFor } from "./effects.mts";

// Extraction must retain the exact application transaction and mutable Session,
// including recursive host calls made while preparing a viewer Loader.
test("nested Loader preparation retains transaction, principal and savepoint state", async () => {
  const tx = { applicationTransaction: true };
  const session = new Session();
  const principal = "viewer";
  const model = {
    name: "Task",
    identity: ["id"],
    fields: [{ name: "id", type: { kind: "scalar", name: "string" } }],
  };
  const retainedTransactions = [];
  const observations = [];
  const dependencies = {
    config: "retained-config",
    descriptor: { schema: { models: [model] } },
    schemaModels: [model],
    actionTable: new Map(),
    actionHandlers: new Map(),
    loaderTable: new Map([
      [
        "Task:1",
        async (call) => {
          assert.strictEqual(call.tx, tx);
          assert.equal(call.userId, principal);
          observations.push("load");
          return call.ids;
        },
      ],
    ]),
    createEffects: effectsFor([model]),
    createLoadEffects: loadEffectsFor([model]),
    createBootstrapEffects: bootstrapEffectsFor([model]),
    refusal: (error) => {
      throw error;
    },
    onError: (error) => {
      throw error;
    },
    options: {
      database: {
        transaction: () => {
          throw new Error("must retain the caller's transaction");
        },
        persistence: (retained) => {
          retainedTransactions.push(retained);
          return {
            call: async (request) => {
              observations.push(request.op);
              return null;
            },
          };
        },
      },
      protocol5: {
        authorizeStream: (owner, stream, retained) => {
          assert.equal(owner, principal);
          assert.equal(stream, "User:viewer");
          assert.strictEqual(retained, tx);
          return true;
        },
      },
      loaderHooks: {
        task: {
          prepareForViewer: async (call) => {
            assert.strictEqual(call.tx, tx);
            assert.equal(call.userId, principal);
            call.streams("User:viewer").track.task("task");
            observations.push("prepare");
          },
        },
      },
    },
    native: {
      settleExternal05: async (config, settlement, callback) => {
        assert.equal(config, "retained-config");
        assert.equal(
          JSON.parse(settlement).declarations[0].stream,
          "User:viewer",
        );
        await callback(
          JSON.stringify({
            op: "applyStreamMembers",
            deltas: [{ publish: true, stream: "User:viewer" }],
          }),
        );
        assert(session.touched.has("User:viewer"));
        return "{}";
      },
    },
  };
  const host = createHost(dependencies, tx, session);
  const invoke = async (request) =>
    JSON.parse(await host(JSON.stringify(request)));
  await invoke({ op: "savepoint", ordinal: 7 });
  assert.equal(
    await invoke({
      op: "protocol05",
      request: {
        op: "admit",
        owner: principal,
        context: { stream: "User:viewer" },
      },
    }),
    true,
  );
  assert.deepEqual(
    await invoke({
      op: "load",
      model: "Task",
      version: 1,
      identities: [{ id: "task" }],
      owner: principal,
    }),
    [{ id: "task" }],
  );
  assert.deepEqual(observations, [
    "savepoint",
    "publicationFence",
    "prepare",
    "applyStreamMembers",
    "load",
  ]);
  assert.equal(retainedTransactions.length, 2);
  assert(retainedTransactions.every((retained) => retained === tx));
  await invoke({ op: "rollback", ordinal: 7 });
  assert.equal(session.touched.size, 0);
  await session.assertCommittable();
});
