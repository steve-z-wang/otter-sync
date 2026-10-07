import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { createBackend } from "../../../packages/server/index.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const database = () => ({
  transaction: async () => {
    throw new Error("startup must not open a transaction");
  },
  persistence: () => {
    throw new Error("startup must not persist");
  },
});
const authenticate = () => "alice";
const schema = {
  enums: [],
  models: [
    {
      name: "Task",
      version: 1,
      identity: ["id"],
      fields: [
        {
          name: "id",
          type: { kind: "scalar", name: "string" },
          nullable: false,
        },
        {
          name: "title",
          type: { kind: "scalar", name: "string" },
          nullable: false,
        },
      ],
    },
  ],
  actions: [],
};
const config = { schema, loaders: ["Task"] };
test("loader registration names every retained model version and a function means v1 only", () => {
  const base = { ...config, schema: structuredClone(schema) };
  const contract = (version) => ({
    name: "Task",
    version,
    identity: ["id"],
    fields: schema.models[0].fields,
    enums: [],
  });
  const register = (models, loaders, currentVersion = 1) => {
    const c = { ...base, schema: structuredClone(schema), models };
    c.schema.models[0].version = currentVersion;
    return createBackend({
      config: c,
      native,
      database: database(),
      authenticate,
      loaders,
    });
  };
  const both = [contract(1), contract(2)];
  assert.throws(
    () => register(both, { task: async () => [] }, 2),
    /Loader task must register v1, v2 of Task; a function registers v1 only/,
  );
  assert.throws(
    () => register([contract(2)], { task: async () => [] }, 2),
    /Loader task must register v2 of Task; a function registers v1 only/,
  );
  assert.throws(
    () => register(both, { task: { v1: async () => [] } }, 2),
    /Missing loader task\.v2 for Task v2/,
  );
  assert.throws(
    () =>
      register(
        both,
        {
          task: { v1: async () => [], v2: async () => [], v3: async () => [] },
        },
        2,
      ),
    /Unknown loader task\.v3 for Task: retained versions are v1, v2/,
  );
  assert.throws(
    () => register(both, { task: { v1: async () => [], v2: "later" } }, 2),
    /Loader task\.v2 for Task v2 must be a function/,
  );
  assert.throws(
    () => register(both, { task: null }, 2),
    /Missing loader task for Task v1, v2/,
  );
  // The engine refuses a schema whose current version is not a retained contract.
  assert.throws(
    () => register([contract(1)], { task: { v1: async () => [] } }, 2),
    /not a retained contract/,
  );
  register(both, { task: { v1: async () => [], v2: async () => [] } }, 2);
  register([contract(1)], { task: { v1: async () => [] } });
  register([contract(1)], { task: async () => [] });
  register([], { task: async () => [] });
});

test("only actual5 native entrypoints are exported; startup preserves device-only and accessor validation", () => {
  for (const name of [
    "processRead05",
    "processDelivery05",
    "processMaterialization05",
    "processLive05",
    "processBatchMember",
    "validateMutationBatch",
    "encodeBatchAcknowledgement",
    "settleExternal05",
    "handshake05",
    "negotiateLive",
    "liveEvent",
    "liveClose",
  ])
    assert.equal(typeof native[name], "function", name);
  for (const name of [
    "processPush",
    "processAction",
    "processFetch",
    "processPull",
    "processLoad",
    "validateLoadBatch",
    "encodeLoadBatch",
    "settleExternal",
    "serverMaterializationId",
    "pullLive",
  ])
    assert.equal(native[name], undefined, name);
  const create = (c, loaders = { task: async () => [] }) =>
    createBackend({
      config: c,
      native,
      database: database(),
      authenticate,
      loaders,
    });
  assert.throws(
    () => create({ ...config, protocol4: {} }),
    /legacy server configuration/,
  );
  assert.throws(
    () => create(config, { task: async () => [], typo: async () => [] }),
    /Unknown loader typo/,
  );
  const draft = { ...schema.models[0], name: "Draft" };
  assert.doesNotThrow(() =>
    create({
      ...config,
      schema: { ...schema, models: [...schema.models, draft] },
    }),
  );
  assert.throws(
    () =>
      create({
        ...config,
        schema: {
          ...schema,
          models: [...schema.models, { ...draft, name: "task" }],
        },
      }),
    /both generate the accessor task/,
  );
  const action = {
    name: "Write",
    version: 1,
    kind: "mutation",
    inputs: [
      {
        kind: "model",
        name: "draft",
        model: "Draft",
        operation: "create",
        cardinality: "single",
      },
    ],
    outputs: [],
  };
  assert.throws(
    () =>
      create(
        {
          ...config,
          schema: { ...schema, models: [draft], actions: [action] },
        },
        {},
      ),
    /which has no Loader/,
  );
});
test("Mutation registration retains every exact version; a function means v1 only", () => {
  const action = {
    name: "Write",
    version: 1,
    kind: "mutation",
    inputs: [],
    outputs: [],
  };
  const register = (actions, mutations) =>
    createBackend({
      config: { ...config, schema: { ...schema, actions } },
      native,
      database: database(),
      authenticate,
      mutations,
      loaders: { task: async () => [] },
    });
  const both = [action, { ...action, version: 2 }];
  assert.throws(
    () => register(both, { write: async () => {} }),
    /must register v1, v2/,
  );
  assert.throws(
    () => register(both, { write: { v1: async () => {} } }),
    /Missing mutation write.v2/,
  );
  assert.throws(
    () =>
      register(both, {
        write: { v1: async () => {}, v2: async () => {}, v3: async () => {} },
      }),
    /Unknown mutation write.v3/,
  );
  assert.throws(
    () => register(both, { write: { v1: async () => {}, v2: "later" } }),
    /must be a function/,
  );
  assert.doesNotThrow(() =>
    register(both, { write: { v1: async () => {}, v2: async () => {} } }),
  );
  assert.doesNotThrow(() => register([action], { write: async () => {} }));
});
