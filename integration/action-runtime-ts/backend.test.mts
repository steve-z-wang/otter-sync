import assert from "node:assert/strict";
import test from "node:test";
import {
  CallRejected,
  Moment,
  Pin,
  createBackend,
  type MutationHandlerCall,
  type Mutations,
  type Queries,
  type Loaders,
  type PutV1Input,
  type Stream,
  type RecordDeclaration,
} from "./backend.ts";
import type { Todo } from "./generated.ts";
import type { StreamIntent } from "../../packages/server/host-contract.mts";

test("generated backend decodes Date values and declares canonical identities through its handles", async () => {
  const first = "2026-01-01T00:00:00.000Z";
  const second = "2026-01-02T00:00:00.000Z";
  const seen: unknown[] = [];
  type Tx = { rows: Map<string, Todo> };
  const put = async ({ ctx, args }: MutationHandlerCall<Tx, PutV1Input>) => {
    assert.equal(args.when.getUTCFullYear(), 2026);
    assert.equal(args.todo.at.getUTCFullYear(), 2026);
    assert.deepEqual(args.statuses, ["open", "closed"]);
    assert.equal(args.note, null);
    ctx.tx.rows.set(args.todo.id, args.todo);
    // The input Todo is inferred by the engine; touches name extra records.
    const moment = { at: new Date(first) };
    ctx.invalidate.moment(moment);
    moment.at = new Date(second);
    ctx.invalidate.moment(moment);
    ctx.invalidate.moment({ at: new Date(second) });
    const scope = ctx.streams(["todos"]);
    scope.track.todo(args.todo);
    scope.track([
      Moment({ at: new Date("2026-01-03T00:00:00.000Z") }),
      Pin({ todo: args.todo.id, at: args.todo.at }),
    ]);
    scope.invalidate.pin({ todo: args.todo.id, at: args.todo.at });
    return {
      todo: { id: args.todo.id },
      echoed: new Date(args.when.getTime()),
      status: args.todo.status,
    };
  };
  const mutations: Mutations<Tx> = {
    put: { v1: put, v2: put },
    async change({ args }) {
      return { todo: null, echoed: args.at };
    },
    async clear() {},
    async ping() {},
    async find() {
      return { todo: null };
    },
    async mark() {},
    async removeMoment({ args }) {
      return { at: args.moment.at };
    },
  };
  const queries: Queries<Tx> = {
    ...emptyQueries(),
    find: {
      async v2() {
        return { todo: null };
      },
    },
  };
  const loaders: Loaders<Tx> = {
    async todo({ ids, tx }) {
      return ids.map(({ id }) => tx.rows.get(id) ?? null);
    },
    async moment({ ids }) {
      assert.equal(ids[0]?.at.getUTCFullYear(), 2026);
      return ids.map(() => null);
    },
    async pin({ ids }) {
      return ids.map(() => null);
    },
  };
  const native = {
    validateConfig() {},
    async processAction(
      _config: string,
      _owner: string,
      _request: string,
      callback: (request: string) => Promise<string>,
    ) {
      for (const version of [1, 2]) {
        const handled = JSON.parse(
          await callback(
            JSON.stringify({
              op: "handleAction",
              context: { binding: { stream: "todos" } },
              name: "Put",
              version,
              owner: "alice",
              callId: `call-${version}`,
              ordinal: version,
              arguments: {
                todo: {
                  id: "one",
                  title: "saved",
                  at: first,
                  status: "open",
                  note: null,
                },
                when: first,
                statuses: ["open", "closed"],
                note: null,
              },
            }),
          ),
        );
        seen.push(handled);
      }
      const loaded = JSON.parse(
        await callback(
          JSON.stringify({
            op: "load",
            model: "Moment",
            version: 1,
            owner: "alice",
            identities: [{ at: first }],
          }),
        ),
      );
      seen.push(loaded);
      return "{}";
    },
    async processPush() {
      return "{}";
    },
    async processFetch() {
      return "{}";
    },
    async processPull() {
      return "{}";
    },
    async settleExternal() {
      return "{}";
    },
    async negotiateLive() {
      return "{}";
    },
    async pullLive() {
      return "{}";
    },
    liveEvent() {
      return "[]";
    },
    liveClose() {},
  };
  const backend = createBackend<Tx>({
    database: {
      transaction: async (body) => body({ rows: new Map() }),
      persistence: () => ({ call: async () => null }),
    },
    authenticate: () => "alice",
    mutations,
    queries,
    loaders,

    native,
  });
  await backend.action("alice", "{}");
  for (const handled of seen.slice(0, 2) as {
    outputs: { todo: { id: string }; echoed: string; status: string };
    changes: { model: string; identity: Record<string, unknown> }[];
    declarations: StreamIntent[];
  }[]) {
    assert.deepEqual(handled.outputs.todo, { id: "one" });
    assert.equal(handled.outputs.echoed, first);
    assert.equal(handled.outputs.status, "open");
    // Each declaration copied its identity at the call, once per record.
    assert.deepEqual(handled.changes, []);
    // Only identity fields, in declaration order; the engine reduces them.
    const add = (model: string, identity: object) => ({
      kind: "track",
      stream: "todos",
      record: { model, identity },
    });
    assert.deepEqual(handled.declarations, [
      { kind: "invalidate", streams: null, record: { model: "Moment", identity: { at: first } } },
      { kind: "invalidate", streams: null, record: { model: "Moment", identity: { at: second } } },
      { kind: "invalidate", streams: null, record: { model: "Moment", identity: { at: second } } },
      add("Todo", { id: "one" }),
      add("Moment", { at: "2026-01-03T00:00:00.000Z" }),
      add("Pin", { todo: "one", at: first }),
      {
        kind: "invalidate",
        streams: ["todos"],
        record: { model: "Pin", identity: { todo: "one", at: first } },
      },
    ]);
  }
  assert.ok(CallRejected.prototype instanceof Error);
});

type Tx = { rows: Map<string, Todo> };
/** A native stub that sends each request to the host and keeps its answer. */
function nativeHost(requests: object[], answers: unknown[]) {
  return {
    validateConfig() {},
    async processAction(
      _config: string,
      _owner: string,
      _request: string,
      callback: (request: string) => Promise<string>,
    ) {
      for (const request of requests)
        answers.push(JSON.parse(await callback(JSON.stringify(request))));
      return "{}";
    },
    async processPush() {
      return "{}";
    },
    async processFetch() {
      return "{}";
    },
    async processPull() {
      return "{}";
    },
    async settleExternal() {
      return "{}";
    },
    async negotiateLive() {
      return "{}";
    },
    async pullLive() {
      return "{}";
    },
    liveEvent() {
      return "[]";
    },
    liveClose() {},
  };
}
const database = {
  transaction: async <R,>(body: (tx: Tx) => Promise<R>) =>
    body({ rows: new Map() }),
  persistence: () => ({ call: async () => null }),
};
const loaders: Loaders<Tx> = {
  async todo({ ids }) {
    return ids.map(() => null);
  },
  async moment({ ids }) {
    return ids.map(() => null);
  },
  async pin({ ids }) {
    return ids.map(() => null);
  },
};
/** A TodoPages handler that completes with no identities. */
function emptyQueries() {
  return {
    todoPages: async () => ({ todos: [], moments: [] }),
  };
}
function mutationHandlers(): Mutations<Tx> {
  const none = async () => {};
  return {
    put: {
      v1: async ({ args }) => ({
        todo: { id: args.todo.id },
        echoed: args.when,
        status: "open",
      }),
      v2: async ({ args }) => ({
        todo: { id: args.todo.id },
        echoed: args.when,
        status: "open",
      }),
    },
    change: async ({ args }) => ({ todo: null, echoed: args.at }),
    clear: none,
    ping: none,
    find: async () => ({ todo: null }),
    mark: none,
    removeMoment: async ({ args }) => ({ at: args.moment.at }),
  };
}

test("Query handlers receive track-only capabilities and no invalidation", async () => {
  const seen: Record<string, unknown>[] = [];
  const answers: unknown[] = [];
  const at = "2026-01-01T00:00:00.000Z";
  const call = (name: string, version: number) => ({
    op: "handleAction",
              context: { binding: { stream: "todos" } },
    name,
    version,
    owner: "alice",
    callId: `${name}-${version}`,
    ordinal: 1,
    arguments: { at },
  });
  const backend = createBackend<Tx>({
    database,
    authenticate: () => "alice",
    mutations: {
      ...mutationHandlers(),
      find: async ({ ctx, args }) => {
        seen.push({ kind: "mutation", keys: Object.keys(ctx).sort() });
        ctx.invalidate.todo({ id: "one" });
        assert.ok(args.at instanceof Date);
        return { todo: { id: "one" } };
      },
    },
    queries: {
      ...emptyQueries(),
      find: {
        async v2({ ctx, args }) {
          seen.push({ kind: "query", keys: Object.keys(ctx).sort() });
          assert.equal(ctx.userId, "alice");
          assert.equal(ctx.callId, "Find-2");
          assert.ok(args.at instanceof Date);
          assert.equal(typeof ctx.stream.track, "function");
          assert.equal("invalidate" in ctx.stream, false);
          // @ts-expect-error a Query context has no change declaration
          assert.equal(ctx.invalidate, undefined);
          return { todo: { id: "one" } };
        },
      },
    },
    loaders,

    native: nativeHost([call("Find", 1), call("Find", 2)], answers),
  });
  await backend.action("alice", "{}");
  assert.deepEqual(seen, [
    { kind: "mutation", keys: ["callId", "invalidate", "stream", "streams", "tx", "userId"] },
    { kind: "query", keys: ["callId", "stream", "streams", "tx", "userId"] },
  ]);
  assert.deepEqual(answers, [
    {
      outputs: { todo: { id: "one" } },
      changes: [],
      declarations: [{ kind: "invalidate", streams: null, record: { model: "Todo", identity: { id: "one" } } }],
    },
    { outputs: { todo: { id: "one" } }, changes: [], declarations: [] },
  ]);
});

test("registration is checked per kind at startup: missing, extra and wrong-kind", () => {
  const start = (
    options: Partial<Parameters<typeof createBackend<Tx>>[0]>,
  ): unknown =>
    createBackend<Tx>({
      database,
      authenticate: () => "alice",
      mutations: mutationHandlers(),
      queries: { ...emptyQueries(), find: { v2: async () => ({ todo: null }) } },
      loaders,

      native: nativeHost([], []),
      ...options,
    } as Parameters<typeof createBackend<Tx>>[0]);
  start({});
  const cases: [Partial<Parameters<typeof createBackend<Tx>>[0]>, RegExp][] = [
    [{ queries: undefined }, /Missing query find for Find v2/],
    [{ queries: {} as Queries<Tx> }, /Missing query find for Find v2/],
    // A bare function is v1 shorthand; Find's only Query version is v2.
    [
      { queries: { find: async () => ({ todo: null }) } as unknown as Queries<Tx> },
      /Query find must register v2 of Find; a function registers v1 only/,
    ],
    [
      {
        queries: {
          ...emptyQueries(),
          find: { v2: async () => ({ todo: null }) },
          ping: async () => {},
        } as unknown as Queries<Tx>,
      },
      /queries\.ping: Ping v1 \(mutation\) retains no query version; register it under mutations/,
    ],
    [
      {
        mutations: {
          ...mutationHandlers(),
          search: async () => {},
        } as unknown as Mutations<Tx>,
      },
      /Unknown mutation search: no retained mutation search/,
    ],
    [
      {
        mutations: {
          ...mutationHandlers(),
          find: { v1: async () => ({ todo: null }), v2: async () => ({ todo: null }) },
        } as unknown as Mutations<Tx>,
      },
      /Unknown mutation find\.v2 for Find: retained mutation versions are v1/,
    ],
    [
      { handlers: { find: async () => ({ todo: null }) } } as never,
      /Handler find names Find v1 \(mutation\), v2 \(query\)/,
    ],
    [
      {
        queries: {
          ...emptyQueries(),
          find: { v1: async () => ({ todo: null }), v2: async () => ({ todo: null }) },
        } as unknown as Queries<Tx>,
      },
      /Unknown query find\.v1 for Find: retained query versions are v2/,
    ],
    [
      { handlers: { ping: async () => {} } } as never,
      /Handler ping names Ping v1 \(mutation\); register each version under mutations or queries by its kind/,
    ],
    [{ queries: { find: { v2: async () => ({ todo: null }) } } as Queries<Tx> }, /Missing query todoPages/],
    [{ mutations: { ...mutationHandlers(), todoPages: async () => ({}) } as never }, /retains no mutation version/],

  ];
  for (const [options, message] of cases)
    assert.throws(() => start(options), message);
});

test("declaration handles close when the handler or external body settles, even when it throws", async () => {
  const escaped: {
    streams: (names: readonly string[]) => Stream;
    invalidate: RecordDeclaration;
  }[] = [];
  const answers: unknown[] = [];
  const settled: string[] = [];
  const at = "2026-01-01T00:00:00.000Z";
  const call = (ordinal: number) => ({
    op: "handleAction",
              context: { binding: { stream: "todos" } },
    name: "Find",
    version: 1,
    owner: "alice",
    callId: `find-${ordinal}`,
    ordinal,
    arguments: { at },
  });
  const native = {
    ...nativeHost([call(1), call(2)], answers),
    async settleExternal(_config: string, settlement: string) {
      settled.push(settlement);
      return "[]";
    },
  };
  const backend = createBackend<Tx>({
    database,
    authenticate: () => "alice",
    mutations: {
      ...mutationHandlers(),
      find: async ({ ctx }) => {
        escaped.push({ streams: ctx.streams, invalidate: ctx.invalidate });
        ctx.streams(["found"]).track.todo({ id: "one" });
        if (ctx.callId === "find-2") throw new Error("after declaring");
        return { todo: null };
      },
    },
    queries: { ...emptyQueries(), find: { v2: async () => ({ todo: null }) } },
    loaders,

    native,
    onError: () => {},
  });
  await backend.action("alice", "{}");
  assert.deepEqual(answers, [
    {
      outputs: { todo: null },
      changes: [],
      declarations: [
        {
          kind: "track",
          stream: "found",
          record: { model: "Todo", identity: { id: "one" } },
            },
      ],
    },
    { error: "after declaring" },
  ]);
  // The external body answers its own value; its declarations settle after it.
  const value = await backend.transaction(async ({ streams: scope, invalidate: touch }) => {
    escaped.push({ streams: scope, invalidate: touch });
    touch.pin({ todo: "one", at: new Date(at) });
    scope(["found"]).invalidate.todo({ id: "one" });
    return { arbitrary: [1, 2] };
  });
  assert.deepEqual(value, { arbitrary: [1, 2] });
  assert.deepEqual(JSON.parse(settled[0]!), {
    changes: [],
    declarations: [
      { kind: "invalidate", streams: null, record: { model: "Pin", identity: { todo: "one", at } } },
      {
        kind: "invalidate",
        streams: ["found"],
        record: { model: "Todo", identity: { id: "one" } },
      },
    ],
  });
  await assert.rejects(
    backend.transaction(async ({ streams: scope, invalidate: touch }) => {
      escaped.push({ streams: scope, invalidate: touch });
      throw new Error("body failed");
    }),
    /body failed/,
  );
  assert.equal(settled.length, 1, "a failed body settles nothing");
  assert.equal(escaped.length, 4);
  for (const { streams: scope, invalidate: touch } of escaped) {
    assert.throws(() => scope(["late"]), /closed/);
    assert.throws(() => touch.todo({ id: "late" }), /closed/);
  }
});
