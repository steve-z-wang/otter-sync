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
  type Loads,
  type PutV1Input,
  type Channel,
  type Touch,
} from "./backend.ts";
import type { Todo } from "./generated.ts";
import type { ChannelIntent } from "../../packages/server/host-contract.mts";

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
    ctx.touch.moment(moment);
    moment.at = new Date(second);
    ctx.touch.moment(moment);
    ctx.touch.moment({ at: new Date(second) });
    const channel = ctx.channel("todos");
    channel.todo.add(args.todo);
    channel.add([
      Moment({ at: new Date("2026-01-03T00:00:00.000Z") }),
      Pin({ todo: args.todo.id, at: args.todo.at }),
    ]);
    channel.pin.remove({ todo: args.todo.id, at: args.todo.at });
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
    loads: emptyLoads(),
    native,
  });
  await backend.action("alice", "{}");
  for (const handled of seen.slice(0, 2) as {
    outputs: { todo: { id: string }; echoed: string; status: string };
    changes: { model: string; identity: Record<string, unknown> }[];
    memberships: ChannelIntent[];
  }[]) {
    assert.deepEqual(handled.outputs.todo, { id: "one" });
    assert.equal(handled.outputs.echoed, first);
    assert.equal(handled.outputs.status, "open");
    // Each declaration copied its identity at the call, once per record.
    assert.deepEqual(handled.changes, [
      { model: "Moment", identity: { at: first } },
      { model: "Moment", identity: { at: second } },
    ]);
    // Only identity fields, in declaration order; the engine reduces them.
    const add = (model: string, identity: object) => ({
      kind: "add",
      channel: "todos",
      record: { model, identity },
      tags: [],
    });
    assert.deepEqual(handled.memberships, [
      add("Todo", { id: "one" }),
      add("Moment", { at: "2026-01-03T00:00:00.000Z" }),
      add("Pin", { todo: "one", at: first }),
      {
        kind: "remove",
        channel: "todos",
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
function emptyLoads(): Loads<Tx> {
  return {
    todoPages: async () => ({ data: { todos: [], moments: [] }, next: null }),
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

test("Query handlers receive no effect capabilities and settle without effects", async () => {
  const seen: Record<string, unknown>[] = [];
  const answers: unknown[] = [];
  const at = "2026-01-01T00:00:00.000Z";
  const call = (name: string, version: number) => ({
    op: "handleAction",
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
        ctx.touch.todo({ id: "one" });
        assert.ok(args.at instanceof Date);
        return { todo: { id: "one" } };
      },
    },
    queries: {
      find: {
        async v2({ ctx, args }) {
          seen.push({ kind: "query", keys: Object.keys(ctx).sort() });
          assert.equal(ctx.userId, "alice");
          assert.equal(ctx.callId, "Find-2");
          assert.ok(args.at instanceof Date);
          // @ts-expect-error a Query context has no membership writer
          assert.equal(ctx.channel, undefined);
          // @ts-expect-error a Query context has no change declaration
          assert.equal(ctx.touch, undefined);
          return { todo: { id: "one" } };
        },
      },
    },
    loaders,
    loads: emptyLoads(),
    native: nativeHost([call("Find", 1), call("Find", 2)], answers),
  });
  await backend.action("alice", "{}");
  assert.deepEqual(seen, [
    { kind: "mutation", keys: ["callId", "channel", "touch", "tx", "userId"] },
    { kind: "query", keys: ["callId", "tx", "userId"] },
  ]);
  assert.deepEqual(answers, [
    {
      outputs: { todo: { id: "one" } },
      changes: [{ model: "Todo", identity: { id: "one" } }],
      memberships: [],
    },
    { outputs: { todo: { id: "one" } }, changes: [], memberships: [] },
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
      queries: { find: { v2: async () => ({ todo: null }) } },
      loaders,
      loads: emptyLoads(),
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
          find: { v1: async () => ({ todo: null }), v2: async () => ({ todo: null }) },
        } as unknown as Queries<Tx>,
      },
      /Unknown query find\.v1 for Find: retained query versions are v2/,
    ],
    [
      { handlers: { ping: async () => {} } } as never,
      /Handler ping names Ping v1 \(mutation\); register each version under mutations or queries by its kind/,
    ],
    // Loads register under `loads`, and nothing else does.
    [{ loads: undefined }, /Missing load todoPages for TodoPages v1/],
    [
      { loads: { todoPages: { v2: emptyLoads().todoPages } } } as never,
      /Missing load todoPages\.v1 for TodoPages v1/,
    ],
    [
      {
        queries: {
          find: { v2: async () => ({ todo: null }) },
          todoPages: emptyLoads().todoPages,
        } as unknown as Queries<Tx>,
      },
      /queries\.todoPages: TodoPages v1 \(load\) retains no query version; register it under loads/,
    ],
    [
      {
        mutations: {
          ...mutationHandlers(),
          todoPages: emptyLoads().todoPages,
        } as unknown as Mutations<Tx>,
      },
      /mutations\.todoPages: TodoPages v1 \(load\) retains no mutation version; register it under loads/,
    ],
    [
      { loads: { ...emptyLoads(), ping: async () => {} } } as never,
      /loads\.ping: Ping v1 \(mutation\) retains no load version; register it under mutations/,
    ],
    [
      { loads: { ...emptyLoads(), find: async () => ({}) } } as never,
      /loads\.find: Find v1 \(mutation\), v2 \(query\) retains no load version; register it under mutations or queries/,
    ],
    [
      { handlers: { todoPages: async () => {} } } as never,
      /Handler todoPages names TodoPages v1 \(load\); register each version under mutations, queries or loads by its kind/,
    ],
  ];
  for (const [options, message] of cases)
    assert.throws(() => start(options), message);
});

test("declaration handles close when the handler or external body settles, even when it throws", async () => {
  const escaped: {
    channel: (name: string) => Channel;
    touch: Touch;
  }[] = [];
  const answers: unknown[] = [];
  const settled: string[] = [];
  const at = "2026-01-01T00:00:00.000Z";
  const call = (ordinal: number) => ({
    op: "handleAction",
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
        escaped.push({ channel: ctx.channel, touch: ctx.touch });
        ctx.channel("found").todo.add({ id: "one" });
        if (ctx.callId === "find-2") throw new Error("after declaring");
        return { todo: null };
      },
    },
    queries: { find: { v2: async () => ({ todo: null }) } },
    loaders,
    loads: emptyLoads(),
    native,
    onError: () => {},
  });
  await backend.action("alice", "{}");
  assert.deepEqual(answers, [
    {
      outputs: { todo: null },
      changes: [],
      memberships: [
        {
          kind: "add",
          channel: "found",
          record: { model: "Todo", identity: { id: "one" } },
          tags: [],
        },
      ],
    },
    { error: "after declaring" },
  ]);
  // The external body answers its own value; its declarations settle after it.
  const value = await backend.transaction(async ({ channel, touch }) => {
    escaped.push({ channel, touch });
    touch.pin({ todo: "one", at: new Date(at) });
    channel("found").todo.remove({ id: "one" });
    return { arbitrary: [1, 2] };
  });
  assert.deepEqual(value, { arbitrary: [1, 2] });
  assert.deepEqual(JSON.parse(settled[0]!), {
    changes: [{ model: "Pin", identity: { todo: "one", at } }],
    memberships: [
      {
        kind: "remove",
        channel: "found",
        record: { model: "Todo", identity: { id: "one" } },
      },
    ],
  });
  await assert.rejects(
    backend.transaction(async ({ channel, touch }) => {
      escaped.push({ channel, touch });
      throw new Error("body failed");
    }),
    /body failed/,
  );
  assert.equal(settled.length, 1, "a failed body settles nothing");
  assert.equal(escaped.length, 4);
  for (const { channel, touch } of escaped) {
    assert.throws(() => channel("late"), /closed/);
    assert.throws(() => touch.todo({ id: "late" }), /closed/);
  }
});

test("Load handlers take decoded args and a context with a channel and no touch, and answer identity pages", async () => {
  const at = "2026-01-01T00:00:00.000Z";
  const seen: Record<string, unknown>[] = [];
  const answers: unknown[] = [];
  const errors: unknown[] = [];
  const page = (callId: string, continuation: unknown) => ({
    op: "handleLoad",
    name: "TodoPages",
    version: 1,
    arguments: { since: at, statuses: ["open", "closed"] },
    continuation,
    owner: "alice",
    callId,
    loadId: "load-1",
  });
  const requests = [
    page("first", null),
    page("null-state", { state: null }),
    page("nan", { state: 1 }),
    page("unsafe", { state: 2 }),
  ];
  const native = {
    ...nativeHost([], []),
    validateLoadBatch: () => ["item"],
    // Assembles the committed pages as the engine's encoder would.
    encodeLoadBatch: (_items: string[], answers: { page: string }[]) =>
      `{"loads":[${answers.map(({ page }) => page).join(",")}]}`,
    async processLoad(
      _config: string,
      _owner: string,
      _item: string,
      callback: (request: string) => Promise<string>,
    ) {
      for (const request of requests)
        answers.push(JSON.parse(await callback(JSON.stringify(request))));
      return '{"page":1}';
    },
  };
  const loads: Loads<Tx> = {
    async todoPages({ ctx, args, continuation }) {
      seen.push({
        keys: Object.keys(ctx).sort(),
        callId: ctx.callId,
        loadId: ctx.loadId,
        userId: ctx.userId,
        since: args.since instanceof Date && args.since.toISOString(),
        statuses: args.statuses,
        continuation,
      });
      // Type-legal numbers the JSON bridge would silently coerce.
      if (ctx.callId === "nan")
        return { data: { todos: [], moments: [] }, next: { state: NaN } };
      if (ctx.callId === "unsafe")
        return {
          data: { todos: [], moments: [] },
          next: { state: { n: 2 ** 53 } },
        };
      return {
        data: { todos: [{ id: "one" }], moments: [{ at: args.since }] },
        next: continuation === null ? { state: { after: "one" } } : null,
      };
    },
  };
  const backend = createBackend<Tx>({
    database,
    authenticate: () => "alice",
    mutations: mutationHandlers(),
    queries: { find: { v2: async () => ({ todo: null }) } },
    loaders,
    loads,
    native,
    onError: (error) => errors.push(error),
  });
  assert.equal(await backend.loads("alice", "{}"), '{"loads":[{"page":1}]}');
  assert.deepEqual(
    seen.map(({ keys, since, statuses, loadId, userId }) => ({
      keys,
      since,
      statuses,
      loadId,
      userId,
    })),
    requests.map(() => ({
      keys: ["callId", "channel", "loadId", "tx", "userId"],
      since: at,
      statuses: ["open", "closed"],
      loadId: "load-1",
      userId: "alice",
    })),
  );
  // First and end are `null`; `{state: null}` is a distinct continuation.
  assert.deepEqual(
    seen.map(({ continuation }) => continuation),
    [null, { state: null }, { state: 1 }, { state: 2 }],
  );
  assert.deepEqual(answers, [
    {
      data: { todos: [{ id: "one" }], moments: [{ at }] },
      next: { state: { after: "one" } },
    },
    { data: { todos: [{ id: "one" }], moments: [{ at }] }, next: null },
    { rejection: "load.invalid_continuation" },
    { rejection: "load.invalid_continuation" },
  ]);
  assert.equal(errors.length, 2);
});

/** Claim/save storage in memory: enough of the host contract for real Load pages. */
function memoryDatabase(rows: Map<string, Todo>) {
  const saved = new Map<string, { request: string; response: string }>();
  const operations: string[] = [];
  const database = {
    transaction: async <R,>(body: (tx: Tx) => Promise<R>) => body({ rows }),
    persistence: () => ({
      async call(request: any): Promise<any> {
        operations.push(request.op);
        const key = `${request.owner}:${request.callId}`;
        switch (request.op) {
          case "claimCall": {
            const found = saved.get(key);
            return found
              ? { fresh: false, ...found }
              : { fresh: true, request: request.request, response: null };
          }
          case "saveCall":
            saved.set(key, {
              request: saved.get(key)?.request ?? "",
              response: request.response,
            });
            return null;
          case "savepoint":
          case "rollback":
          case "release":
            return null;
          case "readStamps":
            return request.identityKeys.map(() => 1);
          default:
            throw new Error(`unexpected ${request.op}`);
        }
      },
    }),
  };
  return { database, operations };
}

test("the native engine refuses a full Model value that type-checks as an identity", async () => {
  const at = new Date("2026-01-01T00:00:00.000Z");
  const todo: Todo = {
    id: "one",
    title: "full",
    at,
    status: "open",
    note: null,
  };
  const { database } = memoryDatabase(new Map([[todo.id, todo]]));
  const errors: unknown[] = [];
  // A record structurally satisfies `TodoIdentity`: only a fresh object
  // literal is refused at compile time, so the engine refuses the rest.
  const loads: Loads<Tx> = {
    async todoPages({ ctx, continuation }) {
      const row = ctx.tx.rows.get("one")!;
      if (continuation === null)
        return { data: { todos: [row], moments: [] }, next: null };
      return {
        data: { todos: [{ id: row.id }], moments: [{ at: row.at }] },
        next: null,
      };
    },
  };
  const backend = createBackend<Tx>({
    database,
    authenticate: () => "alice",
    mutations: mutationHandlers(),
    queries: { find: { v2: async () => ({ todo: null }) } },
    loaders: {
      ...loaders,
      async todo({ ids, tx }) {
        return ids.map(({ id }) => tx.rows.get(id) ?? null);
      },
      async moment({ ids }) {
        return ids.map(({ at }) => ({ at, title: "moment" }));
      },
    },
    loads,
    onError: (error) => errors.push(error),
  });
  const item = (index: number, continuation: unknown) => ({
    loadId: `00000000-0000-4000-8000-00000000000${index}`,
    callId: `00000000-0000-4000-8000-00000000001${index}`,
    name: "TodoPages",
    version: 1,
    args: { since: "2026-01-01T00:00:00.000Z", statuses: ["open"] },
    continuation,
    models: { Todo: 1, Moment: 1 },
  });
  const response = JSON.parse(
    await backend.loads(
      "alice",
      JSON.stringify({
        capabilities: ["channel-membership-v1"],
        loads: [item(1, null), item(2, { state: "ids" })],
      }),
    ),
  );
  const [full, identities] = [1, 2].map((index) =>
    response.loads.find(
      (page: { loadId: string }) => page.loadId === item(index, null).loadId,
    ),
  );
  assert.equal(full.outcome.status, "failed");
  assert.equal(full.outcome.error.code, "handler.invalid");
  assert.match(full.outcome.error.message, /exactly identity fields/);
  assert.deepEqual(full.records, []);
  assert.equal(identities.outcome.status, "succeeded");
  assert.deepEqual(identities.outcome.data, {
    todos: [{ id: "one" }],
    moments: [{ at: "2026-01-01T00:00:00.000Z" }],
  });
  assert.equal(identities.outcome.next, null);
  assert.deepEqual(
    identities.records.map((record: { model: string }) => record.model).sort(),
    ["Moment", "Todo"],
  );
});
