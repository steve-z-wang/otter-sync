import type { Call, CallOutcome, GeneratedClient } from "./client.ts";
import {
  createBackend,
  Moment,
  Pin,
  Todo as TodoRef,
  type Stream,
  type MutationContext,
  type MutationHandlerCall,
  type QueryContext,
  type Mutations,
  type Queries,
  type Loaders,
  type QueryHandlerCall,
  type PutV1Input,
  type RecordRef,
  type TodoPagesHandlerOutput,
  type TodoPagesInput,
} from "./backend.ts";
import type { PutOutput, Todo } from "./generated.ts";
import type { Database } from "../../packages/backend/server/index.mts";
import type {
  Call as SdkCall,
  CallOutcome as SdkCallOutcome,
} from "../../packages/frontend/client-js/index.mts";

declare const client: GeneratedClient;
declare const todo: Todo;
declare const ctx: MutationContext<{ rows: Map<string, Todo> }>;
declare const queryCtx: QueryContext<{ rows: Map<string, Todo> }>;

const call: Promise<
  Call<{ todo: Todo; echoed: Date; status: "open" | "closed" }>
> = client.mutations.put({
  todo,
  when: new Date(),
  statuses: ["open"],
  note: null,
});
const direct: Promise<{ todo: Todo | null }> = client.queries.find({
  at: new Date(),
});
// @ts-expect-error Queries have no durable queue
client.queries.enqueue.find({ at: new Date() });
const outcome: Promise<
  CallOutcome<{ todo: Todo; echoed: Date; status: "open" | "closed" }>
> = call.then((handle) => handle.wait());
const sharedCall: Promise<
  SdkCall<{ todo: Todo; echoed: Date; status: "open" | "closed" }>
> = call;
const sharedOutcome: Promise<
  SdkCallOutcome<{ todo: Todo; echoed: Date; status: "open" | "closed" }>
> = outcome;
const local: Promise<void> = client.models.todo.create(todo);
const optional = client.mutations.change({ at: new Date() });
const identity = client.mutations.mark({
  moment: { at: new Date(), title: "changed" },
});
const removed: Promise<Call<{ at: Date }>> = client.mutations.removeMoment({
  moment: { at: new Date() },
});
void [
  direct,
  outcome,
  sharedCall,
  sharedOutcome,
  local,
  optional,
  identity,
  removed,
];
ctx.tx.rows.set(todo.id, todo);
ctx.stream.track.todo({ id: "A" });
ctx.stream.invalidate.todo({ id: "A" });
ctx.invalidate.todo({ id: "A" });
// @ts-expect-error missing identity
ctx.stream.track.todo({});
// @ts-expect-error old API is gone
ctx.publish({ scope: "project:1" });
// @ts-expect-error old API is gone
ctx.changes.track(todo);
queryCtx.stream.track.todo({ id: "A" });
// @ts-expect-error Query has no change declaration
queryCtx.invalidate.todo({ id: "A" });
queryCtx.tx.rows.get(todo.id);
void queryCtx.callId;
// A structurally compatible record is accepted; only its identity is copied.
ctx.stream.track.todo(todo);
const at = new Date();
// A DateTime identity is a Date, and a composite identity names every component.
ctx.invalidate.moment({ at });
ctx.stream.track.pin({ todo: "A", at });
ctx.invalidate.pin({ todo: "A", at });
// @ts-expect-error a DateTime identity is a Date, not its wire string
ctx.invalidate.moment({ at: "2026-01-01T00:00:00.000Z" });
// @ts-expect-error a composite identity needs every component
ctx.stream.invalidate.pin({ todo: "A" });
// @ts-expect-error touch has one method per Model
ctx.invalidate.nope({ id: "A" });
// Mixed sets take the generated, explicitly typed references.
const scope: Stream = ctx.stream;
ctx.streams(["project:1", "project:2"]).track.todo({ id: "A" });
scope.track([TodoRef({ id: "A" }), Moment({ at }), Pin({ todo: "A", at })]);
scope.invalidate([TodoRef({ id: "B" })]);
scope.track([{ model: "Todo", identity: { id: "C" } }]);
scope.track([]);
// @ts-expect-error a raw identity names no Model
scope.track([{ id: "A" }]);
// @ts-expect-error a reference's identity is its own Model's
scope.track([{ model: "Todo", identity: { at } }]);
scope.invalidate(TodoRef({ id: "A" }));
// @ts-expect-error a constructor takes its own Model's identity
Moment({ id: "A" });
const narrowed: Extract<RecordRef, { model: "Pin" }> = Pin({ todo: "A", at });
// @ts-expect-error a Todo reference is not a Moment reference
const mismatched: Extract<RecordRef, { model: "Moment" }> = TodoRef({ id: "A" });
void [narrowed, mismatched];

type Tx = { rows: Map<string, Todo> };
const queries: Queries<Tx> = {
  todoPages: async ({ args }) => ({ todos: [], moments: [{ at: args.since }] }),
  find: {
    async v2({ ctx }) {
      ctx.tx.rows.get("one");
      // @ts-expect-error Query handlers cannot declare changes
      ctx.invalidate.todo({ id: "one" });
      return { todo: { id: "one" } };
    },
  },
};
// @ts-expect-error Query Model outputs are typed identities, not bare keys
const wholeModel: Queries<Tx> = { find: { v2: async () => ({ todo: "one" }) } };
const queryV1: Queries<Tx> = {
  todoPages: async () => ({ todos: [], moments: [] }),
  find: {
    v2: async () => ({ todo: null }),
    // @ts-expect-error v1 of Find is a Mutation; the Query map holds only v2
    v1: async () => ({ todo: null }),
  },
};
void [queries, wholeModel, queryV1];
const handlers: Mutations<Tx> = {
  put: {
    async v1({ ctx, args }) {
      ctx.tx.rows.set(args.todo.id, args.todo);
      ctx.stream.track.todo(args.todo);
      return {
        todo: { id: args.todo.id },
        echoed: new Date(args.when.getTime()),
        status: args.statuses[0]!,
      };
    },
    async v2({ ctx, args }) {
      ctx.tx.rows.set(args.todo.id, args.todo);
      ctx.stream.track.todo(args.todo);
      return {
        todo: { id: args.todo.id },
        echoed: new Date(args.when.getTime()),
        status: args.statuses[0]!,
      };
    },
  },
  async change({ args }) {
    return { todo: args.todo ? { id: args.todo.id } : null, echoed: args.at };
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
const loaders: Loaders<Tx> = {
  async todo({ ids, tx }) {
    return ids.map((id) => tx.rows.get(id.id) ?? null);
  },
  async moment({ ids }) {
    return ids.map(() => null);
  },
  async pin({ ids }) {
    return ids.map(() => null);
  },
};
void [handlers, loaders];
// Bootstrap and Query preparation have track-only Stream handles.
declare const bootstrapCtx: QueryContext<Tx>;
bootstrapCtx.stream.track([TodoRef({ id: "A" }), Moment({ at: new Date(0) })]);
// @ts-expect-error Bootstrap cannot invalidate
bootstrapCtx.stream.invalidate.todo({ id: "A" });
const standalone = async ({ args }: QueryHandlerCall<Tx, TodoPagesInput>): Promise<TodoPagesHandlerOutput> => ({
  todos: [], moments: [{ at: args.since }],
});
// @ts-expect-error DateTime identities remain Dates
const wireIdentity: TodoPagesHandlerOutput = { todos: [], moments: [{ at: "2026-01-01" }] };
// @ts-expect-error Query outputs contain identities rather than Model payloads
const fullRecords: TodoPagesHandlerOutput = { todos: [{ id: "A", title: "extra" }], moments: [] };
void [standalone, wireIdentity, fullRecords];
declare const database: Database<Tx>;
if (false) {
  const backend = createBackend({
    database,
    authenticate: () => "alice",
    mutations: {
      ...handlers,
      async ping({ ctx }) {
        ctx.tx.rows.set("x", todo);
        // @ts-expect-error inferred application Tx has no missing member
        ctx.tx.missing;
      },
    },
    queries,
    loaders,
    protocol5: { authorizeStream: () => true },
  });
  void backend;
  // The external transaction hands its body the same generated handles and
  // answers the body's own value.
  const external: Promise<number> = backend.transaction(
    async ({ tx, streams: scope, invalidate: touch }) => {
      tx.rows.set(todo.id, todo);
      touch.todo({ id: todo.id });
      scope(["project:1"]).track.todo({ id: todo.id });
      scope(["project:1"]).track([Pin({ todo: todo.id, at })]);
      return tx.rows.size;
    },
  );
  void external;
  // @ts-expect-error the external body has no changes collector
  void backend.transaction(async ({ changes }) => changes);
  void backend.transaction(async ({ streams: scope }) => {
    // @ts-expect-error missing identity
    scope(["project:1"]).track.todo({});
  });
  // @ts-expect-error retained Queries require their handler map
  createBackend({ database, authenticate: () => "alice", mutations: handlers, loaders, protocol5: { authorizeStream: () => true } });
}
const retained = (call: MutationHandlerCall<Tx, PutV1Input>) =>
  call.args.todo.at.getUTCFullYear();
void retained;

// @ts-expect-error every retained Mutation version must be registered
const missingVersion: Mutations<Tx>["put"] = {
  v2: async () => ({ todo: { id: "one" }, echoed: new Date(), status: "open" }),
};
void missingVersion;

// @ts-expect-error required nullable ordinary input must be present
client.mutations.put({ todo, when: new Date(), statuses: [] });
// @ts-expect-error DateTime input uses Date
client.queries.find({ at: "2026-01-01T00:00:00Z" });
// @ts-expect-error enum list members are checked
client.mutations.put({ todo, when: new Date(), statuses: ["typo"], note: null });
// @ts-expect-error Find is a Query now; the Mutation namespace no longer has it
client.mutations.find({ at: new Date() });
// @ts-expect-error a direct result has no wait
client.queries.find({ at: new Date() }).then((result) => result.wait());
// @ts-expect-error a durable Call has no result property
client.mutations.ping({}).then((handle) => handle.result);
// @ts-expect-error the old actions namespace is gone
client.actions.ping({});
// @ts-expect-error store keys name explicit Model outputs only
client.queries.find({ at: new Date() }, { store: { missing: false } });
client.queries.find({ at: new Date() }, { store: false });
// @ts-expect-error Queries cannot enqueue
client.queries.enqueue.find({ at: new Date() }, { store: false });
// @ts-expect-error standalone local models have no named mutation method
client.models.todo.mutate({});
client.transaction(async (tx) => {
  // @ts-expect-error transaction models do not watch
  tx.models.todo.watch({}, () => {});
});
client.transaction(async (tx) => {
  // @ts-expect-error transactions queue Mutations; they have no direct route
  tx.mutations.call.ping({});
  // @ts-expect-error transactions cannot run Queries
  tx.queries.find({ at: new Date() });
  // @ts-expect-error Find is a Query now, so it is not queued in a transaction
  tx.mutations.find({ at: new Date() });
});
// Transactional Mutation enqueue: DateTime args and outputs keep their Date
// types, and each queued Mutation answers its own Call.
const enqueued: Promise<{
  put: Call<PutOutput>;
  cleared: Call<void>;
}> = client.transaction(async (tx) => {
  const put = await tx.mutations.put(async (local) => {
    const at: Date | undefined = (await local.models.moment.get({ at: new Date(0) }))?.at;
    await local.models.pin.delete({ todo: todo.id, at: at ?? new Date(0) });
    return { todo, when: new Date(), statuses: ["open"], note: null };
  });
  const cleared = await tx.mutations.clear({ todo: [{ id: todo.id }] });
  return { put, cleared };
});
enqueued.then(async ({ put }) => {
  const outcome: CallOutcome<PutOutput> = await put.wait();
  const echoed: Date | undefined = outcome.result?.echoed;
  void echoed;
});
client.transaction(async (tx) => {
  // @ts-expect-error a DateTime arg is a Date
  await tx.mutations.change({ todo: null, at: "2026-01-01T00:00:00.000Z" });
  // @ts-expect-error a DateTime identity component is a Date in the callback too
  await tx.mutations.ping(async (local) => { await local.models.moment.delete({ at: "2026" }); return {}; });
  // @ts-expect-error the callback queues no Mutation
  await tx.mutations.ping(async (local) => { await local.mutations.ping({}); return {}; });
});
