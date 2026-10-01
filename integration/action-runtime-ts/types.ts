import type { Call, CallOutcome, GeneratedClient } from "./client.ts";
import {
  createBackend,
  Moment,
  Pin,
  Todo as TodoRef,
  type Scope,
  type MutationContext,
  type MutationHandlerCall,
  type QueryContext,
  type Mutations,
  type Queries,
  type Loaders,
  type Loads,
  type LoadContext,
  type LoadHandlerCall,
  type JsonValue,
  type PutV1Input,
  type RecordRef,
  type TodoPagesHandlerOutput,
  type TodoPagesInput,
} from "./backend.ts";
import type { PutOutput, Todo } from "./generated.ts";
import type { Database } from "../../packages/server/index.mts";
import type {
  Call as SdkCall,
  CallOutcome as SdkCallOutcome,
} from "../../packages/client-js/index.mts";

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
const queued: Promise<Call<{ todo: Todo | null }>> =
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
const removed: Promise<{ at: Date }> = client.mutations.call.removeMoment({
  moment: { at: new Date() },
});
void [
  direct,
  queued,
  outcome,
  sharedCall,
  sharedOutcome,
  local,
  optional,
  identity,
  removed,
];
ctx.tx.rows.set(todo.id, todo);
ctx.scope("project:1").add.todo({ id: "A" });
ctx.scope("project:1").remove.todo({ id: "A" });
ctx.touch.todo({ id: "A" });
// @ts-expect-error missing identity
ctx.scope("project:1").add.todo({});
// @ts-expect-error old API is gone
ctx.publish({ scope: "project:1" });
// @ts-expect-error old API is gone
ctx.changes.add(todo);
// @ts-expect-error Query has no membership writer
queryCtx.scope("project:1").add.todo({ id: "A" });
// @ts-expect-error Query has no change declaration
queryCtx.touch.todo({ id: "A" });
queryCtx.tx.rows.get(todo.id);
void queryCtx.callId;
// A structurally compatible record is accepted; only its identity is copied.
ctx.scope("project:1").add.todo(todo);
const at = new Date();
// A DateTime identity is a Date, and a composite identity names every component.
ctx.touch.moment({ at });
ctx.scope("project:1").add.pin({ todo: "A", at });
ctx.touch.pin({ todo: "A", at });
// @ts-expect-error a DateTime identity is a Date, not its wire string
ctx.touch.moment({ at: "2026-01-01T00:00:00.000Z" });
// @ts-expect-error a composite identity needs every component
ctx.scope("project:1").remove.pin({ todo: "A" });
// @ts-expect-error touch has one method per Model
ctx.touch.nope({ id: "A" });
// Mixed sets take the generated, explicitly typed references.
const scope: Scope = ctx.scope("project:1");
scope.add([TodoRef({ id: "A" }), Moment({ at }), Pin({ todo: "A", at })]);
scope.remove([TodoRef({ id: "B" })]);
scope.add([{ model: "Todo", identity: { id: "C" } }]);
scope.add([]);
// @ts-expect-error a raw identity names no Model
scope.add([{ id: "A" }]);
// @ts-expect-error a reference's identity is its own Model's
scope.add([{ model: "Todo", identity: { at } }]);
scope.remove(TodoRef({ id: "A" }));
// @ts-expect-error a constructor takes its own Model's identity
Moment({ id: "A" });
const narrowed: Extract<RecordRef, { model: "Pin" }> = Pin({ todo: "A", at });
// @ts-expect-error a Todo reference is not a Moment reference
const mismatched: Extract<RecordRef, { model: "Moment" }> = TodoRef({ id: "A" });
void [narrowed, mismatched];

type Tx = { rows: Map<string, Todo> };
const queries: Queries<Tx> = {
  find: {
    async v2({ ctx }) {
      ctx.tx.rows.get("one");
      // @ts-expect-error Query handlers cannot declare changes
      ctx.touch.todo({ id: "one" });
      return { todo: { id: "one" } };
    },
  },
};
// @ts-expect-error Query Model outputs are typed identities, not bare keys
const wholeModel: Queries<Tx> = { find: { v2: async () => ({ todo: "one" }) } };
const queryV1: Queries<Tx> = {
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
      ctx.scope("todos").add.todo(args.todo);
      return {
        todo: { id: args.todo.id },
        echoed: new Date(args.when.getTime()),
        status: args.statuses[0]!,
      };
    },
    async v2({ ctx, args }) {
      ctx.tx.rows.set(args.todo.id, args.todo);
      ctx.scope("todos").add.todo(args.todo);
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
// Loads (#173): typed args, a context whose Scopes only add, and identity pages.
declare const loadCtx: LoadContext<Tx>;
void [loadCtx.tx.rows, loadCtx.userId, loadCtx.callId, loadCtx.loadId];
// A Load context adds page records to Scopes, and only adds.
loadCtx.scope("project:1").add.todo({ id: "A" });
loadCtx.scope("project:1").add([TodoRef({ id: "A" }), Moment({ at: new Date(0) })]);
// @ts-expect-error a Load Scope cannot remove
loadCtx.scope("project:1").remove.todo({ id: "A" });
// @ts-expect-error a Load context has no change declaration
loadCtx.touch.todo({ id: "A" });
const loads: Loads<Tx> = {
  async todoPages({ ctx, args, continuation }) {
    ctx.tx.rows.get("one");
    const since: Date = args.since;
    const statuses: ("open" | "closed")[] = args.statuses;
    void statuses;
    if (continuation === null)
      return {
        data: { todos: [{ id: "one" }], moments: [{ at: since }] },
        next: { state: { after: "one", seen: [1, 2.5, true, null] } },
      };
    const state: JsonValue = continuation.state;
    void state;
    return { data: { todos: [], moments: [] }, next: null };
  },
};
const standalone = async ({
  args,
}: LoadHandlerCall<Tx, TodoPagesInput>): Promise<TodoPagesHandlerOutput> => ({
  data: { todos: [], moments: [{ at: args.since }] },
  next: { state: null },
});
const versionedLoads: Loads<Tx> = { todoPages: { v1: standalone } };
void [loads, versionedLoads];
const wrongArgs: Loads<Tx> = {
  async todoPages({ ctx, args }) {
    // @ts-expect-error a DateTime arg is a Date, not its wire string
    const wire: string = args.since;
    // @ts-expect-error a Load has only its declared args
    void args.cursor;
    // @ts-expect-error a Load handler cannot declare changes
    ctx.touch.todo({ id: "one" });
    void wire;
    return { data: { todos: [], moments: [] }, next: null };
  },
};
// @ts-expect-error a DateTime identity is a Date, not its wire string
const wireIdentity: TodoPagesHandlerOutput = { data: { todos: [], moments: [{ at: "2026-01-01T00:00:00.000Z" }] }, next: null };
// @ts-expect-error a Load page answers identities, not full Model records
const fullRecords: TodoPagesHandlerOutput = { data: { todos: [{ id: "one", title: "T", at: new Date(), status: "open", note: null }], moments: [] }, next: null };
// @ts-expect-error continuation state is portable JSON, not a Date
const dateState: TodoPagesHandlerOutput = { data: { todos: [], moments: [] }, next: { state: new Date() } };
void [wrongArgs, wireIdentity, fullRecords, dateState];
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
    loads,
  });
  void backend;
  // The external transaction hands its body the same generated handles and
  // answers the body's own value.
  const external: Promise<number> = backend.transaction(
    async ({ tx, scope: scope, touch }) => {
      tx.rows.set(todo.id, todo);
      touch.todo({ id: todo.id });
      scope("project:1").add.todo({ id: todo.id });
      scope("project:1").add([Pin({ todo: todo.id, at })]);
      return tx.rows.size;
    },
  );
  void external;
  // @ts-expect-error the external body has no changes collector
  void backend.transaction(async ({ changes }) => changes);
  void backend.transaction(async ({ scope: scope }) => {
    // @ts-expect-error missing identity
    scope("project:1").add.todo({});
  });
  // @ts-expect-error a schema that retains Queries requires the queries map
  createBackend({ database, authenticate: () => "alice", mutations: handlers, loaders, loads });
  // @ts-expect-error a schema that retains Loads requires the loads map
  createBackend({ database, authenticate: () => "alice", mutations: handlers, queries, loaders });
  // @ts-expect-error a Load is registered under loads, not queries
  createBackend({ database, authenticate: () => "alice", mutations: handlers, queries: { ...queries, todoPages: loads.todoPages }, loaders, loads });
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
client.queries.find({ at: new Date() }, { store: { todo: false } });
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
  const put = await tx.mutations.put(
    { todo, when: new Date(), statuses: ["open"], note: null },
    {
      store: { todo: false },
      local: async (local) => {
        const at: Date | undefined = (await local.models.moment.get({ at: new Date(0) }))?.at;
        await local.models.pin.delete({ todo: todo.id, at: at ?? new Date(0) });
      },
    },
  );
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
  await tx.mutations.ping({}, { local: async (local) => local.models.moment.delete({ at: "2026" }) });
  // @ts-expect-error the callback queues no Mutation
  await tx.mutations.ping({}, { local: async (local) => local.mutations.ping({}) });
});
