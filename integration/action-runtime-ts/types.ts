import type { Call, CallOutcome, GeneratedClient } from "./client.ts";
import {
  createBackend,
  Moment,
  Pin,
  Todo as TodoRef,
  type Channel,
  type MutationContext,
  type MutationHandlerCall,
  type QueryContext,
  type Mutations,
  type Queries,
  type Loaders,
  type PutV1Input,
  type RecordRef,
} from "./backend.ts";
import type { Todo } from "./generated.ts";
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
ctx.channel("project:1").todo.add({ id: "A" });
ctx.channel("project:1").todo.remove({ id: "A" });
ctx.touch.todo({ id: "A" });
// @ts-expect-error missing identity
ctx.channel("project:1").todo.add({});
// @ts-expect-error old API is gone
ctx.publish({ channel: "project:1" });
// @ts-expect-error old API is gone
ctx.changes.add(todo);
// @ts-expect-error Query has no membership writer
queryCtx.channel("project:1").todo.add({ id: "A" });
// @ts-expect-error Query has no change declaration
queryCtx.touch.todo({ id: "A" });
queryCtx.tx.rows.get(todo.id);
void queryCtx.callId;
// A structurally compatible record is accepted; only its identity is copied.
ctx.channel("project:1").todo.add(todo);
const at = new Date();
// A DateTime identity is a Date, and a composite identity names every component.
ctx.touch.moment({ at });
ctx.channel("project:1").pin.add({ todo: "A", at });
ctx.touch.pin({ todo: "A", at });
// @ts-expect-error a DateTime identity is a Date, not its wire string
ctx.touch.moment({ at: "2026-01-01T00:00:00.000Z" });
// @ts-expect-error a composite identity needs every component
ctx.channel("project:1").pin.remove({ todo: "A" });
// @ts-expect-error touch has one method per Model
ctx.touch.nope({ id: "A" });
// Mixed sets take the generated, explicitly typed references.
const channel: Channel = ctx.channel("project:1");
channel.add([TodoRef({ id: "A" }), Moment({ at }), Pin({ todo: "A", at })]);
channel.remove([TodoRef({ id: "B" })]);
channel.add([{ model: "Todo", identity: { id: "C" } }]);
channel.add([]);
// @ts-expect-error a raw identity names no Model
channel.add([{ id: "A" }]);
// @ts-expect-error a reference's identity is its own Model's
channel.add([{ model: "Todo", identity: { at } }]);
// @ts-expect-error mixed methods take a list
channel.remove(TodoRef({ id: "A" }));
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
      ctx.channel("todos").todo.add(args.todo);
      return {
        todo: { id: args.todo.id },
        echoed: new Date(args.when.getTime()),
        status: args.statuses[0]!,
      };
    },
    async v2({ ctx, args }) {
      ctx.tx.rows.set(args.todo.id, args.todo);
      ctx.channel("todos").todo.add(args.todo);
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
  });
  void backend;
  // The external transaction hands its body the same generated handles and
  // answers the body's own value.
  const external: Promise<number> = backend.transaction(
    async ({ tx, channel, touch }) => {
      tx.rows.set(todo.id, todo);
      touch.todo({ id: todo.id });
      channel("project:1").todo.add({ id: todo.id });
      channel("project:1").add([Pin({ todo: todo.id, at })]);
      return tx.rows.size;
    },
  );
  void external;
  // @ts-expect-error the external body has no changes collector
  void backend.transaction(async ({ changes }) => changes);
  void backend.transaction(async ({ channel }) => {
    // @ts-expect-error missing identity
    channel("project:1").todo.add({});
  });
  // @ts-expect-error a schema that retains Queries requires the queries map
  createBackend({ database, authenticate: () => "alice", mutations: handlers, loaders });
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
  // @ts-expect-error transactions cannot submit Mutations
  tx.mutations.ping({});
  // @ts-expect-error transactions cannot run Queries
  tx.queries.find({ at: new Date() });
});
