# Backend interfaces

Your backend implements Mutations and Queries through handlers and the read/sync path through loaders. The compiler generates their TypeScript interfaces from your schema. AXTON supplies protocol processing; your application supplies business logic, authorization and a database transaction.

Handler signatures below follow the [generated operation fixture](https://github.com/zanminwang/axton/blob/main/integration/action-contract/schema.model). The background-write example uses an independent `Entry` Model fixture. The working To-do backend is [examples/todo/server.mts](https://github.com/zanminwang/axton/blob/main/examples/todo/server.mts).

## createBackend

```ts title="action-contract"
import { createBackend, devAuth } from './generated/backend.ts';
import { database } from './database.ts';
import { mutations, queries } from './handlers.ts';
import { loaders } from './loaders.ts';

const backend = createBackend<Tx>({
  database,
  authenticate: devAuth(),
  mutations,
  queries,
  loaders,
  onError: error => console.error(error),
});
const server = await backend.listen({ port: 4242 });
console.log(server.url);
```

`database.ts` exports a PostgreSQL shim such as `prisma(db)`, and `Tx` is its transaction type; see [Database](database.md) for the shims and AXTON's metadata migration, which must be applied first. `handlers.ts` and `loaders.ts` contain the implementations below.

The generated `Options<Tx>` requires:

| Option | Responsibility |
| --- | --- |
| `database: Database<Tx>` | A PostgreSQL shim, `pg(pool)`, `prisma(client)` or `drizzle(db)`, or `persistence(driver)` over your own driver ([Database](database.md)) |
| `authenticate: Authenticate` | Resolve the caller's user identity or reject the request |
| `mutations: Mutations<Tx>` | Implement each retained Mutation version |
| `queries: Queries<Tx>` | Implement each retained Query version |
| `loaders: Loaders<Tx>` | Implement the read function for each supported model version |

`mutations` or `queries` is required when the schema retains a contract of that kind, and can be omitted otherwise; the To-do example has no Queries and passes only `mutations`. Optional options are `translateRejection`, `onError`, `loaderHooks` and `native`, described below. The generated function binds the schema and returns the backend synchronously. The generic function in `packages/server/index.mts` additionally requires `config`; normal generated integrations do not pass it.

## What your backend owns

The Rust runtime processes the sync protocol and nothing else. The rules below are yours to implement; the runtime neither enforces nor checks them, and the schema does not make it do so.

| Rule | Who owns it | What the runtime does |
| --- | --- | --- |
| Authorization | Handlers decide what `userId` may write; loaders decide what `userId` may see and return `null` for the rest, whatever channel asked. | Authenticates the request and passes `userId` through. There is no channel-level policy. |
| Unique constraints and identities | Your database schema. `@@unique` and `@@id` are enforced on the client only; the client's local database refuses a violating write, but nothing checks the server. | Decodes identities and patches by shape. A duplicate that your database allows is stored. |
| Child deletion | Your handler. `onTargetDelete: delete` is a client-side cascade: the client deletes the children locally, and those deletes never reach the server. A handler that deletes a parent must delete its children itself and touch them (`ctx.touch.todo(identity)`), leaving them in the Channels that delivered them so those Channels receive the deletion. | Reads the parent back as deleted and delivers it; a child the handler did not touch stays on other clients until a Channel delivers it. |
| Client identity | Each signed-in user gets their own local client database. A client id is bound to the first user that pushed with it; a push from another user with the same client id answers `403 client.owner_mismatch`, and there is no reassignment. | Stores the owner with the client row. |
| Backend language | TypeScript on Node, through the generated `createBackend`. The Dart package is a client SDK; there is no Dart or Rust-hosted backend. | Runs the same Rust engine inside the Node addon. |
| Prerequisite expressions | `@requires(Name(field: self))` is the only supported form: every argument is `self`, the value of the annotated field. The runner that satisfies prerequisites is client code. | Never sees prerequisites; they gate when the client sends a durable call, not what the backend receives. |

These are accepted limits of the current runtime, not planned features. See [deployment](deployment.md) for the process and network boundaries.

## Handlers

A handler receives `{ ctx, args }`: `ctx` holds trusted framework context and `args` holds decoded caller inputs. A create operand is always a complete record: fields the caller omitted were filled from their [creation defaults](../schema/reference.md#creation-defaults) by the client, and the server never fills a missing value. In the snippets, `Tx` stands for the transaction type supplied by your database adapter. A Mutation handler writes to your database and returns explicit output values; a Query handler reads and returns them. AXTON resolves Model outputs through the corresponding versioned Loader in the same transaction. For a durable Mutation, it also reads the batch-final state of each Model input into the receipt, independently of each invocation's result snapshot. Database work and framework metadata share the transaction; external effects such as sending email do not become atomic with it. Use an application outbox or equivalent design where that distinction matters.

```ts title="action-contract"
import { CallRejected, type Mutations } from './generated/backend.ts';

// saveTodo is application code that writes to the business database.
const handleAddTodoV2: Mutations<Tx>['addTodo']['v2'] =
  async ({ ctx, args }) => {
    if (!args.todo.title.trim()) throw new CallRejected('todo.title_empty');
    await saveTodo(ctx.tx, args.todo);
    // The new Todo joins the Channel once; its later changes reach it with no enrollment.
    ctx.channel('todos').todo.add(args.todo);
    return { relatedTodo: null, matches: [], count: 1, state: null };
  };
```

The `todo` input is already a change: AXTON stamps it and returns its authority to the caller, which completes without a subscription, whatever the outputs or the call's `store` option. It is not part of the result. The result holds only the declared outputs: `relatedTodo` and `matches` are identity-selected Model outputs that the Loader resolves at this invocation, and `count` and `state` are ordinary outputs. Other clients learn of the change through the `todos` Channel.

An output is independent of the inputs even when the names match. The fixture's `EditAndRead(todo Todo.update) { todo Todo }` may edit one Todo and return another:

```ts title="action-contract"
import { type Mutations } from './generated/backend.ts';

const handleEditAndRead: Mutations<Tx>['editAndRead'] =
  async ({ ctx, args }) => {
    await saveTodo(ctx.tx, args.todo);
    // The result names the Todo to show; it need not be the one edited.
    return { todo: { id: 'todo-summary' } };
  };
```

A missing output field fails the call; AXTON never fills it from an input. An operation without outputs, such as `Edit(todo Todo.update)`, returns nothing, and its caller inspects its local Model once the call completes.

```ts title="action-contract"
import { type Queries } from './generated/backend.ts';

const handleFindTodos: Queries<Tx>['findTodos'] =
  async ({ ctx, args }) => {
    // searchTodos is an application read; it checks what ctx.userId may see.
    const page = await searchTodos(ctx.tx, ctx.userId, args.text, args.cursor);
    return { todos: page.ids.map(id => ({ id })), nextCursor: page.next };
  };
```

Returning identity objects lets the Loader resolve the visible records in order, including repeated identities. `nextCursor` is an ordinary value your query computes; the framework does not paginate. Authorization remains the application's responsibility.

| Field | `MutationContext<Tx>` | `QueryContext<Tx>` | Meaning |
| --- | --- | --- | --- |
| `tx` | Yes | Yes | Your database transaction object |
| `userId` | Yes | Yes | Authenticated caller; use it for business authorization |
| `callId` | Yes | Yes | Stable identity of this invocation, including retries |
| `touch` | Yes | No | `touch.todo(identity)` declares a record this Mutation changed beyond its Model inputs; see [Channels](#channels) |
| `channel(name)` | Yes | No | A handle for adding records to or removing them from a Channel; see [Channels](#channels) |

A handler returns the generated explicit output shape, or no value when the operation has no explicit outputs. AXTON allocates a **stamp** for each changed record, the Model inputs plus the records the handler touched, and on durable delivery reads the inputs' batch-final content through the Loader into the receipt. Touch every other business record the handler changed: a touched record is delivered to its Channels, but it is not returned to the caller, so the caller need not know its Model.

A Query's context has no `touch` or `channel`, in its type and at runtime. The engine also refuses any Query settlement that reports changes or memberships: that call fails with `query.effects_forbidden`, its savepoint rolls back before any stamp, readback or publication, and adjacent calls in the batch are unaffected. This is not a SQL sandbox. `ctx.tx` is still your application's transaction, and the framework cannot inspect the SQL a handler runs or other clients it has captured, so keeping a Query free of business side effects is your application's responsibility. Framework metadata is still written: each Query outcome is saved by call ID like a Mutation's, so retrying the same call ID replays the saved result and a new invocation reads again.

A loader is channel-independent: the row it returns for a record is the row every client receives for it, in the receipt, in a catch-up page and on the live stream, at the same stamp. What a loader may vary by is `userId`.

One Mutation can have several operands and perform several business writes in one savepoint. The schema's Model operands describe local optimism; the backend can normalize values or use different tables.

`mutations.addTodo` holds every retained version of `AddTodo`. The fixture retains v1 and v2, so register both. Registration follows each retained version's kind: the fixture's `GetTodos` retains v1 as a Mutation and v2 as a Query, so each version is registered under its own kind.

```text
mutations.addTodo = {
  v1: handleOriginalAddTodo, // receives AddTodoV1Input
  v2: handleAddTodoV2,       // receives AddTodoInput
};
mutations.getTodos = handleGetTodosV1; // GetTodos v1 is a Mutation
queries.getTodos = { v2: handleGetTodosV2 }; // GetTodos v2 is a Query
```

A bare function means v1 only. A missing retained version, unknown version key, non-function value, or a registration under the wrong kind is refused at startup. Dispatch uses the requested version and never falls back to another. An unsupported version is a per-call rejection.

An error that is neither `CallRejected` nor translated to a business code rejects that call with `handler.failed` and reaches `onError`. Independent valid calls in the batch can still commit. A retryable database transaction error instead retries the transaction; it is not saved as a permanent business rejection.

## Loaders

```ts title="action-contract"
import type { Loaders } from './generated/backend.ts';

const loadTodoV2: Loaders<Tx>['todo']['v2'] =
  async ({ ids, tx, userId }) =>
    Promise.all(ids.map(id => loadVisibleTodo(tx, userId, id)));
```

`LoaderCall<Tx, Identity>` contains:

| Field | Meaning |
| --- | --- |
| `ids` | Read-only list of typed record identities |
| `tx` | Your transaction, shared with sync persistence for this request |
| `userId` | Caller whose visibility must be checked |

A Loader is not told which channel, if any, asked: it serves Mutation and Query Model outputs, durable authority readback, catch-up pages and the live stream. It sees the same application transaction during a call.

A loader returns `Promise<readonly (Record | null)[]>`. Return exactly one item per identity, in the same order. Do not filter out missing rows or return a differently ordered database result directly.

`loaders.todo` holds every retained version of the `Todo` read contract. The operation fixture retains v1 and v2, so register both while older clients or call results use v1:

```text
loaders.todo = {
  v1: loadTodoV1, // returns TodoV1 rows
  v2: loadTodoV2, // returns Todo rows
};
```

The generated `TodoV1` type is the record shape published for v1, so a v1 Loader maps current rows into it; AXTON does not convert between versions. Registration is checked at startup like handlers: a bare function means v1 only, and missing/unknown versions or non-function values are refused. A load reaches only the version it names.

What each item may be:

| Item | Meaning | Result |
| --- | --- | --- |
| A row object | The record's current state for this user | Delivered with the record's current stamp |
| `null` | The record does not exist, or this user must not see it | Delivered as a deletion. A newer stamp clears the authoritative row, whichever channel delivered it; the client keeps the stamp so older content cannot bring the record back; pending local operations are replayed on that state. |
| a thrown `CallRejected` (or an error `translateRejection` maps to a code) | A refused read | During a call, that call is rejected with the code and rolled back. In a pull, that record is delivered as an error with that code: the client keeps its local copy and reports it, and the rest of the page applies |
| any other thrown error | A failure | Reported to `onError` (default `console.error`). During a call, the call is rejected with `loader.failed`; in a pull, that record is delivered as a `loader.failed` error and the rest of the page applies |
| `undefined`, a missing entry, a non-array result, a nonfinite number | A defect | Reported to `onError` and treated like a thrown error: only the records it affects fail. It is never read as `null` |

A row object must match the generated model type exactly. Include every non-identity field: a nullable field that is absent reads as `null`, but an absent non-nullable field is a defect. The identity fields may be present. Any other property, such as an extra database column or a relation object, is a defect. Map your rows to the model type rather than returning a wider database row.

Loaders run during synchronization and calls, not when the app calls local `get`, `query` or `watch`. A malformed result is never skipped silently: the affected record arrives as an error change, or the call being read back is rejected with `loader.invalid`, and `onError` hears about it.

## Channels

A Channel distributes records to the clients subscribed to it. Its members are stored with your data: add a record once, and every later change to it reaches the Channel, whichever handler or job makes the change.

```ts title="action-contract"
import { Todo, type MutationContext } from './generated/backend.ts';

function organize(ctx: MutationContext<unknown>) {
  const board = ctx.channel('board:1');
  board.todo.add({ id: 'todo-1' });
  board.todo.remove({ id: 'todo-2' });
  board.add([Todo({ id: 'todo-3' }), Todo({ id: 'todo-4' })]);
  ctx.touch.todo({ id: 'todo-5' });
}
```

| Interface | Shape |
| --- | --- |
| `ctx.channel(name)` | `Channel`: a handle for the Channel named `name`; it creates nothing, sends nothing and checks no subscriber |
| `channel.todo.add(identity)`, `channel.todo.remove(identity)` | `ModelMembership<TodoIdentity>`, one per Model |
| `channel.add(records)`, `channel.remove(records)` | `(records: readonly RecordRef[]) => void`, for several Models at once |
| `ctx.touch.todo(identity)` | `Touch`: declare a changed record, one method per Model |
| `RecordRef` | The generated union `{ model: 'Todo', identity: TodoIdentity } \| …` |
| Generated reference function | `Todo(identity: TodoIdentity)`, the only way to put a record in a mixed list |

Every call is synchronous and returns nothing. It validates at the call and copies only the identity fields, so `channel.todo.add(args.todo)` works and later edits to the passed object change nothing. A mixed list is checked whole before anything is declared; an untagged `{ id }` cannot name its Model and fails. The channel name must be nonblank. The handles work only while the handler runs: a handle kept after it returns, or after it throws, refuses every call. Returning from the handler commits nothing yet; if the call is later rejected, its declarations roll back with it.

- **Adding** a record that is not a member delivers its current state to the Channel, even when nothing changed. Adding a member does nothing.
- **Removing** a member stops later deliveries there. Clients keep the rows they have, and no removal event is sent; a response already on its way may still arrive. Removing a non-member does nothing.
- **The last declaration wins** for each Channel and record in one call: removing then re-adding a member, or adding then removing a non-member, changes nothing.
- **Touching** gives the record a new stamp once per call, however often it is declared, and delivers it to every Channel it is a member of. A record with no membership is still stamped but reaches no Channel.
- **Deletion** is a change: touch the deleted record (or delete it through a Model input) and leave it enrolled, so its Loader answers `null` in each Channel and subscribers delete it. A record deleted and removed from a Channel in the same call sends that Channel nothing. Membership belongs to the identity, so a record later created again with the same identity is delivered to the same Channels; use a new identity, or remove the old memberships, for a fresh lifecycle.

Membership decides where a record is delivered, not who may see it: the Loader still runs for each subscriber and answers `null` for a record that user must not see. Channel names are not access control ([#22](https://github.com/zanminwang/axton/issues/22)).

A change allocates one **stamp** per record; delivering it allocates a **cursor** in each of its Channels and carries that same stamp to all of them. Adding an unchanged record reuses its current stamp (a record that has never been stamped gets its first one). Stamps prevent older content delivered later, on any Channel, from overwriting newer content. The membership table starts empty; AXTON never infers membership from earlier deliveries, so an existing database re-enrolls its records explicitly. See [concepts](../concepts.md).

## Authentication

`Authenticate` receives Node's `IncomingMessage` and returns a user ID string, null, undefined, or a promise of those values. Null/undefined or a blank user ID rejects authentication. The SDK calls it for HTTP requests and WebSocket connections. Verify your application's session/token here; enforce read permissions in loaders and write permissions in handlers.

`devAuth(): Authenticate` treats `Authorization: Bearer <userId>` as the identity without verification. It is provided for local development, not production authentication. See [authentication and account changes](../frontend/sync.md#authentication-and-account-changes).

## Errors

| Interface | Use |
| --- | --- |
| `new CallRejected(code)` | Reject one Mutation or Query call with a stable machine-readable code |
| `translateRejection(error)` | Return a stable rejection code for a known application error; return null/undefined for other errors |
| `onError(error)` | Log server failures that are returned to the client as a generic server error |
| `EngineError` | A failure from the native engine: `code` (stable), `message` (readable, may change), `details` (fields the code promises) |

Codes must match `^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`, such as `todo.title_empty`. A recognized business rejection rolls back that call's business writes, stamps, memberships and deliveries. For a queued call, `wait()` returns a `CallError` outcome and any optimistic Model change rolls back; the durable rejection remains inspectable until dismissed. For a direct call, the promise rejects with `CallError`. An unknown transport outcome can be retried with the same call identity; it is not evidence that the handler did nothing.

Business codes come from `CallRejected` or `translateRejection`; `action_version_unsupported`, `handler.failed`, `loader.failed`, `model_version_unsupported` and `query.effects_forbidden` identify framework failures attributable to one call. A durable receipt records each call's outcome. Independent valid calls in the batch can commit. A direct response carries the same final outcome for that call.

Infrastructure errors that make the transaction unusable abort delivery for retry. `onError` receives diagnostic failures, including handler and Loader exceptions. Diagnostic callback exceptions after a committed outcome cannot replace its result, repeat its handler or become a transport error; SDKs report those callback exceptions through their runtime uncaught-error channel.

Protocol refusals use a status and JSON body chosen by the engine error's `code`; message text may change. Per-call content errors are outcomes, while a pull or live subscription with an unsupported Model declaration receives a whole-request `409`.

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `request.invalid` | 400 | Malformed body, or a pull cursor ahead of the channel head |
| `client.owner_mismatch` | 403 | The client identity belongs to another user |
| `gap`, `overlap` | 409 | The batch sequence is not the next one and not a retry of the last |
| `model_version_unsupported` | 409 | Pull and live subscribe: a Model read contract this backend does not serve. During a call it is a per-call failure. |
| `handler.invalid` | 500 `{ code: "server" }` | The handler's settlement could not be used: an invalid rejection code, or a change or membership naming a record without a model or an object identity |
| anything else | 500 `{ code: "server" }` | A server-side failure; the `EngineError` or thrown error goes to `onError` |

## Listener

`await backend.listen({ port, host? })` binds a Node HTTP and WebSocket server. Host defaults to `127.0.0.1`; port zero selects an available port. The result is `{ url, close(): Promise<void> }`.

| Route | Purpose |
| --- | --- |
| `POST /sync/mutations` | Receive durable batches of Mutations and queued Queries |
| `POST /sync/actions` | Execute one direct Mutation or Query and return its result |
| `POST /sync/pull` | Materialize changed records through loaders for catch-up and gap recovery |
| `/sync/live` (WebSocket) | Subscribe to channels and stream ongoing record changes |

The listener has no TLS, CORS or proxy-header handling and binds to loopback by default; run it behind a reverse proxy as described in [Deploy the backend](deployment.md).

Generated clients use all three routes automatically from one `server` configuration. The WebSocket subscription acknowledgement confirms that channel listeners are installed before HTTP catch-up starts, so changes during catch-up can be queued and reconciled. Listener errors reject. `await server.close()` releases the listener and its live connections; your application must separately close its database pool. The supported listener owns its server; mounting into an application-owned HTTP server is not currently exposed.

## Background writes

Writes outside handlers have no readback and no receipt; they reach clients only through Channels. Run them through `backend.transaction`: the framework opens the application transaction and hands the body the same `touch` and `channel` a Mutation handler receives. When the body returns, the framework allocates one new stamp per touched record and applies the membership changes and deliveries inside that same transaction; once it commits, the live subscribers of the affected Channels are woken.

```ts
await backend.transaction(async ({ tx, channel, touch }) => {
  await tx.entry.update({ where: { id: 'entry-1' }, data: { text: 'From a job' } });
  touch.entry({ id: 'entry-1' });
  channel('book:demo').entry.add({ id: 'entry-1' });
});
```

`tx` is the transaction of the shim passed as `database`. The body's return value is returned. If the body throws, the transaction rolls back and nobody is woken; the error propagates so the driver can retry serialization failures, which run the whole body again. Do not call `backend.transaction` from a handler: a handler already has a transaction.

| `TransactionCall<Tx>` member | Contract |
| --- | --- |
| `tx` | The application transaction; write business data through it |
| `touch` | `touch.entry(identity)` declares a changed record; each gets one new stamp when the body returns |
| `channel(name)` | The same Channel handle as in a handler, for adding and removing members |

Same rules as a Mutation handler's, with two differences: there are no Model inputs, because nothing was uploaded, and nothing is read back, because no client is waiting for a receipt. A touched record advances its stamp even without a Channel; adding an unchanged record does not. Wakeups are process-local; distributed wake delivery needs additional application infrastructure.

## Extension points

`loaderHooks` maps model names to `{ prepareForViewer(call): Promise<void> }`. The hook runs before that model's loader in the same request context. Its failure fails the load. Use it only if viewer-specific preparation is needed; a loader already receives the user.

`native?: Native` injects the native bridge when packaging it elsewhere. It implements `validateConfig`, `processPush`, `processPull`, `settleExternal`, `negotiateLive` and `pullLive` with the string/JSON callback contracts in the [SDK source](https://github.com/zanminwang/axton/blob/main/packages/server/index.mts). The default binding comes from this repository's Node addon. This is a packaging seam; the generated Mutations, Queries and Loaders remain the application contract.

Backend methods marked `@internal` (`push`, `pull`, `negotiateLive`, `pullLive`, `onCommitted`, `notifyCommitted`, `closeLive`) are used by the listener and tests. They are not the supported application-facing HTTP integration surface.
