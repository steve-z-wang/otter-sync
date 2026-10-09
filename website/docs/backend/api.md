# Backend interfaces

## Protocol 5 configuration

Set `protocol5` on backend options with `authorizeStream(principal, stream, tx)`. Mutation admission/replay and finite authority planning authorize inside their fenced Serializable transaction. Ordinary Query/Fetch validate the immutable Store binding and authorize without holding Store progress or the global publication fence across arbitrary Handler/Loader awaits. Explicit Query tracking acquires the fence in its Serializable read transaction. `projectionGeneration` defaults to `"1"`; configure the same generation on client and backend. A materialization identifies normalized Model read contracts, independently of credentials.

`materializations` maps supported prior materialization IDs to `{schema, projectionGeneration}` using the retained descriptor and Bootstrap selection. Preserve retained operation schemas while serving frozen Mutation retries.

The optional `bootstrap({ctx})` preparation and Query contexts expose explicit tracking. Returning Model identities does not track them. The initiating `ctx.stream` is supplied by authenticated engine context; the caller cannot choose it as a business argument. Track additional recipients explicitly using the generated multi-Stream declaration API.

Bootstrap and Sync freeze finite plans. Each required unit commits whole; transport fragments cannot advance partial coverage. Mutation execution acknowledgment and committed local settlement are separate boundaries. Protocol 5 requires fresh local files; it does not migrate old pending work automatically.

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
  protocol5: { authorizeStream: (principal, stream, tx) => stream === `User:${principal}` },
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
| `loaders: Loaders<Tx>` | Implement the read function for each supported model version; leave out a [device-only Model](#device-only-models) |
| `protocol5` | Projection generation, retained materializations and transaction-bound Stream authorization |
| `bootstrap({ctx})` | Optional initial-data preparation with typed track-only handles |

`mutations` or `queries` is required when the schema retains a contract of that kind, and can be omitted otherwise; the To-do example has no Queries and passes only `mutations`. Optional options are `admit`, `translateRejection`, `onError`, `loaderHooks` and `native`, described below. The generated function binds the schema and returns the backend synchronously. The generic function in `packages/backend/server/index.mts` additionally requires `config`; normal generated integrations do not pass it.

## What your backend owns

The Rust runtime processes the sync protocol and nothing else. Application code supplies Stream and business permission policies. Rust enforces the Store binding and calls the required Stream policy; schema declarations do not replace business authorization.

| Rule | Who owns it | What the runtime does |
| --- | --- | --- |
| Authorization | Handlers decide what `userId` may write; loaders decide what `userId` may see and return `null` for the rest, whatever stream asked. | Authenticates the request and passes `userId` through. There is a trusted `authorizeStream` policy. |
| Unique constraints and identities | Your database schema. `@@unique` and `@@id` are enforced on the client only; the client's local database refuses a violating write, but nothing checks the server. | Decodes identities and patches by shape. A duplicate that your database allows is stored. |
| Child deletion | Your handler. `onTargetDelete: delete` is a client-side cascade: the client deletes the children locally, and those deletes never reach the server. A handler that deletes a parent must delete its children itself and touch them (`ctx.invalidate.todo(identity)`), leaving them in the Streams that delivered them so those Streams receive the deletion. | Reads the parent back as deleted and delivers it; a child the handler did not touch stays on other clients until a Stream delivers it. |
| Client identity | Each signed-in user gets their own local client database. Admission binds a Store to its authenticated principal and Stream; a mismatch is refused, including on saved replay. | Persists the binding in the Store row. |
| Backend language | TypeScript on Node, through the generated `createBackend`. The Dart package is a client SDK; there is no Dart or Rust-hosted backend. | Runs the same Rust engine inside the Node addon. |
| Prerequisite expressions | `@requires(Name(field: self))` is the only supported form: every argument is `self`, the value of the annotated field. The runner that satisfies prerequisites is client code. | Never sees prerequisites; they gate when the client sends a durable call, not what the backend receives. |

These are accepted limits of the current runtime, not planned features. See [deployment](deployment.md) for the process and network boundaries.

## Handlers

A handler receives `{ ctx, args }`: `ctx` holds trusted framework context and `args` holds decoded caller inputs. An optional Model operand the caller left out arrives as `null`, exactly as if it passed `null`; a `DateTime` arrives as a `Date`, at the millisecond precision every client sends. A create operand is always a complete record: fields the caller omitted were filled from their [creation defaults](../schema/reference.md#creation-defaults) by the client, and the server never fills a missing value. In the snippets, `Tx` stands for the transaction type supplied by your database adapter. A Mutation handler writes to your database and returns explicit output values; a Query handler reads and returns them. AXTON resolves Model outputs through the corresponding versioned Loader in the same transaction. For a durable Mutation, it also freezes the same-transaction private snapshot and required Stream target for each server-visible Model input, independently of each invocation's result snapshot. Database work and framework metadata share the transaction, which runs at Serializable isolation: a handler needs no row locks to stay correct, because PostgreSQL aborts a transaction whose outcome no serial order would produce and the [database shim](database.md) runs it again. A handler and its Loaders can therefore run more than once for one call, and nothing inside them may have an external effect such as sending email, a push notification or an HTTP call. Record the effect in the transaction, an application outbox, and perform it after commit.

```ts title="action-contract"
import { CallRejected, type Mutations } from './generated/backend.ts';

// saveTodo is application code that writes to the business database.
const handleAddTodoV2: Mutations<Tx>['addTodo']['v2'] =
  async ({ ctx, args }) => {
    if (!args.todo.title.trim()) throw new CallRejected('todo.title_empty');
    await saveTodo(ctx.tx, args.todo);
    ctx.invalidate.todo(args.todo);
    // The new Todo joins the Stream once; its later changes reach it with no enrollment.
    ctx.streams(['todos']).track.todo(args.todo);
    return { relatedTodo: null, matches: [], count: 1, state: null };
  };
```

The `todo` input describes local optimism. The handler explicitly invalidates every canonical identity it changes. Its durable receipt identifies the authority required for settlement, separately from the invocation result. The result holds only the declared outputs: `relatedTodo` and `matches` are identity-selected Model outputs that the Loader resolves at this invocation, and `count` and `state` are ordinary outputs. Other clients learn of the change through the `todos` Stream.

An output is independent of the inputs even when the names match. The fixture's `EditAndRead(todo Todo.update) { todo Todo }` may edit one Todo and return another:

```ts title="action-contract"
import { type Mutations } from './generated/backend.ts';

const handleEditAndRead: Mutations<Tx>['editAndRead'] =
  async ({ ctx, args }) => {
    await saveTodo(ctx.tx, args.todo);
    ctx.invalidate.todo(args.todo);
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
| `invalidate` | Yes | No | `invalidate.todo(identity)` declares a canonical identity this Mutation changed; see [Streams](#streams) |
| `stream`, `streams(names)` | Yes | Yes | A handle for tracking records and selected invalidation; see [Streams](#streams) |

A handler returns the generated explicit output shape, or no value when it has no outputs. Explicit invalidation publishes changed canonical records to existing holders; tracking enrolls selected identities. Model inputs do not imply publication. A durable Mutation freezes private input snapshots and required Stream targets in its receipt, independently of its result snapshot.

A Query has track-only `ctx.stream` and `ctx.streams([...])`, with no invalidation. It may establish Stream membership while reading. Business side effects remain forbidden: `ctx.tx` is your application transaction, not a SQL sandbox. Request retries retain the exact invocation identity; a new invocation reads again.

Loaders vary by authenticated viewer and declared Model version, never by which Stream asked. Cache snapshots use the active materialization's Model contracts; retained invocation output contracts may differ. Ordinary Query/Fetch records have null cursors, while Stream delivery records carry authoritative positions.

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

An error that is neither `CallRejected` nor translated to a business code rejects that call with `handler.failed` and reaches `onError`. Other named calls retain their independent outcomes. A retryable database transaction error instead retries the transaction; it is not saved as a permanent business rejection.

## Bootstrap preparation

`bootstrap?: ({ctx}) => Promise<void>` is optional application preparation. Its typed context contains the current `tx`, `userId`, `callId`, initiating `ctx.stream`, and explicit `ctx.streams([...])`. Both Stream handles only track; neither provides invalidation. AXTON acquires its publication fence before preparation and freezes the finite Bootstrap plan in the same transaction.

The schema's `@@bootstrap` marks historical Model types selected for initial materialization. Queries cover named history outside that baseline. Manifest paging, coverage, replay and tail capture belong to the native runtime; applications do not return a Load continuation or manage a Load job. Receipt-target recovery does not call this preparation callback.

Preparation tracks the complete initial scope, including scopes larger than one Query page's enrollment limit. Applications may scan identities in batches; the framework deduplicates them before publication. Native delivery applies its finite-plan capacity and transport paging separately.

## Loaders

```ts title="action-contract"
import type { Loaders } from './generated/backend.ts';

const loadTodoV2: NonNullable<Loaders<Tx>['todo']>['v2'] =
  async ({ ids, tx, userId }) =>
    Promise.all(ids.map(id => loadVisibleTodo(tx, userId, id)));
```

`LoaderCall<Tx, Identity>` contains:

| Field | Meaning |
| --- | --- |
| `ids` | Read-only list of typed record identities |
| `tx` | Your transaction, shared with sync persistence for this request |
| `userId` | Caller whose visibility must be checked |

A Loader is not told which stream, if any, asked: it serves Mutation and Query Model outputs, durable authority readback, catch-up pages, the live stream and a client's [`client.fetch`](../frontend/client-api.md#fetch-a-record-from-the-backend) of one record, which needs no handler of its own. It sees the same application transaction during a call.

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
| A row object | The record's current state for this user | Stream/manifest delivery carries its authoritative position. Query/Fetch delivery is an ordinary null-cursor snapshot. |
| `null` | The record does not exist, or this user must not see it | Stream/manifest null installs canonical absence and retains deletion protection, preserving pending overlays. Ordinary Query/Fetch null returns absence without deleting an authoritative cached row. |
| a thrown `CallRejected` (or a translated rejection) | A refused read | A call fails with the rejection. Delivery cannot commit the required authority unit or its progress. |
| any other thrown error | A failure | Reported to `onError`; it cannot become absence or skip required progress. Independent earlier units can commit when the server proves their prefix; bounded adaptive requests may recover that prefix. |
| `undefined`, a missing entry, a non-array result, a nonfinite number | A defect | The affected read fails. It is never interpreted as `null`. |

A row object must match the generated model type exactly. Include every non-identity field: a nullable field that is absent reads as `null`, but an absent non-nullable field is a defect. The identity fields may be present. Any other property, such as an extra database column or a relation object, is a defect. Map your rows to the model type rather than returning a wider database row.

Loaders run during synchronization and calls, not when the app calls local `get`, `query` or `watch`. A malformed result never discharges delivery progress. The call or required delivery unit fails and `onError` receives the diagnostic.

Every member of `Loaders<Tx>` is optional, so a standalone Loader typed from it uses `NonNullable<…>`, as above. A Model that registers a Loader registers every retained version; a key that names no Model is refused at startup.

### Device-only Models

A Model whose Loader you leave out is device-only: a composer's working copy or a cache of signed URLs, written and read only on the client. Whether a Model syncs follows from where it is written; the schema declares nothing extra.

- **On the client** the Model works like any other for local `create`, `update`, `delete`, `get`, `query` and `watch`, and inside transactions and local companions. Those writes stay in local SQLite and are never sent.
- **At startup** `createBackend` throws when a retained Mutation would carry the Model on the wire, or a Mutation or Query would return it, because each needs its Loader: `Mutation SaveDraft v1 slot draft names Model Draft, which has no Loader; a Model without a Loader is device-only and never on the wire`.
- **In a handler, `backend.transaction` or `backend.publish`** the Model is never published. `invalidate.draft(…)`, `streams(names).track.draft(…)` and mixed `streams(names).track([...])` naming it throw at the call: `invalidate.draft: Model Draft has no Loader, so it is device-only and cannot be published`. In a handler that is the call's `handler.failed`.
- **`client.fetch.draft(…)`** fails with `loader.unregistered`.

A backend that registers a Loader for every Model, including one that always answers `null` for a device-only Model, keeps working unchanged.

## Streams

A Stream is a resumable ordered notification sequence. `track` establishes durable interest in a record; `invalidate` requests current authority for existing interested streams. Neither grants permission. The viewer Loader decides current content or absence independently of the delivery source.

```ts title="action-contract"
import { Todo, createBackend, devAuth } from './generated/backend.ts';

const backend = createBackend<Tx>({
  database, authenticate: devAuth(), mutations, queries, loaders,
  protocol5: { authorizeStream: (principal, stream, tx) => stream === `User:${principal}` },
});
await backend.transaction(async ctx => {
  const records = [Todo({ id: 'A' }), Todo({ id: 'B' })];
  ctx.streams(['User:alice', 'User:bob']).track(records);
  ctx.invalidate.todo(['A', 'B']);
  ctx.streams(['User:alice']).invalidate.todo('C');
});
```

| Interface | Return and behavior |
| --- | --- |
| `ctx.stream` | Callback-bound initiating Stream on Mutation, Query and Bootstrap contexts. |
| `ctx.streams(names)` | Explicit readonly list of Streams. Background contexts only expose this form. Names are nonblank and case-sensitive. |
| `stream.track.todo(identityOrList)`, `stream.track(referenceOrList)` | `void`; establishes durable unique Stream/Model/Identity pairs. A first pair receives the transaction's affected-Stream cursor. Repeating a live pair retains its cursor. |
| `ctx.invalidate.todo(identityOrList)`, `ctx.invalidate(referenceOrList)` | `void`; updates each identity's existing holders at the transaction's affected-Stream cursor. With no holders it enrolls nobody and advances no Stream. |
| `stream.invalidate.todo(identityOrList)`, `stream.invalidate(referenceOrList)` | `void`; updates only selected existing holders at the transaction's affected-Stream cursor. Never enrolls. |

Generated Model methods accept scalar or complete object identities for a single-field identity, complete objects for composite identities, and one identity or a readonly list. Mixed calls require generated `RecordRef` constructors that identify the Model. Models without a viewer Loader cannot be declared.

Declarations are synchronous, copy and canonicalize operands before appending, and expire with the callback. Multiple names and records declare their Cartesian product; use separate declarations for different associations. Empty name or record lists do nothing. Ordinary invalid declarations throw before appending any effects; preparation collectors retain a sticky failure even if caught.

The enclosing settlement combines declarations regardless of order. Each identity advances at most once and each final pair receives at most one position. Targeted sets union; global invalidation dominates. Model inputs do not replace publication declarations: explicitly invalidate all changed identities. Tracking another Stream or reading an output does not change authority. There is no extra batch or execute call and no request per declaration. Separate `backend.publish` invocations remain separate settlements.

### Content and authority

Shared content changes must invalidate all holders. Selected invalidation serves viewer-specific projection or permission changes when other viewers' answers remain valid. A changed row-to-null answer requires a newer canonical position even if only selected streams are notified. Loader errors retain local content and are never absence; a failed required Loader read cannot discharge delivery progress.

Business deletion and access revocation are authority changes: invalidate affected identities and let their viewer Loader return `null`. Tracking survives absence, allowing offline deletion delivery and later reinstatement. The public API has no withdrawal, tags or selectors and no automatic tracking retention policy. Membership Remove is delivery evidence: it retains the client Model and guards while releasing live-content protection. True Stream null supplies canonical absence and keeps deletion protection. The current client follows exactly one bound Stream.

| Context | Available declarations |
| --- | --- |
| Mutation or legacy slot handler; `backend.transaction` or `backend.publish` | Multi-stream tracking, global and selected invalidation. |
| Query or Bootstrap | Initiating and explicitly selected Stream tracking only. |
| Viewer Loader | No declarations; preparation hooks receive explicit typed `streams` and `invalidate`. |
| Client transaction or Mutation input callback | Device-only Model CRUD; no server declarations or Stream subscription management. |

See [Bootstrap](../frontend/loads.md), [cache authority](../frontend/sync.md#accounts-and-cache-authority), and [settlement](https://github.com/zanminwang/axton/blob/main/docs/engineering/architecture/server/engine/publish.md).

## Authentication

`Authenticate` receives Node's `IncomingMessage` and returns a user ID string, null, undefined, or a promise of those values. Null/undefined or a blank user ID rejects authentication. The SDK calls it for HTTP requests and WebSocket connections. Verify your application's session/token here; enforce read permissions in loaders and write permissions in handlers.

`devAuth(): Authenticate` treats `Authorization: Bearer <userId>` as the identity without verification. It is provided for local development, not production authentication. See [authentication and account changes](../frontend/sync.md#accounts-and-cache-authority).

## Admission

`admit?: Admit` decides whether a client may use the listener at all, for example to turn away app builds below your supported floor. It receives Node's `IncomingMessage`, including the `headers` the client was configured with ([server connection](../frontend/runtime.md#server-connection)), and the user ID `authenticate` returned, or `null` when there is none. Return `null` or `undefined` to admit, or an `AdmissionRefusal` `{ status, body }` to refuse:

```ts
const minimumBuild = 42; // your supported floor
const admit: Options<Prisma.TransactionClient>['admit'] = (request) => {
  const build = Number(request.headers['x-app-build']);
  return build < minimumBuild ? { status: 426, body: { minimumBuild } } : null;
};
```

It runs on every listener route and the WebSocket upgrade, after `authenticate` and before anything else, so a refusal wins over `401`. The response has your `status` (400-599), your JSON `body` and the header `axton-admission: refused`; the client SDKs stop the connection and hand `onError` one `AdmissionRefused` instead of retrying. A hook that throws, or returns anything else, is a server error: `500 { code: "server" }`, and the error goes to `onError`. `admit` is optional; without it every authenticated request is admitted.

## Errors

| Interface | Use |
| --- | --- |
| `new CallRejected(code)` | Reject one Mutation or Query call with a stable machine-readable code |
| `translateRejection(error)` | Return a stable rejection code for a known application error; return null/undefined for other errors |
| `onError(error)` | Log server failures that are returned to the client as a generic server error. A handler or Loader failure is reported inside the transaction, so a transaction retried after a serialization failure can report the same failure once per attempt |
| `EngineError` | A failure from the native engine: `code` (stable), `message` (readable, may change), `details` (fields the code promises) |

Codes must match `^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`, such as `todo.title_empty`. A recognized business rejection rolls back that call's business writes, canonical publications, memberships and deliveries. For a queued call, `wait()` returns a `CallError` outcome and any optimistic Model change rolls back; the durable rejection remains inspectable until dismissed. For a direct call, the promise rejects with `CallError`. An unknown transport outcome can be retried with the same call identity; it is not evidence that the handler did nothing.

Business codes come from `CallRejected` or `translateRejection`; `action_version_unsupported`, `handler.failed`, `loader.failed`, `model_version_unsupported` and `query.effects_forbidden` identify framework failures attributable to one call. A durable receipt records each call's outcome. Distinct durable Calls retain independent outcomes. A direct response carries the same final outcome for that call.

Infrastructure errors that make the transaction unusable abort delivery for retry. A serialization conflict that outlasts the shim's `retries` is one of them: the call is not rejected, and a queued call stays queued and is sent again. `onError` receives diagnostic failures, including handler and Loader exceptions. Diagnostic callback exceptions after a committed outcome cannot replace its result, repeat its handler or become a transport error; SDKs report those callback exceptions through their runtime uncaught-error channel.

Protocol refusals use a status and JSON body chosen by the engine error's `code`; message text may change. Per-call content errors are outcomes, while a pull or live subscription with an unsupported Model declaration receives a whole-request `409`.

| Code | HTTP status | Meaning |
| --- | --- | --- |
| (your `admit` refusal) | Your status, with `axton-admission: refused` | Your JSON body ([Admission](#admission)) |
| `protocol.unsupported` | 426 | Unsupported numeric protocol discriminator; refused before handler or progress effects |
| `request.invalid` | 400 | Malformed body, or a pull cursor ahead of the stream head |
| `store.binding` | 403 | The Store belongs to another principal or Stream |
| `stream.forbidden` | 403 | Required Stream authorization refused the principal |
| `batch.sequence`, `batch.conflict`, `batch.progress` | 409 | Invalid Batch sequence, immutable intent or retained progress |
| `page.capacity`, `constraint_group_capacity` | 413 | A required finite authority unit exceeds its capacity bound |
| `model_version_unsupported` | 409 | Pull and live subscribe: a Model read contract this backend does not serve. During a call it is a per-call failure. |
| `handler.invalid` | 500 `{ code: "server" }` | The handler's settlement could not be used: an invalid rejection code, or a change or membership naming a record without a model or an object identity |
| anything else | 500 `{ code: "server" }` | A server-side failure; the `EngineError` or thrown error goes to `onError` |

## Listener

`await backend.listen({ port, host? })` binds a Node HTTP and WebSocket server. Host defaults to `127.0.0.1`; port zero selects an available port. The result is `{ url, close(): Promise<void> }`.

| Route | Purpose |
| --- | --- |
| `POST /sync/handshake` | Bind the Store and capture its authenticated Stream head |
| `POST /sync/mutations` | Execute an immutable Batch of named Mutations with independent member outcomes |
| `POST /sync/actions` | Execute a fresh named Query and return null-cursor invocation snapshots |
| `POST /sync/fetch` | Read one record through its Model's Loader for `client.fetch` |
| `POST /sync/materialize` | Serve settlement-owned or schema-owned finite authority plans |
| `POST /sync/pull` | Serve finite Bootstrap and Sync plan fragments |
| `/sync/live` (WebSocket) | Acknowledge the one bound Stream and deliver proved authoritative units |

The listener has no TLS, CORS or proxy-header handling and binds to loopback by default; run it behind a reverse proxy as described in [Deploy the backend](deployment.md).

Generated clients use all of these routes automatically from one bound `connection` configuration. The WebSocket subscription acknowledgement confirms that stream listeners are installed before HTTP catch-up starts, so changes during catch-up can be queued and reconciled. Listener errors reject. `await server.close()` releases the listener and its live connections; your application must drain admitted database transaction promises before separately closing its database pool. The supported listener owns its server; mounting into an application-owned HTTP server is not currently exposed.

## Background writes

Writes outside handlers have no readback and no receipt; they reach clients only through Streams. Run them through `backend.transaction`: the framework opens the application transaction and hands the body the same `invalidate` and `stream` a Mutation handler receives. When the body returns, the framework reserves one cursor per affected Stream and settles explicit tracking and existing-holder invalidation inside that same transaction; once it commits, the live subscribers of the affected Streams are woken.

```ts
await backend.transaction(async ({ tx, streams, invalidate }) => {
  await tx.entry.update({ where: { id: 'entry-1' }, data: { text: 'From a job' } });
  invalidate.entry({ id: 'entry-1' });
  streams(['book:demo']).track.entry({ id: 'entry-1' });
});
```

`tx` is the transaction of the shim passed as `database`. The body's return value is returned. If the body throws, the transaction rolls back and nobody is woken; the error propagates so the driver can retry serialization failures, which run the whole body again. Do not call `backend.transaction` from a handler: a handler already has a transaction.

| `TransactionCall<Tx>` member | Contract |
| --- | --- |
| `tx` | The application transaction; write business data through it |
| `invalidate` | `invalidate.entry(identity)` declares a changed record; publishes its current canonical state when the body returns |
| `streams(names)` | The same Stream handle as in a handler, for tracking records and selected invalidation |

Same rules as a Mutation handler's, with two differences: there are no Model inputs, because nothing was uploaded, and nothing is read back, because no client is waiting for a receipt. Tracking an unchanged record is idempotent; invalidation updates existing holders using the transaction's affected-Stream cursor. Wakeups are process-local; distributed wake delivery needs additional application infrastructure.

### In a transaction you own

When your code has already opened the transaction, for example another framework's request handler, `backend.publish(tx, body)` settles the same declarations inside it. `tx` must be a transaction of the tool the `database` shim was built on.

```ts
const wake = await db.$transaction(async (tx) => {
  await backend.acquirePublicationFence(tx);
  await tx.entry.update({ where: { id: 'entry-1' }, data: { text: 'From my host' } });
  return backend.publish(tx, ({ streams, invalidate }) => {
    invalidate.entry({ id: 'entry-1' });
    streams(['book:demo']).track.entry({ id: 'entry-1' });
  });
}, { isolationLevel: "Serializable" });
wake();
```

| Behavior | Contract |
| --- | --- |
| Settlement | Runs before `publish` resolves: tracking and Stream positions are written through `tx`, so they commit or roll back with it, a savepoint included. Repeated publication calls reuse one cursor per affected Stream in that SQL transaction; records have no independent publication stamp |
| Wake | `publish` resolves to a function. Call it after `tx` commits; after a rollback, drop it. Until it is called, no live subscriber is told; they catch up on their next wake or reconnect |
| Errors | A refused declaration or a database error rejects `publish` with the original error, so your retry loop can recognize a serialization failure and run the whole transaction again. Wakes from failed attempts are simply never called |
| Isolation | AXTON does not choose the level of your transaction. Use Serializable isolation and acquire the persisted publication fence before relevant application work. Retry the entire transaction on a serialization conflict; a later publication call cannot repair an earlier stale snapshot |
| Refusal | A transaction AXTON is already serving, a handler's or `backend.transaction`'s, is refused: declare through its own `invalidate` and `stream` |

## Extension points

`loaderHooks` maps model names to `{ prepareForViewer(call): Promise<void> }`. The hook runs before that model's loader in the same request context. Its failure fails the affected read. The typed call provides `call.streams([...])` and `call.invalidate` for preparation publications; explicit preparation publications use the same transaction and cursor reservations. Use it only if viewer-specific preparation is needed; a loader already receives the user.

`native?: Native` injects the native bridge when packaging it elsewhere. Its current entrypoints include `validateConfig`, `handshake05`, `validateMutationBatch`, `processBatchMember`, `encodeBatchAcknowledgement`, `processRead05`, `processDelivery05`, `processMaterialization05`, `processLive05` and `settleExternal05`; `negotiateLive`, `liveEvent` and `liveClose` manage the native socket session with the string/JSON callback contracts in the [SDK source](https://github.com/zanminwang/axton/blob/main/packages/backend/server/index.mts). The default binding comes from this repository's Node addon. This is a packaging seam; the generated Mutations, Queries and Loaders remain the application contract.

Backend methods marked `@internal` are used by the listener and tests. They are not the supported application-facing HTTP integration surface.
