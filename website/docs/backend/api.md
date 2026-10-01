# Backend interfaces

Your backend implements Mutations and Queries through handlers and the read/sync path through loaders. The compiler generates their TypeScript interfaces from your schema. AXTON supplies protocol processing; your application supplies business logic, authorization and a database transaction.

Handler signatures below follow the [generated operation fixture](https://github.com/zanminwang/axton/blob/main/integration/action-contract/schema.model). The background-write example uses an independent `Entry` Model fixture. The working To-do backend is [examples/todo/server.mts](https://github.com/zanminwang/axton/blob/main/examples/todo/server.mts).

## createBackend

```ts title="action-contract"
import { createBackend, devAuth } from './generated/backend.ts';
import { database } from './database.ts';
import { mutations, queries } from './handlers.ts';
import { loaders } from './loaders.ts';
import { loads } from './loads.ts';

const backend = createBackend<Tx>({
  database,
  authenticate: devAuth(),
  mutations,
  queries,
  loaders,
  loads,
  onError: error => console.error(error),
});
const server = await backend.listen({ port: 4242 });
console.log(server.url);
```

`database.ts` exports a PostgreSQL shim such as `prisma(db)`, and `Tx` is its transaction type; see [Database](database.md) for the shims and AXTON's metadata migration, which must be applied first. `handlers.ts`, `loaders.ts` and `loads.ts` contain the implementations below.

The generated `Options<Tx>` requires:

| Option | Responsibility |
| --- | --- |
| `database: Database<Tx>` | A PostgreSQL shim, `pg(pool)`, `prisma(client)` or `drizzle(db)`, or `persistence(driver)` over your own driver ([Database](database.md)) |
| `authenticate: Authenticate` | Resolve the caller's user identity or reject the request |
| `mutations: Mutations<Tx>` | Implement each retained Mutation version |
| `queries: Queries<Tx>` | Implement each retained Query version |
| `loaders: Loaders<Tx>` | Implement the read function for each supported model version; leave out a [device-only Model](#device-only-models) |
| `loads: Loads<Tx>` | Implement the page handler of each retained Load version ([Load handlers](#load-handlers)) |

`mutations`, `queries` or `loads` is required when the schema retains a contract of that kind, and can be omitted otherwise; the To-do example has no Queries and passes only `mutations`. Optional options are `admit`, `translateRejection`, `onError`, `loaderHooks` and `native`, described below. The generated function binds the schema and returns the backend synchronously. The generic function in `packages/server/index.mts` additionally requires `config`; normal generated integrations do not pass it.

## What your backend owns

The Rust runtime processes the sync protocol and nothing else. The rules below are yours to implement; the runtime neither enforces nor checks them, and the schema does not make it do so.

| Rule | Who owns it | What the runtime does |
| --- | --- | --- |
| Authorization | Handlers decide what `userId` may write; loaders decide what `userId` may see and return `null` for the rest, whatever scope asked. | Authenticates the request and passes `userId` through. There is no scope-level policy. |
| Unique constraints and identities | Your database schema. `@@unique` and `@@id` are enforced on the client only; the client's local database refuses a violating write, but nothing checks the server. | Decodes identities and patches by shape. A duplicate that your database allows is stored. |
| Child deletion | Your handler. `onTargetDelete: delete` is a client-side cascade: the client deletes the children locally, and those deletes never reach the server. A handler that deletes a parent must delete its children itself and touch them (`ctx.touch.todo(identity)`), leaving them in the Scopes that delivered them so those Scopes receive the deletion. | Reads the parent back as deleted and delivers it; a child the handler did not touch stays on other clients until a Scope delivers it. |
| Client identity | Each signed-in user gets their own local client database. A client id is bound to the first user that pushed with it; a push from another user with the same client id answers `403 client.owner_mismatch`, and there is no reassignment. | Stores the owner with the client row. |
| Backend language | TypeScript on Node, through the generated `createBackend`. The Dart package is a client SDK; there is no Dart or Rust-hosted backend. | Runs the same Rust engine inside the Node addon. |
| Prerequisite expressions | `@requires(Name(field: self))` is the only supported form: every argument is `self`, the value of the annotated field. The runner that satisfies prerequisites is client code. | Never sees prerequisites; they gate when the client sends a durable call, not what the backend receives. |

These are accepted limits of the current runtime, not planned features. See [deployment](deployment.md) for the process and network boundaries.

## Handlers

A handler receives `{ ctx, args }`: `ctx` holds trusted framework context and `args` holds decoded caller inputs. An optional Model operand the caller left out arrives as `null`, exactly as if it passed `null`; a `DateTime` arrives as a `Date`, at the millisecond precision every client sends. A create operand is always a complete record: fields the caller omitted were filled from their [creation defaults](../schema/reference.md#creation-defaults) by the client, and the server never fills a missing value. In the snippets, `Tx` stands for the transaction type supplied by your database adapter. A Mutation handler writes to your database and returns explicit output values; a Query handler reads and returns them. AXTON resolves Model outputs through the corresponding versioned Loader in the same transaction. For a durable Mutation, it also reads the batch-final state of each Model input into the receipt, independently of each invocation's result snapshot. Database work and framework metadata share the transaction, which runs at Serializable isolation: a handler needs no row locks to stay correct, because PostgreSQL aborts a transaction whose outcome no serial order would produce and the [database shim](database.md) runs it again. A handler and its Loaders can therefore run more than once for one call, and nothing inside them may have an external effect such as sending email, a push notification or an HTTP call. Record the effect in the transaction, an application outbox, and perform it after commit.

```ts title="action-contract"
import { CallRejected, type Mutations } from './generated/backend.ts';

// saveTodo is application code that writes to the business database.
const handleAddTodoV2: Mutations<Tx>['addTodo']['v2'] =
  async ({ ctx, args }) => {
    if (!args.todo.title.trim()) throw new CallRejected('todo.title_empty');
    await saveTodo(ctx.tx, args.todo);
    // The new Todo joins the Scope once; its later changes reach it with no enrollment.
    ctx.scope('todos').add.todo(args.todo);
    return { relatedTodo: null, matches: [], count: 1, state: null };
  };
```

The `todo` input is already a change: AXTON stamps it and returns its authority to the caller, which completes without a subscription, whatever the outputs or the call's `store` option. It is not part of the result. The result holds only the declared outputs: `relatedTodo` and `matches` are identity-selected Model outputs that the Loader resolves at this invocation, and `count` and `state` are ordinary outputs. Other clients learn of the change through the `todos` Scope.

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
| `touch` | Yes | No | `touch.todo(identity)` declares a record this Mutation changed beyond its Model inputs; see [Scopes](#scopes) |
| `scope(name)` | Yes | No | A handle for adding records to or removing them from a Scope; see [Scopes](#scopes) |

A handler returns the generated explicit output shape, or no value when the operation has no explicit outputs. AXTON allocates a **stamp** for each changed record, the Model inputs plus the records the handler touched, and on durable delivery reads the inputs' batch-final content through the Loader into the receipt. Touch every other business record the handler changed: a touched record is delivered to its Scopes, but it is not returned to the caller, so the caller need not know its Model.

A Query's context has no `touch` or `scope`, in its type and at runtime. The engine also refuses any Query settlement that reports changes or memberships: that call fails with `query.effects_forbidden`, its savepoint rolls back before any stamp, readback or publication, and adjacent calls in the batch are unaffected. This is not a SQL sandbox. `ctx.tx` is still your application's transaction, and the framework cannot inspect the SQL a handler runs or other clients it has captured, so keeping a Query free of business side effects is your application's responsibility. Framework metadata is still written: each Query outcome is saved by call ID like a Mutation's, so retrying the same call ID replays the saved result and a new invocation reads again.

A loader is scope-independent: the row it returns for a record is the row every client receives for it, in the receipt, in a catch-up page and on the live stream, at the same stamp. What a loader may vary by is `userId`.

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

## Load handlers

A schema with `load` declarations generates `Loads<Tx>`: one handler per retained Load version, registered in the required `loads` option of `createBackend`, as a bare function for a v1-only Load or `{ v1, v2 }`. It receives `{ ctx, args, continuation }` and returns `{ data, next }`: one identity list per declared output and the next continuation, `null` when done. Its `LoadContext<Tx>` has `tx`, `userId`, `callId`, `loadId` and `scope(name)`, and no `touch`. That `scope(name)` is a `LoadScope`: it enrolls and attaches labels only for records this page returns, which commit with the page ([Add loaded records to a Scope](../frontend/loads.md#add-loaded-records-to-a-scope)). Each page runs in its own transaction, and a repeated page request returns the saved page without running the handler again or adding anything. Your handler owns ordering, consistency, authorization and termination. Return identities, not records: a full record returned from an unannotated handler compiles but fails the page with `handler.invalid`. See [Implement the backend handler](../frontend/loads.md#implement-the-backend-handler).

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

A Loader is not told which scope, if any, asked: it serves Mutation and Query Model outputs, durable authority readback, catch-up pages, the live stream and a client's [`client.fetch`](../frontend/client-api.md#fetch-a-record-from-the-backend) of one record, which needs no handler of its own. It sees the same application transaction during a call.

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
| `null` | The record does not exist, or this user must not see it | Delivered as a deletion. A newer stamp clears the authoritative row, whichever scope delivered it; the client keeps the stamp so older content cannot bring the record back; pending local operations are replayed on that state. |
| a thrown `CallRejected` (or an error `translateRejection` maps to a code) | A refused read | During a call, that call is rejected with the code and rolled back. In a pull, that record is delivered as an error with that code: the client keeps its local copy and reports it, and the rest of the page applies |
| any other thrown error | A failure | Reported to `onError` (default `console.error`). During a call, the call is rejected with `loader.failed`; in a pull, that record is delivered as a `loader.failed` error and the rest of the page applies |
| `undefined`, a missing entry, a non-array result, a nonfinite number | A defect | Reported to `onError` and treated like a thrown error: only the records it affects fail. It is never read as `null` |

A row object must match the generated model type exactly. Include every non-identity field: a nullable field that is absent reads as `null`, but an absent non-nullable field is a defect. The identity fields may be present. Any other property, such as an extra database column or a relation object, is a defect. Map your rows to the model type rather than returning a wider database row.

Loaders run during synchronization and calls, not when the app calls local `get`, `query` or `watch`. A malformed result is never skipped silently: the affected record arrives as an error change, or the call being read back is rejected with `loader.invalid`, and `onError` hears about it.

Every member of `Loaders<Tx>` is optional, so a standalone Loader typed from it uses `NonNullable<…>`, as above. A Model that registers a Loader registers every retained version; a key that names no Model is refused at startup.

### Device-only Models

A Model whose Loader you leave out is device-only: a composer's working copy or a cache of signed URLs, written and read only on the client. Whether a Model syncs follows from where it is written; the schema declares nothing extra.

- **On the client** the Model works like any other for local `create`, `update`, `delete`, `get`, `query` and `watch`, and inside transactions and local companions. Those writes stay in local SQLite and are never sent.
- **At startup** `createBackend` throws when a retained Mutation would carry the Model on the wire, or a Mutation, Query or Load would return it, because each needs its Loader: `Mutation SaveDraft v1 slot draft names Model Draft, which has no Loader; a Model without a Loader is device-only and never on the wire`.
- **In a handler, `backend.transaction` or `backend.publish`** the Model is never published. `touch.draft(…)`, `scope(name).add.draft(…)` / `scope(name).remove.draft(…)` and a mixed `scope(name).add/remove([...])` naming it throw at the call: `touch.draft: Model Draft has no Loader, so it is device-only and cannot be published`. In a handler that is the call's `handler.failed`.
- **`client.fetch.draft(…)`** fails with `loader.unregistered`.

A backend that registers a Loader for every Model, including one that always answers `null` for a device-only Model, keeps working unchanged.

## Scopes

A Scope holds identities for delivery to subscribed clients. Enroll a record once; later changes reach every Scope holding it. The viewer's Loader decides what each subscriber may see. Scope names and labels grant no access.

```ts title="action-contract"
import { Todo, type MutationContext } from './generated/backend.ts';

function organize(ctx: MutationContext<unknown>) {
  const s = ctx.scope('User:alice');
  s.add.todo(['A', 'B']).tag('journal:1');
  s.add(Todo({ id: 'C' }));
  s.remove.todo('D');
  s.where({ tags: { only: ['journal:1'] } }).remove();
  s.tag('journal:1').remove();
  ctx.touch.todo(['E', 'F']);
}
```

| Interface | Return value and effect |
| --- | --- |
| `ctx.scope(name)` | A callback-bound `Scope` handle; creates no membership and sends no request. The name must be nonblank. |
| `s.add.todo(identityOrList)` | `AddDeclaration`; ensures membership for one Model. |
| `s.add(referenceOrList)` | `AddDeclaration`; ensures membership for generated references, which may name different Models. |
| `s.remove.todo(identityOrList)`, `s.remove(referenceOrList)` | `void`; withdraws the entire membership and its labels. An absent membership is a no-op. |
| `addition.tag(labelOrList)` | The same `AddDeclaration`; attaches labels to exactly the addition's captured identities. |
| `s.tag(labelOrList)` | A label editor, with record-targeted `add` and `remove`; its argument-free `remove()` detaches those labels from every current member. |
| `s.where(predicate)`, `s.where.todo(predicate)` | A selection handle; selects current members, optionally restricted to one generated Model. |
| `selection.remove()` | `void`; withdraws matching memberships. |
| `selection.tag(labelOrList).add()` / `.remove()` | `void`; edits labels on matching memberships. |
| `ctx.touch.todo(identityOrList)`, `ctx.touch(referenceOrList)` | `void`; declares changed business content. |
| `Todo(identity)` | A generated `RecordRef`, for mixed operations. |

For a single-field Identity, typed methods accept either the field's scalar value or the complete Identity object. Composite identities require complete objects. Each record operation accepts one operand or a readonly list. Mixed operations require generated references: a bare `{ id: 'A' }` does not identify its Model. Empty record lists do nothing; argument-free root `s.add()` and `s.remove()` are invalid.

Declarations are synchronous. They copy operands when called and settle with the enclosing operation, in invocation order. No `await`, execute or terminal commit call is needed: ignoring an add's return value still declares enrollment. Scope, add, label and selection handles expire when their originating callback returns or throws. A later rejection or failure rolls back the enclosing unit's effects.

### Add declarations and label editors

```ts title="action-contract"
import type { MutationContext } from './generated/backend.ts';

function label(ctx: MutationContext<unknown>) {
  const s = ctx.scope('board:1');
  s.add.todo('A').tag(['X', 'Y']).tag('Z');
  s.tag('X').add.todo('A');
  s.tag(['X', 'Y']).remove.todo('A');
  s.tag('Z').remove();
}
```

`add` preserves existing labels; repeated enrollment is idempotent. Chained `.tag(...)` adds labels after the add declaration. Each later call to a saved add handle appends its label attachment at that later invocation position. It does not rewrite the original add or resurrect a removed member.

Standalone `s.tag('X').add.todo('A')` requires A to be a member at that declaration's settlement position. A missing member fails the enclosing handler or host operation and rolls back its effects. To enroll and label, use `s.add.todo('A').tag('X')`. Saving that add handle, removing A and then calling the saved handle's `.tag('X')` also fails.

Label removal is idempotent, including for absent memberships. Removing the last label leaves membership present. Labels are backend-only grouping data: editing labels alone invokes no Loader, allocates no content stamp or delivery cursor and emits no client event. A label editor's argument-free `add()` is invalid; its argument-free `remove()` explicitly detaches its labels Scope-wide.

Labels are case-sensitive, opaque nonblank strings of at most 256 UTF-8 bytes each. Accepted spelling is preserved. Label operations copy and deduplicate their input and accept at most 64 distinct labels. A label editor accepts one string or a nonempty readonly list; the list names labels to edit, rather than a matching condition.

### Select members

```ts title="action-contract"
import type { MutationContext } from './generated/backend.ts';

function retireLabel(ctx: MutationContext<unknown>) {
  const s = ctx.scope('board:1');
  s.where({ tags: { only: ['X'] } }).remove();
  s.tag('X').remove();
}
```

If A has X and Y, B has only X, and C has only Y, these declarations leave A/Y and C/Y present and withdraw B. Only B emits a withdrawal. Reversing the calls detaches X first, so `only: ['X']` matches nothing. `all: ['X'], none: ['Y']` is broader than `only: ['X']`: it also matches a member with X and an additional Z.

| Predicate | Matching members |
| --- | --- |
| `tags.all: ['X', 'Y']` | Have every listed label; extra labels are allowed. |
| `tags.any: ['X', 'Y']` | Have at least one listed label. |
| `tags.none: ['X', 'Y']` | Have none of the listed labels. |
| `tags.only: ['X']` | Have exactly the listed label set. |
| `tags.only: []` | Have no labels. |
| `and: [predicate, ...]` | Match every child. |
| `or: [predicate, ...]` | Match at least one child. |
| `not: predicate` | Do not match the child. |

Sibling conditions combine with AND; label order and duplicates do not affect matching. Predicates are copied at invocation. Building a selection changes nothing and freezes no database result. Each terminal call evaluates it at its settlement position, after earlier declarations; reusing a selection evaluates it again.

A predicate may have depth at most 16, counting the root as 1; at most 128 predicate nodes; at most 64 distinct labels per leaf operator; and at most 65,536 UTF-8 bytes of JSON. Empty predicates, empty `tags`, empty `and`/`or`, empty `all`/`any`/`none`, unknown keys, null conditions and malformed values are invalid. `only: []` is valid.

Selectors inspect Scope membership and labels. They do not query business fields, execute application code or SQL, or join business tables. There is no selection `.add()`; selected records are already members. `selection.tag('Y').add()` adds labels to them.

### Content and withdrawal

`touch` allocates one content stamp per identity per enclosing operation and publishes through all Scopes holding it. A Mutation's Model inputs already declare their authority; touch additional identities affected by the operation. Labels never imply touch.

Withdrawal releases one Scope's holding; it does not delete the business row. Another current Scope holding the same Model/Identity preserves the client's replicated base. Releasing the last hold uses the local membership ledger and request fences to evict that base while preserving pending and device-local work. Withdrawal invokes neither `onStore` nor a schema cascade. Explicitly enroll and withdraw dependents according to your application's publication policy.

Business deletion is authoritative content. Touch a deleted identity and keep it enrolled when subscribers must receive its Loader's `null`. A Model without a Loader is device-only and cannot be enrolled, withdrawn, labeled, targeted by generated selection methods or touched through publication interfaces.

| Context | Allowed declarations |
| --- | --- |
| Mutation or legacy slot handler; `backend.transaction` or `backend.publish` | Membership add/remove, label edits, selection and touch. |
| Load handler | Membership add, chained labels and explicit label add, only for identities returned by the current page. |
| Query handler or viewer Loader | No Scope effects or touch. |
| Client transaction or `onStore` | Local subscription intent through `tx.scopes`; no server Scope effects. |

Settlement emits at most one final membership event per Scope/Identity: initially absent add/remove emits none; existing remove/add emits one upsert; final withdrawal emits an identity-only removal. Label-only edits emit none. Adding unchanged content reuses its stamp; each delivering Scope has its own cursor. See [How state moves](../concepts.md#scope-and-cursor).

## Authentication

`Authenticate` receives Node's `IncomingMessage` and returns a user ID string, null, undefined, or a promise of those values. Null/undefined or a blank user ID rejects authentication. The SDK calls it for HTTP requests and WebSocket connections. Verify your application's session/token here; enforce read permissions in loaders and write permissions in handlers.

`devAuth(): Authenticate` treats `Authorization: Bearer <userId>` as the identity without verification. It is provided for local development, not production authentication. See [authentication and account changes](../frontend/sync.md#authentication-and-account-changes).

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

Codes must match `^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`, such as `todo.title_empty`. A recognized business rejection rolls back that call's business writes, stamps, memberships and deliveries. For a queued call, `wait()` returns a `CallError` outcome and any optimistic Model change rolls back; the durable rejection remains inspectable until dismissed. For a direct call, the promise rejects with `CallError`. An unknown transport outcome can be retried with the same call identity; it is not evidence that the handler did nothing.

Business codes come from `CallRejected` or `translateRejection`; `action_version_unsupported`, `handler.failed`, `loader.failed`, `model_version_unsupported` and `query.effects_forbidden` identify framework failures attributable to one call. A durable receipt records each call's outcome. Independent valid calls in the batch can commit. A direct response carries the same final outcome for that call.

Infrastructure errors that make the transaction unusable abort delivery for retry. A serialization conflict that outlasts the shim's `retries` is one of them: the call is not rejected, and a queued call stays queued and is sent again. `onError` receives diagnostic failures, including handler and Loader exceptions. Diagnostic callback exceptions after a committed outcome cannot replace its result, repeat its handler or become a transport error; SDKs report those callback exceptions through their runtime uncaught-error channel.

Protocol refusals use a status and JSON body chosen by the engine error's `code`; message text may change. Per-call content errors are outcomes, while a pull or live subscription with an unsupported Model declaration receives a whole-request `409`.

| Code | HTTP status | Meaning |
| --- | --- | --- |
| (your `admit` refusal) | Your status, with `axton-admission: refused` | Your JSON body ([Admission](#admission)) |
| `protocol.unsupported` | 426 | Missing or unsupported `scope-membership-v1`; refused before handler or progress effects |
| `request.invalid` | 400 | Malformed body, or a pull cursor ahead of the scope head |
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
| `POST /sync/fetch` | Read one record through its Model's Loader for `client.fetch` |
| `POST /sync/loads` | Serve batched pages of native Loads for `client.loads` |
| `POST /sync/pull` | Materialize changed records through loaders for catch-up and gap recovery |
| `/sync/live` (WebSocket) | Subscribe to scopes and stream ongoing record changes |

The listener has no TLS, CORS or proxy-header handling and binds to loopback by default; run it behind a reverse proxy as described in [Deploy the backend](deployment.md).

Generated clients use all of these routes automatically from one `server` configuration. The WebSocket subscription acknowledgement confirms that scope listeners are installed before HTTP catch-up starts, so changes during catch-up can be queued and reconciled. Listener errors reject. `await server.close()` releases the listener and its live connections; your application must separately close its database pool. The supported listener owns its server; mounting into an application-owned HTTP server is not currently exposed.

## Background writes

Writes outside handlers have no readback and no receipt; they reach clients only through Scopes. Run them through `backend.transaction`: the framework opens the application transaction and hands the body the same `touch` and `scope` a Mutation handler receives. When the body returns, the framework allocates one new stamp per touched record and applies the membership changes and deliveries inside that same transaction; once it commits, the live subscribers of the affected Scopes are woken.

```ts
await backend.transaction(async ({ tx, scope, touch }) => {
  await tx.entry.update({ where: { id: 'entry-1' }, data: { text: 'From a job' } });
  touch.entry({ id: 'entry-1' });
  scope('book:demo').add.entry({ id: 'entry-1' });
});
```

`tx` is the transaction of the shim passed as `database`. The body's return value is returned. If the body throws, the transaction rolls back and nobody is woken; the error propagates so the driver can retry serialization failures, which run the whole body again. Do not call `backend.transaction` from a handler: a handler already has a transaction.

| `TransactionCall<Tx>` member | Contract |
| --- | --- |
| `tx` | The application transaction; write business data through it |
| `touch` | `touch.entry(identity)` declares a changed record; each gets one new stamp when the body returns |
| `scope(name)` | The same Scope handle as in a handler, for adding and removing members |

Same rules as a Mutation handler's, with two differences: there are no Model inputs, because nothing was uploaded, and nothing is read back, because no client is waiting for a receipt. A touched record advances its stamp even without a Scope; adding an unchanged record does not. Wakeups are process-local; distributed wake delivery needs additional application infrastructure.

### In a transaction you own

When your code has already opened the transaction, for example another framework's request handler, `backend.publish(tx, body)` settles the same declarations inside it. `tx` must be a transaction of the tool the `database` shim was built on.

```ts
const wake = await db.$transaction(async (tx) => {
  await tx.entry.update({ where: { id: 'entry-1' }, data: { text: 'From my host' } });
  return backend.publish(tx, ({ scope, touch }) => {
    touch.entry({ id: 'entry-1' });
    scope('book:demo').add.entry({ id: 'entry-1' });
  });
});
wake();
```

| Behavior | Contract |
| --- | --- |
| Settlement | Runs before `publish` resolves: stamps, memberships and Scope positions are written through `tx`, so they commit or roll back with it, a savepoint included. Each call is its own settlement, so a record touched in two calls gets two stamps |
| Wake | `publish` resolves to a function. Call it after `tx` commits; after a rollback, drop it. Until it is called, no live subscriber is told; they catch up on their next wake or reconnect |
| Errors | A refused declaration or a database error rejects `publish` with the original error, so your retry loop can recognize a serialization failure and run the whole transaction again. Wakes from failed attempts are simply never called |
| Isolation | AXTON does not choose the level of your transaction. The settlement works at Read Committed, Repeatable Read or Serializable; run at Serializable, as every AXTON transaction does, if your own reads and writes rely on it, and retry serialization failures as `backend.transaction` does |
| Refusal | A transaction AXTON is already serving, a handler's or `backend.transaction`'s, is refused: declare through its own `touch` and `scope` |

## Extension points

`loaderHooks` maps model names to `{ prepareForViewer(call): Promise<void> }`. The hook runs before that model's loader in the same request context. Its failure fails the load. Use it only if viewer-specific preparation is needed; a loader already receives the user.

`native?: Native` injects the native bridge when packaging it elsewhere. It implements `validateConfig`, `processPush`, `processPull`, `settleExternal`, `negotiateLive` and `pullLive` with the string/JSON callback contracts in the [SDK source](https://github.com/zanminwang/axton/blob/main/packages/server/index.mts). The default binding comes from this repository's Node addon. This is a packaging seam; the generated Mutations, Queries and Loaders remain the application contract.

Backend methods marked `@internal` (`push`, `pull`, `negotiateLive`, `pullLive`, `onCommitted`, `notifyCommitted`, `closeLive`) are used by the listener and tests. They are not the supported application-facing HTTP integration surface.
