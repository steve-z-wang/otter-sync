# TypeScript backend SDK

This TypeScript SDK embeds the shared Rust server runtime in your Node application. Business Handlers and Loaders are implemented in TypeScript, against the `Mutations`, `Queries` and `Loaders` interfaces the compiler generates from your `.model` file.

`index.mts` runs on Node with TypeScript support (Node 22.18+), or can be compiled with TypeScript. It loads the native engine from `@axtonjs/native`; in a source checkout, `npm ci` then `bash scripts/build.sh` builds that addon. Supply an injected `native` implementation to use another build of it.

Given a schema with `Todo`, a Mutation `AddTodo`, a Query `FindTodos` and a Load `ProjectTodos`, the compiler emits `generated/backend.ts`, which already binds the schema. Your application implements the generated `Mutations<Tx>`, `Queries<Tx>`, `Loaders<Tx>` and `Loads<Tx>` contracts:

```ts title="action-contract"
import { createBackend, devAuth } from './generated/backend.ts';
import { mutations, queries } from './handlers.ts';
import { loaders } from './loaders.ts';
import { loads } from './loads.ts';

const backend = createBackend<Tx>({
  database, // a shim such as prisma(db); Tx is its transaction type
  authenticate: devAuth(),
  mutations,
  queries,
  loaders,
  loads,
});
const server = await backend.listen({ port: 4242 });
console.log(server.url);
```

The generated `createBackend` needs no `config` option: the schema is already bound. The runtime's own `createBackend` (`packages/server/index.mts`) still takes `config` explicitly, for callers that build the schema themselves.

`mutations`, `queries`, `loaders` and `loads` are application modules typed against the generated interfaces; a schema without Queries omits `queries`, one without Mutations omits `mutations`, and one without Loads omits `loads` ([Load handlers](api.md#load-handlers)). The application enforces write and read permissions. Authorization, unique constraints, child deletion and client identity are the application's responsibility ([What your backend owns](api.md#what-your-backend-owns)).

## Call objects

A Mutation or Query Handler receives `{ctx, args}`. `args` is typed from the retained input of that version; `ctx` supplies the application's `tx`, authenticated `userId` and stable `callId`, and a Mutation's also has `invalidate` and `stream(name)`. A [Load handler](api.md#load-handlers) receives `{ctx, args, continuation}`, and its `stream(name)` tracks only for records the page returns. The Handler returns only the explicit outputs its operation declares, never an input under the same name; Model outputs are identities resolved by a Loader. A Query must not change business state; the framework refuses Query effects it can see but cannot inspect your SQL ([Handlers](api.md#handlers)). A Loader receives `{ids, tx, userId}` and returns one record or null per identity. It is never told a stream.

## Track and invalidate

`ctx.stream(nameOrNames).track.todo(identityOrList)` establishes persistent interest; repeat tracking is idempotent. `ctx.invalidate.todo(identityOrList)` advances authority and notifies every tracking Stream. `ctx.stream(names).invalidate.todo(identityOrList)` notifies only selected existing holders, for viewer-specific changes. Mutation inputs already invalidate globally; invalidate every additional business record changed, including deleted children whose Loader now answers `null`.

Tracking is independent of permission and survives absence. Shared content changes must reach all holders. See [Streams](api.md#streams) for mixed references, declaration lifetimes and authority rules.

## Authentication

`authenticate` is `(request) => userId | null | undefined`, called per HTTP/WebSocket request; returning `null` or `undefined` rejects the request. `devAuth()` is a development-only implementation that trusts the `Authorization: Bearer <userId>` header verbatim — never use it in production.

## Errors

`onError?: (error) => void` on `BackendOptions` is called for server-side failures that clients only see as `{ code: "server" }` over HTTP: `authenticate` throws, persistence faults, settlement errors, loader refusals while serving a page, and live drain failures. A failure raised by the native engine arrives as an `EngineError` with a stable `code` and a readable `message`; branch on the code, never on the message. See [Errors](api.md#errors) for the codes that map to HTTP statuses.

## Background jobs

Outside a Handler there is no readback and no receipt, so a change reaches clients only through Streams. Use `backend.transaction`; its body gets the same `invalidate` and `stream` as a Mutation Handler, the framework stamps, enrolls and delivers what it collected inside the same transaction as your writes, and wakes live subscribers after commit:

```ts
await backend.transaction(async ({ tx, stream, invalidate }) => {
  await tx.entry.update({ where: { id: 'entry-1' }, data: { text: 'From a job' } });
  invalidate.entry({ id: 'entry-1' });
  stream('book:demo').track.entry({ id: 'entry-1' });
});
```

See [background writes](api.md#background-writes).

## Transaction ownership

The outer transaction belongs to the application. Persistence, Handler, and Loader callbacks all receive that same transaction. The runner must provide serializable isolation, roll back on rejected promises, and retry serialization conflicts by running the whole body again. Every shim of [`@axtonjs/postgres`](database.md) supplies this contract.

## Call results

A successful Handler returns the explicit outputs declared by its Mutation or Query; an operation with no explicit outputs may return nothing. AXTON resolves Model outputs through the versioned Loader at that invocation. A later call in the batch may change the same record before batch-final authority is read, so the call result snapshot can differ from the receipt's record content. `CallRejected` or a registered `translateRejection` code rolls back that call's savepoint; ordinary Handler/Loader exceptions become `handler.failed` / `loader.failed` and reach `onError`. Retryable database errors retry the transaction, and persistence faults abort it. Durable calls expose final outcomes through `Call.wait()`; direct calls return a final result or throw `CallError`. See [handlers](api.md#handlers) and [client Mutations and Queries](../frontend/client-api.md#mutations-and-queries).

## Loaders and Pull

Loaders return one state object or null for every identity, in precisely the supplied order. A missing or unauthorized row is null. One pull covers every stream the client follows and delivers a record that belongs to several of them once. Each stream scans at most 50 compacted invalidations of its current members, and the pull materializes their current state with each record's current stamp.

A record that cannot be read fails alone ([#95](https://github.com/zanminwang/axton/issues/95)). When a Loader throws or refuses a batch, AXTON retries each identity on its own. The record that still fails is delivered as an error change carrying `loader.failed` or the refusal code, and the rest of the page is served. The failure is reported to `onError`, which defaults to `console.error`. The client keeps its copy of that record and reports it. The record is corrected the next time it is delivered, for example when you touch it. A Loader that returns the wrong number of entries is retried the same way, and a row that does not match the model type fails only its record with `loader.invalid`.

Run `integration/persistence/server/run.sh` for the disposable PostgreSQL/Prisma integration suite. Its database is created, used, and destroyed by the runner.

`backend.listen({ port, host? })` starts a Node HTTP+WebSocket server that serves durable `/sync/mutations`, direct `/sync/actions`, `/sync/fetch`, `/sync/loads`, `/sync/pull`, and `/sync/live` on one port, and returns `{ url, close() }`. It resolves once the listener is bound.

For every option, callback, return value and failure mode, see the [backend interface reference](api.md). For process placement, the reverse-proxy configuration and trust boundaries, see [Deploy the backend](deployment.md).
