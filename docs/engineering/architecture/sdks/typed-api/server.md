# Server

## 1. Introduction and Goals

A backend author writes retained Mutation and Query handlers and versioned Model Loaders against their own database transaction. The server typed API gives handlers `{ctx, args}` with trusted transaction context and decoded caller input, plus typed explicit outputs. The Rust engine decides execution, resolves Model outputs through Loaders, and distributes changed records.

## 3. Context and Scope

`createBackend({database, authenticate, mutations?, queries?, handlers?, loaders, loaderHooks?, translateRejection?, onError?})` returns `{listen, transaction, …}`. Generated `backend.ts` supplies `Mutations<Tx>`, `Queries<Tx>`, `Loaders<Tx>`, `MutationContext<Tx>`, `QueryContext<Tx>`, `TransactionCall<Tx>`, the schema's `Channel`, `Touch`, `ModelMembership<Identity>` and discriminated `RecordRef`, and `CallRejected`, plus `Handlers<Tx>` for legacy [slot mutations](../../schema/mutations.md); missing or misnamed handlers are type errors. Its `Options<Tx>` requires each of `mutations`, `queries` and `handlers` only when that group retains a contract. It binds the compiled config to `createBackend` ([Compiler / Generate](../../compiler/generate.md)).

| Piece | Shape |
| --- | --- |
| Mutation handler | `({ctx, args}: MutationHandlerCall<Tx, Args>) => Promise<Outputs>`; `MutationContext` has `tx`, `userId`, `callId`, `channel(name)`, `touch` |
| Query handler | `({ctx, args}: QueryHandlerCall<Tx, Args>) => Promise<Outputs>`; `QueryContext` has only `tx`, `userId`, `callId`, in the types and at runtime ([enforcement and its limit](../../schema/actions.md#8-crosscutting-concepts)) |
| Changed records | the engine infers the operation's input targets; `touch.todo(identity)` declares a record the handler changed beyond them, which is stamped and distributed but is not caller authority |
| Outputs | only explicitly declared outputs: a value for a scalar or enum output, an identity for a Model output (`null` for an optional one, an array for a list), even when an input has the same name; a missing field fails the call and is never filled from an input. An operation without outputs returns `void` |
| Loader | `({ids, tx, userId}: LoaderCall) => Promise<(Row \| null)[]>`, aligned with `ids`; one per retained model version, `Row` being that version's record type; no channel |
| Rejecting one call or refusing a read | throw `CallRejected(code)` (the same class as the legacy `MutationRejected`), or throw anything `translateRejection` maps to a code; during a call this rejects the call, in a pull it reports the affected record |
| Channel membership | `channel(name).todo.add(identity)` / `.remove(identity)`, or `channel(name).add([Todo({id}), Project({id})])` / `.remove([…])` with generated references; outside a handler, `backend.transaction(async ({tx, channel, touch}) => …)` hands the body the same handles, settles them when it returns and answers the body's value ([Publish](../../server/engine/publish.md)) |
| Serving | `backend.listen({port, host?})` → `{url, close}` |
| Development auth | `devAuth()` treats the bearer token as the user id; documented as development only |

## 5. Building Block View

The runtime package holds `createBackend`, the HTTP and WebSocket servers and the SQL-free `Database<T>` contract; `@axton/postgres` builds that object from a two-method driver and ships the `pg`, `prisma` and `drizzle` shims ([Persistence](../../server/persistence.md)). `touch`, `channel(name)` and its writers are synchronous, callback-scoped declarations collected by `createEffects` in [server/effects.mts](../../../../../packages/server/effects.mts): each call validates and copies the Model's identity fields (so `channel(name).todo.add(args.todo)` keeps only `id`), a mixed call resolves every reference before declaring any, a raw identity names no Model and fails, and every handle refuses use after its callback settles. The dictionaries have no prototype, so any lower-first Model accessor is an ordinary key; the compiler refuses two Models sharing an accessor and Models whose accessor is `add` or `remove`. `Channel`, `Touch`, `ModelMembership`, `RecordRef`, `HandlerCall` and `TransactionCall` are reserved generated backend names, so no Model or enum may use them (a Model cannot be named `Channel`). A handler that returns has not committed anything yet: settlement, the input readback and the output reads can still refuse the call and roll its declarations back.

Code: [server/index.mts](../../../../../packages/server/index.mts); generated signatures from `backend_typescript` in [compiler/emit.rs](../../../../../crates/compiler/src/emit.rs).

## 9. Architecture Decisions

**Handler and Loader registration by version ([#91](https://github.com/zanminwang/axton/issues/91)).** Group versions under the operation or Model name. For a v1-only contract, a function is shorthand for `{v1: implementation}`. Once multiple versions are retained, register each explicitly:

```ts
mutations: {
  addTodo: {
    v1: handleOriginalAddTodo,
    v2: handleNewAddTodo,
  },
},
queries: {
  searchTodos: handleSearchTodos,
},
loaders: {
  todo: {
    v1: loadOriginalTodo,
    v2: loadNewTodo,
  },
}
```

Handlers receive generated input types for their version and return typed explicit outputs. Loaders return record types for an independent [Model read version](../../schema/models.md#9-architecture-decisions). Registration keys use `v1`, `v2`; wire versions remain numbers. Shorthand always means v1, never the latest version. Client calls use the operation version embedded in their generated method.

**Registration by kind ([#157](https://github.com/zanminwang/axton/issues/157)).** Generated `Mutations<Tx>` and `Queries<Tx>` hold one key per operation, `lowerFirst(name)`, with a `v<n>` member for every retained version of that kind; v1-only operations also accept a bare function. Grouping follows each retained version's own kind, so a name that retains Mutation v1 and Query v2 registers v1 under `mutations` and v2 under `queries`, while the client exposes it only under its current kind. Legacy slot mutations stay on `handlers`. The runtime refuses missing, unknown, wrong-kind (including an operation registered under `handlers`) or non-function registrations at startup. Dispatch is keyed by name and version, so a request never falls back to another version.

Generated `Loaders<Tx>` holds one key per Model and a `v<n>` member per retained read version, each returning that version's record type. The current shape uses `Todo`; an older retained shape uses `TodoV1`. A Model retaining only v1 also accepts a bare function. The engine names the Loader version for each operation output separately from the client's current authority version; a pull uses the client's declared version ([Server / Engine / Pull](../../server/engine/pull.md#6-runtime-view)).

**Declarations replace publication helpers ([#140](https://github.com/zanminwang/axton/issues/140)).** The generated API names the resource before the verb, as the client's `client.models.todo.create()` does: `ctx.channel(name).todo.add(identity)`, `ctx.touch.todo(identity)`. `channel(name)` is a handle, not a request; it creates nothing and checks no subscriber. There is no `publish`, `changes.add` or `changes.records` alias: this was a coordinated prelaunch change. A mixed list takes generated `Todo(identity)` references only, because an untagged identity cannot name its Model.

## 10. Quality Requirements

- **Startup fails on an invalid config or a missing handler or loader.** Evidence: [runtime.test.mjs](../../../../../integration/persistence/server/runtime.test.mjs) `backend validates config and complete registrations at startup`.
- **Registration names every retained version; a bare function registers v1 only.** Evidence: `handler registration names every retained version and a function means v1 only`.
- **A version reaches only its own handler, whichever way v1 was registered.** Evidence: `a version dispatches only to its own handler and a function registers v1`.
- **Declarations snapshot identity fields at the call, and the handles close when the handler or `backend.transaction` body settles, even when it throws.** Evidence: [generated backend test](../../../../../integration/action-runtime-ts/backend.test.mts) `generated backend decodes Date values and declares canonical identities through its handles`, `declaration handles close when the handler or external body settles, even when it throws`; [effects.test.mjs](../../../../../integration/persistence/server/effects.test.mjs).
- **Handlers of both kinds return explicit outputs and Model identity objects while the framework resolves Model snapshots through a Loader; an output named like an input is independent of it and is never filled from it.** Evidence: [server Action tests](../../../../../crates/server/tests/actions.rs) `an_input_and_a_same_name_output_are_independent_and_a_missing_output_never_falls_back`, `no_declared_outputs_answer_null_and_keep_input_authority`; [action.test.mts](../../../../../integration/action-e2e/action.test.mts) `an edit of A that explicitly returns B: A is reconciled, the result is B, and only A is stamped`; the handler-output type negatives in [negative.ts](../../../../../integration/action-contract/negative.ts); [generated backend test](../../../../../integration/action-runtime-ts/backend.test.mts).
- **Generated maps group retained versions by kind and share one `Tx` with Loaders.** Evidence: [operation contract fixture](../../../../../integration/action-contract/backend.ts), [compiler tests](../../../../../crates/compiler/tests/compiler.rs) `a_kind_change_at_a_new_version_registers_each_version_under_its_own_kind` and [generated backend test](../../../../../integration/action-runtime-ts/backend.test.mts).
- **Registration is checked per kind at startup, and a Query handler receives no effect capabilities and settles without effects.** Evidence: [backend.test.mts](../../../../../integration/action-runtime-ts/backend.test.mts) `registration is checked per kind at startup: missing, extra and wrong-kind`, `Query handlers receive no effect capabilities and settle without effects`.
- **Loader registration names every retained model version, a bare function registers v1 only, and a pull reaches only the served version's loader.** Evidence: [runtime.test.mjs](../../../../../integration/persistence/server/runtime.test.mjs) `loader registration names every retained model version and a function means v1 only`, `a pull reaches the loader of the declared model version and normalizes rows with that contract`; [compiler/tests/compiler.rs](../../../../../crates/compiler/tests/compiler.rs) `backend_emitter_groups_loader_versions_under_the_model_name`; the `@ts-expect-error` loader negatives (bare function, missing and unknown versions, a value outside the v1 contract) in [test.ts](../../../../../integration/generated-api/test.ts).

The earlier legacy mutation evidence in the runtime tests remains useful for unchanged lower-level savepoint and Loader rules; the operation API is verified by the fixtures above.

## 11. Risks and Technical Debt

**Accepted limitation.** The backend runtime exists for TypeScript only. Generated Dart carries `Mutation{Name}Handlers` and `Query{Name}Handlers` contract interfaces, but no Dart backend runs them.
