# Client

## 1. Introduction and Goals

The generated API names application Models, Mutations and Queries. The generic SDK translates those typed calls into tasks for one Rust runtime. It owns language scopes, decoding and handles; Rust owns durable requests, cache admission, authority, progress and settlement.

## 3. Context and Scope

| Surface | Boundary |
| --- | --- |
| `GeneratedClient.open({path, stream, connection?})` | Open one physical SQLite file for one Stream. Rust creates the Store identity; transport starts only when a connection is supplied. |
| `client.models.<model>.get / query / watch / <relation>` | Read committed local projections. Observers report distinct committed results. |
| `client.models.<model>.create / update / delete` | Device-only writes, each in its own local transaction. They send no backend request. |
| `client.transaction(body)` | One local atomic boundary for reads, direct writes and several named Mutations. Return the callback value after commit. |
| `client.mutations.<name>(input)` or `tx.mutations.<name>(input)` | Commit typed input, inferred optimism and durable intent; answer a `Call<Output>`. |
| Mutation callback returning typed input | Run device-only companion writes before applying input optimism. Companions follow that Call's acceptance/refusal. |
| `call.wait()` | Wait for backend outcome and this Store's committed settlement. It does not wait for other clients. |
| `client.queries.<name>(input, options?)` | Direct named read; await the invocation result and permitted local cache commit. Never enqueue a Query. |
| `client.fetch.<model>(identity, options?)` | Direct versioned Model Loader read; no user-written Query handler. |
| `client.bootstrap()` | Await finite Bootstrap coverage at a captured head. |
| `client.connection`, `connect`, `close`, `resetStore`, sync/recovery APIs | Lifecycle and recovery within the same Store binding. Reset changes incarnation; ordinary reconnect/reopen does not. |

The [public guide](../../../../website/docs/frontend/client-api.md) owns runnable TypeScript/Dart examples. TypeScript accepts a typed Input or a callback returning Input. Dart uses the typed invoker's `withTransaction` for that callback form. Both expose local typed `models` inside the callback. The companion scope cannot queue another Mutation, perform remote reads or escape its lifetime. `tx.mutations` supports the same invocation forms inside an outer transaction. React Native uses the generated TypeScript surface with its own transaction adapter.

One local transaction is not a distributed backend transaction: its Calls retain independent outcomes. Backend atomic work belongs in one named Mutation. Standalone direct Model writes and unrelated outer-transaction writes are not companions.

Public anonymous mutations, direct Mutation `call`, queued Query `enqueue`, Load jobs, custom incoming-store hooks and multiple Stream subscriptions are retired. Concrete Models retain local CRUD; there is no `@@local` distinction.

## 5. Building Block View

[Compiler emission](../../../../crates/compiler/src/emit.rs) and [transaction emission](../../../../crates/compiler/src/emit_transactions.rs) generate typed facades/codecs. [Node runtime](../../../../packages/frontend/client-js/api/runtime.mts), [Dart client](../../../../packages/frontend/dart/lib/src/api/client.dart) and the [RN adapter](../../../../packages/frontend/client-react-native/api/transaction.mts) submit through native Bridges. [Bindings](bindings.md) own platform loading and per-client actors. [Protocol 5](../protocols/sync.md) owns persisted semantics.

[Command accounting](../../../../packages/frontend/client-js/api/command-accounting.mts) owns pending work, draining and first command failure for Node, RN and local companion scopes. Each adapter retains its own admission and lifetime guards; Node savepoint rollback restores the enclosing scope's recorded failure.

## 6. Runtime View

A Mutation invocation resolves after local commit; a Call obtained inside an outer transaction remains provisional until that commit. `wait()` before commit refuses promptly with `transaction_uncommitted`; rollback ends the handle with `transaction_rolled_back`. Callback input encoding errors reject the invocation and roll back its local scope without queuing a Call. Backend acceptance followed by local apply failure remains accepted-awaiting-settlement. Callback code runs once: retry/reopen replay retained operations, not application code.

Query/Fetch use request-level boolean `store`, default true. Results always describe the invocation snapshot. Ordinary returned Model content has `cursor:null`; true writes only where no current Stream content/deletion guard prevents it. A protected no-op succeeds and may return content different from the Store. False installs no Model/authority. Returning a Model does not track it, and missing results do not mean canonical deletion.

The protocol-5 facades submit a fresh task for every Query invocation. Public `once`, `refresh` and Query invalidation controls are removed; authentication refresh remains. Open takes a path, schema and Stream with an optional connection, allowing offline operation. Joined native lifecycle evidence belongs to `integration/v05-sdk/run-host.sh`; installed mobile evidence remains separate.

Language scopes refuse captured/expired capabilities and unawaited operations before commit. Node uses async-context ownership. RN's guard conservatively refuses remote/write operations while its callback runs; ordinary reads can queue behind the active transaction. Cross-Store overlapping RN callbacks require async-context support and are refused. Bootstrap and named Mutation entry checks prevent waiting behind their own callback. Priority close settles outstanding tasks and cancels runtime effects without waiting for unresolved user code.

## 10. Quality Requirements

- Bound files, lifecycle, requests, observations, cache and queues stay independent. Evidence: [native protocol tests](../../../../crates/sqlite/tests), [RN binding tests](../../../../integration/bindings/client-react-native) and [protocol-5 host](../../../../integration/v05-sdk/README.md).
- Typed input and callback-before-input preserve local atomicity and independent Call fates. Evidence: [generated contract checks](../../../../integration/action-contract), [Node native Mutation checks](../../../../packages/frontend/client-js/mutation-native05.test.mjs) and [Dart package tests](../../../../packages/frontend/dart/test).
- Ordinary reads cannot overwrite current Stream protection; result snapshots remain independent. Evidence: [Query/Fetch bindings](../../../../integration/bindings/client-js) and the [protocol-5 host](../../../../integration/v05-sdk/README.md).
- Installed generated APIs and bundled libraries work outside the checkout. Evidence: [installed-package runner](../../../../integration/release/verify-installed.sh).


## 11. Risks and Technical Debt

RN lacks Node's exact async-context ownership; its conservative guards and refusal of overlapping callbacks are intentional. A hung user callback can hold this Store's transaction until priority close cancels it. Remote reads remain concurrent with synchronization; no SDK serializes whole remote request lifecycles. Required-group Loader/constraint failures can delay initialization or settlement and remain observable rather than fabricating completion.
