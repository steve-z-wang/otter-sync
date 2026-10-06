# Client

## 1. Introduction and Goals

The generated API names application Models, Mutations and Queries. The generic SDK translates those typed calls into tasks for one Rust runtime. It owns language scopes, decoding and handles; Rust owns durable requests, cache admission, authority, progress and settlement.

## 3. Context and Scope

| Surface | Boundary |
| --- | --- |
| `GeneratedClient.open({path, stream, connection})` | Open one physical SQLite file bound to backend/viewer/Stream/contract, then start transport. Identity is supplied offline and is independent of credentials. |
| `client.models.<model>.get / query / watch / <relation>` | Read committed local projections. Observers report distinct committed results. |
| `client.models.<model>.create / update / delete` | Device-only writes, each in its own local transaction. They send no backend request. |
| `client.transaction(body)` | One local atomic boundary for reads, direct writes and several named Mutations. Return the callback value after commit. |
| `client.mutations.<name>(input)` or `tx.mutations.<name>(input)` | Commit typed input, inferred optimism and durable intent; answer a `Call<Output>`. |
| Mutation callback returning typed input | Run device-only companion writes before applying input optimism. Companions follow that Call's acceptance/refusal. |
| `call.wait()` | Wait for backend outcome and this Store's committed settlement. It does not wait for other clients. |
| `client.queries.<name>(input, options?)` | Direct named read; await the invocation result and permitted local cache commit. Never enqueue a Query. |
| `client.fetch.<model>(identity, options?)` | Direct versioned Model Loader read; no user-written Query handler. |
| `client.bootstrap()` | Await finite historical manifest coverage and delivery through one captured tail. |
| `client.connection`, `connect`, `close`, `resetStore`, sync/recovery APIs | Lifecycle and recovery within the same Store binding. Reset changes incarnation; ordinary reconnect/reopen does not. |

The [public guide](../../../../../website/docs/frontend/client-api.md) owns runnable TypeScript/Dart examples. TypeScript accepts a typed Input or a callback returning Input. Dart uses the typed invoker's `withTransaction` for that callback form. Both expose local typed `models` inside the callback. The companion scope cannot queue another Mutation, perform remote reads or escape its lifetime. `tx.mutations` supports the same invocation forms inside an outer transaction. React Native uses the generated TypeScript surface with its own transaction adapter.

One local transaction is not a distributed backend transaction: its Calls retain independent outcomes. Backend atomic work belongs in one named Mutation. Standalone direct Model writes and unrelated outer-transaction writes are not companions.

Public anonymous mutations, direct Mutation `call`, queued Query `enqueue`, Load jobs, custom incoming-store hooks and multiple Stream subscriptions are retired. Concrete Models retain local CRUD; there is no `@@local` distinction.

## 5. Building Block View

[Compiler emission](../../../../../crates/compiler/src/emit.rs) and [transaction emission](../../../../../crates/compiler/src/emit_transactions.rs) generate typed facades/codecs. [Node runtime](../../../../../packages/client-js/runtime.mts), [Dart client](../../../../../packages/dart/lib/src/client.dart) and the [RN adapter](../../../../../packages/client-react-native/transaction.mts) submit through native Bridges. [Bindings](../bindings.md) own platform loading and per-client actors. [Client protocol 4](../../client/protocol4.md) owns persisted semantics.

## 6. Runtime View

A Mutation invocation resolves after local commit; a Call obtained inside an outer transaction remains provisional until that commit. `wait()` before commit refuses promptly with `transaction_uncommitted`; rollback ends the handle with `transaction_rolled_back`. Backend acceptance followed by local apply failure remains accepted-awaiting-settlement. Callback code runs once: retry/reopen replay retained operations, not application code.

Query/Fetch use request-level boolean `store`, default true. Results always describe the invocation snapshot. Ordinary returned Model content has `cursor:null`; true writes only where no current Stream content/deletion guard prevents it. A protected no-op succeeds and may return content different from the Store. False installs no Model/authority. Returning a Model does not track it, and missing results do not mean canonical deletion.

Query `once` distinguishes storage modes and saves successful complete results. A hit returns a decoded copy without reapplying Models or tracking. Refresh makes a new request; refusal preserves prior saved success. Invalidation fences an older active request. Fetch shares matching active requests but has no durable Load job.

Language scopes refuse captured/expired capabilities and unawaited operations before commit. Node uses async-context ownership. RN's guard conservatively refuses remote/write operations while its callback runs; ordinary reads can queue behind the active transaction. Cross-Store overlapping RN callbacks require async-context support and are refused. Bootstrap and named Mutation entry checks prevent waiting behind their own callback. Priority close settles outstanding tasks and cancels runtime effects without waiting for unresolved user code.

## 10. Quality Requirements

- Bound files, lifecycle, requests, observations, cache and queues stay independent. Evidence: [native protocol tests](../../../../../crates/sqlite/tests), [RN binding tests](../../../../../integration/bindings/client-react-native) and [protocol transport verification](../../../testing/0.4.md).
- Typed input and callback-before-input preserve local atomicity and independent Call fates. Evidence: [generated contract checks](../../../../../integration/action-contract), [Node native Mutation checks](../../../../../packages/client-js/mutation-native04.test.mjs) and [Dart package tests](../../../../../packages/dart/test).
- Ordinary reads cannot overwrite current Stream protection; result snapshots remain independent. Evidence: [Fetch/once bindings](../../../../../integration/bindings/client-js) and [named-read E2E](../../../../../integration/load-e2e).
- Installed generated APIs and bundled libraries work outside the checkout. Evidence: [installed-package runner](../../../../../integration/release/verify-installed.sh).

On 2026-10-05, current Dart package tests passed 152 with analyzer clean; parent RN binding checks passed 63 using real SQLite, HTTP and WebSocket. These package results do not establish Expo/Flutter device startup or final all-platform CI.

## 11. Risks and Technical Debt

RN lacks Node's exact async-context ownership; its conservative guards and refusal of overlapping callbacks are intentional. A hung user callback can hold this Store's transaction until priority close cancels it. Remote reads remain concurrent with synchronization; no SDK serializes whole remote request lifecycles. Required-group Loader/constraint failures can delay initialization or settlement and remain observable rather than fabricating completion.
