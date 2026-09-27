# Rust-owned client runtime and thin SDK bridge

Status: proposed design for [#134](https://github.com/zanminwang/axton/issues/134). The ownership direction and single-PR/four-checkpoint delivery are agreed; this document makes the engineering contract concrete for review. No implementation is included.

## Goal and current boundary

An SDK submits a complete application task to Rust, executes platform effects requested by Rust, and delivers Rust's final outcome to the correct language-level waiter. Rust owns the task's lifecycle and local transaction. Adding a language must not require reimplementing scheduling, retries, Query completion, or Bootstrap state rules.

Today `RuntimeHost::call` dispatches individual commands to a client, `SyncCycle`, `ConnectionDriver`, and `DownlinkWorker`. SDK exclusive queues serialize native calls and multi-call transactions. Direct requests are split into `prepareAction`, host HTTP, and `applyActionResponse`; the SDK organizes that sequence. Subscription projection, Bootstrap waiter rules, query-flight plumbing and watch re-query decisions are also spread through the SDKs. The Downlink receive queue and the actual database/authority logic already belong to Rust. Node, Dart and mobile bindings each hold a process-wide host mutex during a command; the carriers use different thread/isolate arrangements.

Preserve the existing [guarantees](../../engineering/guarantees.md), [bindings](../../engineering/architecture/sdks/bindings.md), [Downlink rules](../../engineering/architecture/client/connection/controller/downlink-worker.md) and [storage contracts](../../engineering/architecture/client/storage/README.md). This is an ownership refactor, not a new synchronization protocol.

## 1. Components and ownership

| Component | Owns | Does not own |
| --- | --- | --- |
| Rust `ClientRuntime<S>` | Client state, local execution queue, task continuations, active transaction, connection policy, effect correlation and observer state | HTTP sockets, language objects, user callback bodies |
| Native runtime actor | One runtime and SQLite connection per client on a background execution thread; mailbox and event outbox | Product retry/status rules or a lock held across all clients |
| SDK Bridge, one per client | Request-to-Promise/Future routes, typed decoding, Call/Subscription objects and callback registrations; dispatch of effects and events | Task progression, database scheduling, retry or Bootstrap policy |
| Platform adapters | HTTP/WebSocket, timers, clock/entropy inputs, credential acquisition/refresh and invocation of language callbacks | Deciding when a task succeeds, retrying independently, applying database changes |

Keep deterministic orchestration in `crates/client/src/runtime/`, generic over `ClientStore`, so it is testable without OS threads or real networking. The native actor in `bindings/common` constructs the concrete SQLite client on its own thread; the global registry stores senders and outboxes, not a globally locked live `Client`. The same runtime state machine can later run inside the browser Worker in #59, without assuming OS threads are available there.

## 2. Message contract

Keep JSON at the existing cross-language boundary. Use Rust tagged enums for messages, with checked SDK envelope adapters and shared contract fixtures. Do not adopt all of JSON-RPC or change the backend's wire protocol. Names below are the internal bridge contract, not new frontend methods.

The carrier exposes two operations and a wake notification:

```text
submit(message) -> admission acknowledgement or bridge error
drain(runtimeId) -> ordered batch of events already available
notifyReady(runtimeId) -> tells the SDK to drain; carries no database work
```

`submit` acknowledges copying/admitting the message, not completion of the task. The public Promise resolves only from a matching `taskCompleted` event. Notification is edge-triggered with a drain/recheck handshake; a message arriving while the SDK drains must not lose its wake. Never poll continuously, synchronously wait for task completion in a carrier, or call SDK code while holding a Rust registry/outbox lock.

### Identities

| Identity | Allocator and lifetime | Purpose |
| --- | --- | --- |
| `runtimeId` | Rust, fresh opaque value per open; never reused | Fence a closed/replaced runtime from later events |
| `requestId` | SDK, increasing string counter per bridge, never reused | Route a submitted task or transaction command's one terminal outcome |
| `effectId` | Rust, unique string counter across the runtime lifetime | Correlate one HTTP/timer/callback effect or the lifetime of a socket stream |
| `transactionId` | Rust, fresh opaque capability per active callback transaction; nested scopes add their own token | Admit commands only into their owning transaction/savepoint |
| `callId` | Existing durable call identity | Backend deduplication and final Call outcomes; not replaced by request IDs |
| `observerId` | Rust, per runtime | Route watch/subscription snapshots; not a database ownership claim |

New bridge counters are serialized as strings to avoid JavaScript number precision issues. They are not Model IDs, record stamps or Channel cursors. Duplicate active request IDs are protocol errors, never a second execution. SDK routing uses the runtime ID and request ID together. A successful replica rebuild invalidates replica-bound effects/handles using Rust-owned generations while keeping the runtime's bridge identifiers non-reusable; preserve #162's stale-response fence.

Representative messages:

```json
{"type":"task","runtimeId":"r1","requestId":"42","command":{"kind":"invoke","name":"GetTodo","args":{"id":"todo-1"},"delivery":"direct"}}
{"type":"effectResult","runtimeId":"r1","effectId":"101","outcome":{"ok":true,"value":{"status":200,"body":"..."}}}
{"type":"transactionCommand","runtimeId":"r1","requestId":"43","transactionId":"tx7","command":{"kind":"read","model":"Todo","identity":{"id":"todo-1"}}}
```

Rust-to-SDK event families:

```text
effect(effectId, operation)            HTTP, socket, timer, auth, application callback
cancelEffect(effectId)                abort host work; late answers remain fenced in Rust
taskCompleted(requestId, outcome)     one submitted task's terminal success/error
callCompleted(callId, outcome)        durable call outcome, after local settlement commits
observerChanged(observerId, snapshot) committed query/subscription state
report(diagnostic)                   existing record/runtime diagnostics
runtimeClosed                        terminal lifecycle notification
```

A socket is a streaming effect: `opened`, `message`, `overflow` and `closed` inputs share its effect ID; a frame does not consume the ID. HTTP, timer and callback results are single-use. An effect may belong to a task or to autonomous engine work, such as Channel delivery; there is no requirement that background work have a live frontend Promise.

## 3. SDK Bridge behavior

The Bridge registers a waiter before calling `submit`. An admission failure removes and rejects that waiter. Its event dispatcher handles transport/callback effects, typed completion decoding and observer delivery; it never selects the task's next engine operation.

On `taskCompleted`, remove the route before calling application code and complete it exactly once. A task error is decoded into the existing public error surface; an engine-completed outcome that fails language decoding is an observation error, not a backend rejection. Preserve the existing Call result snapshot and weak-reference behavior. Internal dispatch must finish registrations before invoking application listeners: durable acceptance creates/registers the Call object before a subsequent `callCompleted` event can be delivered. A listener exception cannot interrupt delivery to other waiters or alter a committed outcome.

The Bridge retains maps for waiters, language handles and platform cancellation resources. Those are adapters, not a second task state machine. Remove SDK exclusive queues, `prepare -> HTTP -> apply` orchestration, local query-flight decision logic, status projection rules and watcher re-query decisions once their Rust-owned replacement is active.

## 4. Task execution and local transaction ownership

Local work has these states:

```text
Ready -> WaitingForEffect -> Ready -> Applying -> Completed
Ready -> Applying -> WaitingForCallback -> Applying -> Completed
```

`WaitingForEffect` for framework network I/O holds no SQLite transaction. The actor can advance unrelated ready work while an HTTP request is outstanding. Each application of authority is a complete transaction, including its path-specific progress/settlement updates; never enqueue its records as unrelated writes. The actor takes at most one ordinary transaction unit per scheduling turn and gives ready foreground and sync work turns, preserving the Downlink worker's existing internal ordering and Bootstrap fairness. It does not reorder dependent uplink work or parallelize frozen batches as a side effect of this issue.

During an application transaction callback, Rust retains the transaction ownership token while yielding control to the host. Only commands bearing the current transaction and nested-scope capability may access that SQLite transaction. All ordinary reads and writes wait outside it; they cannot accidentally route into the open session. Lifecycle aborts and callback responses remain serviceable. Network frames can be received/bounded but are not applied until the writer is free.

The callback's own `tx` commands use a continuation lane, not the ordinary queue behind their parent. Nested savepoints have a Rust-owned stack, scoped tokens, and outstanding-command accounting. Finishing a callback with outstanding operations or an invalid nested scope fails the unit according to the current transaction contract. SDK async-context guards still prevent calling the captured outer client from the callback; Rust tokens and checks enforce ownership even if an SDK guard is bypassed. Do not claim Rust can infer whether a language Promise was awaited after it already completed: language-context checks remain adapter evidence where needed.

Callback success releases/commits the relevant scope, failure rolls it back. Public transaction callback return values may be arbitrary language objects: retain the value in SDK memory, send success/failure to Rust, and resolve with that value only after Rust confirms commit. Do not force it through JSON. Framework remote calls from a local transaction remain prohibited. User code can await its own external work as today; the framework does not magically prevent that, and such code holds its own transaction open.

The callback seam is exercised by existing public local transactions in #134. Server-authority downlink hooks are not installed here; #17 later uses the same ownership mechanism.

## 5. Existing operation behavior

| Path | Rust owns the whole flow | Completion boundary |
| --- | --- | --- |
| Local Model operation | Validate, schedule, transaction, commit | Task completes after commit |
| Durable Mutation / enqueued Query | Canonical intent and optimism where applicable commit together, then existing uplink scheduling | Submission task returns Call identity after local acceptance; final `callCompleted` follows receipt commit |
| Direct Query / direct Mutation | Prepare exact request, issue network effect, enforce timeout/lifecycle, validate response, schedule/apply | Task success only after required local writes commit; no-model/no-cache result needs no artificial write transaction |
| Query once | Rust selects cached/join/fetch, owns joined task IDs and completes each caller | One authority/cache commit for a fetched result; cache hits do not reapply authority |
| Channel / Bootstrap | Existing worker consumes correlated network events and applies under current cursor/stamp rules | Status/change events only after corresponding commits; Bootstrap fixed-barrier semantics unchanged |
| Local watch | Rust registers query, reruns after relevant existing change evidence, compares result and emits snapshot | Initial/current committed snapshot and existing semantic change behavior; SDK does not re-query |

Direct results remain the invocation's Loader snapshots, not a reread of the current optimistic local row. Later local optimism may remain visible in a Model after the result is returned. No call waits for unrelated Channel catch-up. Required writes include any chosen Query once cache write, even for `store:false`; a direct invocation without required storage does not acquire a writer solely to make the diagram uniform.

Retry eligibility, elapsed deadlines, auth-refresh coordination and stale-response decisions are Rust-owned. The host supplies clock/entropy facts, executes timers and invokes token/auth callbacks. Preserve existing direct-call timeout/defaults and authentication behavior; no new automatic replay policy for direct calls. Use an engine-wide connection coordinator to prevent duplicate auth refresh while allowing concurrent network requests.

Watch/status policy migration preserves current public snapshots and observable behavior; it does not add #16 APIs or tighter query dependency tracking. Async callbacks and weak references remain language-owned. User callbacks can fail independently of the committed event which requested observation.

## 6. Lifecycle, resource handling and pressure

Close is priority control, not a task parked behind an indefinitely waiting callback. Rust fences effects, rolls back any uncommitted transaction, resolves or rejects every pending local task, requests host cancellation, and terminates observers before announcing `runtimeClosed`. Durable accepted work remains in SQLite for reopen; dropping a Call observer or closing the app is not cancellation of the backend operation. If an external effect may have executed, preserve execution-unknown semantics rather than inventing rejection.

The SDK drains terminal events, aborts platform resources and releases callback handles only after the native runtime has detached the wake sink. Native callback pointers cannot be freed while a worker can still call them. A lost carrier must detach and terminate the actor rather than leave the SQLite transaction alive. Rebuild continues to respect pending-work refusal and handle invalidation, and preserves independently fixed #162/#163 behavior. Old effect results and callbacks never attach to fresh registrations or tasks.

Keep the existing bounded Downlink frame queue and recover from durable cursors on overflow. Task completions, effect replies and lifecycle control are lossless; never silently evict accepted user work or final results. Foreground task/result memory remains proportional to outstanding callers, as with the existing SDK Promise queue; this issue does not invent a new public overload policy. Document that accepted limitation and measure pressure with a stalled consumer. Notifications may coalesce only where current semantics permit it; coalescing a wake cannot drop its pending events. Admission, callback responses and close must not deadlock one another behind a full application work queue.

## 7. Carrier implementation

Use the same JSON envelopes and state machine in every carrier. The worker's wake sink only indicates that outbox data is available; it never invokes application callbacks directly. `drain` moves events out under a short lock and serializes outside it.

- **Node:** use an N-API thread-safe wake callback to schedule the central dispatcher. Native submit only admits work; SQLite runs on the client's actor, outside the process-wide registry lock.
- **Dart:** a `NativeCallable.listener` wake callback can be called from the Rust worker and schedules into its owning isolate. Pass only a numeric/opaque wake token, not a borrowed JSON pointer. Drain/copy owned messages through the C ABI and free allocations once. Stop native wake production before closing the callback. A platform isolate may remain a carrier, but it no longer owns scheduling policy or SQLite work. See the [Dart callback contract](https://api.dart.dev/dart-ffi/NativeCallable/NativeCallable.listener.html).
- **React Native/iOS:** retain the existing Expo/Swift carrier, replacing long-running client calls with admission and an event/wake adapter to the shared JS Bridge. Keep native callback context alive through worker shutdown; dispatch notifications on the platform-required queue. Do not assert Android support if the existing native module does not supply it.
- **Browser:** define an adapter seam usable through `postMessage` and a Worker-owned runtime; actual WASM/storage/browser implementation remains #59.

Open must install event routing before background initialization can complete. A failure to open returns a normal failed task and releases its native resources. Wake and shutdown races need carrier-level tests, not just simulated core tests.

## 8. Delivery and verification

One branch, one draft PR, four checkpoints: (1) executor/bridge/transaction ownership; (2) all authority paths; (3) remaining lifecycle/status/watch policy; (4) removal of duplicate SDK paths, documentation and full verification. Each checkpoint gets coherent commits, focused tests and review. These are engineering gates, not repeated user permission requests. The final PR updates from main, incorporates independently landed fixes, reviews the combined diff and merges only with required checks passing. #17 follows that merge.

Verification must include: overlapping tasks returning to the correct waiter; network concurrency without a held writer; wrong/stale transaction tokens; callback reentrancy and rollback; close during callback/network; repeated rebuild and stale responses; acceptance before durable-call outcome without a registration race; once cache/join/fetch parity; Bootstrap fairness/fixed barriers; TypeScript/Dart/RN event parity; and independent clients making progress without a global database lock. Assert commit-before-success by inspecting the database when completion is observed and by injecting commit failure.

Use deterministic controlled network effects and synchronization barriers rather than timing-dependent sleeps. Native UI responsiveness and callback teardown require real carrier integration tests. Use the existing [test strategy](../../engineering/testing/strategy.md) and [commands](../../engineering/testing/running.md); record baseline and final measurements without claiming performance gains in advance. Preparation is documentation/source inspection only, not proof that the target runtime already works.

## Alternatives and design review points

Keeping per-language orchestration duplicates lifecycle rules. Putting all I/O in Rust conflicts with the agreed platform boundary and future browser adapter. A single process-wide actor would serialize independent clients. A durable response inbox is unnecessary for this refactor and would change direct-call semantics. Merely routing every message through one FIFO would deadlock transaction callbacks behind their own parent; control and transaction continuations must remain serviceable.

The most important implementation review points are the wake/drain shutdown handshake, ownership of an open transaction across language callbacks, and preserving completion order while removing SDK queues. No public API redesign or backend migration is required by this proposal. The message spellings and module boundaries here are internal design choices and can be refined during implementation only if their guarantees and checkpoint evidence remain intact.
