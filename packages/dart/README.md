# AXTON Dart client

See the [documentation](../../website/docs/frontend/setup.md).

## How it runs

The client is a thin carrier over the Rust-owned client runtime. The runtime owns the database, task ordering, both sync lanes, direct calls, retries, timeouts, credential-refresh coordination, and every status it publishes. This package only moves messages and runs the platform work the runtime asks for. See [SDK bindings](../../docs/engineering/architecture/sdks/bindings.md) for the contract.

- **Admission.** Each call submits one complete task through the C ABI (`axton_runtime_submit`). Admission only copies the task into the runtime's mailbox. The returned `Future` settles from the task's `taskCompleted` event.
- **Wake and drain.** The runtime wakes the isolate through a single `NativeCallable.listener`. The isolate drains the published events in order on its own event loop. Each `taskCompleted` settles its waiter, `observerChanged` feeds subscription and watch streams, `callCompleted` settles `Call` handles, and `report` reaches the connection's `onError`. A handle that a completion names, such as a `Call`, a subscription, or a watch, is registered while that completion is dispatched. A later event in the same batch therefore always finds it.
- **Platform adapters.** The package supplies HTTP and the live WebSocket (`dart:io`), timers, and the application's callbacks: the transaction body, `refreshAuth`, and prerequisite handlers. Each callback runs in the zone that registered it. Aborting one of them only frees the platform resource, because the runtime fences late answers.
- **Transactions.** Inside a transaction callback, use the `Transaction`'s own commands. A call on the outer client that would submit a task would wait behind the open transaction, so it fails at once with `StateError('transaction_active')`, or with `CallError` code `transaction_active` for Actions.
- **Close and isolate exit.** `close()` submits the runtime's priority `close` before it waits for anything, so a callback that still holds the transaction does not delay it. If an isolate exits while a runtime is still attached, a native finalizer detaches that runtime, so the database file is released.

A `watch` stream whose first query fails ends with that error. If a later re-run fails, the error goes to the connection's `onError` and the watch stays open.
