# axton

> **Alpha.** AXTON is alpha software: its API is unstable and it is not ready for production use.

AXTON's Dart client for Dart and Flutter applications. See [client setup](https://github.com/zanminwang/axton/blob/main/website/docs/frontend/setup.md) for usage and reference.

## Native library

The client runs AXTON's Rust runtime from a native library. The package's build hook bundles it for these targets: macOS arm64 and Linux x64 hosts; iOS arm64 devices and arm64/x64 simulators; Android arm64-v8a, armeabi-v7a and x86_64. It downloads the library for the target from this version's [GitHub release](https://github.com/zanminwang/axton/releases), checks its SHA-256 against the one the package records and caches it; a later build reuses the cache without the network. An unsupported target, a failed download or a different file fails the build.

To build without GitHub, for example before a release is published, put the release's library files in a directory and name it in the application's `pubspec.yaml`. Relative paths are resolved from that file. Each file must still match its recorded SHA-256.

```yaml
hooks:
  user_defines:
    axton:
      local_artifacts: path/to/libraries
```

`Client.open(libraryPath: ...)` loads a library file instead of the bundled one. A checkout of the AXTON repository bundles none, so its tests pass `libraryPath`.

## How it runs

The client is a thin carrier over the Rust-owned client runtime. The runtime owns the database, task ordering, both sync lanes, direct calls, retries, timeouts, credential-refresh coordination, and every status it publishes. This package only moves messages and runs the platform work the runtime asks for. See [SDK bindings](https://github.com/zanminwang/axton/blob/main/docs/engineering/architecture/sdks/bindings.md) for the contract.

- **Admission.** Each call submits one complete task through the C ABI (`axton_runtime_submit`). Admission only copies the task into the runtime's mailbox. The returned `Future` settles from the task's `taskCompleted` event.
- **Wake and drain.** The runtime wakes the isolate through a single `NativeCallable.listener`. The isolate drains the published events in order on its own event loop. Each `taskCompleted` settles its waiter, `observerChanged` feeds subscription and watch streams, `callCompleted` settles `Call` handles, and `report` reaches the connection's `onError`. A handle that a completion names, such as a `Call`, a subscription, or a watch, is registered while that completion is dispatched. A later event in the same batch therefore always finds it.
- **Platform adapters.** The package supplies HTTP and the live WebSocket (`dart:io`), timers, and the application's callbacks: the transaction body, `refreshAuth`, and prerequisite handlers. Each callback runs in the zone that registered it. Aborting one of them only frees the platform resource, because the runtime fences late answers.
- **Transactions.** Inside a transaction callback, use the `Transaction`'s own commands. A call on the outer client that would submit a task would wait behind the open transaction, so it fails at once with `StateError('transaction_active')`, or with `CallError` code `transaction_active` for Actions. `submitMutation` queues a Mutation in the transaction; its typed input callback runs in the same zone before returning the Mutation input, and its `WritePort` writes are that call's local companions. Await such a submission: until it completes, every other command of the transaction fails with `invalid transaction capability` and fails the transaction. The returned `Call` stays provisional until the commit.
- **Close and isolate exit.** `close()` submits the runtime's priority `close` before it waits for anything, so a callback that still holds the transaction does not delay it. If an isolate exits while a runtime is still attached, a native finalizer detaches that runtime, so the database file is released.

A `watch` stream whose first query fails ends with that error. If a later re-run fails, the error goes to the connection's `onError` and the watch stays open. A `watchSql` stream behaves the same.

## Bound Store and application directory

`Client.open` requires `path`, `schema`, `stream` and `StoreConnection`. Its `StoreIdentity(backend, viewer, contract)` is stable offline; credentials refresh never changes a Store binding. `projectionGeneration` defaults to `'1'` and must match the backend. Generated clients provide schema automatically.

Android hosts must call `Client.configureApplicationData(stableApplicationSupportOrFilesPath)` once before any Store opens. Obtain the stable directory from the application platform provider. This process-wide initializer supports both the bundled ABI and `libraryPath` development ABI. Repeating the same path is safe; a different path is refused. It is not a per-client lock directory.

Dart named Mutations use `.name(typedInput)` or `.name.withTransaction((tx) async => typedInput)` on client and transaction. The first await commits local acceptance; `Call.wait()` waits for backend outcome and local settlement. The callback's `tx.models` writes are owned device-only companions. Query/Fetch `store` is a boolean, default true. `await client.bootstrap()` covers the one bound Stream's full manifest and catch-up.
