# Prerequisite Handlers at Open Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The client runtime runs the application's prerequisite handlers, registered once at open, whenever a task becomes pending - after a commit, at open after a restart, after a reset - and retries a handler-declared transient failure with engine-owned backoff ([#185](https://github.com/zanminwang/axton/issues/185)).

**Architecture:** The Rust runtime replaces the `runPrerequisites` task loop with a scheduler it owns: fixed handler names from the open request, a dirty flag set by commits and outcomes, one `prerequisite` effect at a time, per-key backoff in memory and one `timer` effect for the earliest due retry. SDKs install the handler map and the timer executor before the runtime can issue effects, and map a thrown `PrerequisiteRetry` to `error.retry`. `runPrerequisites` is removed.

**Tech Stack:** Rust client runtime and actor, SQLite tests, TypeScript client-js and React Native, Dart SDK, compiler emitter for the generated facades.

## Global Constraints

- The accepted design is the `[design]` comment on #185; it owns the option names, outcomes, backoff policy and the removal of `runPrerequisites`.
- Work on `codex/185-prerequisite-handlers` in `.worktrees/185-prerequisite-handlers`, based on `cc4f025`. Other agents work on #179-#183 in other worktrees; touch none of them.
- Unchanged: the task key derivation, `pendingTasks()`, `setReadiness()`, `record_status`, the push gating (guarantee P3) and the Most Days surfaces (`@sequence(after:)`, `backend.publish(tx, body)`, `createBackend({ admit })`, Dart `SyncServer(headers:)` / `AdmissionRefused` / stop-once).
- This plan is not evidence. Record the commands actually run in the PR.

## Task 1: Rust runtime scheduler

**Files:** `crates/client/src/runtime/{prerequisites.rs,mod.rs,tasks.rs,effects.rs,lanes.rs,protocol.rs}`, `crates/client/src/lib.rs`, `bindings/common/src/actor.rs`, `fixtures/bridge/envelopes.json`. Tests: new `crates/sqlite/tests/runtime_prerequisites.rs`; update `runtime_lanes.rs`, `push.rs` and `bindings/common/tests/runtime.rs` where they used `runPrerequisites` / `next_task`.

- [ ] Write failing runtime tests over SQLite, the test acting as host with a fixed clock:
  - a `mutate` of a `@requires(RemoteBlob(key: self))` field issues a `prerequisite` effect with no other task; success resolves the task and wakes the push lane;
  - a task pending when the runtime is dropped runs at the first step after reopening with the same handlers;
  - an answer `{ok:false,error:{message,retry:true}}` keeps the task pending, issues a `timer` of 1000 ms, then 2000 ms after the next transient failure, and runs again when it fires; `readiness pending` clears the backoff and runs at once;
  - a plain failure leaves the task `failed` with its reason in `tasks`, and no timer or new effect follows;
  - close while a handler runs cancels its effect, answers `runtimeClosed` and ignores a late result;
  - a name the schema does not declare is refused at registration; a declared prerequisite without a handler keeps its task pending and untouched.
- [ ] Implement: `EffectError.retry` (serde default false, omitted when false); `ClientRuntime::register_prerequisite_handlers(names)` validated against the target schema; the scheduler state in `prerequisites.rs` (handlers, dirty, running effect and key, backoff map, timer); a lane turn when dirty and idle; `EffectKind::Prerequisite { key }` and `EffectKind::PrerequisiteTimer`; dirty on `wake_lanes`, readiness, outcome, timer, rebuild; rebuild cancels and clears; close drops the state after cancelling effects. Backoff uses `load_backoff(attempts, entropy)`.
- [ ] Remove `Command::RunPrerequisites`, the `runPrerequisites` fixture envelope and `Client::next_task`; add an `effectResult` fixture with `retry: true`.
- [ ] Actor: parse `prerequisiteHandlers` from the open request (array of strings) and register.
- [ ] Run `cargo test -p axton-client -p axton-sqlite -p axton-bindings-common --locked` (actual crate names from `Cargo.toml`), `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`. Commit.

## Task 2: TypeScript and React Native

**Files:** `packages/client-js/{runtime.mts,bridge.mts,connection.mts,index.mts}`, `packages/client-react-native/index.ts`, tests `integration/bindings/client-js/{prerequisite.test.mjs,prerequisite-harness.mjs,connection.test.mjs,runtime.test.mjs}`, `integration/bindings/client-react-native/prerequisite.test.mjs`.

- [ ] Write the shared harness scenarios (commit runs the handler; restart runs after reopen; transient retry with growing recorded delays through a carrier wrapper that shortens `timer` effects; terminal failure visible in `pendingTasks()`; close during a run settles `close()` and reports nothing), run for client-js and for RN's `Transaction` + `createServerConnection`.
- [ ] Implement: `Bridge.open` accepts an `install(bridge)` hook run before `runtimeOpen`; the client builds `Effects` there and installs the `prerequisite` executor for `options.prerequisites`, passing `prerequisiteHandlers` names; the executor passes an `AbortSignal` aborted on cancel and answers `retry: true` for `PrerequisiteRetry`. Export `PrerequisiteRetry` and `PrerequisiteHandler` from client-js and RN. Remove `runPrerequisites`.
- [ ] Run `node --test integration/bindings/client-js/*.test.mjs`, `node --test integration/bindings/client-react-native/*.test.mjs`, prettier check on touched `.mts`. Commit.

## Task 3: Dart

**Files:** `packages/dart/lib/src/{client.dart,bridge.dart,connection.dart}`, `packages/dart/lib/axton.dart`, tests `packages/dart/test/{prerequisite_test.dart,connection_test.dart,runtime_bridge_test.dart}`.

- [ ] Write the same scenarios in `prerequisite_test.dart`; the backoff test opens the client in a zone whose `createTimer` records and shortens delays.
- [ ] Implement: `Client.open(prerequisites:)`; `Bridge.open` takes the handler names and installs `timer` and `prerequisite` effect handlers synchronously after construction; the timer executor moves from `RuntimeConnection` to client level and runs in the opening zone; `PrerequisiteRetry` answers `retry: true`; the handler receives a `cancelled` future. Remove `runPrerequisites`.
- [ ] Run `dart analyze` and `dart test` in `packages/dart` with `AXTON_LIBRARY` set. Commit.

## Task 4: Generated facades

**Files:** `crates/compiler/src/emit.rs`, `crates/compiler/tests/compiler.rs`, every checked-in generated client (`examples/todo/generated/*`, `integration/*/client.ts`, `integration/**/generated.dart`).

- [ ] Replace the emitted `runPrerequisites` member with a `prerequisites` open option passed to `Client.open` in TS and Dart; update the compiler test.
- [ ] Regenerate with the repository's generators (`integration/generated-api/verify.sh` and the example/integration generation scripts); inspect the diff. Run `cargo test -p axton-compiler --locked` and `bash integration/generated-api/verify.sh`. Commit.

## Task 5: Documentation

- [ ] Update `docs/engineering/architecture/schema/prerequisites.md` (runner, quality evidence, limitations), `docs/engineering/architecture/client/runtime.md` (messages, scheduling paragraph, module table, evidence), `docs/engineering/architecture/sdks/typed-api/client.md`, `docs/engineering/testing/integration/bindings.md`, `website/docs/frontend/runtime.md#prerequisites`, `website/docs/api-index.md`, `website/docs/schema/define.md`, `website/docs/frontend/client-api.md`, `packages/client-react-native/README.md`.
- [ ] Check relative links and anchors in touched documents. Commit.

## Task 6: Verify and open the PR

- [ ] Run the focused suites again at the final commit; do not run `bash scripts/test.sh` on the shared machine (CI runs it).
- [ ] Open the PR with the API added/removed per SDK, `Closes #185`, executed vs inspected evidence and limits; swap labels; comment `[pr]`; watch CI.
