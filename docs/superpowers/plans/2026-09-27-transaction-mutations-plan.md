# Transactional Mutation Enqueue and Local Companions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Delegate implementation only when the user authorizes it.

**Goal:** Atomically read local data, enqueue typed Mutations and apply per-Mutation local companions, preserving independent later writes through acceptance, rejection and restart.

**Architecture:** Extend the Rust-owned application transaction with named Mutation submission and a restricted companion capability. Reuse the durable queue and settlement machinery, correcting its ordering where needed; generated SDKs carry typed commands and callbacks. Each Mutation retains its own Call; the transaction returns the callback's ordinary value after commit.

**Tech Stack:** Rust core/runtime/compiler, SQLite, TypeScript Node/React Native SDKs, Dart SDK, existing PostgreSQL integration harness.

## Global Constraints

- Follow the [design spec](../specs/2026-09-27-transaction-mutations-design.md); its sections 3 and 6 own API and lifecycle behavior.
- Work on `codex/transaction-mutations` in an isolated worktree. Planning base is `39f629c`; the spec was committed as `70dc998`. Check current upstream before implementation and review overlapping runtime changes without discarding them.
- This plan is not implementation evidence. Commands below are required future checks, not tests already run.
- Application transactions expose `tx.mutations.<name>` only; no `.call`, Query enqueue or remote read. onStore remains local-only.
- The `local` option belongs only to transaction Mutation methods in this scope. It is never serialized into args, a server request or a saved callback.
- Normal `tx.models` writes remain independent. Companion writes are explicitly owned by one call, including when a transaction has several calls.
- Local commit is atomic; backend outcomes remain per Mutation. Existing Model-derived dependencies remain; sharing a transaction creates none.
- Rust owns admission, sequencing, durability, settlement and call lifecycle. SDKs do not implement a second queue or settlement algorithm.
- No new business schema syntax, backend operation version bump or general nested transaction API. Preserve existing supported savepoints and failure checks.
- Use a single business Mutation when Entry/media/placement must succeed together at the backend.
- Keep generated output changes reproducible and repository documentation in English.

## Preparation and review checkpoints

- [ ] Read the spec, `docs/engineering/architecture.md`, `docs/engineering/guarantees.md`, `docs/engineering/testing/strategy.md` and `docs/engineering/testing/running.md`.
- [ ] Record `git status --short --branch`, `git worktree list` and the implementation base. Run `cargo test --workspace --locked` to establish the Rust baseline. Diagnose failures before attributing them to this feature.
- [ ] Before SDK work, run `npm ci` and `bash scripts/build.sh`. Before Dart work, run `dart pub get` in `packages/dart` and set `AXTON_LIBRARY` and `AXTON_DART_LIBRARY` as the running-tests guide specifies.
- [ ] Implement tasks in order. At each checkpoint inspect the diff against its invariants, run the listed focused checks, and commit only that checkpoint's files. Use failing behavior tests before implementation; record the actual commands and outcomes.

## Task 1: Make local companion settlement preserve later writes

**Files**
- Modify: `crates/client/src/mutate.rs`, `crates/client/src/push.rs`, `crates/client/src/queue.rs`.
- Modify if durable ordering metadata is required: `crates/client/src/ddl.rs`, `crates/client/src/schema_store.rs`, `crates/client/src/lib.rs`.
- Test: `crates/sqlite/tests/push.rs`, `crates/sqlite/tests/settlement.rs`, `crates/sqlite/tests/client.rs`, `crates/sim/tests/local.rs`.

**Interfaces:** Keep `Mutation.companion: Vec<Operation>` as the distinction between local companion intent and schema-derived wire operations. Preserve the existing `Engine::enqueue`, `Engine::direct` and receipt entry points. Any new persistence representation remains internal; later tasks need correct same-call settlement, not a new backend protocol.

- [ ] Add named regressions using the existing low-level companion fixtures before enabling named transaction Mutations. Exercise both accepted and rejected outcomes:

```text
existing Composition(title="old")
  -> companion delete owned by A
  -> independent create same identity with title="new"
  -> settle A
expected: title="new" for either outcome

existing Shelf(rank=0)
  -> companion update rank=1 owned by A
  -> independent update rank=2
  -> settle A
expected: rank=2 for either outcome

existing Shelf(rank=0)
  -> companion update rank=1 owned by A
  -> companion update rank=2 owned by B
  -> reject A, accept B
expected: rank=2; acceptance/rejection delivery order cannot change it
```

Also cover no-later-edit restore, later independent delete, callback-owned cascades and a record whose only creation is still pending. Use separate test scenarios for lifecycle-dependent operations; do not expect a dependent call to execute after its prerequisite is rejected.

- [ ] Run `cargo test -p axton-sqlite --locked --test push --test settlement --test client`. Confirm that added tests expose unsupported behavior or pass for a demonstrated existing guarantee; preserve already passing behavior tests.
- [ ] Correct settlement ordering. The current acceptance loop in `push.rs` folds companions into `before` after later direct writes may already have updated that base. Merely removing the canonical companion rejection is insufficient. Retain enough durable order/provenance to distinguish earlier pending companion effects from later independent writes, including delete/recreate and descendants.

The reconstruction contract is:

```text
record history ordered by committed local operation position:
  pending wire operation       -> retain existing optimism/authority rules
  pending companion            -> apply while its owner remains pending
  accepted companion           -> retain its local effect at its original position
  rejected companion           -> omit its effect
  independent direct operation -> retain at its original position

settlement:
  stage authority using existing stamp rules
  change only the settling owner's disposition
  reconstruct affected records in order
  compact a settled prefix only when no unresolved earlier operation needs it
  commit records, queue/completion and recovery metadata together
```

Do not interpret a previously validated direct recreation as a new conflicting insert during replay: it must preserve the newer independently created content. Preserve existing pending-create and divergence rules. A new ordered local-write journal, if needed to meet this contract, must have transactional sequence allocation, explicit owner/disposition, indexed identity lookup and bounded cleanup after outstanding work settles; it must not become an SDK history or a second outbound queue. Record the final table/encoding change in the storage architecture before completing this checkpoint, including how old retained rows open or rebuild under existing compatibility policy.

- [ ] Preserve authoritative readback precedence for server-owned records. Existing tests `accepted_wire_rows_do_not_promote_companion_over_server_authority` and `companions_settle_locally_except_where_the_server_answered` are regression gates. Local companions cannot override newer server authority by bypassing stamp application.
- [ ] Add reopen checks with later writes still pending and cleanup checks after settlement. Run the focused SQLite tests again, then `cargo test -p axton-sim --locked`. Commit after reviewing operation ordering and cleanup.

## Task 2: Submit canonical named Mutations inside an existing transaction

**Files**
- Modify: `crates/client/src/actions.rs`, `crates/client/src/lib.rs`, `crates/client/src/mutate.rs`, `crates/client/src/queue.rs`.
- Test: `crates/sqlite/tests/actions.rs`, `crates/sqlite/tests/defaults.rs`.

**Interfaces:** Add the following Rust transaction methods, using existing types. `append_companion` is crate-internal and is callable only through the runtime-owned companion capability in Task 3.

```rust
ClientTransaction::submit_mutation(
    &mut self,
    name: &str,
    version: u64,
    args: Value,
    options: ActionCallOptions,
) -> Result<SubmittedCall>

ClientTransaction::append_companion(
    &mut self,
    ordinal: u64,
    operation: Operation,
) -> Result<()>
```

- [ ] Add failing tests for named submission in one existing transaction: create a seed Composition, read it, submit the business Mutation, append its local delete, then either commit or throw. Assert queue rows, visible records and recovery metadata together.
- [ ] Run `cargo test -p axton-sqlite --locked --test actions --test defaults`.
- [ ] Extract shared fresh-intent preparation from `Client::submit_action_with_options`: fill defaults once, normalize args, validate bindings/store and derive wire operations. Reuse it from transaction submission without opening another outer transaction. Refuse Query descriptors at this new entry; retain queued Query behavior at the existing standalone entry.
- [ ] Keep call ID allocation and canonical args inside the local atomic unit. Appending a companion validates ownership, normalizes the Model operation, captures its before state/cascades and records it durably without sending it. Do not let arbitrary raw queue input masquerade as a valid schema-derived intent.
- [ ] Test omitted create defaults, scalar-only Mutations, two calls, independent direct writes, invalid args/store, duplicate identities, a failed companion and existing savepoint rollback. Freeze only after commit and assert decoded wire requests contain canonical args and no Composition or companion metadata.
- [ ] Re-run the focused tests. Commit after confirming standalone action behavior and wire encoding are unchanged.

## Task 3: Rust transaction capabilities and provisional Call lifecycle

**Files**
- Modify: `crates/client/src/runtime/protocol.rs`, `crates/client/src/runtime/transactions.rs`, `crates/client/src/runtime/effects.rs`, `crates/client/src/runtime/commands.rs`, `crates/client/src/runtime/observers.rs`, `crates/client/src/runtime/mod.rs`.
- Test: `crates/sqlite/tests/runtime.rs`, `crates/sqlite/tests/runtime_lanes.rs`, `crates/sqlite/tests/store_hooks.rs`.

**Interfaces:** Add a typed transaction command rather than exposing raw low-level `Enqueue`. Proposed bridge payload:

```json
{"kind":"submitMutation","name":"PublishEntry","version":1,"args":{},"local":true}
```

`store` is optional and follows existing encoding. `local` is a boolean requesting a callback effect, not executable code. Without a callback the command completes with the existing `{callId, ordinal}` submission value. With a callback, Rust issues a `mutationLocal` effect carrying `transactionId`, a fresh `companionId` and the initiating `requestId`; successful callback completion resumes the submission and returns that value. Associate the SDK callback using the request ID, never a schema/business input.

Add an optional `companionId` to transaction-command and callback-result envelopes. It is distinct from the existing savepoint `scope`. Runtime-issued Call lifecycle events are `transactionCallState {callId, state}`, where internal state is `committed` or `rolledBack`. Registration from the submission response starts provisional; do not add a public `CallStatus` enum variant.

- [ ] Extend protocol round-trip tests for callback correlation, absent tokens, wrong tokens and stale tokens. Extend the existing runtime harness to submit a named Mutation during an application callback and during onStore; only the former is admitted.
- [ ] Run `cargo test -p axton-sqlite --locked --test runtime --test runtime_lanes --test store_hooks` and `cargo test -p axton-client --locked`.
- [ ] Add the restricted child capability to the current writer transaction. While it is active, allow its local read/write commands only; reject captured parent writes/enqueue, Channel operations, watch and savepoints. Direct writes from this capability call `append_companion`, not independent `direct`. The parent submission cannot complete until the callback and its outstanding commands finish.
- [ ] Keep outer and companion callback completion correlation separate: ending the local callback never commits or closes the outer transaction. Restore the parent capability when the companion finishes. Callback failure poisons its owning transaction/savepoint using existing failure rules. Reject expired or wrong-scope handles in Rust even if SDK checks are bypassed.
- [ ] Track provisional call IDs per transaction and existing savepoint scope. Emit committed transitions only after SQLite commit succeeds, before returning the transaction task success. On rollback/savepoint rollback invalidate only the discarded scope's Calls. On close/cancellation/commit failure, terminate observers and release callback registrations; no provisional call becomes sendable.
- [ ] Exercise a queued downlink delivery while a media read/diff/submission callback is open. Assert that it cannot alter the transaction's view, watchers see no intermediate state, no push effect includes provisional work and delivery resumes after commit. Exercise hooked receipt settlement as well as the no-hook path.
- [ ] Re-run the focused gates and commit after reviewing every exit path and event ordering.

## Task 4: Thin SDK callbacks and Call observation

**Files**
- Modify: `packages/client-js/transaction.mts`, `packages/client-js/runtime.mts`, `packages/client-js/actions.mts`, `packages/client-js/index.mts`, `packages/client-js/bridge.mts`.
- Modify: `packages/client-react-native/transaction.mts`, `packages/client-react-native/index.ts`.
- Modify: `packages/dart/lib/src/client.dart`, `packages/dart/lib/src/port.dart`, `packages/dart/lib/src/bridge.dart`, `packages/dart/lib/src/actions.dart`.
- Test: `integration/bindings/client-js/transaction.test.mjs`, `integration/bindings/client-js/actions.test.mjs`, `integration/bindings/client-react-native/transaction.test.mjs`, `integration/bindings/client-react-native/actions.test.mjs`, `packages/dart/test/client_test.dart`, `packages/dart/test/actions_test.dart`.

**Interfaces:** The raw transaction adapter implements `submitMutation(name, version, args, decode, options)` returning `Promise<Call<T>>` or `Future<Call<T>>`. Options contain existing store policy and an optional local callback. The raw local callback adapter exposes reads/direct Model operations through a companion capability, not the full Transaction class. The existing ActionRegistry routes completions and now mirrors the Rust lifecycle transitions.

- [ ] Add focused bridge-backed tests before implementing SDK changes. Assert `await tx.submitMutation(...)` returns only after the local callback; the enclosing transaction is still uncommitted.
- [ ] Implement callback registration by request ID and delivery of `mutationLocal` effects, retaining existing outstanding-command, first-failure and async-context checks. Build a separate restricted adapter rather than removing properties from a shared object. Retire callback references on every finish/error/close path.
- [ ] Extend CallState with private provisional/committed/rolled-back lifecycle. The public status remains pending/succeeded/failed. `wait()` while provisional rejects with `transaction_uncommitted` without settling the Call. Once rolled back it rejects with `transaction_rolled_back` and status becomes failed. Once committed it uses existing backend `CallOutcome` behavior. A caught early-wait error leaves the call usable after commit; an uncaught error still fails the outer callback normally.

```ts
// Assertions to include in a transaction fixture:
await assert.rejects(call.wait(), { code: "transaction_uncommitted" });
// Following successful outer commit, the same call can wait normally.
// In a separate fixture, leak a call then throw from the transaction:
await assert.rejects(leaked.wait(), { code: "transaction_rolled_back" });
```

- [ ] Route lifecycle notifications even when no `wait()` has been called. Preserve weak ownership: an unobserved Call does not retain the task, callbacks or application data. Scope rollback terminates discarded Calls without invalidating surviving ones. Handle an immediate completion after commit without a registration race.
- [ ] Verify captured parent/client misuse, unawaited operations, expired handles, close, rollback and onStore on Node, Dart and the existing React Native native-host harness. Do not claim device coverage from the Node-hosted mobile tests.
- [ ] After native build, run:

```sh
node --test integration/bindings/client-js/transaction.test.mjs integration/bindings/client-js/actions.test.mjs
node --test integration/bindings/client-react-native/transaction.test.mjs integration/bindings/client-react-native/actions.test.mjs
(cd packages/dart && dart analyze && dart test test/client_test.dart test/actions_test.dart)
```

Expected: all focused tests pass with the newly built native runtime. Commit after checking there is no SDK-owned durable queue or rollback algorithm.

## Task 5: Generated application and onStore surfaces

**Files**
- Modify: `crates/compiler/src/emit.rs`, `packages/dart/lib/src/port.dart`.
- Modify fixture sources/tests: `fixtures/compiler`, `integration/generated-api/test.ts`, `integration/generated-api/generated_test.dart`, `integration/generated-api/negative/misuse.ts`, `integration/generated-api/negative/misuse.dart`, `integration/action-contract/positive.ts`, `integration/action-contract/negative.ts`, `integration/action-contract/positive.dart`, `integration/action-contract/negative.dart`, `integration/action-runtime-ts/types.ts`.
- Regenerate outputs in `integration/generated-api`, `integration/action-contract`, `integration/action-runtime-ts`, `integration/action-runtime-dart` using repository runners.

**Interfaces:** Generate three capability surfaces: application Transaction with local Models, Channels and queued Mutations; local companion context with Models only; onStore context retaining its current local Model/Channel capabilities without Mutations. Share Model codecs/accessors without sharing an overpowered transaction type. Generate transaction-only Mutation options extending existing store options with `local`; do not accidentally add `local` to standalone/direct/query methods.

- [ ] Add positive fixtures for no callback, companion callback, scalar-only Mutation, one Call and two Calls returned from a transaction, and normal arbitrary transaction return values. Check inferred `Call<DeclaredOutput>` in both languages.
- [ ] Add negative fixtures for `tx.mutations.call`, transaction Query/Fetch/Load, local callback mutation/channel/savepoint/watch and onStore mutation. Keep these runtime-negative tests too; type hiding is not authorization.
- [ ] Implement the generated facade, ports and type imports. Preserve schemas without Mutations: their transactions need no generated Mutation namespace. Preserve model-only and model-free schema generation.
- [ ] Regenerate via `bash integration/generated-api/verify.sh` and `bash integration/action-runtime-ts/verify.sh`. For action-contract fixtures run the compiler command used in `scripts/test.sh`, then its TypeScript and Dart positive/negative checks. Run the Dart generated tests as in `scripts/test.sh`. Inspect output diffs; change obsolete negative expectations only for the newly allowed transaction enqueue surface.
- [ ] Commit after checking API consistency across generated Node/Dart and React Native adapters.

## Task 6: Application recovery, documentation and final review

**Files**
- Modify: `integration/action-e2e/source/app.model`, `integration/action-e2e/action.test.mts` and regenerated outputs for that fixture; extend its existing backend handler fixture in `integration/action-e2e`.
- Create: `crates/sqlite/tests/transaction_mutation_crash.rs` for isolated-process SQLite recovery tests.
- Modify: `docs/engineering/architecture/client/frontend-interface.md`, `docs/engineering/architecture/client/runtime.md`, `docs/engineering/architecture/client/engine/local-operations/writes.md`, `docs/engineering/architecture/client/engine/settlement.md`, `docs/engineering/architecture/client/storage/reconciliation.md`, `docs/engineering/architecture/sdks/typed-api/client.md`, `website/docs/frontend/client-api.md`, `website/docs/frontend/storage.md`.
- Update guarantee L4's wording/evidence only if the final ordered representation changes or clarifies its existing rule; do not promise arbitrary-crash coverage from a clean reopen test.

- [ ] Add a real-backend PublishEntry fixture with local Composition, Entry/media/placement inputs and an explicit backend rejection switch. Prove acceptance keeps the local deletion, rejection restores editing, no companion is sent and direct local writes survive. Run `bash integration/action-e2e/run.sh`.
- [ ] Add process-interruption tests using a spawned test process and parent-controlled barriers: before local commit, after local commit, before receipt commit and after receipt commit. Terminate the child without client close; reopen the same SQLite file and assert queue/records/recovery metadata agree. Retrying a frozen request retains its existing identity; callbacks are not rerun. Use the existing test executable in a child-only mode rather than a shell timing sleep. Cover rollback of a failed receipt transaction and exactly one visible settled result.
- [ ] Run `cargo test -p axton-sqlite --locked --test transaction_mutation_crash`. Expected: each named interruption boundary passes. State explicitly that these tests cover those boundaries, not every hardware/filesystem failure.
- [ ] Update the owning documentation to distinguish ordinary local atomicity, per-Mutation companion fate and independent backend outcomes. Document Call timing, before-commit wait errors, multiple calls, unchanged onStore restrictions and authoritative-data precedence. Keep this plan/spec as historical design, not the sole API reference.
- [ ] Run `bash scripts/test.sh` once after focused gates pass. Inspect generated fixture changes, `git diff --check`, documentation links and unchanged backend operation histories. Report missing tools/services and any unexecuted platform checks rather than claiming a full gate passed.
- [ ] Review the final diff against all ten spec quality requirements. Correct any gap before declaring implementation complete. Commit the final integration/docs checkpoint. PR creation or merge follows the user's execution authorization, not this planning document.

## Spec coverage and handoff

| Spec requirement | Owning task |
| --- | --- |
| Atomic read/enqueue/local write | 2, 3, 6 |
| Offline/reopen and acceptance/rejection | 1, 2, 6 |
| Later independent edits and overlapping companions | 1, 6 |
| Multiple independent Call outcomes | 2, 3, 4 |
| Stable media read/diff and committed watchers | 3 |
| No companion wire leakage; canonical validation | 2, 6 |
| Provisional/rollback Call and existing savepoints | 3, 4 |
| Typed and runtime capability restrictions | 3, 4, 5 |
| Callback failure and lifetime | 3, 4 |
| Process-interruption evidence | 6 |

The implementation agent should start at Task 1, not by adding `mutations` to the SDK facade. The ordered-settlement regressions are a checkpoint: existing low-level companion support must not be mistaken for proof of the new guarantees. Keep business behavior in the spec and execution evidence in the eventual PR; do not change approved semantics merely to preserve an old negative test.
