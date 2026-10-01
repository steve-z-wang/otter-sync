# Load Channel Enrollment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Delegate only with user authorization.

**Goal:** Let a Load handler enroll its returned records using existing Channel add methods, atomically with the saved page, so future touches distribute updates automatically.

**Architecture:** Extend the internal Load host response with add-only membership intents, collect them through a restricted backend context and settle them in Rust through existing publication logic. Preserve per-page Serializable transactions, saved-call replay and after-commit wakes. Client APIs, schema syntax, storage tables and wire page formats stay unchanged.

**Tech Stack:** Rust server/compiler, TypeScript backend and generated interfaces, existing pg/Prisma/Drizzle PostgreSQL adapters, TypeScript/Dart Load integration clients.

## Global Constraints

- The [spec](../specs/2026-09-29-load-channel-enrollment-design.md) owns the contract and eight acceptance requirements.
- Work on isolated branch `codex/load-channel-enrollment`, planning base `08e20cca`. Inspect current upstream before implementation and preserve unrelated work.
- Current backend isolation is Serializable, not the earlier Repeatable Read design. Do not import assumptions from older planning branches.
- Load receives add-only Channel handles. Mutation/external-write contexts keep full behavior; Query, Model Loader and client onStore gain no server enrollment capability.
- The Handler still returns `{data, next}`. Internal membership metadata comes from the host's collector, never from a client option or an extra application return property.
- Added records must occur in the validated page outputs and resolve through registered Loaders. No implicit enrollment merely because a record is returned.
- Membership is global to a Channel/record pair, not owned by a Load or subscriber. Cancel/forget/once reuse do not undo or repeat it.
- Same-call replay must skip settlement; only fresh calls can execute enrollment.
- Reuse shared settlement, canonical identity encoding, stamp rules, savepoint rollback and after-commit wake infrastructure.
- Bound additions to 1,000 distinct Channel/record pairs and 1 MiB encoded membership metadata per page; duplicate pairs count once. Use named constants and cross-language fixtures.
- Implement tests before the behavior they exercise, commit at each checkpoint and record actual results. This plan contains no executed test evidence.

## Preparation

- [ ] Read `AGENTS.md`, the spec, `docs/engineering/architecture.md`, `docs/engineering/guarantees.md`, `docs/engineering/architecture/server/engine/publish.md`, `docs/engineering/testing/strategy.md` and `docs/engineering/testing/running.md`.
- [ ] Check `git status --short --branch` and the implementation base. Establish the Rust baseline with `cargo test --workspace --locked`.
- [ ] For integration checks, install root dependencies and native artifacts with `npm ci` and `bash scripts/build.sh`; configure PostgreSQL and Dart using the running-tests guide. Existing runners create disposable PostgreSQL clusters. Report unavailable prerequisites rather than substituting mock evidence.

## Task 1: Define the internal add-only Load settlement contract

**Modify:** `crates/server/src/host.rs`, `packages/server/host-contract.mts`, `fixtures/protocol/host-operations.json`.
**Test:** `crates/server/tests/host_contract.rs`, `integration/persistence/server/host-contract.test.mjs`.

**Interfaces:** Extend `HandledLoad::Settled` and its TypeScript equivalent with `memberships`, using the existing `MembershipIntent` shape. Missing field decodes to an empty list; explicit null or malformed values are invalid. Handler error/rejection answers cannot carry memberships. Preserve strict rejection of changes and unknown fields.

```json
{
  "data": {"todos": [{"id": "t1"}]},
  "next": null,
  "memberships": [
    {"channel": "project:p1", "model": "Todo", "identity": {"id": "t1"}, "present": true}
  ]
}
```

This is the host/native answer, not the application's return shape or HTTP response. Decode structurally valid membership intents, then apply Load-specific add/coverage validation in Task 2 so the engine reports a page failure consistently across hosts.

- [ ] Add round-trip fixtures for the new field, an old success with no field and a successful empty list. Add negative fixtures for null, wrong types, memberships beside rejection/error, and changes/unknown fields. Retain missing/malformed continuation classification.
- [ ] Run `cargo test -p axton-server --locked --test host_contract` and confirm the new success fixture initially fails for the old contract.
- [ ] Extend the wire decoder's success arm and serialized response shape. Preserve all existing Mutation/Query host contracts. Update hosts constructing `HandledLoad::Settled` with an explicit empty list where needed; use `rg -n 'HandledLoad::Settled' crates` to find exhaustive matches.
- [ ] Update the shared host-operation fixture and TypeScript union together. Do not extend the client Load intent, output descriptor or saved response.
- [ ] Re-run Rust host-contract tests and the protocol fixture checks in the existing persistence runner. Commit the contract checkpoint.

## Task 2: Validate and settle enrollment inside the Rust Load page

**Modify:** `crates/server/src/loads.rs`; `crates/server/src/settlement.rs` only if a small internal visibility/helper change is necessary.
**Test:** `crates/server/tests/loads.rs`; update `crates/sim/src/host.rs` or existing test-host support only where the new host answer requires it.

**Interfaces:** Add Load-specific helpers in `loads.rs`:

```rust
// Names/signatures for the new internal unit; use existing imported types.
fn validate_enrollment(
    config: &Config,
    data_keys: &BTreeSet<String>,
    memberships: Vec<MembershipIntent>,
) -> Result<Vec<MembershipIntent>>;

// Existing shared settlement remains the only membership/publication algorithm:
settle_changes(config, &Changes::new(), &memberships, host).await?;
```

`data_keys` contains canonical encoded `(model, identity)` keys from validated output lists. `validate_enrollment` checks registered Models, valid Channel names, `present == true`, page coverage and bounds, returning canonical deduplicated pairs. Define `LOAD_ENROLLMENT_PAIRS = 1000` and `LOAD_ENROLLMENT_BYTES = 1024 * 1024` beside the Load validation logic; mirror them through test fixtures for the JS collector.

- [ ] Add red behavior cases: first enrollment creates membership and a position at the unchanged stamp; re-add produces no extra position; one record can join two Channels; duplicates across outputs and mixed-list declarations collapse correctly.
- [ ] Add failure cases for false/remove intent, out-of-page identity, unregistered Model, bad Channel/identity, over-limit metadata, empty page with additions and invalid continuation. Assert the page's existing terminal error classification and no partial state, not just an error string.
- [ ] Run `cargo test -p axton-server --locked --test loads`.
- [ ] Extract canonical page keys while building the existing batched resolution groups. Validate memberships before settlement. Retain batched `readStamps` and Loader resolution and complete existing response shape/size validation before enrollment. Then invoke shared `settle_changes` with an empty change map inside the page savepoint. Never derive touches from returned identities or call advanceStamp just because a row was loaded.
- [ ] Test Loader null/refusal/throw/invalid row and oversized assembled response: all additions, initialized stamps and publications roll back with the page. Test host persistence failure during settlement/saveCall: the outer transaction must fail, preserving existing retry behavior rather than saving false success.
- [ ] Test same-ID replay with no handler, Loader or membership operations; remove a member after success and prove old replay does not re-add it. A fresh call may re-add it. Test independently successful/failed items in one batch.
- [ ] Retain existing no-enrollment fixed-round-trip tests for 1,000 identities. Measure/report added host operations for enrollment without promising a constant-cost write path or introducing unrelated batching. Re-run the focused Rust tests and commit.

## Task 3: Runtime Load context and owned add-only declarations

**Modify:** `packages/server/effects.mts`, `packages/server/index.mts`.
**Test:** `integration/persistence/server/effects.test.mjs`, `integration/persistence/server/loads.test.mjs`.

**Interfaces:** Introduce `RuntimeLoadChannel` and `LoadEffectCollector` rather than exposing `RuntimeChannel` directly. `LoadContext<Tx>` gains `channel(name): RuntimeLoadChannel`. The public runtime handle has only Model `.add` writers and mixed-list `.add`; no `.remove` or `.touch` exists at runtime.

```ts
interface LoadEffectCollector {
  channel(name: string): RuntimeLoadChannel;
  memberships(): readonly MembershipIntent[];
  close(): void;
}
```

Implement `createLoadEffects(models, enums, loaded)` by sharing existing identity snapshot/validation internals. An internal full collector may be reused behind a restricted facade only if its remove/touch handles cannot escape. Deduplicate while collecting to bound storage, and retain an invalid/overflow state even if handler code catches a declaration error that would otherwise cause partial unintended enrollment.

- [ ] Add collector tests for typed scalar/composite/UUID/Date identities, snapshotting after caller mutation, duplicate pairs, mixed lists, bad/blank Channels, unregistered/device-only Models, closed handles and absent remove/touch. Test caught overflow still fails the page without partial enrollment.
- [ ] Run `node --test integration/persistence/server/effects.test.mjs` after native/dependency setup where required.
- [ ] Build one fresh collector per `handleLoad` invocation. Supply only its restricted channel facade in `ctx`. After the Handler returns, validate/encode `{data,next}` and attach owned membership metadata. Always close in `finally`, including thrown getters, invalid continuation and transaction-retry exits. The Handler cannot inject a membership list by returning an extra property; only declarations feed it.
- [ ] Preserve `isRetryableTransactionError` propagation. A fresh database retry gets a fresh collector; never carry the prior attempt's wake or membership set forward.
- [ ] Use the existing session's handling of `publish` and savepoint rollback for wake collection, and the existing `run` wrapper for notification after commit. Do not emit wakes from the collector or call `backend.publish` inside the bound transaction.
- [ ] Add runtime assertions that Query and Model Loader objects remain without channel/touch, Load has add only, and Mutation/external contexts retain full behavior. Update the existing Load context-key assertion to include `channel` without weakening its other checks.
- [ ] Run `node --test integration/persistence/server/effects.test.mjs` and `bash integration/persistence/server/run.sh`; commit the host integration checkpoint.

## Task 4: Generated backend API and compile-time boundaries

**Modify:** `crates/compiler/src/emit.rs`, `crates/compiler/src/emit_loads.rs`; inspect compiler reserved-name handling using `rg -n 'LoadContext|LoadHandlerCall' crates/compiler/src` and update its owning source for added type names.
**Test/fixtures:** `integration/load-e2e/backend.ts`, `integration/load-e2e/server.mts`, `integration/generated-api/test.ts`, `integration/generated-api/negative/misuse.ts`, `integration/action-contract/positive.ts`, `integration/action-contract/negative.ts`, plus affected generated outputs.

**Interfaces:** Generate a `LoadChannel` with existing Model identity types and RecordRef constructors. Reuse the same lower-first Model accessors. The generated Load context returns this restricted type; generated Mutation Channel remains unchanged.

```ts
export interface LoadChannel {
  todo: { add(identity: TodoIdentity): void };
  // Emit equivalent entries for the schema's other Models.
  add(records: readonly RecordRef[]): void;
}
```

The application Handler still returns its existing `NameHandlerOutput`, with no memberships property or schema changes. Device-only Models remain guarded at runtime by Loader registration, consistent with existing declaration APIs.

- [ ] Add positive compilation cases for `ctx.channel(name).todo.add(...)`, mixed-list `.add`, composite identity and retained Load versions. Add negative cases for wrong identities, `.remove` on either handle form, `ctx.touch`, Query channel and Loader channel.
- [ ] Generate the restricted type and extend only LoadContext. Keep no-Load/model-only generated projects valid; reserve additional names only where their declarations are emitted, following current compiler conventions.
- [ ] Regenerate using `bash integration/generated-api/verify.sh`, `bash integration/action-runtime-ts/verify.sh` and the compiler command in `integration/load-e2e/run.sh`. Run action-contract positives/negatives as in `scripts/test.sh`; run TypeScript checks for Load fixture. Inspect diffs and confirm business schema histories and client API signatures are unchanged.
- [ ] Commit code generation and fixtures after the compile-time boundary review.

## Task 5: Real PostgreSQL commit, wake and concurrency evidence

**Modify:** `integration/persistence/server/loads.test.mjs`, `integration/persistence/server/membership.test.mjs` only when sharing existing fixture support is necessary.
**Reference:** current Serializable adapter tests, session rollback and publication tests in `integration/persistence/server/runtime.test.mjs`.

- [ ] Across the existing pg/Prisma/Drizzle shim loop, load old domain rows and add them to a Channel. Observe `axton_call`, record stamps, memberships, Channel head and invalidations through the independent pg pool. Verify initial add, re-add and two-Channel cases.
- [ ] Hold the page transaction before commit with an explicit barrier. Assert another connection sees neither saved success nor new membership/publication. Verify wakes occur only after commit. Force saveCall/COMMIT failure and page-savepoint rejection; observe no new durable membership or wake from the failed unit. Successful batch siblings must remain committed.
- [ ] With a real live subscriber, verify Load enrollment can wake an already initialized subscription without reconnecting. Replay the page and verify no new membership/publication work; do not assert exactly-once network delivery.
- [ ] Force concurrent update/touch and enrollment with database barriers and separate transactions. Cover update-before-enrollment and enrollment-before-update snapshots. Classify committed attempts by saved claim/transaction identity, because Serializable retries can rerun handlers. Assert either a page with the new version or a later published update, with no permanently stale result and no double stamp/cursor advancement from aborted attempts.
- [ ] Test loader denial, out-of-page enrollment and malicious host remove/change input with database assertions on every affected table. Test existing membership in another Channel does not receive an unchanged enrollment publication.
- [ ] Run `bash integration/persistence/server/run.sh`. Preserve existing membership and shared-settlement regression coverage. Record actual versions/results and commit.

## Task 6: Client experience, docs and final gate

**Modify:** `integration/load-e2e/load.test.mts`, `integration/load-e2e/server.mts`, `integration/load-e2e/client.ts`, `integration/load-e2e/client.dart` as needed; generated outputs from its runner.
**Documentation:** `website/docs/frontend/loads.md`, `website/docs/frontend/sync.md`, `website/docs/backend/api.md`, `website/docs/backend/setup.md`, `docs/engineering/architecture/server/backend-interface.md`, `docs/engineering/architecture/server/engine/publish.md`, and owning Load/protocol architecture pages located through the documentation index.

- [ ] Add an end-to-end scenario: subscribe to a Channel, wait using existing status/watch until initialization is ready, Load unenrolled existing records, then mutate/touch one record on the backend without adding it again. Assert the local Model updates through ordinary Downlink and watch. Run for TypeScript and Dart using existing harnesses where practical; distinguish any platform not exercised.
- [ ] Gate/delay the Load response so a newer Channel update can arrive first; assert existing stamp rules prevent regression when the old page is stored. Verify duplicate enrollment/page delivery is harmless and explicit local-write protection remains intact.
- [ ] Verify completed once reuse performs no enrollment calls; a fresh traversal can establish missing membership. Verify cancellation after a committed page leaves its membership, and later created records need their own enrollment. Keep these tests focused on observed behavior, not internal counter mirroring.
- [ ] Update docs with the capability matrix, the add-only Handler example, page coverage restriction, shared-membership meaning, limits and atomic/replay behavior. Replace the blanket claim that Load creates no membership with the optional-enrollment contract. Explain that `await subscribe()` is local intent; gap-free usage needs the initial handshake before Load. Do not introduce a nonexistent `subscription.ready()` API.
- [ ] Run `bash integration/load-e2e/run.sh`, then the affected compiler/host gates and `bash scripts/test.sh`. Inspect fixture changes and `git diff --check`; check documentation links and schema histories.
- [ ] Self-review all eight spec acceptance requirements and the context matrix. Resolve findings before preparing the PR. Report executed tests, unexecuted environments, added membership cost and any remaining risk. Merge/publish follows execution authorization; this plan itself authorizes no implementation deployment.

## Review and handoff

| Spec requirement | Tasks |
| --- | --- |
| 1: capability surfaces | 1, 3, 4 |
| 2: page coverage, membership and stamp semantics | 2, 3, 5 |
| 3: atomicity and after-commit wakes | 2, 3, 5 |
| 4: replay versus new execution | 2, 5, 6 |
| 5: Serializable concurrency | 5 |
| 6: Load followed by touch reaches clients | 6 |
| 7: invalid inputs, lifetime, limits and isolation | 1, 2, 3, 5 |
| 8: documented boundaries, no schema/client change | 4, 6 |

Planning checks only: current source paths and runners were inspected. No implementation tests were run while writing this document. Start with the internal contract and Rust behavior; simply adding a method to LoadContext would leave enrollment uncommitted and unvalidated.
