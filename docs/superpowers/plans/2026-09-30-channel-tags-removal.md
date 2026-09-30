# Channel Tags and Synchronized Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use superpowers:subagent-driven-development only if the user authorizes delegation. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add backend selection tags and atomic bulk channel removal, with compacted per-record removal delivery and automatic client replica release.

**Architecture:** Tags remain server metadata. The server reduces ordered declarations into live membership plus one latest channel log row per record; the client merges channel evidence independently of content stamps and evicts only an unheld replicated base. Enrollment provenance and durable request epochs fence delayed bodies without requiring application cleanup hooks.

**Tech Stack:** Rust core/server/client/compiler/simulation; PostgreSQL adapter and TypeScript backend; SQLite store; Node and Dart bindings.

**Spec:** [Channel tags and synchronized removal](../specs/2026-09-30-channel-tags-removal-design.md).

## Global Constraints

- Target release: AXTON `0.2.0`; do not ship this behavior/protocol/storage change as a `0.1.x` patch.
- Capability: `channel-membership-v1`; refuse incompatible requests before handler execution or cursor progress.
- Package release versions and protocol capability checks are separate; application Model/Mutation versions remain independent.
- Tags are backend selection labels, not access grants or reference counts.
- `remove({tag: X})` removes the whole matching membership, including members carrying other tags.
- First release retention: do not prune removal log rows or local removal evidence by TTL.
- No client tags, tag wire event, separate tag cursor, log payload, or backend membership cursor.
- Preserve existing single-model identity and mixed-record array forms.
- Tag limits: 256 UTF-8 bytes per name; 64 distinct tags per add declaration.
- Channel pages retain the current limit of 50 events per channel.
- Content stamps and cursors remain safe integers, at most `9007199254740991`.
- No database reset, dropped pending work, app cleanup hook, or domain-table cleanup in this change.
- Native Load enrollment depends on [#214](https://github.com/zanminwang/axton/pull/214); inspect its final contract before implementing the Load task. Do not merge it merely to execute this plan.
- All code tasks below are proposed work. Only this spec and plan have been written in the planning session.

## Execution and file map

Work in an isolated AXTON worktree, starting from a clean/reviewed branch. The planning baseline is `08e20cca`; re-check current main and #214 before making code changes. Preserve unrelated changes. Do not deploy a partial capability.

| Owner | Files | Change |
|---|---|---|
| Wire contract | `crates/core/src/protocol.rs`, `fixtures/protocol/*`, `crates/core/tests/contracts.rs` | Channel events, claims, capability and validation |
| Effect declarations | `packages/server/effects.mts`, `packages/server/host-contract.mts`, `crates/compiler/src/emit.rs`, `crates/compiler/src/emit_loads.rs` | Typed options/selector; ordered copied intents |
| Settlement | `crates/server/src/settlement.rs`, `crates/server/src/host.rs`; new `crates/server/src/channel_members.rs` | Selection, final-state reduction, bulk host contract |
| Persistent server state | `packages/postgres/migration.sql`, `packages/postgres/src/sql.mts`, `packages/postgres/src/persistence.mts`; new `packages/postgres/migrations/2026-09-30-channel-members.sql` | Eight-table target and atomic batched persistence |
| Delivery | `crates/server/src/lib.rs`, `crates/server/src/live.rs`, `crates/server/src/loading.rs`, `crates/server/src/loads.rs`, `crates/server/src/readback.rs` | Removal scan, Loader-free tombstones, enrollment claims |
| Client bookkeeping | `crates/client/src/ddl.rs`, `crates/client/src/engine.rs`; new `crates/client/src/channel_members.rs` | Durable holds, absent evidence, evicted-base state |
| Shared client application | `crates/client/src/downlink.rs`, `crates/client/src/bootstrap.rs`, `crates/client/src/authority.rs`, `crates/client/src/store_delivery.rs` | Merge evidence then stage content/release atomically |
| Request fencing | `crates/client/src/load_ledger.rs`, `crates/client/src/load_worker.rs`, `crates/client/src/fetch.rs`, `crates/client/src/actions.rs`, `crates/client/src/queue.rs`, `crates/client/src/push.rs` | Freeze/persist store epochs; settle suppressed bodies |
| Runtime surfaces | `packages/server/index.mts`, `packages/client-js/transport.mts`, `packages/client-js/live.mts`, `packages/dart/lib/src/port.dart`, `packages/dart/lib/src/live.dart`, `bindings/common/src/lib.rs` | Admission and transport; no app tag state |
| Upgrade/simulation | `crates/client/src/subscriptions.rs`, `crates/client/src/bootstrap_ledger.rs`, `crates/sim/src/host.rs`, `crates/sim/src/step.rs`, `crates/sim/src/invariants.rs` | Reconciliation and adversarial ordering |

Register new Rust modules in the corresponding crate's `src/lib.rs`. Keep unrelated modules intact. Test paths and exact commands are listed with the task that owns them.

The type names below are proposed interfaces to implement, not existing APIs. Use existing `RecordKey`, `AuthorityRecord` and error types. Code fragments specify new branches/data shapes; integrate with the established async host dispatch and transaction helpers instead of creating another engine.

## Task 1: Freeze and test the wire contract

**Files:** Modify `crates/core/src/protocol.rs`, `crates/core/tests/contracts.rs`, `fixtures/protocol/pull-page.json`, `fixtures/protocol/bootstrap-page.json`, `fixtures/protocol/live-messages.json`; create `fixtures/protocol/channel-membership.json`.

**Interfaces:** Produce `ChannelChange`, `MembershipClaim`, and capability constant for both server and client. The upsert flattens existing authority fields; removal denies them. Keep `AuthorityRecord` itself usable for non-channel authority. Introduce new page types alongside legacy types while staging the implementation, so contract-only commits do not force unimplemented server/client paths to compile against the new union. Switch runtime selection only when the vertical capability is complete.

```rust
pub const CHANNEL_MEMBERSHIP_CAPABILITY: &str = "channel-membership-v1";

// Serde wire: tagged by kind, camelCase variants; validated after decoding.
pub enum ChannelChange {
    Upsert { channel: String, cursor: u64, record: AuthorityRecord },
    Remove { channel: String, cursor: u64, key: RecordKey },
}
pub struct MembershipClaim {
    pub channel: String,
    pub cursor: u64,
    pub model: String,
    pub identity: serde_json::Value,
}
```

- [ ] Add fixture cases for two channels sharing one identity, stamped null, Loader error, and a removal without authority fields. Add refusal cases for unknown kind, missing channel, duplicate pair, out-of-range cursor, unsafe counter, and removal with stamp/state/error. Canonical wire example:

```json
{"cursors":{"U":{"from":1,"to":3,"head":3}},"changes":[
  {"channel":"U","cursor":2,"kind":"upsert","model":"Entry","identity":{"id":"a"},"stamp":1,"state":{"text":"A"}},
  {"channel":"U","cursor":3,"kind":"remove","model":"Entry","identity":{"id":"b"}}
]}
```

- [ ] Extend fixture-driven tests using the existing `contracts.rs` harness. Run `cargo test -p axton-core --test contracts --locked`; first confirm the new accepted cases fail against the old shape.
- [ ] Implement strict union decoding/validation. Replace page-wide record uniqueness with `(channel, RecordKey)` uniqueness. Enforce event-range membership and 50 per channel; keep existing page/run envelope checks. Add claims metadata to enrollment-capable response envelopes and validate unique claim pairs tied to returned identities.
- [ ] Add `capabilities: ["channel-membership-v1"]` to new request envelopes, including live subscribe. Requests with absent/unsupported capability produce `protocol.unsupported` before business handlers; malformed capability fields produce `request.invalid`. Exclude negotiation metadata from saved-call logical request equality so an upgraded retry matches its stored request. Keep legacy saved-response decoding a separate explicit mode for Task 8.
- [ ] Re-run the core suite. Expected: old version fixtures remain testable through explicit legacy parsing where required, while new pages cannot accidentally pass the old record-only decoder. Commit the contract and fixtures together.

## Task 2: Collect typed tags and ordered selectors

**Files:** Modify `packages/server/effects.mts`, `packages/server/host-contract.mts`, `crates/compiler/src/emit.rs`, `crates/compiler/src/emit_loads.rs`; test `integration/persistence/server/effects.test.mjs` and generated fixtures under `integration/generated-api/`.

**Interfaces:** Produce the spec's `MembershipOptions` and `TagSelector` overloads. Replace unordered membership booleans with an ordered intent union consumed by Task 3:

```ts
type ChannelIntent =
  | { kind: 'add'; channel: string; record: HostRecordRef; tags: readonly string[] }
  | { kind: 'remove'; channel: string; record: HostRecordRef }
  | { kind: 'removeTag'; channel: string; tag: string };
```

- [ ] Add collector tests: omitted/empty tags; duplicate labels; invalid/oversized label; more than 64 distinct labels; mutable caller array after collection; escaped handle; invalid selector shape. For a mutation of the caller array, assert collected intent stays exactly:

```json
[{"kind":"add","channel":"U","record":{"model":"Entry","identity":{"id":"a"}},"tags":["X"]},
 {"kind":"removeTag","channel":"U","tag":"X"}]
```

- [ ] Run the focused persistence runner after prerequisites below; record the missing-overload/new-intent failures before changing the collector.
- [ ] Add overload discrimination (`Array.isArray(argument)` versus an exact `{tag}` object), copy validated tags, and preserve declaration order. Do not give a Load remove/touch handles. Generated per-model identities remain strongly typed; generated mixed arrays remain `RecordRef` unions.
- [ ] Regenerate and compile examples proving `.entry.add({id}, {tags:['X']})`, mixed `.add([...], {tags:['X']})`, and `.remove({tag:'X'})` work. Add compile-negative examples for wrong identity and Load remove.
- [ ] Run `bash integration/generated-api/verify.sh` and `bash integration/persistence/server/run.sh`. Expected: declaration snapshots are immutable and the generated API exposes only the intended overloads. Inspect generated diffs; commit this unit.

## Task 3: Reduce ordered membership changes in the server

**Files:** Modify `crates/server/src/host.rs`, `crates/server/src/settlement.rs`, `crates/server/src/lib.rs`, `packages/server/host-contract.mts`; create `crates/server/src/channel_members.rs`; update `crates/server/tests/support/mod.rs`, `crates/server/tests/membership.rs`, `crates/server/tests/host_contract.rs`, `fixtures/protocol/host-operations.json`.

**Interfaces:** Consume `ChannelIntent`. Produce a final delta used by PostgreSQL and simulation:

```text
MemberState = { key: RecordKey, tags: sorted set<string> }
MemberDelta = { channel, key: RecordKey, present: bool,
                tags: sorted set<string>, publish: bool }
MemberPosition = { channel, key: RecordKey, cursor, kind: upsert|remove }

Host operations (through existing async request/response dispatch):
lockChannels(channels sorted) -> unit
readChannelMembers(channel, explicitKeys, tags) -> MemberState[]
applyChannelMembers(deltas: MemberDelta[]) -> MemberPosition[]
```

`readChannelMembers` returns the union selected by explicit identities and indexed tags, with their complete current tag sets. `applyChannelMembers` consumes final states; it is not allowed to re-evaluate selectors or open/commit its own transaction. A removed state must have empty tags. `publish=false` permits tag-only metadata changes without a new position. An unchanged returned membership reuses its existing position for enrollment claims.

- [ ] Add pure reducer table cases with these exact initial/intents/final/event expectations:

```text
{}          ; add A/X, removeTag X       => {}          ; no event
A:{X,Y}     ; removeTag X                => {}          ; remove A
A:{X}       ; add A/Y                    => A:{X,Y}     ; no event
A:{X}       ; remove A, add A/Y          => A:{Y}       ; upsert A
{}          ; removeTag X, add A/X       => A:{X}       ; upsert A
A:{X},B:{Y} ; removeTag X, touch B        => B:{Y}       ; remove A, upsert B
```

- [ ] Run `cargo test -p axton-server --test membership --locked`; observe failures for final-state semantics that do not exist yet.
- [ ] Implement the reducer over transaction-local member maps. Track initially present pairs and remove→re-add transitions separately from final tag equality. Union touches with membership changes, yielding at most one publish per pair and one stamp advancement per touched record.
- [ ] Implement channel-before-record guard ordering for every path, including existing touch fan-out. Resolve all explicit channels plus touch recipients, lock canonically, guard record keys canonically, and validate the resolved set. If a competing membership write invalidates that set, fail with the existing retryable serialization category and retry the entire owning transaction. Do not add a second retry loop inside the adapter.
- [ ] Extend the fake host/host-operation fixtures and conformance checks to the new operations. Reject mismatched channels, duplicate results and missing metadata; preserve rollback/savepoint behavior.
- [ ] Run `cargo test -p axton-server --test membership --test host_contract --locked`. Expected: the table cases and existing one-stamp/final-membership tests pass. Commit the reducer and shared host contract.

## Task 4: Persist batched changes and migrate PostgreSQL

**Files:** Modify `packages/postgres/migration.sql`, `packages/postgres/src/sql.mts`, `packages/postgres/src/persistence.mts`; create `packages/postgres/migrations/2026-09-30-channel-members.sql`; update `packages/postgres/README.md`, `integration/persistence/server/membership.test.mjs`, `integration/persistence/server/driver-conformance.test.mjs`; create `integration/persistence/server/channel-tags.test.mjs` and include it in `integration/persistence/server/run.sh` if not discovered automatically.

**Interfaces:** Implement Task 3 host operations using the eight target tables in spec §4. Preserve the shared driver abstraction used by pg/Prisma/Drizzle. Internal bigint IDs stay bigint/string across the host boundary.

- [ ] Add real-PostgreSQL assertions for unique pair membership, same-name tags in different channels, rejected cross-channel associations, reverse lookup, whole-member removal, idempotent removal, rollback, and one compacted row per pair. Explicit database expectations after removing X from A/X/Y and B/X:

```sql
-- Scope queries to the fixture channel in the real test.
SELECT count(*) FROM axton_channel_member;      -- 0
SELECT count(*) FROM axton_channel_member_tag;  -- 0
SELECT kind, count(*) FROM axton_channel_log GROUP BY kind; -- remove, 2
```

- [ ] Run `bash integration/persistence/server/run.sh`; observe missing-table/operation failures for these new tests before adapter implementation.
- [ ] Implement fresh-install DDL and the forward upgrade separately. Add record IDs/identity, create the new tables and indexes, copy memberships, map invalidations to current presence, allocate positions for previously unlogged members, and verify old identity JSON agrees with canonical `identity_key`. Retain old tables. Make a repeated upgrade a verified no-op rather than allocating positions twice.
- [ ] Implement the association constraint trigger and immutable channel ownership; test it with direct SQL, not just trusted adapter calls. Delete associations when their member disappears; garbage-collect unused tags without deleting record/log evidence.
- [ ] Implement one range reservation per affected channel and set-based log/member/join updates. Use bound parameters/JSON arrays; chunk parameter-heavy statements within the caller's transaction. Keep `publish=false` metadata changes cursor-neutral. Guard overflow before any externally visible commit.
- [ ] Add concurrency cases with a real barrier: add versus removeTag, tag union versus empty selector, and touch versus removal. Assert the final domain/member/log result equals one serial order, and a forced retry leaves no extra committed cursor increments.
- [ ] Add rollback failure injection after log writes and before member deletion, plus a 10,000-member case. Assert zero Loader calls for removal and bounded SQL statement groups, not an unrealistically constant number of row writes.
- [ ] Run the persistence runner against every existing driver conformance surface. Expected: fresh and migrated schemas converge and retries/rollbacks preserve the complete transaction. Commit this unit.

## Task 5: Deliver removals and enrollment claims

**Files:** Modify `crates/server/src/lib.rs`, `crates/server/src/live.rs`, `crates/server/src/loading.rs`, `crates/server/src/loads.rs`, `crates/server/src/readback.rs`, `packages/postgres/src/sql.mts`, `packages/postgres/src/persistence.mts`; test `crates/server/tests/stamp.rs`, `crates/server/tests/bootstrap.rs`, `crates/server/tests/live.rs`, `crates/server/tests/loads.rs`, `integration/persistence/server/loads.test.mjs`.

**Interfaces:** Convert `MemberPosition` plus centralized identity into `ChannelChange`; return `MembershipClaim` with authority-bearing responses that enroll their returned records. Ordinary Fetch/Query create no enrollment claim.

- [ ] Add server cases for a channel containing only removals, mixed upserts/removals, compacted gaps, page boundaries beyond 50, same record in two channels, and a record moved beyond bootstrap's upper bound. Assert the removal-only page never enters the Model Loader.
- [ ] Add the enrollment retry regression: create and save a Load page claiming A at cursor 10; remove A at 11; retry the original call ID. Assert replayed claim is still 10 and no membership/log/head mutation occurs.
- [ ] Run `cargo test -p axton-server --test stamp --test bootstrap --test live --test loads --locked`; verify the new cases fail before scan/serialization changes.
- [ ] Scan the log without filtering away removals. For upserts, load current stamp/body in the same snapshot and memoize by record key. For removals, use only centralized identity. Preserve per-channel provenance instead of record-only deduplication.
- [ ] Apply this continuation algorithm to both delta and bounded bootstrap:

```text
rows = scan(channel, after < cursor <= bound, limit=50)
if a later retained row exists within bound:
    to = rows.last.cursor
else:
    to = bound
# delta bound = observed head; bootstrap bound = fixed until
```

- [ ] Integrate #214's final add-only Load settlement. Save the matching upsert claims with each idempotent page response. Extend mutation/direct readback similarly for returned records enrolled in that transaction. Bind claims to exact returned identities and requested authorized channels; do not fabricate claims for extra unpublished records.
- [ ] Run the four focused Rust tests and the persistence runner. Expected: Loader-null and Loader-error tests retain their meaning, while channel removals have no authority fields and replay never re-enrolls. Commit this unit.

## Task 6: Track client holds and release only replicated bases

**Files:** Create `crates/client/src/channel_members.rs`, `crates/sqlite/tests/channel_members.rs`; modify `crates/client/src/lib.rs`, `crates/client/src/ddl.rs`, `crates/client/src/engine.rs`, `crates/client/src/downlink.rs`, `crates/client/src/bootstrap.rs`, `crates/client/src/authority.rs`, `crates/client/src/store_delivery.rs`; update `crates/sqlite/tests/downlink.rs`, `crates/sqlite/tests/store_hooks.rs`, `crates/sqlite/tests/direct_writes.rs`.

**Interfaces:** Produce durable local member evidence and an evicted-base marker. No client tag schema. Suggested internal types:

```rust
pub struct MemberEvidence {
    pub channel: String,
    pub key: RecordKey,
    pub cursor: u64,
    pub present: bool,
}
pub enum MembershipMerge { Newer, Identical, Older }
// Engine methods, sharing its active transaction:
// merge_member(MemberEvidence) -> Result<MembershipMerge>
// held(&RecordKey) -> Result<bool>
// release_replica(&RecordKey) -> Result<()>
```

`release_replica` is a new cache operation; do not implement it by calling the current authoritative-null cascade. Extend internal record metadata to distinguish materialized authority, authoritative absence and an evicted base, retaining the stamp.

- [ ] Add SQLite scenarios: two holds then one removal; last removal; cached record with no recorded hold; remove+other-channel-upsert in one page; stale bootstrap after newer claim; equal evidence conflict; same-stamp re-add; stamped null still global; parent release leaves held child alone.
- [ ] Use these expected rows as the shared test oracle:

```text
A upsert@1 stamp7 + B upsert@4 stamp7 => row present, two holds, stamp7
A remove@2                         => row present, B hold, stamp7
B remove@5                         => replica absent, zero holds, stamp7 evicted
A upsert@3 stamp7                   => row restored, A hold, stamp7 materialized
```

- [ ] Run `cargo test -p axton-sqlite --test channel_members --locked`; confirm the first implementation gap before adding ledger code.
- [ ] Merge all evidence before deciding eviction. On an upsert whose membership evidence is older than a stored removal, do not admit its body merely because its global content stamp is high; it needs another current hold or a separately eligible fresh read. Malformed control messages roll back the whole page.
- [ ] Implement base release without mutation enqueue, stamp advancement or `onTargetDelete`. Preserve pending queue/direct-write layers and rebuild their presentation. Add tests where pending updates no longer have a base, where a direct local row shares the identity, and where accepted/rejected receipts settle after release.
- [ ] In prepared-store preflight/replay, include membership and eviction writes in savepoint rollback and session guards. Existing hooks receive normal accepted authority only; reactive queries receive final committed local changes. Test a preparation rolled back before replay leaves no member/cursor changes.
- [ ] Run `cargo test -p axton-sqlite --test channel_members --test downlink --test store_hooks --test direct_writes --locked`. Expected: same-stamp restoration is legal only for evicted content, and local work is not discarded. Commit this unit.

## Task 7: Fence all storage-producing response paths

**Files:** Modify `crates/client/src/load_ledger.rs`, `crates/client/src/load_worker.rs`, `crates/client/src/fetch.rs`, `crates/client/src/actions.rs`, `crates/client/src/queue.rs`, `crates/client/src/push.rs`, `crates/client/src/store_delivery.rs`, `crates/client/src/channel_members.rs`, `crates/client/src/ddl.rs`; test `crates/sqlite/tests/channel_members.rs`, `crates/sqlite/tests/load_worker.rs`, `crates/sqlite/tests/push.rs`, `crates/sqlite/tests/query_cache.rs`.

**Interfaces:** Add internal `StoreToken { epoch: u64 }` to the logical request/durable queued work, and `evicted_at` to record evidence. They are client-local metadata; do not ask application code or the backend to provide them.

- [ ] Add a table-driven delayed-body test for Load, stored Query/direct action, Fetch and mutation receipt. Each starts the logical request at epoch 0, applies last-hold removal at epoch 1, then delivers the old positive body. Assert no replica resurrection, but acknowledgements/Load continuation still advance correctly.
- [ ] Add restart/retry versions for durable Load pages and queued writes. Capture the request once, close/reopen SQLite, retry it and assert the token stays 0. Add a fresh Fetch at epoch 1 that is admitted when its Loader allows it.
- [ ] Run `cargo test -p axton-sqlite --test channel_members --test load_worker --test push --test query_cache --locked`; verify the old request is currently unfenced.
- [ ] Freeze tokens at logical creation. Increment the store epoch on a newly accepted removal with no remaining holds, including cached legacy/untracked records; duplicate/stale removals do not increment it. Apply this admission predicate after merging claims:

```rust
fn admit_positive_body(held: bool, request_epoch: u64, evicted_at: u64) -> bool {
    held || request_epoch >= evicted_at
}
```

- [ ] Apply content-stamp checks after that predicate, not instead of it. Never refresh a token on network retry. Keep authoritative null on its existing stamped path. Old claims remain ordered by their channel/record evidence; a genuinely newer claim can restore a hold and admit equal-stamp content.
- [ ] Keep response processing separate from body storage: settle receipts, rejection evidence and page continuation even when a body is suppressed. Suppressed content does not trigger authority hooks or stamp updates. Ensure request cache hits do not reclassify a historical response as fresh enrollment.
- [ ] Re-run the focused suites plus `cargo test -p axton-client --locked`. Expected: all stored-read entry points share one admission rule, and retry/restart cannot bypass it. Commit this unit.

## Task 8: Gate protocols and migrate existing clients safely

**Files:** Modify `packages/server/index.mts`, `crates/server/src/lib.rs`, `crates/client/src/subscriptions.rs`, `crates/client/src/bootstrap_ledger.rs`, `crates/client/src/bootstrap.rs`, `crates/client/src/ddl.rs`, `packages/client-js/transport.mts`, `packages/client-js/live.mts`, `packages/dart/lib/src/port.dart`, `packages/dart/lib/src/live.dart`, `bindings/common/src/lib.rs`; test `integration/persistence/server/runtime.test.mjs`, `crates/sqlite/tests/ddl.rs`, `crates/sqlite/tests/bootstrap.rs`, `packages/dart/test/admission_test.dart`; create `crates/sqlite/tests/channel_upgrade.rs`.

**Interfaces:** Consume capability/claims and the local epoch ledger. Produce a per-subscription reconciliation state using the existing bootstrap machinery with its own run/progress, without resetting the ordinary delivery cursor.

- [ ] Add old-client tests: missing capability on pull, live, Load and mutation routes must be refused before handler execution or cursor progress. HTTP uses status 426 and body code `protocol.unsupported`; live rejects the subscribe operation before its subscribed acknowledgement. Native entry points return the same semantic error.
- [ ] Create a legacy SQLite fixture with a delivered record, a queued mutation, existing subscription cursor, and a saved Load page. Upgrade it, deliver a tombstone and replay the saved call. Assert the queue/call identities survive and the old positive body cannot restore the replica.
- [ ] Run the affected tests and observe the missing-gate/migration failures.
- [ ] Make new SDK-generated requests advertise the capability automatically. Enforce the framework check on all transports while preserving application authentication/admission. Do not treat a negotiated event as an ignorable extension.
- [ ] Add transactional local migrations: empty holding ledger, eviction evidence, epoch zero for historical queued work. Schedule reconciliation from 0 to a fixed head for each retained subscription; retain ordinary delta progress and merge both lanes by pair cursor. Complete only after the delta lane reaches the historical walk's observed barrier. Resume reconciliation after crashes.
- [ ] Decode legacy saved responses explicitly, with no fabricated claims and no new enrollment execution. Test both server cutover replay and native Load's once/cache behavior, including the same saved call retried with the new transport capability but unchanged logical arguments. On unsubscribe, keep existing data semantics; on resume, reconcile retained evidence before announcing caught up.
- [ ] Run `cargo test -p axton-sqlite --test channel_upgrade --test ddl --test bootstrap --locked`, the persistence runner, and Dart admission tests using the documented native-library setup. Expected: no wipe, unsupported clients blocked, old saved work settles safely. Commit this unit.

## Task 9: Exercise the complete system under reordered delivery

**Files:** Modify `crates/sim/src/host.rs`, `crates/sim/src/step.rs`, `crates/sim/src/invariants.rs`, `crates/sim/src/sim.rs`, `crates/sim/tests/distribution.rs`, `crates/sim/tests/resilience.rs`, `crates/sim/tests/upgrade.rs`; create `crates/sim/tests/channel_tags.rs`; extend `integration/load-e2e/run.sh` and its existing clients for one enrollment/removal scenario.

**Interfaces:** Extend simulation actions with ordered tag operations and delayed enrolled responses; use production protocol and reducer, not a second implementation of their semantics. Invariants distinguish channel release from authoritative deletion.

- [ ] Add deterministic traces for the spec's final acceptance table. Include this compaction regression:

```text
device stores A from U at cursor 100
device disconnects
server tags existing A with X, removes X, re-adds A/Y
server log retains only A's final upsert
device reconnects and converges without any tag state
server removes Y; device receives one identity-only removal and evicts A
```

- [ ] Add randomized histories mixing tag unions, single removals, removeTag, touches, delayed Loads, offline clients, retries, restart and pending writes. Compare final presence to the final server membership for caught-up clients, subject to explicit local work/fresh unheld reads. A stamped null must still dominate older content.
- [ ] Run `cargo test -p axton-sim --test channel_tags --locked`; record any invariant failure before changing simulation assumptions. Update the old “records always remain after removal” expectation only where the new capability intentionally changes it; preserve unsubscribe expectations.
- [ ] Add a JS/Dart end-to-end case: enroll through native Load, receive a live removal, restart offline, and verify it stays absent with no application cleanup hook. Also assert a second channel hold prevents eviction.
- [ ] Run `cargo test -p axton-sim --locked`, `bash integration/load-e2e/run.sh`, and `bash integration/action-e2e/run.sh`. Expected: identical semantics across native and binding surfaces, with reproducible seeds recorded for failures. Commit the evidence and necessary harness changes.

## Task 10: Document, measure and verify the release boundary

**Files:** Update `docs/engineering/guarantees.md`, `docs/engineering/architecture/server/persistence.md`, `docs/engineering/architecture/protocol/pull.md`, `docs/engineering/architecture/protocol/common.md`, `docs/engineering/architecture/protocol/subscriptions.md`, `docs/engineering/architecture/client/storage/reconciliation.md`, `docs/engineering/architecture/sdks/typed-api/server.md`, `website/docs/backend/api.md`, `website/docs/backend/database.md`, `website/docs/frontend/sync.md`, `website/docs/frontend/loads.md`, `packages/postgres/README.md`.

**Interfaces:** User-facing documentation must describe the implemented capability and migration, with exact API examples from generated tests. The spec remains the decision record; living docs own shipped behavior.

- [ ] Prepare the `0.2.0` release notes and compatibility matrix: backend, PostgreSQL adapter, generated tooling, JS/Dart runtimes, server migration and client migration must form one verified release. Check the actual release packaging/version mechanism on the implementation branch; validate produced package versions and dependency references rather than assuming private workspace manifest versions are published versions. Do not publish in this task.
- [ ] Replace the old channel-removal guarantee with the distinction between synchronized release, authoritative null and unsubscribe. Show the exact three API forms and make X/Y whole-member selection explicit. Document the absence of automatic cleanup for un-enrolled one-shot reads.
- [ ] Document protocol gating, server cutover order, retained old tables, local reconciliation, legacy saved-response handling and no TTL pruning. State that custom persistence hosts must implement the new operations and channel/tag constraints before opting in.
- [ ] Include an Oasis adoption handoff, without editing Oasis: its inspected pin is `0.1.1`; the target is exactly `0.2.0` for backend/mobile dependencies and contract tooling. Sequence it as verified AXTON release → prepare compatible mobile release and matching dependency/lock updates → coordinate backend migration/cutover with per-platform minimum-build and capability gates → enable synchronized removal. The adoption task chooses actual build floors and runs Oasis's pin/contract checks; a package version bump is not a substitute for those gates.
- [ ] Measure PostgreSQL removal of 1, 1,000 and 10,000 members: transaction duration, statement count, rows/WAL if available, response identity bytes and Loader count. Record the machine, PostgreSQL version and fixture size; report measurements, not universal latency guarantees. Run `cargo run -p axton-sim --example capacity --release` separately as a diagnostic, not a correctness gate.
- [ ] Run `cargo test --workspace --locked`, `bash integration/generated-api/verify.sh`, then `bash scripts/test.sh` once after focused suites pass. Expected: Rust, bindings, persistence, generated APIs, end-to-end and documentation examples all pass. Use CI's macOS and Linux jobs for the platform gate; do not claim both from one local run.
- [ ] Inspect `git diff --check`, generated fixtures and the affected documentation links. Verify no domain tables, tag wire messages or automatic log-pruning jobs slipped into the patch. Commit documentation and prepare a review describing intentional wire/host compatibility changes and measured limits.
- [ ] Stop at a reviewable implementation. Deployment and Oasis adoption are separate authorized work; do not silently enable removal against old application clients.

## Prerequisites and commands

Commands run from the AXTON repository root. Follow [test setup](../../engineering/testing/running.md) for current toolchain/native library requirements. For the first JS/Dart integration task:

```sh
npm ci
bash scripts/build.sh
```

The persistence/end-to-end runners create their own temporary PostgreSQL clusters. `integration/generated-api/verify.sh` regenerates checked-in files; inspect them. Set the documented `AXTON_LIBRARY` and `AXTON_DART_LIBRARY` paths before standalone Dart tests. Do not run synthetics against deployed Oasis or change its pinned AXTON version as part of this plan.

## Coverage and handoff checklist

| Spec requirement | Implementation/evidence owner |
|---|---|
| Existing API plus tags; ordered whole-member selection | Tasks 2–3 |
| Eight-table schema, reverse indexes, constraints, atomic batches | Task 4 |
| One latest pair state, no Loader for removal, bounded pages | Tasks 1, 5 |
| Enrollment claims; no retry re-enrollment | Tasks 5, 7–8 |
| Last-hold release, same-stamp restoration, no destructive cascade | Task 6 |
| Pending/direct local work and delayed response safety | Tasks 6–7 |
| Capability gate, saved calls, forward server/client upgrade | Tasks 4, 8 |
| Offline/reordered cross-language convergence | Task 9 |
| `0.2.0` release boundary, Oasis adoption handoff, retention limits and performance evidence | Task 10 |

At handoff, report the implementation commit, exact commands run/results, protocol/cutover status and any failure that remains. A feature is not complete if tags work in SQL but stale responses or old clients can undo removal. No code has been implemented by this planning document.
