# Stream Tracking and Invalidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved `track`/`invalidate` declaration API, combined bulk settlement, and coordinated Stream vocabulary migration without losing authority or durable work.

**Architecture:** Keep synchronous declarations inside the existing settlement boundary; normalize tracking pairs and global/selected invalidations before host effects. Rust owns reduction, validation and retries; PostgreSQL owns set-based persistence; JS captures operands and the compiler exposes typed namespaces. Migrate delivery metadata and wire vocabulary together while retaining identity-only removal evidence and the client holding ledger.

**Tech Stack:** Rust workspace, generated TypeScript/Dart, Node SDK and PostgreSQL adapters (`pg`, Prisma, Drizzle), SQLite client storage.

## Global Constraints

- Requirements: [approved specification](../specs/2026-10-01-stream-tracking-invalidation-design.md); baseline `66232e525648188e8a8986870ea7a272741029a9`, branch `codex/stream-tracking-api`, existing worktree `/Users/stevewang/Github/axton/.worktrees/scope-membership-api-design`.
- This document is planning only. Do not implement, execute tests, commit, merge or publish while drafting it. The checkboxes below belong to a later authorized implementation.
- Implement two verbs, `track` and `invalidate`; retire tags, selectors, `add`/`touch`, and public withdrawal. No `batch`, `combine`, terminal execute call, extra network request or Loader cache.
- Names remain nonblank, opaque and case-sensitive. Empty name/record arrays are no-ops. Copy and canonicalize before appending any declaration; escaped handles throw. Ordinary failed declarations append nothing; Load collectors retain their existing sticky failure.
- Multi-stream/multi-record calls mean Cartesian products. Declarations combine in one existing settlement; separate `backend.publish(tx, callback)` invocations remain separate settlements and return their existing after-commit wake.
- `ctx.invalidate` and all inferred changed Mutation inputs are global. Targeted invalidations union their stream names; global dominates. Every invalidated identity advances its authority stamp once, even with no holders; explicitly empty selected names declare nothing.
- First tracking creates one pair/position and ensures a stamp; existing tracking is idempotent. Tracking another stream never changes authority. Targeting never enrolls. Absence never deletes server tracking automatically.
- Native Load exposes tracking only for records returned by its page; limits remain `1,000` distinct pairs and `1,048,576` encoded bytes after Cartesian expansion. Queries and viewer Loaders expose neither declaration.
- Keep the existing `ids[]` Loader, Model/version grouping, deduplication, error fallback and exact saved-call replay. Errors are not absence. Preserve stamps, cursors, fences, pending layers and local companions.
- Use `stream-membership-v1`; reject old runtime negotiation before state changes. Rename delivery-specific Scope identifiers, fields and persisted metadata; retain transaction/savepoint `scope` tokens, business Models/fields/JSON, opaque names and dated historical documents.
- Preserve heads, catalog IDs, tracking pairs, log positions including removals, receipts, calls, SQLite paths, subscriptions/cursors, holdings, frozen Loads/continuations, queued/pending/rejected work and local Models. Rewrite only top-level saved framework `memberships[*].scope` claims, never business payloads.
- Use bounded set-based SQL with the existing `1,000` item batch size. Statement/host round-trip counts scale with chunks; row, lock, WAL and log work still scales with affected records/pairs. Preserve canonical UTF-8 key order across chunks and mixed guard modes, no-op write fencing and whole-transaction retry. Do not equate output sorting with lock acquisition.
- Standing-authority absence plus local onStore cleanup is an application pattern, not automatic traversal and not a Most Days implementation. Query eligibility follows current standing and independent paths; the hook reclaims cache. Direct `tx.models.delete` creates no stream-withdrawal/request-epoch fence: delayed newer-stamp Load/fetch or saved replay can rematerialize children. Shared content changes must invalidate all holders; changed children still require their own invalidation, while mere child cache release does not.
- Do not modify Most Days, change application release floors, select a registry version, overwrite `0.2.0`, merge or publish. Retention policy and cross-process wakes remain separate work.

## Source map and execution ownership

Current settlement is `crates/server/src/settlement.rs`; it loops per identity through `memberships`, `advanceStamp`/`ensureStamp`/`lockRecord`, then reads members per Scope. `packages/postgres/src/persistence.mts` already batches final head/log/member writes through `SCOPE_BATCH = 1000` in `sql.mts`; JSON statements bind at most three parameters, so that bound controls payloads/rows rather than the PostgreSQL 65,535 parameter limit. Existing record locks are no-op **writes**, including at Repeatable Read. The current `LOCK_SCOPES` orders a locking subquery before its no-op update; `RESERVE_HEADS` orders its INSERT source. Keep those properties when renaming.

Execute Task 1 serially under the integration owner. Its contract and settlement phases define host interfaces and update shared Rust/simulation support as one compile-coherent review gate; do not assign competing edits of them. Then Tasks 2, 3 and 4 have feasible ownership: compiler/server JS, PostgreSQL, and client storage/SDK respectively. Task 2 owns all compiler output, including generated Dart subscription names; Task 4 owns SDK implementations, not the emitter. Task 3 owns persistence test SQL fixtures and runner updates; Task 5 owns assembled fixture regeneration and cross-boundary examples. Agree shared-file edits before dispatch, and integrate/review each branch before final gates; these boundaries do not promise conflict-free parallel edits. No parallel implementation is required during planning.

New source files are explicitly designated below; every other path already exists. Rename operations preserve history rather than copying implementations.

## Task 1: Establish the Stream contract and combined Rust settlement

**Files**
- Modify: `crates/core/src/protocol.rs`, `crates/core/src/lib.rs`, `crates/core/src/actions.rs`, `crates/core/src/loads.rs`, `crates/core/tests/contracts.rs`, `crates/core/tests/compatibility.rs`.
- Modify: `crates/server/src/host.rs`, `packages/server/host-contract.mts`, `crates/server/tests/host_contract.rs`, `crates/server/tests/protocol_admission.rs`.
- Modify consumers required to compile the contract: `crates/server/src/{lib,actions,calls,loads,loading,live}.rs`, `crates/client/src/{downlink,connection,live,store_delivery,bootstrap_ledger,load_ledger}.rs`, `crates/client/src/runtime/protocol.rs`, `crates/sim/src/host.rs`, `crates/server/tests/support/mod.rs` and `crates/server/tests/capability/mod.rs`. Brace notation enumerates existing files, not a new directory.
- Modify: `fixtures/protocol/load-enrollment-limits.json` only if its encoded declaration fixture contains the retired shape; numeric limits stay unchanged.

**Interfaces**
- Consumes: existing `RecordRef { model: String, identity: Value }`, canonical `RecordKey` encoding, counters and typed `HostRequest` response validation.
- Produces: the following TS/Rust-equivalent host contract. These are internal representation choices for the approved API, not additional application verbs:

```ts
type MemberKey = { model: string; identityKey: string };
type TrackingPair = MemberKey & { stream: string };
type ReadTrackingRequest = { op: "readTracking"; records: MemberKey[]; pairs: TrackingPair[] };
type ReadTrackingResponse = TrackingPair[];
type GuardRecordsRequest = {
  op: "guardRecords";
  records: (MemberKey & { mode: "advance" | "ensure" | "lock" })[];
};
type GuardRecordsResponse = (number | null)[];
type TrackIntent = { kind: "track"; stream: string; record: HostRecordRef };
type StreamIntent = TrackIntent |
  { kind: "invalidate"; streams: string[] | null; record: HostRecordRef };
// `changes` remains inferred/legacy global invalidation; explicit calls use declarations.
type SettlementEffects = { changes: HostRecordRef[]; declarations: StreamIntent[] };
type TrackingDelta = TrackingPair & { identity: object; publish: boolean };
type ApplyStreamMembersRequest = { op: "applyStreamMembers"; deltas: TrackingDelta[] };
type MemberPosition = TrackingPair & { cursor: number; kind: "upsert" | "remove" };
// Existing framework compatibility guards may retain their scalar operations.
// Fresh final-pair application only creates/upserts; persisted removals still decode.
```

`lockStreams` consumes strictly ordered unique names. `readTracking` returns the union of all holders of `records` and existing `pairs`; no duplicates or unrelated pairs. Validate response types, allowed pair membership and uniqueness; an all-holder response is trusted for completeness, and the engine cannot detect a valid holder the host omitted. Adapter tests establish completeness against actual persisted rows. `guardRecords` is request-aligned: ensure/advance positive safe stamps, lock positive safe stamp or null. Claims retain the `memberships` envelope field but rename each claim's `scope` to `stream`. Handler and external result envelopes carry `declarations`; a successful Load carries only optional `tracking: TrackIntent[]`. Rejections/failures carry neither. The settlement phase below consumes these exact shapes; Tasks 2/3 implement their JS and PostgreSQL boundaries.

- [ ] Add failing contract cases in the existing tests: decode the two requests above; reject extra fields, duplicate/unsorted guards, unknown modes, unrelated/duplicate tracking responses, wrong guard cardinality, zero/unsafe stamps and null ensure/advance. Retain removal decode using this exact fresh wire fixture:

```json
{"cursors":{"User:alice":{"from":3,"to":4,"head":4}},"changes":[{"kind":"remove","stream":"User:alice","cursor":4,"model":"Todo","identity":{"id":"t"}}]}
```

Assert `scope-membership-v1` and absent capability reject before handler/store/progress calls; `stream-membership-v1` succeeds. Keep business identity `{ "scope": "unchanged" }` byte-identical and retain the bridge savepoint fixture's outer `scope:"sp1"`.
- [ ] Run `cargo test -p axton-core --test contracts --test compatibility` and `cargo test -p axton-server --test host_contract --test protocol_admission`; expect failures for missing new variants/old capability before implementation.
- [ ] Implement typed requests/results and their exact-field validation; add host-operation response mappings in Rust and TS. Rename delivery protocol types/fields/capability and required Rust consumers together; inspect each `scope` occurrence by responsibility. Do not alter Model schema versions or arbitrary business values. Adapt support hosts to bulk semantics so subsequent core tests can run.

```rust
// Deserialization shape; attach the existing exact-field/counter validators.
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GuardRecord { model: String, identity_key: String, mode: GuardMode }
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum GuardMode { Advance, Ensure, Lock }
// Bulk result validator checks len == request.records.len(), then mode-specific bounds.
```

- [ ] Rerun the focused commands; expect all cases pass. Run `cargo check --workspace --locked` to expose rename consumers at the unified Task 1 review gate. Review this contract as a phase checkpoint within Task 1; update mock implementations without weakening assertions. Do not commit or hand off an intermediate contract that breaks current settlement, and do not silently discard selected declarations. Complete the settlement phase below before the unified compile/test/commit gate.

### Task 1, settlement phase: Coalesce declarations and settle final tracking in Rust

**Files**
- Modify: `crates/server/src/settlement.rs`, `crates/server/src/actions.rs`, `crates/server/src/lib.rs`, `crates/server/src/loads.rs`, `crates/server/src/readback.rs`.
- Rename: `crates/server/src/scope_members.rs` → `crates/server/src/stream_members.rs`; remove `crates/server/src/scope_predicate.rs` after removing its exports/uses.
- Modify: `crates/server/tests/membership.rs`, `crates/server/tests/loads.rs`, `crates/server/tests/readback.rs`, `crates/server/tests/support/mod.rs`, `crates/sim/src/host.rs`, `crates/sim/tests/distribution.rs`.
- Replace retired tag scenarios in `crates/sim/tests/scope_tags.rs` by renaming it to `crates/sim/tests/stream_tracking.rs`; it tests tracking/invalidation, not tag compatibility.

**Interfaces**
- Consumes Task 1's `StreamIntent`, `readTracking`, `guardRecords`, `lockStreams`, `applyStreamMembers` and unchanged `Changes = BTreeMap<String, RecordKey>` for inferred global changes.
- Produces the existing settlement result (`stamps` and `positions`) with Stream positions and claims, using `settle_changes(config, changed, declarations, host)` and existing `settle_locked(..., held, host)` entry points. `changed` means global invalidation even if a targeted declaration names that key. Loads pass only tracks and hold declared stream locks before read stamps as they do today.

- [ ] Add a regression in `membership.rs` using its `Backend`, `record`, `reference`, `push`, `edit`, `authority` and `publishes` helpers. Seed Todo/t at 7, enroll A/t and B/t, then exercise the external settlement path without a Mutation input (the input would itself be global): targeted A invalidation produces stamp 8 and one A position; B head stays fixed; targeting C advances to 9 without enrolling C. A Mutation editing t with targeted A still publishes to both A/B at one newer stamp.

```rust
// External script envelope consumed by the existing publication test harness.
let declarations = json!([
  {"kind":"invalidate","streams":["A"],"record":reference("Todo","t")},
  {"kind":"track","stream":"C","record":reference("Todo","t")},
  {"kind":"invalidate","streams":["C","A"],"record":reference("Todo","t")}
]);
// Assert one guard with Advance for t, final A/C each once, existing B untouched.
// Repeat with declarations reversed: same stamps, heads and pair set.
```

Also assert repeated track produces no cursor/stamp, new track inherits stamp, global plus selected gives all final pairs, an unheld invalidation still advances, and explicit empty selection gives no guard. Multi-Model keys deduplicate canonically. A malformed host response aborts and rolls back; global re-read discovering a new unlocked holder raises `transaction.conflict`, never a late lock. Validate only declared tracking pairs get returned membership claims.
- [ ] Run `cargo test -p axton-server --test membership --test loads --test readback` and `cargo test -p axton-sim --test stream_tracking --test distribution`; expect new behavioral assertions fail first.
- [ ] Replace ordered tag reduction with a pure normalized plan. Use canonical maps keyed by `RecordKey::encoded()`; normalize explicit tracks as a pair set and invalidations as `Global | Selected(BTreeSet<String>)`. Merge selected sets and let global absorb selected. Include inferred `Changes` as Global before choosing guards.

```text
before = readTracking(all globally invalidated keys, explicit track/selected pairs)
locks = names(before) union names(explicit tracks) union selected names
lockStreams(sort_utf8(locks))
guards = guardRecords(sort_canonical(each affected key -> advance else ensure else lock))
after = readTracking(same request)
if names(global holders after) - locks is nonempty: transaction.conflict
final = after union explicit tracks
publish(pair) = newly_tracked(pair) OR invalidation_selects(pair)
applyStreamMembers(final affected pairs, each once, retaining unpublished existing positions)
```

Use a constant number of bulk host requests independent of identities/streams; the adapter chunks their payloads. Do not loop a host call per record or locked stream. Validate all pair keys and returned positions, including consecutive per-stream published cursors and retained unchanged positions. Retain compatibility lock-only paths and exact replay bypass.
- [ ] Keep `readback.rs` batching by Model/version and per-identity fallback. Add a viewer A row→null test at the new stamp with B unnotified; add an error test that produces diagnostics without substituting null. Load tests cover 1,000 distinct pairs, 1,048,576 bytes, page subset enforcement, expansion overflow and rollback; replay runs neither Loader nor settlement.
- [ ] Rerun focused suites; expect pass. Run `cargo check --workspace --locked` and review contract, normalizer, request count and transaction retry behavior together before Tasks 2–4 consume the interface. This is the first independently reviewable implementation deliverable; there is no broken intermediate contract commit.

- [ ] After focused passing checks and independent review, inspect `git diff --check` and `git diff --stat`, then stage only this task’s owned paths and commit. Review `git diff --cached --stat` before the commit; exclude unrelated or another owner’s changes. This is a later implementation instruction, not authorization to commit while drafting.

```sh
git add -A -- \
  crates/core/src/protocol.rs \
  crates/core/src/lib.rs \
  crates/core/src/actions.rs \
  crates/core/src/loads.rs \
  crates/core/tests/contracts.rs \
  crates/core/tests/compatibility.rs \
  crates/server/src/host.rs \
  packages/server/host-contract.mts \
  crates/server/tests/host_contract.rs \
  crates/server/tests/protocol_admission.rs \
  crates/server/src/lib.rs \
  crates/server/src/actions.rs \
  crates/server/src/calls.rs \
  crates/server/src/loads.rs \
  crates/server/src/loading.rs \
  crates/server/src/live.rs \
  crates/client/src/downlink.rs \
  crates/client/src/connection.rs \
  crates/client/src/live.rs \
  crates/client/src/store_delivery.rs \
  crates/client/src/bootstrap_ledger.rs \
  crates/client/src/load_ledger.rs \
  crates/client/src/runtime/protocol.rs \
  crates/sim/src/host.rs \
  crates/server/tests/support/mod.rs \
  crates/server/tests/capability/mod.rs \
  fixtures/protocol/load-enrollment-limits.json \
  crates/server/src/settlement.rs \
  crates/server/src/readback.rs \
  crates/server/src/scope_members.rs \
  crates/server/src/stream_members.rs \
  crates/server/src/scope_predicate.rs \
  crates/server/tests/membership.rs \
  crates/server/tests/loads.rs \
  crates/server/tests/readback.rs \
  crates/sim/tests/distribution.rs \
  crates/sim/tests/scope_tags.rs \
  crates/sim/tests/stream_tracking.rs
git diff --cached --stat
git commit -m "feat: settle Stream tracking and invalidation in bulk"
```

## Task 2: Capture the two verbs and emit typed TS/Dart APIs

**Files**
- Rename: `packages/server/scope.mts` → `packages/server/stream.mts`; `crates/compiler/src/emit_scope.rs` → `crates/compiler/src/emit_stream.rs`.
- Modify: `packages/server/effects.mts`, `packages/server/index.mts`, `crates/compiler/src/lib.rs`, `crates/compiler/src/emit.rs`, `crates/compiler/src/emit_loads.rs`, `crates/compiler/src/emit_transactions.rs`.
- Modify tests: `integration/persistence/server/effects.test.mjs`, `integration/action-contract/positive.ts`, `integration/action-contract/negative.ts`, `integration/generated-api/backend.ts`, `integration/generated-api/client.ts`, `crates/compiler/tests/compiler.rs`, `crates/compiler/tests/loads.rs`.

**Interfaces**
- Consumes Task 1 `SettlementEffects`, `StreamIntent`, `TrackIntent` and Task 1 settlement behavior.
- Produces `RuntimeStream { track: Namespace<void>; invalidate: Namespace<void> }`, `RuntimeLoadStream { track: Namespace<void> }`, `RuntimeInvalidate = Namespace<void>`, and collectors `stream(nameOrNames: string | readonly string[])`, `invalidate`, `settlement()`, `close()`. Load collector exposes `stream`, `tracking(): readonly TrackIntent[]`, `failure()`, `close()`.
- Generated types: `RecordDeclaration` callable namespace; `Stream { readonly track: RecordDeclaration; readonly invalidate: RecordDeclaration }`; `LoadStream { readonly track: RecordDeclaration }`; `stream(names: string | readonly string[]): Stream/LoadStream`; global `invalidate: RecordDeclaration` on Mutation, legacy and external contexts only. Preserve Model constructor identity unions. Generated JS/Dart clients and transactions expose `streams.subscribe/unsubscribe`; Task 4 supplies runtime methods `subscribeStream` and transaction `streams`.

- [ ] Replace collector expectations with executable behavioral tests:

```js
const effects = fresh();
const names = ['A', 'B', 'A'];
const ids = [{id:'t'}, {id:'u'}];
const handle = effects.stream(names);
handle.track.todo(ids);
handle.invalidate.todo(['t', 't']);
ids[0].id = 'changed'; names[0] = 'changed';
assert.equal(effects.settlement().declarations.filter(d=>d.kind==='track').length,4);
assert.deepEqual(new Set(effects.settlement().declarations.filter(d=>d.kind==='track').map(d=>d.stream)),new Set(['A','B']));
effects.close();
assert.throws(()=>handle.track.todo('late'),/closed/);
```

Add scalar/complete composite/Date/UUID canonicalization and mixed references, readonly arrays, empty no-ops, nonblank names and Loader-less refusal. A failed ordinary mixed call leaves all prior valid declarations and adds no prefix; a caught invalid/overflow Load still fails the whole page. Test two streams × 501 records overflow, duplicates collapsing below 1,000, and byte bounds with the new encoded TrackIntent.
- [ ] Run `node --test integration/persistence/server/effects.test.mjs`; expect missing `.stream` failure. Add generated positive calls and `@ts-expect-error` negatives:

```ts
ctx.stream(['A','B']).track.todo(['t', {id:'u'}]);
ctx.invalidate([Todo({id:'t'}), Moment({at:new Date()})]);
ctx.stream('A').invalidate.pin({todo:'t',at:new Date()});
// @ts-expect-error composite identity needs every component
ctx.invalidate.pin({todo:'t'});
// @ts-expect-error retired public withdrawal
ctx.stream('A').remove.todo('t');
// @ts-expect-error retired tag API
ctx.stream('A').track.todo('t').tag('x');
```

Use these fixture Models that already exist rather than adding Most Days Models. Query and Load type negatives reject global/selected invalidation; Loader-less runtime config rejects declaration. Generate with `cargo run -p axton-compiler --locked -- compile integration/action-contract integration/action-contract --backend-runtime ../../packages/server/index.mts --client-runtime ../../packages/client-js/index.mts`, then `node_modules/.bin/tsc -p integration/action-contract`; expect negatives fail until the emitter exposes the approved shape.
- [ ] Implement namespaces using existing identity snapshot/canonical code. Resolve and freeze the entire name/record operand list before appending effects; Cartesian-expand tracks; append per-record selected invalidation using captured unique names. Empty handle operations append nothing, but still check handle lifetime. Keep the ordinary `guard(body) => body()` behavior and Load failure guard distinct.
- [ ] Route `stream/invalidate` through all existing handler, legacy transaction and external publish contexts; Load gets only tracking. Dispatch the new bulk operations and keep the existing session after-commit wake accumulation from published deltas. One callback settles once; saved outcomes replay unchanged.
- [ ] Emit model-specific operands as `Identity | Identity[singleField]` only for single-field Models and `readonly Operand[]` for every accessor. Mixed calls accept `RecordRef | readonly RecordRef[]`. Rename generated `Scopes` to `Streams` and delegate to `client.subscribeStream`; no DSL/schema syntax change. Remove tag emitter/runtime code and exports.
- [ ] Run `cargo test -p axton-compiler --test compiler --test loads`, collector tests, `npm run typecheck` and `bash integration/generated-api/verify.sh` after Task 4 runtime methods are integrated. Review generated diff; final shared-fixture regeneration belongs to Task 5.

- [ ] After focused passing checks and independent review, inspect `git diff --check` and `git diff --stat`, then stage only this task’s owned paths and commit. Review `git diff --cached --stat` before the commit; exclude unrelated or another owner’s changes. This is a later implementation instruction, not authorization to commit while drafting.

```sh
git add -A -- \
  packages/server/scope.mts \
  packages/server/stream.mts \
  crates/compiler/src/emit_scope.rs \
  crates/compiler/src/emit_stream.rs \
  packages/server/effects.mts \
  packages/server/index.mts \
  crates/compiler/src/lib.rs \
  crates/compiler/src/emit.rs \
  crates/compiler/src/emit_loads.rs \
  crates/compiler/src/emit_transactions.rs \
  integration/persistence/server/effects.test.mjs \
  integration/action-contract/positive.ts \
  integration/action-contract/negative.ts \
  integration/generated-api/backend.ts \
  integration/generated-api/client.ts \
  crates/compiler/tests/compiler.rs \
  crates/compiler/tests/loads.rs
git diff --cached --stat
git commit -m "feat: expose typed Stream declaration APIs"
```

## Task 3: Implement set-based PostgreSQL settlement and forward cutover

**Files**
- Modify: `packages/postgres/src/sql.mts`, `packages/postgres/src/persistence.mts`, `packages/postgres/migration.sql`, `packages/postgres/src/statements.mts` as needed for exported statement references.
- Create: `packages/postgres/migrations/2026-10-01-streams.sql` (new forward migration).
- Modify: `integration/persistence/server/host-contract.test.mjs`, `integration/persistence/server/membership.test.mjs`, `integration/persistence/server/loads.test.mjs`, `integration/persistence/server/driver-conformance.test.mjs`, `integration/persistence/server/run.sh`.
- Rename: `integration/persistence/server/scope-tags.test.mjs` → `integration/persistence/server/stream-tracking.test.mjs`; preserve migration fixtures `integration/persistence/server/fixtures/v02-framework.sql`, `integration/persistence/server/fixtures/postgres-state.sql`, `integration/persistence/server/fixtures/envelopes.json` as inputs to upgrade checks.

**Interfaces**
- Consumes Task 1 exact bulk host requests and Task 1 canonical request ordering. Implements `readTracking(q, request): Promise<TrackingPair[]>`, `guardRecords(q, request): Promise<(number|null)[]>`, `applyStreamMembers(q, request): Promise<MemberPosition[]>`, plus renamed stream head/scan/lock operations.
- Produces `axton_stream`, `axton_stream_member`, `axton_stream_log` and framework `stream` columns, preserving unique pair identities/log kinds; retains `axton_record` catalog and transactional calls/receipts. `STREAM_BATCH = 1000` replaces the constant's delivery name, not its size.

- [ ] Add real-database tests using the existing `Pool`, `driver`, `plans`, `backend.transaction`, latches and adapter instrumentation. Use two bounded cases: (a) 2,001 mixed Model records × 3 stream names (6,003 pairs), and (b) 2 mixed Model records × 1,001 stream names (2,002 pairs). Assert guard/pair/head statement counts independently scale with their respective chunks and host operation counts remain constant, rather than one SELECT/write per identity or stream. Count each operation separately; apply statement groups can legitimately have different chunk counts. Do not construct a 2,001 × 1,001 Cartesian fixture.
- [ ] Seed mixed guards in canonical order: absent-low ensure, existing-middle lock, absent-middle advance, existing-high advance; assert `[1,old,1,old+1]` and absent lock returns null without catalog creation. Race reversed caller operand order with more than 1,000 mixed keys crossing chunk boundaries, two first creates, and stale Repeatable Read enrollment/invalidation; require successful whole-transaction retries rather than silent old authority. Use latches to fix snapshots, and PostgreSQL lock observation/blocked writer outcomes to establish actual acquisition order. Inject failure in chunk 2 and assert all business rows/heads/stamps/pairs/logs/calls roll back and no wake occurs. Assert targeted row→null, exact replay and after-commit-only wake on pg/Prisma/Drizzle paths.
- [ ] Run `bash integration/persistence/server/run.sh`; expect new bulk/stream cases fail first. This runner supplies `DATABASE_URL` and temporary PostgreSQL; it has no test-filter argument. For a prepared test database, focus with `node --test --test-name-pattern='bulk|targeted|mixed guard|Stream upgrade' integration/persistence/server/stream-tracking.test.mjs`; do not run database files concurrently against a cluster whose schema they alter.
- [ ] Implement `readTracking` with JSON record/pair candidates, `UNION` of the all-holder join and explicit-pair join, deduplicate across 1,000-element chunks. Prevalidate the entire request before SQL. Implement the mixed guard in one ordered INSERT source per contiguous canonical chunk, not one statement per mode:

```sql
WITH wanted AS (
  SELECT v->>'model' model,v->>'identityKey' identity_key,v->>'mode' mode,ord
  FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v,ord)
), guarded AS (
  INSERT INTO axton_record(model,identity_key,stamp)
  SELECT w.model,w.identity_key,1 FROM wanted w
  WHERE w.mode <> 'lock' OR EXISTS (
    SELECT 1 FROM axton_record r WHERE r.model=w.model AND r.identity_key=w.identity_key)
  ORDER BY w.ord
  ON CONFLICT(model,identity_key) DO UPDATE SET stamp = CASE
    WHEN (SELECT w.mode FROM wanted w WHERE w.model=EXCLUDED.model
      AND w.identity_key=EXCLUDED.identity_key)='advance'
    THEN axton_record.stamp+1 ELSE axton_record.stamp END
  RETURNING model,identity_key,stamp
)
SELECT w.ord,g.stamp FROM wanted w LEFT JOIN guarded g
  ON g.model=w.model AND g.identity_key=w.identity_key ORDER BY w.ord;
```

Reject duplicate keys before this SQL. Excluded snapshot-absent `lock` keys return null, matching existing `LOCK_RECORD` absence behavior; ensure/advance always participate. The **ordered INSERT input** controls conflicting insertion/update acquisition, not the final SELECT. Ensure/lock conflict updates remain no-op writes; use the existing safe-stamp validation to abort overflow. Persist request ordinality across chunk slicing and restore request-aligned answers. Prove this actual statement on PostgreSQL; if query plans/lock observations contradict canonical acquisition, stop and revise the statement before accepting the task, never substitute sorted RETURNING as evidence.
- [ ] Rename existing ordered stream locks/head reservation and bounded log writes; remove tag dictionaries/joins/collection statements. Apply all final pairs without per-stream reads, preserve unpublished existing positions, reserve grouped heads once per stream, validate exact returned positions. Keep log `remove` rows queryable; fresh application operations emit only upserts.
- [ ] Write the new transaction-wrapped forward migration: run old Channel→Scope scripts through their existing path first, then Scope→Stream; reject conflicting/incomplete layouts; rename tables/columns/indexes/sequences/constraints and retained legacy framework columns. Retire only framework tag join/dictionary tables and their tag triggers/functions after verifying references; keep member immutability constraints. Do not edit dated migrations.

```sql
ALTER TABLE axton_scope RENAME TO axton_stream;
ALTER TABLE axton_stream RENAME COLUMN scope TO stream;
ALTER TABLE axton_scope_member RENAME TO axton_stream_member;
ALTER TABLE axton_stream_member RENAME COLUMN scope TO stream;
ALTER TABLE axton_scope_log RENAME TO axton_stream_log;
ALTER TABLE axton_stream_log RENAME COLUMN scope TO stream;
-- In a validated top-level claim only:
-- claim := (claim - 'scope') || jsonb_build_object('stream', claim->'scope');
```

Mirror the existing migration's claim validation (object, unique old/new ownership field, Model/identity/cursor types and safe positive cursor), transforming only `axton_client.receipt` and `axton_call.response` top-level `memberships`. Leave unrelated payload strings byte-identical when no claim changed. Use current fresh DDL for new databases; old migrations stay immutable.
- [ ] Upgrade both prior Channel and current Scope fixtures; compare catalog IDs, heads, each live pair/log row/removal cursor, receipts/calls and business JSON. Repeat migration to verify supported idempotent entry behavior; malformed/conflicting metadata must roll back. Rerun persistence runner and `npm run typecheck`; review lock ordering and migration evidence independently.

- [ ] After focused passing checks and independent review, inspect `git diff --check` and `git diff --stat`, then stage only this task’s owned paths and commit. Review `git diff --cached --stat` before the commit; exclude unrelated or another owner’s changes. This is a later implementation instruction, not authorization to commit while drafting.

```sh
git add -A -- \
  packages/postgres/src/sql.mts \
  packages/postgres/src/persistence.mts \
  packages/postgres/migration.sql \
  packages/postgres/src/statements.mts \
  packages/postgres/migrations/2026-10-01-streams.sql \
  integration/persistence/server/host-contract.test.mjs \
  integration/persistence/server/membership.test.mjs \
  integration/persistence/server/loads.test.mjs \
  integration/persistence/server/driver-conformance.test.mjs \
  integration/persistence/server/run.sh \
  integration/persistence/server/scope-tags.test.mjs \
  integration/persistence/server/stream-tracking.test.mjs \
  integration/persistence/server/fixtures/v02-framework.sql \
  integration/persistence/server/fixtures/postgres-state.sql \
  integration/persistence/server/fixtures/envelopes.json
git diff --cached --stat
git commit -m "feat: persist Stream tracking with ordered bulk guards"
```

## Task 4: Preserve client holdings and durable work through Stream migration

**Files**
- Modify: `crates/client/src/ddl.rs`, `crates/client/src/schema_store.rs`, `crates/client/src/store_delivery.rs`, `crates/client/src/bootstrap_ledger.rs`, `crates/client/src/load_ledger.rs`, `crates/client/src/runtime/protocol.rs` (coordinate final bridge edits with Task 1 owner).
- Modify: `packages/client-js/index.mts`, `packages/client-js/subscriptions.mts`, `packages/client-js/transaction.mts`, `packages/client-js/bridge.mts`, `packages/client-js/connection.mts`, `packages/client-js/scope-protocol.test.mjs`.
- Modify: `packages/dart/lib/src/client.dart`, `packages/dart/lib/src/subscriptions.dart`, `packages/dart/lib/src/bridge.dart`, `packages/dart/lib/src/connection.dart`, `packages/dart/lib/axton.dart`, `bindings/common/src/ffi.rs`, `bindings/node/src/client.rs`, `bindings/dart/src/lib.rs` only where delivery commands are serialized.
- Rename tests: `crates/sqlite/tests/scope_upgrade.rs` → `crates/sqlite/tests/stream_upgrade.rs`; modify `crates/sqlite/tests/ddl.rs`, `crates/sqlite/tests/loads.rs`, `integration/bindings/client-js/subscriptions.test.mjs`, `integration/bindings/client-js/loads.test.mjs`, `integration/bindings/client-js/loads-harness.mjs`, `packages/dart/test/subscriptions_test.dart`, `packages/dart/test/runtime_bridge_test.dart`.

**Interfaces**
- Consumes Task 1 Stream wire/claims/capability. Produces JS/Dart `subscribeStream(name)` runtime delegation and `transaction.streams.subscribe/unsubscribe`; generated `Streams` wrappers are Task 2-owned. Existing handles/bootstrap/status/cancellation remain unchanged.
- SQLite opening migrates prior Channel through existing upgrade, then Scope→Stream: `axton_scope_member`→`axton_stream_member`, `axton_subscription.scope`→`stream`, `scope_membership_version`→`stream_membership_version`; keep `present`, pair cursor, request/eviction epochs and holding semantics. No database file rename/rebuild and no JSON traversal of application query-cache results or Model descriptors.

- [ ] Add upgrade tests by creating an old Scope file with two holdings for one replicated Entry, a saved removal for another, cursor/bootstrap progress, pending/rejected Mutation plus local companion, device-only Composition and frozen Load continuation. Reopen at the identical path and assert every identity/counter/call ID and business value survives. Feed fresh Stream removal evidence for one holding: base survives the other hold; last removal releases base without Loader/onStore/cascade. Assert pending/local layers remain and a stale pre-release fetch/page cannot re-admit without the existing fence.
- [ ] Add a separate authority-null regression in existing SQLite tests plus the JS onStore/Load harness: application onStore hook receives a newer absent standing record in the local transaction and releases its application-owned cache subset; own Entry and Entry reached through another standing remain, as do pending/local work. Use test hook/domain fixtures, not Most Days edits. Implement the fixture hook through existing direct `tx.models.delete` semantics and assert its actual pending/local effects rather than assuming every hook is safe. Hold a child Load/fetch response or saved server replay, delete standing authority and reclaim cache, then release the late response: the child may rematerialize, but the fixture Query excludes it while standing is absent unless an independent valid path reaches it. Preserve own/independently reachable records and pending/local work in this concrete fixture. Incomplete Load and unsubscribe trigger no standing absence hook; actual changed child authority still applies normally. Add no new framework fence and no per-child cache-release invalidation.
- [ ] Run `cargo test -p axton-sqlite --test stream_upgrade --test ddl --test loads`, `node --test integration/bindings/client-js/subscriptions.test.mjs`, and `(cd packages/dart && dart test test/subscriptions_test.dart test/runtime_bridge_test.dart)`; expect new migration/vocabulary cases fail first. Native artifacts require `bash scripts/build.sh`; on this macOS host set `AXTON_LIBRARY="$PWD/target/debug/libaxton_dart.dylib"` and `AXTON_DART_LIBRARY="$AXTON_LIBRARY"` from repository root before Dart integration tests.
- [ ] Add the migration before fresh DDL/layout reconciliation inside the existing opening transaction; check old/new layout conflicts and completeness before renaming. Preserve the existing Channel upgrade as a first stage. Execute only framework DDL renames:

```sql
ALTER TABLE axton_scope_member RENAME TO axton_stream_member;
ALTER TABLE axton_stream_member RENAME COLUMN scope TO stream;
ALTER TABLE axton_subscription RENAME COLUMN scope TO stream;
ALTER TABLE axton_client RENAME COLUMN scope_membership_version TO stream_membership_version;
```

Recreate the renamed membership index while retaining all rows. Update SQL consumers and marker checks; do not clear subscriptions or rebuild replicas to make a test pass. Keep request epoch and holding admission logic, and both upsert/removal decoding.
- [ ] Rename delivery bridge commands, observer fields and runtime methods; preserve transactionCommand/savepoint `scope` tokens exactly. Test an actual nested savepoint plus `streams.subscribe` in JS/Dart, not only string output. Keep unsubscribe as delivery-intent cancellation with no cache eviction or server tracking edits.
- [ ] Rerun focused tests plus `cargo test -p axton-sqlite --locked`, `node --test integration/bindings/client-js/*.test.mjs`, `(cd packages/dart && dart analyze && dart test)` after native rebuild. Review stored-state diff and onStore/removal separation independently.

- [ ] After focused passing checks and independent review, inspect `git diff --check` and `git diff --stat`, then stage only this task’s owned paths and commit. Review `git diff --cached --stat` before the commit; exclude unrelated or another owner’s changes. This is a later implementation instruction, not authorization to commit while drafting.

```sh
git add -A -- \
  crates/client/src/ddl.rs \
  crates/client/src/schema_store.rs \
  crates/client/src/store_delivery.rs \
  crates/client/src/bootstrap_ledger.rs \
  crates/client/src/load_ledger.rs \
  crates/client/src/runtime/protocol.rs \
  packages/client-js/index.mts \
  packages/client-js/subscriptions.mts \
  packages/client-js/transaction.mts \
  packages/client-js/bridge.mts \
  packages/client-js/connection.mts \
  packages/client-js/scope-protocol.test.mjs \
  packages/dart/lib/src/client.dart \
  packages/dart/lib/src/subscriptions.dart \
  packages/dart/lib/src/bridge.dart \
  packages/dart/lib/src/connection.dart \
  packages/dart/lib/axton.dart \
  bindings/common/src/ffi.rs \
  bindings/node/src/client.rs \
  bindings/dart/src/lib.rs \
  crates/sqlite/tests/scope_upgrade.rs \
  crates/sqlite/tests/stream_upgrade.rs \
  crates/sqlite/tests/ddl.rs \
  crates/sqlite/tests/loads.rs \
  integration/bindings/client-js/subscriptions.test.mjs \
  integration/bindings/client-js/loads.test.mjs \
  integration/bindings/client-js/loads-harness.mjs \
  packages/dart/test/subscriptions_test.dart \
  packages/dart/test/runtime_bridge_test.dart
git diff --cached --stat
git commit -m "feat: migrate client delivery state to Streams"
```

## Task 5: Integrate round trips, current documentation and final evidence

**Files**
- Modify assembled tests/examples: `integration/action-contract/backend.ts`, `integration/action-contract/client.ts`, `integration/action-contract/positive.dart`, `integration/action-contract/negative.dart`, `integration/action-runtime-ts/backend.ts`, `integration/action-runtime-dart/generated_test.dart`, `integration/action-e2e/`, `integration/load-e2e/`, `integration/e2e/fixtures/round-trip/`, `examples/todo/` (these existing directories own their checked-in generated fixtures).
- Modify: `scripts/test.sh`, `integration/persistence/server/run.sh` only to register renamed/new tests, coordinating with Task 3 owner.
- Modify current guidance: `docs/engineering/guarantees.md`, `docs/engineering/architecture/server/backend-interface.md`, `docs/engineering/architecture/server/persistence.md`, `docs/engineering/architecture/server/engine/publish.md`, `docs/engineering/architecture/server/engine/loads.md`, `docs/engineering/architecture/client/storage/README.md`, `docs/engineering/architecture/client/frontend-interface.md`, `website/docs/backend/api.md`, `website/docs/backend/database.md`, `website/docs/backend/deployment.md`, `website/docs/frontend/client-api.md`, `website/docs/frontend/sync.md`, `website/docs/frontend/loads.md`, package READMEs.

**Interfaces**
- Consumes all earlier interfaces and migrations; produces matching shipped-source Rust/JS/Dart protocol, generated clients and executable examples. Integration owner owns regeneration and final evidence; no registry release is part of this task.

- [ ] Add failing TS/Dart round trips: a Load tracks two returned Models into A/B, later selected A invalidation changes a viewer row to null at a newer stamp without B cursor movement, and a global Mutation input change reaches all final tracking streams once. Pause page/receipt delivery to show stale answers cannot replace newer authority, replay runs no handler/Loader/declarations, Loader error retains local records, and unsubscribe leaves cache/server tracking intact.
- [ ] Run `bash integration/action-runtime-ts/verify.sh`, `bash integration/action-e2e/run.sh`, `bash integration/load-e2e/run.sh`; expect missing/new shape mismatches to fail before fixture correction. Include a generated TS compile test covering mixed/single/list/composite calls and a generated Dart subscription/nested transaction smoke in the existing fixtures.
- [ ] Integrate owners sequentially, regenerate only through each fixture's existing runner and inspect output; remove executable old tag/withdrawal examples and replace them with:

```ts
await backend.transaction(async ctx => {
  ctx.stream(['User:alice', 'User:bob']).track(records);
  ctx.invalidate(sharedContent);
  ctx.stream('User:alice').invalidate(viewerAuthority);
});
await client.streams.subscribe('User:alice');
await client.transaction(tx => tx.streams.subscribe('User:bob'));
```

Explain durable interest versus permission, targeted versus global responsibility, retained removal evidence/holdings, no automatic retention, and the standing-absence hook application pattern. Update links/anchors when renaming headings. Current deployment guidance requires coordinated migration/negotiation; source manifests remain `0.2.0`, with future release coordination explicitly separate. Do not rewrite historical specs/plans/migrations or opaque fixture/business fields named Scope/scope.
- [ ] Run focused integration and documentation checks after integration:

```sh
npm run typecheck
bash integration/persistence/transaction-probe/run.sh
bash integration/persistence/server/run.sh
bash integration/generated-api/verify.sh
bash integration/action-runtime-ts/verify.sh
bash integration/action-e2e/run.sh
bash integration/load-e2e/run.sh
python3 website/scripts/check_examples.py
node scripts/release/version.mjs check
node --test integration/release/*.test.mjs
bash integration/release/verify-installed.sh
```

Expect each command to exit 0; inspect regenerated fixture diffs. Installed-package verification installs third-party dependencies and builds this host's packages but does not publish. For Dart installed-package acceptance use the existing `integration/release/verify-dart.sh` runner with a staged package/library directory following `docs/engineering/testing/running.md`; no registry version or new publishing step is chosen here.
- [ ] Run `bash scripts/test.sh` as the final host gate (Rust fmt/clippy/workspace tests, JS/Dart SDK, persistence, generated API, Action/Load/e2e, examples and installed npm checks). Record command/results and any actual environment blocker; never infer pass from source review. Device smoke is separate: follow `integration/platform/README.md` and report platform/run status independently.
- [ ] Review accepted spec coverage against evidence: declaration lifetimes/atomic failure, Load limits, stamp dominance, targeted projection absence, errors, constant bulk host operations/chunked SQL, mixed-mode acquisition/races, rollback/replay/wakes, both server upgrades, SQLite work/holdings preservation, negotiation/savepoint separation and release checks. Keep unresolved evidence failures visible; do not claim completion until required gates pass. Hand the reviewed change back without merge or publication.

- [ ] After focused passing checks and independent review, inspect `git diff --check` and `git diff --stat`, then stage only this task’s owned paths and commit. Review `git diff --cached --stat` before the commit; exclude unrelated or another owner’s changes. This is a later implementation instruction, not authorization to commit while drafting.

```sh
git add -A -- \
  integration/action-contract/backend.ts \
  integration/action-contract/client.ts \
  integration/action-contract/positive.dart \
  integration/action-contract/negative.dart \
  integration/action-runtime-ts/backend.ts \
  integration/action-runtime-dart/generated_test.dart \
  integration/action-e2e/ \
  integration/load-e2e/ \
  integration/e2e/fixtures/round-trip/ \
  examples/todo/ \
  scripts/test.sh \
  integration/persistence/server/run.sh \
  docs/engineering/guarantees.md \
  docs/engineering/architecture/server/backend-interface.md \
  docs/engineering/architecture/server/persistence.md \
  docs/engineering/architecture/server/engine/publish.md \
  docs/engineering/architecture/server/engine/loads.md \
  docs/engineering/architecture/client/storage/README.md \
  docs/engineering/architecture/client/frontend-interface.md \
  website/docs/backend/api.md \
  website/docs/backend/database.md \
  website/docs/backend/deployment.md \
  website/docs/frontend/client-api.md \
  website/docs/frontend/sync.md \
  website/docs/frontend/loads.md
git diff --cached --stat
git commit -m "docs: verify coordinated Stream contract and migration"
```

## Planning verification and risk notes

Paths and commands above were checked against this worktree's source and `docs/engineering/testing/running.md`/`scripts/test.sh`; no tests were run while drafting. Existing retirement-focused tests must be replaced by tracking/invalidation scenarios, while retained-removal tests must remain. The mixed guard statement is an implementation sketch requiring actual PostgreSQL acquisition evidence in Task 3; its final SELECT order is deliberately not that evidence. Snapshot-absent lock returns null just as today's no-op `UPDATE` does; this is not permission to split existing/missing keys into independently reordered write passes.

The specification is internally consistent with inspected source. The implementation must still establish its concurrency claims experimentally; no alternative API, Loader cache, Most Days change or extra retention policy is needed to resolve a contradiction.
