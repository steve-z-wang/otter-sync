# Local Authority Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Use one implementer at a time in the shared worktree, with specification and code reviews between tasks. Steps use checkbox (`- [ ]`) syntax for tracking. Execution is already authorized; do not request another execution choice.

**Goal:** Deliver stamped viewer-authorized Models into one local store without assigning cached records a lifetime through Stream ownership.

**Architecture:** Negotiate `stream-authority-v1` before server effects, preserve historical envelope decoding and frozen logical calls, and remove the client's holding ledger and automatic reconstruction lane. Preserve the ordinary authority, pending replay and transaction machinery. A stopped-writer PostgreSQL repair re-establishes historical withdrawn tracking pairs and publishes newer current authority through ordinary viewer Loaders.

**Tech Stack:** Rust workspace, SQLite/rusqlite, PostgreSQL 16, TypeScript/Node SDKs and adapters, Dart SDK, existing simulation and integration runners.

## Global Constraints

- Approved design: `docs/superpowers/specs/2026-10-01-local-authority-delivery-design.md`; baseline `134294d0e7b0d6161e9c9a8dfed85eee02ca1d1b`; planning starts at `51350755` on `codex/local-authority-delivery`.
- Work only in `/Users/stevewang/Github/axton/.worktrees/scope-membership-api-design`. Preserve unrelated changes; do not modify Most Days, release pins or package versions. No merge or registry publication.
- Required capability is exactly `stream-authority-v1`; stop advertising `stream-membership-v1`. Admission precedes handlers, claims, durable progress and live acknowledgement.
- Capabilities decorate transport; they are excluded from logical identity. Keep call IDs, sequence numbers, frozen logical bytes, contracts, results and continuations. Adjust negotiation headroom without increasing business payload limits.
- Fresh Push, Action and Load envelopes omit top-level `memberships`. Decode valid historical metadata, without local ownership merging. Preserve nested business fields with the same name; saved replay runs no Handler or Loader.
- Local migration marker is `axton_client.local_authority_version = 1`. Fresh SQLite files contain neither `axton_stream_member` nor its record index. Existing compatible files upgrade in one opening transaction before network work.
- Preserve cached Models and stamps, client identity, subscription identity/cursors, explicit bootstrap, queued/failed calls, companions, direct local work, rejections, Query cache, Load IDs/continuations and frozen work.
- `authority.rs::Held` is a pending-replay accumulator. `axton_local_replica_layer` preserves direct local operation history. Neither is the per-Stream holding ledger; retain both.
- Preserve legacy `base_state='evicted'`, `evicted_at`, store epochs and frozen request tokens for pre-cutover recovery. Do not allocate new ownership eviction epochs. Canonical `absent` must never become restorable positive authority.
- Historical `Remove` is identity and cursor evidence only: advance validated delivery progress without Model writes, hooks, cascades, stamp changes or synthetic null.
- Server tracking, `track`, global/selected `invalidate`, viewer Loaders and client Stream subscriptions remain. No Event, tag, source-release, cache eviction API or automatic business graph traversal.
- Four sequential implementation/review units below. The root worker owns regeneration, assembled host verification and final independent review. Do not run multiple implementers concurrently.

## Source map and test responsibilities

| Responsibility | Owning files |
| --- | --- |
| Negotiation, size limits, logical identity, historical decoding | `crates/core/src/protocol.rs`, `crates/core/src/actions.rs`, `crates/core/src/loads.rs` |
| Server ingress and fresh/saved responses | `crates/server/src/lib.rs`, `actions.rs`, `loads.rs`, `live.rs`, `settlement.rs` |
| Transport decoration | `crates/client/src/lib.rs`, `transport.rs`, `bootstrap.rs`, `push.rs`, `runtime/effects.rs` |
| Client authority, local work and compatibility epochs | `crates/client/src/stream_members.rs`, `authority.rs`, `store_epoch.rs`, `push.rs`, `actions.rs`, `loads.rs`, `fetch.rs`, `downlink.rs`, `store_delivery.rs` |
| SQLite layout and open gate | `crates/client/src/ddl.rs`, `lib.rs`, `schema_store.rs` |
| Ordinary subscription/bootstrap/runtime scheduling | `crates/client/src/subscriptions.rs`, `bootstrap.rs`, `bootstrap_ledger.rs`, `engine.rs`, `transport.rs`, `downlink_worker.rs`, `runtime/lanes.rs` |
| Durable server tracking and bulk settlement | `crates/server/src/settlement.rs`, `stream_members.rs`, `packages/postgres/src/sql.mts`, `persistence.mts`, `packages/postgres/migration.sql` |
| Real historical repair | New `packages/postgres/migrations/2026-10-01-local-authority.sql`; existing `integration/persistence/server/stream-tracking.test.mjs` |
| State-transition oracle | `crates/sim/src/invariants.rs`, `sim.rs`, `host.rs`; `crates/sim/tests/authority.rs`, `historical_removals.rs`, `stream_tracking.rs`, `upgrade.rs` |

Read the architecture index and owning component pages before implementation: `docs/engineering/architecture.md`, `docs/engineering/guarantees.md`, `docs/engineering/architecture/client/storage/reconciliation.md`, `docs/engineering/architecture/server/engine/publish.md`, and the protocol/client-engine pages linked by the index. Follow `docs/engineering/testing/strategy.md` and `running.md`. Tests below are new evidence to obtain, not claims that they already pass.

### Task 1: Negotiate authority-only transport and preserve saved outcome replay

**Files:**
- Modify: `crates/core/src/protocol.rs`, `crates/core/src/actions.rs`, `crates/core/src/loads.rs`.
- Modify: `crates/server/src/lib.rs`, `crates/server/src/actions.rs`, `crates/server/src/loads.rs`, `crates/server/src/live.rs`, `crates/server/src/settlement.rs`.
- Compile-callsite edits: `crates/client/src/lib.rs`, `crates/client/src/transport.rs`, `crates/client/src/bootstrap.rs`, `crates/client/src/push.rs`, `crates/client/src/runtime/effects.rs`, `crates/client/src/live.rs`, `crates/client/src/load_worker.rs`.
- Test: `crates/core/tests/contracts.rs`, `crates/server/tests/protocol_admission.rs`, `crates/server/tests/actions.rs`, `crates/server/tests/loads.rs`, `crates/server/tests/membership.rs`, `crates/server/tests/capability/mod.rs`.
- Rename-only test callers: `crates/sqlite/tests/load_worker.rs`, `crates/sqlite/tests/stream_upgrade.rs`, `crates/sqlite/tests/runtime_lanes.rs`. Update active core capability fixtures referenced by `contracts.rs`; retain historical fixture bytes.

**Interfaces:**
- Produce `pub const STREAM_AUTHORITY_CAPABILITY: &str = "stream-authority-v1"` in `axton_core`; transport continues to use `with_capabilities(envelope: &[u8], capabilities: &[&str]) -> Result<Vec<u8>>`.
- Preserve `logical_request(envelope: &Value) -> Result<Value>`: it already strips capabilities, so do not rewrite durable requests.
- Preserve `MembershipClaim` and historical `read_memberships` validation as decode compatibility. Fresh response construction passes empty claim vectors, whose existing serializers omit the field. Saved outcomes retain their valid historical fields.
- Keep the host tracking and settlement interface intact; remove fresh claim generation/aggregation from response assembly, not durable tracking itself.

- [ ] **1.1 Add a failing core negotiation regression.** Add the following named test to `contracts.rs`; use existing imports or fully qualified names.

```rust
#[test]
fn authority_capability_is_transport_decoration_for_a_frozen_call() {
    let frozen = br#" {"call":{"callId":"01890f47-1234-7123-8123-000000000001","name":"Find","version":1,"args":{"memberships":["business"]}},"models":{}} "#;
    let decorated = axton_core::with_capabilities(frozen, &["stream-authority-v1"]).unwrap();
    axton_core::require_capability(&decorated, "stream-authority-v1").unwrap();
    let logical = axton_core::logical_request(&serde_json::from_slice(&decorated).unwrap()).unwrap();
    assert_eq!(logical, serde_json::from_slice::<serde_json::Value>(frozen).unwrap());
    assert_eq!(axton_core::STREAM_MEMBERSHIP_CAPABILITY, "stream-authority-v1");
}
```

The final assertion is deliberately RED against the old constant. In GREEN, rename it to `STREAM_AUTHORITY_CAPABILITY` in the test and every production callsite, and remove the old public constant rather than keeping a misleading alias.

- [ ] **1.2 Extend ingress and replay tests before implementation.** In `protocol_admission.rs`, include `{"capabilities":["stream-membership-v1"]}` in the existing rejected ingress matrix; assert `protocol.unsupported` and the untouched host log for Push, Action, Fetch, Load, pull/bootstrap and live negotiation. Add a new accepted envelope using `stream-authority-v1` to the existing capable helper. In `membership.rs`, replace its fresh claim assertion with absence of fresh top-level metadata while retaining tracking assertions. In `actions.rs` and `loads.rs`, extend the existing saved-call replay cases with a saved valid top-level claim and nested result `{"memberships":[{"scope":"business"}],"stream":"business"}`; assert identical result/continuation, no Handler/Loader/stamp increment, and preserved historical metadata. Add a fresh tracking case asserting serialized response has no top-level `memberships` while the host still records tracking.

```sh
cargo test -p axton-core --locked --test contracts authority_capability_is_transport_decoration_for_a_frozen_call
cargo test -p axton-server --locked --test protocol_admission
cargo test -p axton-server --locked --test actions
cargo test -p axton-server --locked --test loads
cargo test -p axton-server --locked --test membership
```

Expected RED: old capability is admitted, new capability is refused, or fresh metadata is emitted. A compile error from a newly named constant is not sufficient behavioral RED evidence; use the old symbol in 1.1 first.

- [ ] **1.3 Implement the negotiation and envelope boundary.** Apply the following changes at the current capability/size allowance definitions and response construction sites:

```rust
pub const STREAM_AUTHORITY_CAPABILITY: &str = "stream-authority-v1";
// In check_request_size, replace only the decoration allowance and capability check:
const HEADROOM: usize = br#","capabilities":["stream-authority-v1"]"#.len();
// Fresh PushReceipt/DirectActionResponse/LoadPageResponse constructors:
memberships: Vec::new(),
```

Use the same literal format as the current `HEADROOM` source (raw JSON without escaped quote characters). Replace `STREAM_MEMBERSHIP_CAPABILITY` references with the new symbol. Keep the pre-effects `require_capability` call position. Do not call `Settlement::claims` for fresh outcomes or aggregate saved claims into fresh Push receipts. Saved direct/Load response paths must still decode and replay historical fields. Keep `current_claims` only where historical validation/version adaptation requires it. Preserve nested business JSON and saved bytes; do not rerun operations or strip metadata through recursive JSON rewriting. Ensure the size test accepts a maximum business payload with the new decoration and rejects a business payload one byte over its original limit.

- [ ] **1.4 Run GREEN and review this unit.** Rerun 1.2 and `cargo test -p axton-core -p axton-server --locked`. Run `cargo check --workspace --locked` to catch transport callers. Commit this task's source/tests with `feat: negotiate authority-only stream delivery`, then have the reviewer assess the task base..HEAD for admission timing, absent fresh metadata, exact replay and unchanged tracking. Commit fixes before their review. Task 2 starts only after approval of this gate.

### Task 2: Remove local Stream ownership and migrate real SQLite files

**Files:**
- Modify all client authority/local-work and opening/scheduling files in the source map; retain `authority.rs` pending replay and the direct/companion local journal.
- Test: `crates/sqlite/tests/stream_upgrade.rs`, `subscriptions.rs`, `store_hooks.rs`, `direct_writes.rs`, `settlement.rs`, `loads.rs`, `actions.rs`, `bootstrap.rs`, `bootstrap_worker.rs`, `ddl.rs`.
- Fixtures: keep `crates/sqlite/tests/fixtures/v02-framework.sql`, `sqlite-state.sql`, `frozen-push-logical.json` as original pre-upgrade inputs. Add authority-era expectations in the tests, not by modernizing old fixtures.

**Interfaces:**
- Preserve `Client::open`, `Client::open_at`, ordinary `StoreDelivery::StreamPage` and `StreamBootstrap`, subscription ID fencing, and explicit bootstrap API.
- Keep `apply_enrolled_records_at(records: &[AuthorityRecord], claims: &[MembershipClaim], token: StoreToken) -> Result<ApplyReport>` as the transition entry point: validate compatibility claims, ignore them for storage ownership, stage every canonical record through `stage_isolated` and `rebuild_held`. Its callers may be renamed together after correctness is established; no new public cache API.
- `apply_stream_changes(changes: &[StreamChange]) -> Result<ApplyReport>` stages Upsert records without a holding check and treats validated Remove frames as no Model occurrence. Existing page handlers own cursor advancement.
- `admit_positive_body(key: &RecordKey, token: StoreToken) -> Result<bool>` retains only legacy global epoch recovery. Remove its `held()` shortcut; for records with `evicted_at=0`, all ordinary authority is admissible. Fresh Stream authority bypasses a historic request token; delayed pre-cutover one-shot work retains its frozen token check.

- [ ] **2.1 Add real-file migration RED assertions to `stream_upgrade.rs`.** Extend `original_store` variants to original Channel (markers 0 and 1), Scope and current Stream layouts. Reuse the existing Scope conversion SQL and add the corresponding Stream rename variant. Snapshot raw work bytes and named subscription fields using the existing `hex(CAST(... AS BLOB))` queries. Open and reopen the same path; assert preservation, no sidecar/new generation, marker one and no holding table/index:

```rust
assert_eq!(c.read_sql("SELECT local_authority_version FROM axton_client", &[]).unwrap()[0]["local_authority_version"], 1);
assert_eq!(c.read_sql("SELECT count(*) AS n FROM sqlite_master WHERE name IN ('axton_stream_member','axton_stream_member_record','axton_scope_member','axton_channel_member')", &[]).unwrap()[0]["n"], 0);
assert_eq!(c.cursor("Channel:business-scope").unwrap(), Some(11));
assert_eq!(axton_client::schema_store::current_file(&path), path);
assert!(!axton_client::schema_store::sidecar_of(&path).exists());
```

Preservation comparisons must omit only the removed ownership table and newly added marker. Keep explicit `bootstrap_*`, starting/delivery cursors and all raw work byte assertions. Existing `reconcile_*` columns may remain inert for compatibility; their stored values do not authorize new network work. Add a fresh-file assertion. Extend malformed-layout tests with missing required membership columns/table before marker completion, conflicting marker values, and wrong-owner holding index; snapshot catalog and work and prove failed open leaves both unchanged.

- [ ] **2.2 Add delivery and worker RED regressions.** Replace final-holder eviction expectations in existing Stream tests with these outcomes: A/B upserts reach one Model row; removing either/both sources keeps row and stamp; unsubscribe keeps row; an identity-only Remove produces no prepared hook changes; a newer null removes the row with existing hook/cascade behavior; older positive authority cannot resurrect it. Hold a stale Load page across original-file upgrade and reopen: keep its result/continuation completion and legacy epoch suppression, while a fresh read/current Stream upsert can apply according to stamps. Keep legacy `absent` stronger than equal-stamp positive restoration. In the existing downlink harness, set old reconciliation requested/loading/failed states, pump and assert no reconstruction HTTP bootstrap request; explicitly requested bootstrap still dispatches, completes and rejects stale run/registration answers. Hook refusal must roll back authority, local hook writes and cursors together.

```sh
cargo test -p axton-sqlite --locked --test stream_upgrade
cargo test -p axton-sqlite --locked --test subscriptions
cargo test -p axton-sqlite --locked --test store_hooks
cargo test -p axton-sqlite --locked --test bootstrap_worker
```

Expected RED: holding table persists, removal evicts, or reconstruction schedules. First run preserves old test fixtures and records the behavioral failures before production changes.

- [ ] **2.3 Implement the transactional migration before client scheduling.** Run original-name/layout validation before dropping anything; distinguish an old valid layout from a completed authority layout. Reuse `upgrade_stream_names` for supported Channel/Scope shapes, but remove its automatic reconciliation scheduling. Inside the existing opening transaction, after validation and before fresh DDL, add the marker and drop only the verified holding table/index:

```sql
ALTER TABLE axton_client ADD COLUMN local_authority_version INTEGER NOT NULL DEFAULT 0;
DROP INDEX IF EXISTS axton_stream_member_record;
DROP TABLE axton_stream_member;
UPDATE axton_client SET local_authority_version=1;
```

The ALTER runs only when the column is absent; DROP runs only for a verified old layout. Fresh `FRAMEWORK_DDL` declares marker default 1 and omits the ownership table/index; remove it from required modern `FRAMEWORK_TABLES`. Completed marker-one layouts require no holding table and accept reopen without replaying migration. An old membership marker claiming a holding table that is missing remains a malformed modern file, not fresh. Keep unsupported checkpoint-era files under the existing nondestructive rebuild policy. Do not remove Models, before images, local journals or legacy record metadata.

- [ ] **2.4 Implement authority-only staging and retire reconstruction.** Replace the holding-merge/release logic with the core of this loop, using the existing imported types and final report assembly:

```rust
let mut report = ApplyReport::default();
let mut pending = Held::new();
for change in changes {
    if let StreamChange::Upsert { record, .. } = change {
        let (applied, diagnostic) = self.stage_isolated(record, &mut pending)?;
        report.applied += usize::from(applied);
        report.reports.extend(diagnostic);
    }
}
report.reports.extend(self.rebuild_held(&pending)?);
Ok(report)
```

Remove `MemberEvidence`, `MembershipMerge`, merge/held/release ownership code and all callers. Keep `replica_evicted`, `set_base_state`, `clear_local_layer`, `retain_local_operation`, and local operation decoding where authority/direct-write recovery uses them. In positive one-shot staging use only the retained global epoch predicate; no claims create a hold. Remove `evict_at_next_epoch` automatic callers and ownership scheduling at DDL/open/subscription acknowledgement/resubscription. Remove `StoreDelivery::StreamReconciliation`, active progress/action variants and worker priority/retry/barrier handling; simplify the shared explicit-bootstrap functions to one lane. Leave inert old columns where dropping them would obscure preservation. Do not remove schema reconciliation, observer registration reconciliation or Load schema reconciliation: they are unrelated.

- [ ] **2.5 Run GREEN and review preservation.** Rerun 2.2, then:

```sh
cargo test -p axton-sqlite --locked
cargo test -p axton-client --locked
cargo check --workspace --locked
```

Review the migration ordering and fresh layout separately from the authority loop; check direct-write/newer-authority, pending rejection/acceptance/companions, legacy evicted and authoritative absent tests. Check no active SQL or worker references to the removed ownership table survive. Commit source/tests with `feat: replace stream ownership with local authority` before independent review of the task base..HEAD; commit fixes before their review. Task 3 starts only after approval of this gate.

### Task 3: Repair historical PostgreSQL withdrawals without synthesizing absence

**Files:**
- Create: `packages/postgres/migrations/2026-10-01-local-authority.sql`.
- Test: `integration/persistence/server/stream-tracking.test.mjs` (existing runner already executes it).
- Modify commentary only where needed: `packages/postgres/migration.sql`, `packages/postgres/src/sql.mts`; preserve server `axton_stream_member` and host interface.

**Interfaces:**
- Migration consumes the Stream layout after existing Channel→Scope→Stream upgrades. No new server host/public API.
- It repairs only latest `axton_stream_log.kind='remove'` pairs, re-tracks those pairs, advances each affected record once and publishes an ordinary Upsert to every currently tracked Stream for that identity.
- It allocates strictly newer positions under existing head bounds and never rewinds clients. Current viewer Loaders decide state/null on subsequent delivery.

- [ ] **3.1 Add PostgreSQL RED tests in the existing harness.** In an isolated schema using existing fixture/reset helpers, install Stream tables; seed `Entry:e` stamp 7, Stream A head 11/latest Remove 11 and Stream B head 4/latest Upsert 4 with surviving B tracking. Add unrelated record U, saved `axton_call.request/response` and `axton_client.receipt` with nested business `memberships`. Snapshot unrelated rows and raw text bytes. Run the new migration via `await source('migrations/2026-10-01-local-authority.sql')`; assert:

```javascript
assert.equal((await q("SELECT stamp FROM axton_record WHERE model='Entry' AND identity_key=$1", [key('e')]))[0].stamp, '8');
assert.deepEqual((await q("SELECT stream,kind,cursor::text cursor FROM axton_stream_log WHERE record_id=$1 ORDER BY stream", [recordId])).map(r => [r.stream,r.kind,r.cursor]), [['A','upsert','12'],['B','upsert','5']]);
assert.equal((await q("SELECT count(*)::int n FROM axton_stream_member WHERE record_id=$1", [recordId]))[0].n, 2);
```

Then rerun the migration and compare all framework rows/heads/stamps exactly: no allocation on replay. Add >1,000 removed pairs, several removals for one identity, null/positive current viewer Loader outcomes and different viewers' valid access. Counter overflow or an injected late SQL error must roll back tracking, stamps, heads and logs; saved business/receipt/call bytes must be unchanged in both success and failure. Existing old cursors must observe repaired positions without reset. A historical Remove alone must never produce canonical null.

```sh
bash integration/persistence/server/run.sh
```

Expected RED before adding the migration: missing migration file; then use an empty transactional migration to demonstrate stamp/position assertions fail before implementing repair. This runner creates and cleans a real PostgreSQL cluster and tests pg/prisma/drizzle boundaries sequentially.

- [ ] **3.2 Add the atomic stopped-writer SQL repair.** Put the following set-based algorithm in the new file. It follows existing bulk semantics: one advance per affected identity, union of repaired/surviving tracking, one reservation per Stream and one latest log row per pair. No business or saved-response rewrite.

```sql
-- Stop old writers and live sessions before running this forward repair.
-- Apply previous layout migrations first; keep authority-only traffic stopped
-- until this transaction commits. Reapplying finds no latest removals.
BEGIN;
LOCK TABLE axton_stream, axton_record, axton_stream_member, axton_stream_log IN EXCLUSIVE MODE;
CREATE TEMP TABLE axton_authority_removed ON COMMIT DROP AS
 SELECT stream,record_id FROM axton_stream_log WHERE kind='remove';
CREATE TEMP TABLE axton_authority_records ON COMMIT DROP AS
 SELECT DISTINCT record_id FROM axton_authority_removed;
INSERT INTO axton_stream_member(stream,record_id)
 SELECT stream,record_id FROM axton_authority_removed ORDER BY stream COLLATE "C",record_id
 ON CONFLICT(stream,record_id) DO NOTHING;
UPDATE axton_record r SET stamp=r.stamp+1
 FROM axton_authority_records w WHERE r.id=w.record_id;
CREATE TEMP TABLE axton_authority_pairs ON COMMIT DROP AS
 SELECT m.stream,m.record_id,
 row_number() OVER(PARTITION BY m.stream ORDER BY r.model COLLATE "C",r.identity_key COLLATE "C") AS offset
 FROM axton_stream_member m JOIN axton_authority_records w USING(record_id)
 JOIN axton_record r ON r.id=m.record_id;
CREATE TEMP TABLE axton_authority_heads ON COMMIT DROP AS
 SELECT s.stream,s.head AS old_head,count(p.record_id)::bigint AS count
 FROM axton_stream s JOIN axton_authority_pairs p USING(stream)
 GROUP BY s.stream,s.head;
-- Existing CHECK constraints refuse stamp or head overflow atomically.
UPDATE axton_stream s SET head=s.head+w.count
 FROM axton_authority_heads w WHERE s.stream=w.stream;
INSERT INTO axton_stream_log(stream,record_id,cursor,kind)
 SELECT p.stream,p.record_id,h.old_head+p.offset,'upsert'
 FROM axton_authority_pairs p JOIN axton_authority_heads h USING(stream)
 ORDER BY p.stream COLLATE "C",p.offset
 ON CONFLICT(stream,record_id) DO UPDATE SET cursor=EXCLUDED.cursor,kind='upsert';
COMMIT;
```

Use the single transaction to check supported required table/column shapes before writes, following the existing migration refusal style. The SQL's temp names are transaction-local and disappear at commit/rollback. Existing FK and counter constraints must reject corruption instead of silently losing rows. An empty repair leaves stamps and heads unchanged. Retain current server tracking even when a viewer Loader returns null. Do not add triggers that infer permission or null from tracking.

- [ ] **3.3 Verify GREEN and review operational cutover.** Rerun the real PostgreSQL runner. Commit migration/tests with `fix: restamp historical stream withdrawals`, then independently review task base..HEAD for repeated repair, union/global invalidation, overflow/rollback and exact unrelated saved-row preservation. Confirm the server's scan delivers repaired Upsert positions through current viewer Loaders, and legacy Remove decode remains cursor-only. Commit any review fixes before their review and obtain approval before Task 4. The root records the stopped-writer requirement in deployment docs in Task 4; no deployed database is mutated in this task.

### Task 4: Assemble simulation, SDK boundaries and documented guarantees

**Files:**
- Modify: `crates/sim/src/invariants.rs`, `crates/sim/src/sim.rs`, `crates/sim/src/host.rs`, `crates/sim/tests/authority.rs`, `historical_removals.rs`, `stream_tracking.rs`, `upgrade.rs`.
- Modify: `integration/persistence/server/protocol-admission.test.mjs`, `actions.test.mjs`, `loads.test.mjs`, `stream-tracking.test.mjs`; `integration/persistence/client/reopen.mts`.
- Modify affected fixtures/capability literals in `integration/bindings/client-js/*.test.mjs`, `packages/dart/test/*_test.dart`, `integration/generated-api/`, `integration/action-e2e/`, `integration/load-e2e/` and `integration/e2e/` as discovered by the literal scan below. Preserve original historical fixtures as migration inputs.
- Modify: `docs/engineering/guarantees.md`, `docs/engineering/architecture/protocol/common.md`, `push.md`, `actions.md`, `loads.md`, `pull.md`, `subscriptions.md`; `docs/engineering/architecture/client/storage/reconciliation.md`, `client/engine/README.md`, `client/connection/controller/downlink-worker.md`; `docs/engineering/architecture/server/engine/publish.md`, `server/persistence.md`; `website/docs/backend/deployment.md`, `backend/api.md`, `frontend/sync.md`.

**Interfaces:**
- Public track/invalidate/subscription signatures remain as already generated. Regeneration must not invent a cache operation or change Model/action versions.
- Simulation oracle uses Model authority and application permission, never whether the client has any holding row. Server tracking still decides delivery destinations.
- SDK tests exercise existing `onStore` transactions, Stream delivery and Model SQL/queries. No SDK-specific ownership implementation.

- [ ] **4.1 Add cross-path and replay RED assertions before adapting the oracle.** In existing authority/historical-removal scenarios, deliver a newer null through each canonical path and then an older positive through another; assert no resurrection. Add duplicate/reordered Remove followed by repaired newer authority and crash/restart, preserving unrelated rows. In `historical_removals.rs`, change the old final-source-loss assertion only after adding explicit assertions that row/stamp survive the Remove and current Loader authority decides later absence. Generated operation sequences continue to mix pending mutations, direct/companion work, delayed native Loads, rejection, subscriptions and restarts. Retain failure-isolation, bootstrap cancellation and equal-stamp diagnostics rather than deleting conflicting scenarios.

```sh
cargo test -p axton-sim --locked --test authority
cargo test -p axton-sim --locked --test historical_removals
cargo test -p axton-sim --locked --test upgrade
```

Expected RED: old oracle equates no tracking/hold with no cached content. GREEN removes that equation and asserts authority/local-write rules instead; do not suppress invariant checks.

- [ ] **4.2 Exercise published-facing boundaries.** Update active negotiation literals to authority capability, retaining old literals only as explicit rejection/history inputs:

```sh
rg -n 'stream-membership-v1|axton_stream_member|reconcile_state|memberships' crates integration packages docs/engineering website/docs
```

Add JS and Dart cases to existing subscription/Load hook harnesses: fresh delivery materializes without holding SQL, unsubscribe retains it, a hook cleanup and incoming authority commit/rollback together, and migrated pending work reopens with the same logical call. For saved historical metadata, compare nested result/continuation data and Handler counters; do not merely assert response parses. Existing fresh SDK transport must advertise `stream-authority-v1`, including frozen batch retries. The root builds native artifacts before language runs and regenerates checked-in generated fixtures only through existing scripts:

```sh
bash scripts/build.sh
node --test integration/bindings/client-js/subscriptions.test.mjs integration/bindings/client-js/loads.test.mjs integration/bindings/client-js/actions.test.mjs
bash integration/persistence/server/run.sh
bash integration/generated-api/verify.sh
bash integration/load-e2e/run.sh
```

For Dart, use the library environment documented in `docs/engineering/testing/running.md`, then `(cd packages/dart && dart analyze && dart test test/subscriptions_test.dart test/loads_test.dart test/actions_test.dart)`. Review regenerated diffs: capability expectation changes are allowed, action/Model contract versions and application payloads are unchanged.

- [ ] **4.3 Document ownership and the coordinated cutover.** Replace the guarantees' obsolete local membership/cache admission section with delivery by Model/identity/stamp, application-owned reclamation and the retained legacy global recovery exception. State that unsubscribe and Remove never delete Models, while newer viewer Loader null is canonical absence. Update N8 wording so server tracking remains enrollment/invalidation routing without implying cache retention by client holds. Document exact migration marker, supported original layouts, atomic refusal, preserved pending/frozen work and inert reconciliation fields. Deployment steps are: stop old writers/live sessions, apply prior layout upgrades, run idempotent local-authority PostgreSQL repair, deploy coordinated authority-capable server/SDKs and admission, then resume traffic; never rewind/reset cursors or reinterpret old Remove as null.

Use existing `website/docs/frontend/sync.md` onStore transaction example infrastructure to show a small current access-information Model hook cleaning application-defined stale cache while preserving local work. If the existing example cannot express this safely without new Models/compiler infrastructure, keep the explanation and reuse its current hook snippet; do not expand this change into a new framework feature. Explain cache presence is not permission and a later canonical upsert may repopulate application-cleared cache under existing stamps. No changes to Most Days or its CAP-799 behavior.

- [ ] **4.4 Run the assembled gate and final independent review.** The root runs the following from the shared worktree, reads the complete output, and fixes failures through diagnosis/RED/GREEN rather than weakening assertions:

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
bash scripts/test.sh
git diff --check
git status --short
```

The host script performs dependency setup/build, Rust/language lint and tests, real PostgreSQL persistence, generated APIs, end-to-end Action/Load paths, docs examples and installed npm verification. Record actual commands/results and infrastructure limitations; the plan does not claim they were executed. Commit fixtures/docs with `test: verify authority-only delivery across runtimes` before review. Have an independent reviewer assess the final assembled baseline..HEAD diff against every required-evidence bullet in the approved design, focusing on old saved replay, corrupted-layout rollback, legacy absence, hook/cursor atomicity and historical repair. Address actionable findings, rerun only affected checks unless changes justify another assembled gate, and commit any fixes before their independent review. Report branch/commits and evidence to the user; do not merge or publish.

## Acceptance checklist

- [ ] Multi-Stream authority shares one record; unsubscribe/removal never reclaims it automatically.
- [ ] Fresh Stream/Push/Action/Load delivery requires neither holding table nor claims.
- [ ] Newer stamped null wins across paths; older positive cannot resurrect it.
- [ ] Hooks, authority and progress share rollback; explicit bootstrap/cancellation/fences still work.
- [ ] Original Channel/Scope/Stream real files reopen in place with all work, stamps and cursors; malformed files remain untouched.
- [ ] Legacy evicted/token work settles safely and canonical absent never becomes evicted/restorable.
- [ ] Historical repair re-tracks pairs, publishes newer current authority, preserves other viewers and saved business bytes, and repeats with zero allocation.
- [ ] Old capability is refused before effects; new decoration preserves logical identity and saved nested business JSON.
- [ ] Runtime, simulation, PostgreSQL, JS/Dart and generated-boundary evidence is recorded; full host gate and independent final review are complete.
