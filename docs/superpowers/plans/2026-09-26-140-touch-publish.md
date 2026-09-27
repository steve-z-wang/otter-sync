# Channel Membership and Explicit Results Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking. Use implementation subagents only when the assigned task authorizes delegation; the user's preferred implementation model is Sol. This preparation session writes documents only.

**Goal:** Implement persistent Channel membership with automatic distribution of declared changes, independent explicit business results, and automatic input-target reconciliation.

**Architecture:** Rust separates changed records, required input authority and explicit outputs. A shared settlement module serializes record/membership decisions inside the application's transaction, while PostgreSQL persists relationships and cursor positions. The backend SDK collects typed synchronous intents through Channel handles and touch methods.

**Tech Stack:** Rust compiler/core/server/simulation, generated TypeScript/Dart clients, TypeScript backend SDK, PostgreSQL persistence with pg/prisma/drizzle adapters.

## Global constraints

- The [spec](../specs/2026-09-26-140-touch-publish-design.md) is authoritative. Earlier versions of these files and issue comments are superseded. Keep work isolated; start from current main with this planning branch's documents.
- Concrete proposed API: `ctx.channel(name).todo.add/remove(identity)`, mixed `channel.add/remove(RecordRef[])`, and `ctx.touch.todo(identity)`. No public publish/attach/detach/changes collector aliases.
- Inputs and explicit outputs are independent, including same-name fields. Never fill a missing handler output from the input. All new source outputs are explicit.
- Changed records = inferred inputs union explicit touches; mandatory caller authority = input targets. Extra touches are not automatic caller authority. Explicit output storage follows the existing store policy.
- One changed-record stamp per settlement; one publication position per distinct affected Channel/record pair. Saved-call replay performs neither again.
- Persist membership separately from invalidations. Removal does not evict client rows; filter removed membership before pagination LIMIT in both server pull modes.
- SDK intent declarations are synchronous and callback-scoped. Rust owns settlement; SQL remains in packages/postgres. Do not put membership fan-out in SDK loops.
- Preserve operation isolation, authorization on actual reads, bounded PostgreSQL transaction retries, after-commit wakes, Bootstrap origin/barrier, and client completion after committed authority application.
- No runtime compatibility facade or production backfill: this is a coordinated prelaunch upgrade. Do not weaken history validation; rebuild only reviewed repository fixture baselines/evolution fixtures.
- One implementation PR with reviewed checkpoints below. Do not call a passing compiler-only checkpoint evidence that server membership or delivery is correct.

## Start and file ownership

- [ ] Read AGENTS.md, the issue and comments, spec, [testing strategy](../../engineering/testing/strategy.md) and [running guide](../../engineering/testing/running.md). Inspect current main and other active work before editing. Preparation baseline was `9cfb0b8`; re-evaluate changed owning files if main has moved.
- [ ] Install/build only for implementation: `npm ci` and `bash scripts/build.sh`. Use the documented Rust 1.98.1, Node 26.4.0, Dart 3.12.1 and PostgreSQL 16 environment. Capture any baseline failure separately from regressions.

| Area | Files |
| --- | --- |
| Source output contract | `crates/compiler/src/{validate,generate,emit,history}.rs`, `crates/compiler/tests/compiler.rs` |
| Descriptor/runtime compatibility | `crates/core/src/actions.rs`, `crates/core/tests/{contracts,compatibility}.rs` |
| Shared settlement | new `crates/server/src/settlement.rs`; `crates/server/src/{lib,actions,readback,action_results,host}.rs` |
| Persistence | `packages/postgres/migration.sql`, `packages/postgres/src/{sql,persistence}.mts`, `packages/server/host-contract.mts` |
| SDK intent lifecycle | new `packages/server/effects.mts`; `packages/server/index.mts` |
| Rust validation | `crates/server/tests/{host_contract,readback,stamp,actions,action_store,bootstrap}.rs`; new `crates/server/tests/membership.rs` |
| Simulation | `crates/sim/src/host.rs`, `crates/sim/tests/{distribution,bootstrap,authority,resilience}.rs` |
| Real database | `integration/persistence/server/{driver-conformance,runtime,host-contract,actions}.test.mjs`, `run.sh`; new `effects.test.mjs` and `membership.test.mjs` |
| Generated/client end-to-end | `integration/action-contract`, `integration/action-runtime-ts`, `integration/action-runtime-dart`, `integration/generated-api`, `integration/action-e2e`, `integration/e2e/bootstrap.test.mjs` |
| Lasting docs | compiler/schema actions, backend interface, server engine/persistence, guarantees, server typed API, website backend and frontend operation guides |

Only the files listed as new need to be created. Reuse existing test host/setup utilities; do not invent a second production settlement path to simplify fixtures.

## Checkpoint 1: Explicit source outputs and generated results

**Consumes:** schema operation inputs and explicit output AST. **Produces:** new descriptors whose outputs contain only handler-value/handler-identity sources, separate generated handler/result types, and unchanged input operand descriptors.

- [ ] Add compiler tests compiling these two operations in the existing fixture harness:

```axton
mutation Edit(todo Todo.update)
mutation EditAndRead(todo Todo.update) {
  todo Todo
}
```

Assert Edit has an empty output descriptor and no result.todo field; EditAndRead accepts overlapping input/output names and its output source is `handlerIdentity`, not `inputIdentity`. Add delete, optional/list input and scalar-result variants. Assert duplicate outputs still fail independently of duplicate inputs.
- [ ] Run `cargo test -p axton-compiler --test compiler --locked`; confirm failures are the implicit output injection/name collision. In validate.rs remove output generation and output-name reservation from the Model-input branch; preserve input validation/inference data. Keep parsing unchanged. Generate handler identity types for every explicit Model output, including those sharing input names.
- [ ] Add TS/Dart positive/negative assertions: Edit returns no business value, EditAndRead exposes a loaded Todo result, and the handler must provide todo identity. This concrete TS handler result must type-check:

```ts
const output: EditAndReadHandlerOutput = { todo: { id: "B" } };
// @ts-expect-error explicit output is required even though input has the same name
const missing: EditAndReadHandlerOutput = {};
```

Use the actual generated fixture export names after adding the operation. Do not manually edit generated files instead of updating schema/history/emitters.
- [ ] Regenerate repository prelaunch fixture histories intentionally. For retained-version fixtures, rebuild each source version in order so unrelated compatibility tests remain meaningful. Keep history.rs rejecting incompatible external retained outputs at an unchanged version; add a regression asserting the existing diagnostic. Do not globally strip old output entries on history load. Existing InputIdentity decoder branches can remain for historical descriptor tests but no newly compiled source may produce them.
- [ ] Run compiler/core tests and the contract checks:

```sh
cargo test -p axton-compiler -p axton-core --locked
cargo run -p axton-compiler --locked -- compile integration/action-contract integration/action-contract --backend-runtime ../../packages/server/index.mts --client-runtime ../../packages/client-js/index.mts
node_modules/.bin/tsc -p integration/action-contract
dart pub get --directory integration/action-contract
dart analyze integration/action-contract/generated.dart
dart analyze integration/action-contract/positive.dart
bash integration/action-contract/check-negative.sh
```

Review generated output and history diffs for accidental contract changes, then commit. Other integration consumers migrate at Checkpoint 6; list their pending generated-result adjustments explicitly.

## Checkpoint 2: Persistent membership and record serialization primitives

**Consumes:** canonical Model/identity and the existing application transaction. **Produces:** `lockRecord`, `memberships`, `setMembership` host operations; membership table/indexes; real adapter and simulation support. Scan filtering is enabled at Checkpoint 5, after callers enroll records.

- [ ] Add typed operations in Rust host.rs and the TS host contract. Wire shapes are:

```ts
{ op: "lockRecord", model, identityKey } // -> number | null
{ op: "memberships", model, identityKey } // -> string[]
{ op: "setMembership", channel, model, identityKey, present } // -> null
```

Validate safe positive stamps, unique valid Channel names, required boolean present and unknown fields. Add cases to the existing shared host conformance fixtures and mock-host exhaustive matches.
- [ ] Add the spec's table and two index orders to migration.sql. In sql.mts add these statements alongside existing stamp operations:

```sql
UPDATE axton_record SET stamp=stamp
 WHERE model=$1 AND identity_key=$2 RETURNING stamp;
SELECT channel FROM axton_membership
 WHERE model=$1 AND identity_key=$2 ORDER BY channel;
INSERT INTO axton_channel(channel,head) VALUES($1,0)
 ON CONFLICT(channel) DO NOTHING;
INSERT INTO axton_membership(channel,model,identity_key)
 VALUES($1,$2,$3) ON CONFLICT DO NOTHING;
DELETE FROM axton_membership
 WHERE channel=$1 AND model=$2 AND identity_key=$3;
```

Use named constants in sql.mts; persistence.mts binds parameters and maps typed answers. Record metadata must already exist for insertion. Do not increment a Channel head for membership deletion or idempotent insertion.
- [ ] Extend driver-conformance with per-adapter assertions: add/list/remove, duplicate insert/delete, no head increment without publish, absent lock returns null, lock preserves stamp, membership survives a new transaction, rollback restores relationships. Add a real RR test proving a membership-only writer's no-op record UPDATE makes a competing stale-snapshot writer retry. A lock-only mock does not establish this property.
- [ ] Extend the simulation host state with membership sets included in savepoint/transaction snapshots and operations. Keep cursor/stamp fields distinct. Use canonical identities and sorted answers as production does.
- [ ] Run `cargo test -p axton-server --test host_contract --locked`, `cargo test -p axton-sim --locked`, rebuild native artifacts, then `bash integration/persistence/server/run.sh`. Inspect all three adapters' conformance results; commit the tested additive storage/host capability.

## Checkpoint 3: Rust settlement and caller-authority separation

**Consumes:** changed/input sets, ordered membership intents and the primitives above. **Produces:** one shared settlement engine, mandatory input-only readback, and strict `{changes,memberships}` host effects for modern/legacy/external paths.

- [ ] Introduce `MembershipIntent {channel, model, identity, present}` and replace handled effects' `publications` field with `memberships`. Keep action outputs and refusal/error variants. Update all host producers and test fixtures to the new shape, including the temporary SDK collector serialization until Checkpoint 4 replaces its public API. There must be no implicit publish-all fallback. Reject forged Query changes or memberships.
- [ ] In settlement.rs define the shared boundary (concrete types use existing canonical keys):

```rust
pub(crate) async fn settle_changes(
    config: &Config,
    changed: &Changes,
    memberships: &[MembershipIntent],
    host: &impl Host,
) -> Result<BTreeMap<String, u64>>;
```

Return allocated stamps for changed records. Validate every reference with backend schema/Loader registration, without requiring a caller's read-version map. Reduce membership intents by pair to final desired state, retain record keys, then process the union in canonical order. Changed: advanceStamp. Nonchanged with final add: ensureStamp. Remove-only: lockRecord; null is a no-op. Read initial membership only after its record guard. After all guards, calculate final memberships and net new pairs; create/apply membership changes in sorted Channel/key order. Publish the union of changed-record/final-member pairs and unchanged/net-new-member pairs once each, sorted by Channel/key. No application SQL lives in this function.
- [ ] Separate `input_targets` from `changed` in actions.rs and legacy push. Call shared settlement with their union plus extras. Read back only input_targets using the already allocated stamps. Remove stamping/publication responsibilities from read_back; do not accidentally allocate a second stamp there. Feed those input records into action_results.rs and retain explicit output storage/Loader semantics. External settlement calls shared settlement without any client readback.
- [ ] Add named server regressions in membership.rs and actions.rs with these exact observable outcomes:

| Test case | Assertions |
| --- | --- |
| input A, same-name output B | receipt contains A; result.todo is B; omitted output fails, never substitutes A |
| no declared outputs | wire result null, input authority exists, SDK void result |
| extra Project touch, caller declares only Todo | success, Project stamp/fan-out advance; Project Loader is not invoked as caller and Project is absent from caller authority |
| touched Project also requested as output | actual output read checks version/auth and failure rolls back mutation |
| duplicate/inferred touch | one stamp, not two |
| changed member in A/B | one record stamp and one new position per Channel |
| newly added and changed | one position at final stamp, regardless touch/add declaration order |
| membership operations cancel | compare initial/final membership; no spurious publication |
| output-only/unchanged enrollment | existing record stamp unchanged |
| saved call replay | identical saved result, unchanged stamps/heads/relationships |

- [ ] Update store regressions: false suppresses output-only storage, never mandatory input authority; pure extra touches do not force storage. Preserve operation-level Loader failure rollback and adjacent-call progress. Run:

```sh
cargo test -p axton-server --test membership --test actions --test action_store --test readback --test stamp --test host_contract --locked
cargo test -p axton-sim --locked
```

Review modern, legacy and external paths together and commit. Note that the public SDK spelling is finalized in the next checkpoint, not a second supported API.

## Checkpoint 4: Generated Channel handles and touch collector

**Consumes:** schema identity descriptors and typed membership intents. **Produces:** callback-scoped public API and owned effect payloads.

- [ ] Create effects.mts with a single shared record-reference definition/symbol ownership where needed by other internals, avoiding import cycles. Its collector interface is:

```ts
interface EffectCollector {
  touch: RuntimeTouch;
  channel(name: string): RuntimeChannel;
  seed(record: RecordRef): void;
  settlement(): {
    changes: RecordRef[];
    memberships: MembershipIntent[];
  };
  close(): void;
}
```

RuntimeTouch and RuntimeChannel are runtime dictionaries for the spec's generated methods. `createEffects(models)` accepts existing configured Model descriptors. Seed stays private for legacy operands; modern inference stays Rust-owned. All exported methods assert the collector remains open. Snapshot identities using schema identity codecs at each call, not at settlement. Channel selection validates a nonblank name but creates no durable state.
- [ ] Add effects.test.mjs and include it in run.sh. Core declaration example to assert:

```ts
const identity = { id: "A" };
const channel = effects.channel("project:1");
channel.todo.add(identity);
identity.id = "B";
effects.touch.todo({ id: "A" });
assert.deepEqual(effects.settlement(), {
  changes: [{ model: "Todo", identity: { id: "A" } }],
  memberships: [{ channel: "project:1", model: "Todo", identity: { id: "A" }, present: true }],
});
```

Build `effects` with a real Todo schema descriptor in the test. Add Date/composite identities, empty arrays, missing/null references, unknown Model, raw untagged mixed identity rejected, a mixed call with a later invalid element appends no intents even if caught, explicit constructor accepted, safe `__proto__` property and closed escaped handles. SDK preserves membership declaration order; final-state reduction is tested in Rust, not duplicated here.
- [ ] Wire createEffects into modern/legacy/external callbacks. Close in finally on success and failure; keep settlement data retrievable after close without allowing mutation. Query contexts remain tx/userId/callId only. Remove public Changes/Publish and implicit publication helpers; external arbitrary return values are untouched.
- [ ] Generate Touch, Channel, ModelMembership and concrete context aliases in emit.rs; do not re-export the broad raw runtime MutationContext as the final application type. Type the generated external transaction callback too. Narrow named reference constructors to their discriminated Model identity variants. Diagnose duplicate lower-first keys and Channel reserved keys add/remove at compile time and reject malformed hand-authored runtime configs likewise.
- [ ] Add generated positive/negative type examples in action-runtime-ts/types.ts and generated-api fixtures:

```ts
ctx.channel("project:1").todo.add({ id: "A" });
ctx.channel("project:1").todo.remove({ id: "A" });
ctx.touch.todo({ id: "A" });
// @ts-expect-error missing identity
ctx.channel("project:1").todo.add({});
// @ts-expect-error old API is gone
ctx.publish({ channel: "project:1" });
// @ts-expect-error Query has no membership writer
queryCtx.channel("project:1").todo.add({ id: "A" });
```

Use the existing Todo/Moment fixture and add a composite-identity compile case. Test no function/Model property collision tricks are needed. The backend is TS; do not invent a Dart server runtime, but regenerate generic Dart contracts and frontend result types.
- [ ] Run focused collector tests, compiler tests, `bash integration/action-runtime-ts/verify.sh` and `bash integration/generated-api/verify.sh` after rebuilding native artifacts. Review generated signatures and runtime parity; commit.

## Checkpoint 5: Removal, pagination and real concurrent delivery

**Consumes:** enrolled relationships and shared fan-out. **Produces:** membership-filtered delta/Bootstrap and proven transaction ordering.

- [ ] Change SQL.SCAN to filter membership BEFORE ORDER/LIMIT while retaining the left join that diagnoses missing record stamps:

```sql
SELECT i.channel,i.cursor,i.model,i.identity_key,i.identity,r.stamp
FROM axton_invalidation i
JOIN axton_membership m
 ON m.channel=i.channel AND m.model=i.model AND m.identity_key=i.identity_key
LEFT JOIN axton_record r
 ON r.model=i.model AND r.identity_key=i.identity_key
WHERE i.channel=$1 AND i.cursor>$2
ORDER BY i.cursor LIMIT $3;
```

Mirror the predicate in simulation/test hosts. Keep removed invalidation rows and heads. Update fixtures to enroll records before low-level publication; no automatic membership backfill from old rows. Review both lib.rs delta progression and loading.rs Bootstrap terminal logic against filtered rows, rather than adding an SDK workaround.
- [ ] Add server/real-PG cases: all remaining rows removed yields a terminal advancing page; removed rows exceeding a page do not starve later active rows; remove then touch elsewhere cannot expose current content through the old Channel; re-add gives a fresh position; move above Bootstrap origin is covered by live barrier; a deletion retains membership and yields null; same-identity recreation distributes again; explicit removal keeps existing client rows.
- [ ] Create membership.test.mjs using the existing temporary PG runner infrastructure and include it in run.sh. Coordinate concurrency with barriers/latches, not sleeps: establish transaction snapshots before letting competing operations execute. Assert both possible valid serialization orders for touch/add and touch/remove; the committed outcome must match one complete order, with no missed enrolled update. Include an initial absent record and membership-only changes whose record stamp remains unchanged. Inspect retries rather than claiming SELECT FOR UPDATE alone proves freshness.
- [ ] Verify rollback of business writes, relationships, versions, heads, saved outcomes and wake sets. Verify a later subscriber read failure is isolated from the already committed mutation. Extend sim distribution/bootstrap tests with generated add/remove/touch sequences and restart/duplicate delivery scenarios using existing harness conventions.
- [ ] Run:

```sh
cargo test -p axton-server --test membership --test bootstrap --test live --locked
cargo test -p axton-sim --test distribution --test bootstrap --test resilience --locked
bash integration/persistence/server/run.sh
```

Review actual PostgreSQL race assertions and page progress, then commit.

## Checkpoint 6: User-visible workflows, documentation and final acceptance

**Consumes:** all preceding contracts. **Produces:** migrated first-party examples, end-to-end proof, reviewed PR.

- [ ] Update action-e2e source fixtures and handler implementations. Create joins its Channel once; later update/delete handlers need no enrollment call. Retitle reports explicit extra touches. Callers needing result.todo explicitly declare that output and handlers return its identity. Callers needing only local synchronization await completion and inspect the local model instead. Preserve intentional scalar-only mutations and read-only Queries.
- [ ] Add an end-to-end operation editing A while explicitly returning B. Check local A corrected after completion, result.todo contains B's snapshot, and no duplicate stamp occurs. Add a no-output edit whose call completes with reconciled local A, and a touch-only extra Model unknown to the initiating client's descriptor that is delivered to a different authorized subscriber. Test both direct and durable paths, immutable retry results and output `store: false`.
- [ ] Regenerate TS/Dart/RN consuming fixtures and run:

```sh
bash integration/action-runtime-ts/verify.sh
bash integration/generated-api/verify.sh
bash integration/action-e2e/run.sh
```

Use scripts/test.sh's existing Dart setup/commands for action-runtime-dart. Keep frontend runtime ownership and call completion logic intact unless a concrete regression shows a required contract adjustment; do not rewrite the client engine for this backend feature.
- [ ] Search active code/docs with `rg` for `changes.add`, `changes.records`, `publish(`, `inputIdentity`, and implicit result assumptions. Migrate application examples, host producers and generated fixtures; retain only intentionally tested legacy descriptor decoding and historical task documents. Do not globally rename protocol change arrays or low-level HostRequest::Publish.
- [ ] Update `docs/engineering/architecture/schema/actions.md`, server `backend-interface.md`, `engine/README.md`, `engine/publish.md`, `engine/pull.md`, `persistence.md`, `sdks/typed-api/server.md`, compiler generating/history guidance, and `docs/engineering/guarantees.md`. Update website backend API/setup/database and operation result examples discovered by the search. Explain input authority vs result, no same-name binding, store policy, callback lifetime, per-Channel membership vs authorization, deletion/recreation, removed-history filtering and Bootstrap's refined coverage. Link named evidence, not test counts alone.
- [ ] Review the whole diff against every acceptance item in spec section 8. Confirm all three paths (modern, legacy, external) use one settlement algorithm; all actual reads keep auth/version checks; metadata guard writes do not accidentally bump stamps; idempotent member changes do not manufacture cursor events; and migrations never infer membership from historical publications.
- [ ] Run `bash scripts/test.sh` with the documented prerequisites. Record executed commands, failures/fixes and limits. Repeat only affected checks after subsequent changes, then ensure required CI passes on the final PR head. Device testing and performance claims require separate evidence; do not invent it.
- [ ] Open one PR with `Closes #140`, spec/plan links, intentional API/schema changes, and validation. Attach the PR to the assigned task when supported. Review the final diff and resolve findings before handing back the PR. Merge only under authorization in the assigned implementation task; this handoff itself does not introduce new merge permission.

## Coverage map and preparation evidence

| Spec | Checkpoints |
| --- | --- |
| Public handles, snapshots, generated types, context lifetime | 4, 6 |
| Explicit outputs, independent same-name identities, delete/optional/list | 1, 3, 6 |
| Mandatory input authority, extra-touch separation, store/version rules | 3, 6 |
| Persistent membership, final-state reduction, one stamp and pair | 2, 3, 5 |
| Deletion/recreation, removal filtering, Bootstrap progress | 5, 6 |
| Real transaction concurrency, rollback, replay, after-commit wake | 2, 3, 5, 6 |
| Prelaunch fixture/history handling without weaker compatibility checks | 1, 6 |

Preparation is source inspection and document review only. The plan does not report runtime test results. The receiving agent must capture its own baseline and implementation evidence. The [handoff](2026-09-26-140-touch-publish-handoff.md) supplies the assignment and planning branch.
