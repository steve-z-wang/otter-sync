# Scope Membership API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the approved Scope, label editing and bulk selection API with a complete Scope vocabulary and lossless coordinated wire/storage cutover.

**Architecture:** The typed SDK declares ordered server effects. The Rust server validates identities/predicates and reduces final membership/label state; PostgreSQL reads candidate sets and applies final deltas in batches inside the application transaction. Clients consume existing identity-level enrollment/removal evidence and expose Scope subscription terminology.

**Tech Stack:** Rust, generated TypeScript/Dart, Node SDK, native SQLite, PostgreSQL adapters, Node tests, Rust simulation and real-boundary e2e.

## Global Constraints

- The governing spec is `docs/superpowers/specs/2026-09-30-scope-membership-api-design.md`.
- Rename APIs, generated types, implementation concepts, protocol fields, database tables/columns and living documentation to Scope. No Channel public aliases or old wire-field acceptance; old names occur only in explicit legacy migration inputs and dated history. Never rename application-owned Model names/fields/arguments or external standard/dependency APIs such as `std::sync::mpsc::channel`; those transport mechanisms are not the framework Scope abstraction.
- Preserve v0.2.0 removal delivery, enrollment claims, local hold accounting and request fences. No tags, predicates or SQL travel to clients.
- Tags are labels, not grants or retention sources. Removing the last tag leaves membership present.
- Standalone label add requires current membership; missing membership fails and rolls back its enclosing operation. Chained add/tag first ensures membership.
- Scope removal withdraws membership, never deletes a business row, runs `onStore` or triggers a schema cascade.
- Preserve the viewer Loader as the permission authority and Models without Loaders as device-only.
- A Load may only add membership/labels for its returned identities. No removal, where, touch or tag-wide detachment; saved replay performs no effects.
- Single/list typed/mixed operands, complete composite identities, immutable capture and callback-bound handles must agree.
- Predicate limits: depth 16 (root depth 1), 128 nodes, 64 distinct labels per leaf operator, 65,536 UTF-8 JSON bytes. Reject empty/malformed predicates and unknown keys; `only: []` is valid.
- Labels are nonblank, case-sensitive opaque strings of at most 256 UTF-8 bytes; at most 64 distinct labels per label operation.
- Use only the canonical Scope API in the final branch; remove legacy Channel facades, Model-first membership access and tag-selector removal.
- Enforce `scope-membership-v1` and reject old clients before handler/progress effects. Forward-migrate PostgreSQL/SQLite state without wiping records, subscriptions, cursors, pending/frozen work or saved outcomes. Do not bump packages, merge, publish or alter Most Days.
- Work only in this isolated branch. Use test-first changes, task reviews and a final whole-branch review. Record evidence, limitations and completed tasks in `.superpowers/sdd/progress.md`.

## File responsibilities and execution order

| Unit | Responsibility |
| --- | --- |
| `crates/server/src/scope_predicate.rs` | Predicate validation, set matching and conservative candidate collection. |
| `crates/server/src/channel_members.rs` (renamed to `scope_members.rs` in Task 3) | Ordered membership/label reduction and final deltas. |
| `crates/server/src/host.rs`, `packages/server/host-contract.mts` | Shared declaration/read shapes; renamed together at the Scope protocol cutover. |
| `crates/server/src/settlement.rs`, `loads.rs` | Lock/guard/read/reduce/apply flow, claims and returned-identity admission. |
| `packages/postgres/src/persistence.mts`, `sql.mts` | Set-based candidate reads and batched final-state persistence. |
| `packages/server/scope.mts` | Canonical Scope handles and predicate/operand declaration helpers. |
| `packages/server/effects.mts`, `index.mts` | Shared collector lifetime/accounting and context exposure; legacy facades. |
| `crates/compiler/src/emit_scope.rs`, `emit.rs`, `emit_loads.rs` | Generated canonical Scope and restricted LoadScope types as the only canonical membership types. |
| JS/Dart runtime and generated transaction surfaces | Canonical local `scopes`; old facades removed in Task 3. |
| Living guides, example, architecture and integration scenarios | Observable API and delivery behavior; old names confined to migration/deployment instructions. |

Tasks run sequentially with fresh implementer and reviewer gates. The controller can inspect architecture, prepare evidence and run independent environment checks while an implementer works. Do not run concurrent implementation agents against this checkout.

### Task 1: Server label and selection effects

**Files:**
- Create: `crates/server/src/scope_predicate.rs`
- Modify: `crates/server/src/channel_members.rs`, `host.rs`, `settlement.rs`, `loads.rs`, `lib.rs`
- Modify: `packages/server/host-contract.mts`, `packages/postgres/src/persistence.mts`, `packages/postgres/src/sql.mts`
- Test: `crates/server/tests/host_contract.rs`, `crates/server/tests/membership.rs`, `crates/server/tests/loads.rs`, module tests and necessary simulation-host request adapters
- Test: `integration/persistence/server/host-contract.test.mjs`, `integration/persistence/server/channel-tags.test.mjs`

**Interfaces:**
- Consume: existing `ChannelIntent`, `Declaration`, `MemberState`, `MemberDelta`, `ReadChannelMembers` and final-state adapter, whose baseline wire/storage names are changed in Task 3.
- Produce: `ScopePredicate` matching the spec's optional `tags`, `and`, `or`, `not` JSON shape; strict validate and `matches(tags)` behavior.
- Produce these ordered host intents, temporarily using the baseline `channel` host-contract field until Task 3 renames it to `scope`: `tagAdd {channel, record, tags}`, `tagRemove {channel, record, tags}`, `detachTags {channel, tags}`, `select {channel, model?: string, predicate, action}`.
- Selection actions: `{kind:'remove'}`, `{kind:'tagAdd',tags:string[]}`, `{kind:'tagRemove',tags:string[]}`. Model filtering is optional, validates a registered Model/Loader, and does not alter identity matching.
- Add an optional `all` read flag, default false and omitted on serialization when false for baseline request-fixture round trips, to `ReadChannelMembers`. A true flag reads all present members; explicit keys and tag candidates continue to select a union. Retain complete tag snapshots.
- New record label intents expose their record to identity resolution/guarding. Only actual `add` intents grant enrollment claims; a label edit creates no inferred enrollment.

- [ ] **Step 1: Add failing behavioral tests before production edits.** Extend the reducer's existing `key/member/tags` helpers with tests like:

```rust
let initial = vec![member("A", &["X", "Y"]), member("B", &["X"]), member("C", &["Y"])];
let predicate: ScopePredicate = serde_json::from_value(json!({"tags":{"only":["X"]}})).unwrap();
let declarations = vec![
    Declaration::Select { model: None, predicate, action: SelectionAction::Remove },
    Declaration::DetachTags { tags: tags(&["X"]) },
];
let actual = reduce("U", initial, &declarations, &BTreeSet::new()).unwrap();
assert_eq!(actual, vec![delta("A", true, &["Y"], false), delta("B", false, &[], true)]);
```

Add separate assertions for missing-membership tagAdd failure, last-tag detach preserving membership, no-op label edits, model filtering, additional Z labels, each boolean predicate and each validation bound. Extend saved Load cases to reject tag edits of an identity absent from output and reject remove/select/detach effects. Use existing real-host test fixtures for raw new intent JSON until the canonical collector is available in Task 2.

- [ ] **Step 2: Verify RED.** Run `cargo test -p axton-server --lib --locked` and focused host/membership/Load tests. A new type/variant compilation failure establishes the missing interface; then check the newly implemented interface still leaves behavioral failures before completing its reducer. Capture commands and relevant output in the task report.
- [ ] **Step 3: Implement predicate validation and pure reduction.** Use structured predicates and set operations:

```rust
// All sibling conditions intersect; present membership remains independent of labels.
let matches_all = leaf.all.as_ref().is_none_or(|wanted| wanted.iter().all(|t| tags.contains(t)));
let matches_any = leaf.any.as_ref().is_none_or(|wanted| wanted.iter().any(|t| tags.contains(t)));
let matches_none = leaf.none.as_ref().is_none_or(|wanted| wanted.iter().all(|t| !tags.contains(t)));
let matches_only = leaf.only.as_ref().is_none_or(|wanted| wanted.iter().collect::<BTreeSet<_>>() == tags.iter().collect());
```

Validate the complete predicate before evaluating it. TagAdd mutates a present tag set or returns the same settlement error class as other invalid publication declarations; tagRemove/detach remove associations only. Select evaluates current tags and optional Model at that declaration position, then applies its action. Reuse final-delta reduction and preserve existing RemoveTag semantics until Task 3 removes the obsolete variant.

- [ ] **Step 4: Connect settlement, candidate reads and Load admission.** Collect explicit records plus every referenced label across all declarations. A conservative all-scope read is required for predicates that may match untagged records (`none`, `not`, `only: []`); positive predicates can read the union of referenced-label candidates and explicit keys. Candidate coverage must include records changed by earlier declarations, and validation of the host response must agree with the read mode. Preserve Scope-before-record lock ordering and stamp initialization rules. `applyChannelMembers` already applies exact final label snapshots in batches; reuse it without per-association SQL. Add `$4` boolean all-mode to the candidate SQL and default absent host flags to false. Preserve Load pair/byte accounting for add plus label-add and reject the other variants before effects.
- [ ] **Step 5: Verify GREEN and real storage.** Run `cargo test -p axton-server --locked`, `cargo test -p axton-sim --locked`, `npm run typecheck`, and `bash integration/persistence/server/run.sh`. New real-PG assertions must show atomic rollback, unchanged stamps/log/head for label-only changes, exact-only-X retention of X/Y and X/Z, all-mode untagged selection, declaration order and host adapter defaults. A known later-task public API absence is not a failing baseline; these tests exercise raw host effects.
- [ ] **Step 6: Self-review, commit and report.** Commit only Task 1 files, record RED/GREEN, SQL batching, concurrency coverage and any concrete limit, then return the commit range for review.

### Task 2: Canonical backend Scope API and generated types

**Files:**
- Create: `packages/server/scope.mts`, `crates/compiler/src/emit_scope.rs`
- Modify: `packages/server/effects.mts`, `packages/server/index.mts`, `crates/compiler/src/emit.rs`, `crates/compiler/src/emit_loads.rs`, `crates/compiler/src/lib.rs`
- Test: `integration/persistence/server/effects.test.mjs`, `integration/action-contract/positive.ts`, `negative.ts`, `integration/action-runtime-ts/backend.test.mts`, compiler generation tests
- Regenerate affected checked-in compiler fixtures, without hand-editing generated outputs.

**Interfaces:**
- Consume: Task 1 `tagAdd/tagRemove/detachTags/select` host shapes and predicate bounds.
- Produce: `ctx.scope(name)`, `scope.add/remove.<model>(oneOrMany)`, mixed callable add/remove, `AddDeclaration.tag(stringOrTags): AddDeclaration`, `scope.tag(stringOrTags)` typed/mixed label editor, `scope.where(predicate)`/`where.<model>(predicate)` selection and selection-label editor.
- Produce: callable typed/mixed `touch`, restricted `LoadScope`, and `scope` on Mutation, legacy handler, host transaction/publication and Load contexts only.
- Preserve callback lifetime, identity validation and shared settlement; old Channel surfaces are removed in Task 3.

- [ ] **Step 1: Write failing collector and generated contract tests.** Add an observable ordered-effects test:

```js
const effects = fresh();
const scope = effects.scope('U');
scope.add.todo(['A', 'B']).tag(['X', 'Y']);
scope.tag('X').remove.todo('A');
scope.where({tags:{only:['X']}}).remove();
assert.deepEqual(effects.settlement().memberships.map(x => x.kind),
  ['add', 'add', 'tagAdd', 'tagAdd', 'tagRemove', 'select']);
assert.deepEqual(effects.settlement().changes, []);
```

Positive contracts must compile scalar/list/composite/mixed calls, typed Model filtering and restricted Load add/tag. Negative contracts must reject wrong identities, untyped mixed references, Load remove/where/detach/touch, Query/Loader scope, root no-argument add/remove, tag editor no-argument add and selection add. Add runtime schema cases for Models `Tag`, `Name`, `Length`, `Call` and `Prototype`; callable namespaces must work despite function built-ins. Assert deep captured operands/predicates do not change when caller inputs mutate, invalid mixed operands declare no prefix, and handles refuse calls after callback closure.

- [ ] **Step 2: Verify RED.** Run `node --test integration/persistence/server/effects.test.mjs` and the compiler/action-contract runner from `scripts/test.sh`; record expected missing `scope` and type failures.
- [ ] **Step 3: Implement canonical handles around the shared collector.** Keep the shared collector until Task 3 removes its obsolete Channel facade; move new handle construction to the dedicated module. Each add records its effect before returning; its handle snapshots references and only exposes `.tag`. Each `.tag` appends TagAdd intents at invocation time. The standalone tag editor emits label-only intents. Selection builders are pure; terminal actions snapshot/validate predicate, optional Model and action and append Select. Build typed function namespaces using property descriptors/own-property maps rather than treating Function `name/length/prototype` as reserved Model names. Share operand normalization/Loader guards and poison/callback lifetime rules with the collector; do not implement a second settlement engine.

```ts
export interface AddDeclaration { tag(labels: string | readonly string[]): AddDeclaration; }
export type ScopePredicate = {
  readonly tags?: { readonly all?: readonly string[]; readonly any?: readonly string[];
    readonly none?: readonly string[]; readonly only?: readonly string[] };
  readonly and?: readonly ScopePredicate[];
  readonly or?: readonly ScopePredicate[];
  readonly not?: ScopePredicate;
};
// One generated Model method (the mixed callable namespace uses RecordRef):
type TodoAdd = (ids: string | TodoIdentity | readonly (string | TodoIdentity)[]) => AddDeclaration;
```

Do not accept empty label scopes, return a lazy unexecuted add, or fold late chained tags retroactively into earlier declarations. Standalone label add does not enroll. Copy/freeze caller data and reject malformed predicates before recording.

- [ ] **Step 4: Generate the complete and restricted APIs.** Emit canonical interfaces in `emit_scope.rs` and call it from the backend emitter. Emit `LoadScope` with add and label-add only, allowing only explicit record operands. Add canonical scope properties to all relevant runtime context assemblers; leave Queries/Loaders without them. `touch` accepts scalar/list Model operands and single/list mixed references while preserving identity-object calls. Regenerate all compiler fixtures through their existing runners.
- [ ] **Step 5: Verify GREEN.** Run collector tests, `cargo test -p axton-compiler --locked`, `npm run typecheck`, action-contract TypeScript positive/negative checks, `bash integration/action-runtime-ts/verify.sh`, `bash integration/generated-api/verify.sh`, and persistence tests using the canonical API for Task 1's scenarios. Show the canonical facade addresses the expected persisted pair and label removal never withdraws it.
- [ ] **Step 6: Self-review, commit and report.** Include copied-input/lifetime, restricted Load, collision and canonical API evidence. Keep production responsibilities split between handle construction, collector and generator.

### Task 3: Scope-only APIs, protocol and fresh storage

**Files:**
- Modify: `packages/client-js/index.mts`, `packages/client-js/subscriptions.mts`, `packages/dart/lib/src/client.dart`, `packages/dart/lib/src/subscriptions.dart`, `crates/compiler/src/emit.rs`
- Modify/rename: Scope-related Rust modules/types and internal variables in `crates/core`, `crates/server`, `crates/client`, `crates/sim`, SDK bindings and PostgreSQL adapter constants; update module/import/call sites.
- Test: `integration/bindings/client-js/subscriptions.test.mjs`, `integration/bindings/client-react-native/subscriptions.test.mjs`, `packages/dart/test/subscriptions_test.dart`, generated transaction contracts and host-contract serialization tests
- Modify fresh PostgreSQL/SQLite DDL, protocol/native task fields, capability admission and serialized framework names to Scope. Forward upgrades of pre-existing state belong to Task 4.

**Interfaces:**
- Produce: `tx.scopes.subscribe/unsubscribe` in TypeScript and Dart runtime/generated transactions, identical to existing local subscription intent.
- Preserve the behavior of `client.scopes.subscribe(): Subscription`, `Subscription.bootstrap/unsubscribe/status`; remove `channels` facades and rename serialized ownership fields to `scope`.
- Canonical new code uses `ScopeIntent`, Scope member/position concepts and scope-named modules. Do not retain Channel exported type aliases.

- [ ] **Step 1: Write failing Scope-only contract and wire tests.** Exercise JS/Dart `tx.scopes` subscribe/unsubscribe, rollback and reopen. Negative generated contracts reject `ctx.channel`, client/transaction `.channels`, Model-first membership access and old tag-selector removal. Assert emitted requests carry `scope`, never `channel`, and capability `scope-membership-v1`; old marker/old-field requests receive 426 or malformed-frame refusal before progress. Assert new PostgreSQL and SQLite installations expose only scope-named framework objects.

```ts
await client.transaction(async tx => {
  await tx.scopes.subscribe('U');
  await tx.scopes.unsubscribe('U');
});
const followed = await client.scopes.subscribe('U');
assert.equal(followed, await client.scopes.subscribe('U'));
await followed.unsubscribe();
assert.equal(followed.status.active, false);
```

- [ ] **Step 2: Verify RED.** Run focused JS/Dart subscription tests, protocol admission/serialization tests and generated API typechecks before adding the Scope-only surfaces and fields.
- [ ] **Step 3: Rename canonical concepts and fresh storage systematically.** Remove old public facades/overloads, replace Channel Rust/TS/Dart types and module names with Scope, and rename native commands/HTTP/WebSocket fields and capability constants. Fresh DDL uses scope-named tables, columns, constraints, indexes, sequences and trigger functions. Rename PostgreSQL SQL constants and all SQL table references. Remove temporary tag-selector withdrawal variants once the canonical where path covers them. Regenerate fixtures. Keep application Model names/data untouched; this is a framework vocabulary change, not text replacement in opaque data.

```dart
late final scopes = TransactionScopes._(this);
// No transaction.channels facade remains.
```

- [ ] **Step 4: Audit residual terminology.** Run `rg -n 'Channel|channel' crates packages bindings examples website/docs docs/engineering`. Each surviving occurrence must be explicit legacy migration input, an intentional application-owned schema field/name used by a migration regression, an external standard/dependency API, or dated history. No public aliases, old protocol-field parsing, old capability identifiers in live code, or canonical channel tables remain. Living-guide cleanup finishes in Task 6.
- [ ] **Step 5: Verify GREEN for fresh-layout behavior.** Run focused Rust protocol/server/client tests, JS and React Native subscription tests, Dart analyze/test, generated positive/negative contracts and real-PG new-database scenarios. Existing-layout upgrades are Task 4 acceptance; record that boundary instead of claiming complete migration evidence here.
- [ ] **Step 6: Self-review, commit and report.** Report canonical names, new admission floor and exact fresh-layout assertions; identify old-layout migration fixtures for Task 4. This is a breaking coordinated change, never a patch silently compatible with 0.2.

### Task 4: Forward storage and saved-state migration

**Files:**
- Create: `packages/postgres/migrations/2026-09-30-scopes.sql`
- Modify: `packages/postgres/migration.sql`, migration docs, migration fixtures and `integration/persistence/server/channel-tags.test.mjs` (its Scope filename after Task 3)
- Modify: `crates/client/src/ddl.rs`, `schema_store.rs`, migration/reconciliation entry point and owning SQLite tests
- Modify framework saved-outcome migration helpers only where source audit proves a structural rewrite is required.

**Interfaces:**
- Consume Task 3 fresh scope-only schemas/protocol.
- Produce transactional, idempotent upgrade from the exact v0.2.0 server/client layouts. Preserve record IDs, membership/tag relationships, heads/cursors, hold/absence evidence, client identity, subscription bootstrap/reconciliation state, request epochs and queued/frozen business work.
- Convert framework-owned membership claim keys in PostgreSQL receipt/call responses. SQLite queue/Load state has no persisted framework claim envelope to rewrite; preserve its stored values unchanged unless a source-proven additional claim location is found. Do not recursively rename arbitrary JSON keys: Model data, identities, args, continuation and opaque values may legitimately contain `channel`.
- Preserve frozen logical request bytes and IDs. Capability advertisement is transport metadata, stripped from durable logical request identity by the existing core protocol functions; change advertisement to the Scope marker without rewriting business requests.

- [ ] **Step 1: Write failing migration scenarios from original layout fixtures.** Create v0.2.0 data with live/absent holds, tag associations and tombstones, current subscriptions, a pending mutation with frozen request, server-saved page/result claims, and a business Model field/argument named `channel`. Snapshot unaffected bytes/IDs/cursors before upgrade. Run the new runtime/migration and independently query resulting Scope tables and saved outcomes.

```js
assert.equal(await exists('axton_scope_member'), true);
assert.equal(await exists('axton_channel_member'), false);
assert.deepEqual(await scopeHeads(), originalHeads);
assert.equal(await frozenBusinessRequest(), originalFrozenBytes);
assert.equal((await restoredBusinessRow()).channel, 'unchanged-business-value');
```

Add interrupted/failed upgrade rollback and second-run idempotency; saved-response replay must still return the original business snapshot and claim cursors using new scope keys, without running a handler/Loader or re-enrolling a withdrawn record.
- [ ] **Step 2: Verify RED against the fresh-only Task 3 implementation.** Run focused old-layout PG/SQLite reopen fixtures; record failure to migrate existing tables/state as the intended regression.
- [ ] **Step 3: Implement forward schema migration before fresh-layout reconciliation.** Rename existing PostgreSQL tables/columns and all owned indexes/sequences/constraints/triggers/functions transactionally; update trigger bodies to new identifiers. Handle only known original layout versions and refuse conflicting parallel old/new state without wiping either. SQLite must rename existing framework tables/columns before required-table checks or CREATE statements, within an atomic transaction. Do not signal incompatible schema rebuild merely because framework table names changed. Persist layout completion only after all schema and known framework claim rewrites succeed.
- [ ] **Step 4: Migrate only structural framework claims.** Walk known receipt/direct/Load envelope claim locations, or use structural SQL JSON transforms there; preserve request/result Model payloads and raw request bytes. Preserve all arrays, correlation IDs, claim cursor values and terminal states. Re-open before network work and apply the new capability floor at HTTP/live admission before any handlers, saved-call claims or progress changes.
- [ ] **Step 5: Verify GREEN and replay.** Run `cargo test -p axton-sqlite --locked`, focused core/client migration tests, real-PG migration/host tests, generated JS/Dart reopen tests and Load saved replay scenarios. Resume previously queued/frozen work against the migrated server and compare accepted effects and settled IDs with the original snapshots. Confirm all fresh schema objects use Scope and old names remain only as migration inputs.
- [ ] **Step 6: Self-review, commit and report.** Include transactional failure, idempotency, old/new layout conflict, opaque application-data preservation and pending/replay continuity evidence. No server/client reset or queue loss is acceptable.

### Task 5: Simulation and assembled Scope API acceptance

**Files:**
- Modify: `crates/sim/tests/channel_tags.rs` (rename to Scope filename after Task 3), `integration/load-e2e/server.mts`, `integration/load-e2e/load.test.mts`
- Modify: `integration/e2e/fixtures/round-trip/server.mts`, `integration/e2e/round-trip.test.mjs`
- Add scenarios to existing persistence/Action e2e owners rather than introducing another runner.

**Interfaces:**
- Consume: implemented canonical Scope/tag/where APIs and the Scope-named enrollment/removal wire with existing membership semantics.
- Produce: reproducible integration evidence covering overlapping tag cleanup, returned-identity Load enrollment and client hold/fencing outcomes.

- [ ] **Step 1: Add failing assembled behavior scenarios.** A shared fixture starts with A/X/Y, B/X and C/Y; a canonical handler executes exact-only-X withdrawal then X detachment. Assert backend membership snapshots, one B removal, A/C rows still present locally, no tag metadata in the response and no Loader invocation for removal. Add a second Scope holding B, then withdraw the first Scope and verify B remains; withdraw the second and verify its replicated base is released while pending/local work follows the current contract.

```ts
await backend.transaction(({scope}) => {
  const s = scope('U');
  s.where({tags:{only:['X']}}).remove();
  s.tag('X').remove();
});
assert.deepEqual(await heldIds('U'), ['A', 'C']);
```

Use the existing fixture's native client/real PostgreSQL helpers for `heldIds`; assert server and client state independently rather than reusing production selection code to calculate expected values. Add a Load chaining add/tag, saved page replay after removal, a late held read following eviction, rollback, and fresh page re-enrollment. Include an initially untagged record matched by `only: []` and a label mutation before selection that changes membership candidates.
- [ ] **Step 2: Observe RED where new acceptance assertions expose gaps.** Existing supported behavior may already pass; record that honestly rather than manufacturing failure. Any production fix must have the newly failing scenario as its regression first.
- [ ] **Step 3: Fix gaps at the owning boundary.** Preserve existing v0.2.0 client machinery, including claim cursors and request epochs. Do not add frontend labels, per-tag events, remote SQL, automatic cascades or snapshot replacement. Extend simulation using direct Scope intents to cover reordered/duplicate delivery and reopen for the new backend selections.
- [ ] **Step 4: Verify assembled GREEN.** Run `cargo test -p axton-sim --locked`, `bash integration/persistence/server/run.sh`, `bash integration/action-e2e/run.sh`, `bash integration/load-e2e/run.sh`, `bash integration/e2e/run.sh`, and `bash integration/e2e/todo-run.sh`. Re-run only amended focused checks during iteration; broad suites run once on the final task state.
- [ ] **Step 5: Commit and report acceptance evidence.** Name assertions, commands/results and concrete coverage limits. Record the exact tested commit.

### Task 6: Public documentation and final host gate

**Files:**
- Modify: `website/docs/backend/api.md`, `website/docs/frontend/client-api.md`, `website/docs/frontend/sync.md`, `website/docs/frontend/loads.md`, `website/docs/concepts.md`, `website/docs/api-index.md`
- Modify: affected owning component docs under `docs/engineering/architecture`, `docs/engineering/guarantees.md`, `docs/writing/guides.md`, package READMEs and `examples/todo`
- Modify: canonical Scope references in remaining living website/engineering docs and code snippets; leave dated history and migration spellings intact.

**Interfaces:**
- Consume: final implemented public signatures, tested cutover boundaries and previous task evidence.
- Produce: current-behavior guides/snippets for Scope actions, label editors, selectors, touch, context capability restrictions and client subscriptions; final verification evidence.

- [ ] **Step 1: Rewrite guides around observable behavior.** Show the spec's canonical examples and explain operation receivers rather than forcing every API word to be a verb. Document the breaking Scope cutover and forward migration; expose no old Channel API aliases. Document signatures, operands, return handles, synchronous declarations, missing-membership failure/rollback, chained invocation order, predicate limits and label-only no-event behavior. Update incoming links when headings/modules change.

```ts
const s = ctx.scope('User:alice');
s.add.todo(['A', 'B']).tag('journal:1');
s.where({tags:{only:['journal:1']}}).remove();
s.tag('journal:1').remove();
```

TypeScript/Dart client examples must agree. Describe bootstrap, permission checks, authoritative deletion versus replica release and untracked one-shot cache honestly; do not claim hooks can all be removed.
- [ ] **Step 2: Verify docs and Scope-only surfaces.** Run `python3 website/scripts/check_examples.py`, the repository's strict website build command from `website/README.md`, local links/anchors and terminology residual checks. Update the interface index and writing convention term list to Scope. Verify surviving Channel strings are only legacy migration inputs, dated history, external standard/dependency APIs or intentional application-field regression data.
- [ ] **Step 3: Run the complete host gate once.** Run `bash scripts/test.sh` on the final branch, saving the log and exact commit. It includes release checks, Rust fmt/clippy/tests, SDK bindings, persistence, generated APIs, Action/Load/e2e, snippets and installed-package tests. Also run the relevant React Native subscription mock suite and `node scripts/release/version.mjs check` if the host script does not include them. Do not publish packages to test them.
- [ ] **Step 4: Resolve failures and review the branch.** Diagnose each failure, add a targeted regression for production defects, rerun affected checks, and re-run the full gate only after meaningful fixes. Dispatch a whole-branch reviewer against v0.2.0 with the spec, final task reports, migration-boundary inventory and exact validation evidence. Fix Critical/Important findings and re-review.
- [ ] **Step 5: Commit documentation/evidence and report.** Confirm clean worktree, completed task ledger, latest tested SHA and no version/publish/merge changes. Provide the user the implementation result, remaining material limitations and reviewable branch/PR artifact if requested or created under the authorized workflow.
