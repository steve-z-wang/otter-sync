# Reconciliation

## 1. Introduction and Goals

A local database records the schema it was built for. Opening a client compares that stored descriptor with the compiled schema it is given and takes one of three ways: open as is, apply an additive change in place, or leave the file behind and build a fresh one beside it that resynchronises from the server. No row, pending mutation or direct record ever moves between schemas, and the runtime never deletes a file an application could still need ([#20](https://github.com/zanminwang/axton/issues/20)).

## 3. Context and Scope

The application passes one `path`; the client chooses the file. Opening reads the sidecar `<path>.current` (one line, the file name; missing means `path` itself), opens that file, and runs the **layout gate** on the committed reader before any DDL: a file laid out by the checkpoint-era runtime is a legacy layout, a file without `axton_client` is fresh, anything else is current. It then runs the framework DDL, reads the stored descriptor from `axton_schema` and classifies the difference with the shared rule `Schema::compatibility(stored, incoming)` from [core/schema.rs](../../../../../crates/core/src/schema.rs). **Reconciliation** proper, the DDL that makes the model tables match, runs once inside the opening transaction for a fresh file and for an additive change; a rebuild runs it on the new file. Input: the sidecar, the `axton_` table names, the stored descriptor and the compiled schema. Output: an open client whose `schema_state()` says what happened, or an error and an untouched file.

Callers are [`Client::open_at`](../frontend-interface.md) (the [runtime](../runtime.md), simulation) and `Client::open` for a store the caller opened itself; the latter has no path, so it refuses a legacy layout instead of rebuilding.

## 5. Building Block View

Per model there are two tables with identical columns: the visible table named after the model and `axton_before_<Model>` for before images ([Writes](../engine/local-operations/writes.md)). Column types follow [Types](../../schema/types.md); the identity is the primary key in `@@id` order; each `@@unique` becomes a unique index on the visible table. Ten framework tables (`axton_schema`, `axton_client`, `axton_record`, `axton_subscription`, the four queue tables, `axton_rejection`, `axton_query_cache`) are created with `IF NOT EXISTS`, so an existing database gains a missing one in place without a rebuild; `axton_query_cache` holds Query `once` snapshots (key, contract, name, version, canonical args and store, generation, and a nullable result where SQL NULL is an invalidated tombstone), and opening deletes its rows of any other contract fingerprint ([frontend interface](../frontend-interface.md)); `axton_schema` holds one row, the canonical JSON of the descriptor and when it was written; `axton_client` carries `last_completed_push`, `push_models` and the `next_subscription` allocator ([Settlement](../engine/settlement.md)).

The subscription ledger is one row per followed Scope ([#150](https://github.com/zanminwang/axton/issues/150)):

```sql
CREATE TABLE axton_subscription (
  channel           TEXT PRIMARY KEY,  -- the Scope name; renamed by #152
  subscription_id   INTEGER NOT NULL UNIQUE,
  starting_cursor   INTEGER,           -- the boundary the first initialization committed
  cursor            INTEGER,           -- how far delivery has committed
  bootstrap_state   TEXT NOT NULL DEFAULT 'not_requested',
  bootstrap_run     INTEGER NOT NULL DEFAULT 0,
  bootstrap_cursor  INTEGER NOT NULL DEFAULT 0,
  bootstrap_barrier INTEGER,
  bootstrap_error   TEXT,
  CHECK ((starting_cursor IS NULL AND cursor IS NULL) OR
         (starting_cursor IS NOT NULL AND cursor IS NOT NULL AND
          starting_cursor >= 0 AND cursor >= starting_cursor))
)
```

The five `bootstrap_` columns are the same row's historical load ([#151](https://github.com/zanminwang/axton/issues/151)): the phase (`not_requested`, `requested`, `loading`, `catching_up`, `complete`, `failed`), the run that fences retries and in-flight responses, the committed progress B through the interval below `starting_cursor`, the completion barrier the terminal page fixed, and a bounded JSON failure. Their defaults are a load that was never requested, so a ledger from before them **gains them in place** with every identity and boundary intact. SQLite can add a column-level `CHECK` with a new column, but not the table-level constraint across the five that their coherence needs - a barrier only once the interval finished, a failure only on a failed run - so that coherence is enforced in [client/bootstrap.rs](../../../../../crates/client/src/bootstrap.rs), on every read and every write of a row in [client/bootstrap_ledger.rs](../../../../../crates/client/src/bootstrap_ledger.rs).

A row means subscribed; no row means unsubscribed. **Both cursors NULL** means the intent is durable but its first delivery boundary is not committed yet - what an offline registration leaves behind - and zero is an initialized position, never a stand-in for uninitialized. The `CHECK` is why a half-initialized pair cannot be stored: only the first initialization writes both fields, together, and later page application moves `cursor` alone while `starting_cursor` stays fixed for that identity. `subscription_id` comes from the `next_subscription` allocator in `axton_client`, is never recycled, and fences a handle, an acknowledgement or a request against the registration it was made for, including a recreation at the same Scope name. Who writes what is in [Frontend interface](../frontend-interface.md) and [Downlink worker](../connection/controller/downlink-worker.md).

Beside the database: the sidecar `<path>.current`, written as `<path>.current.tmp` and renamed, and the numbered files `<path>.<n>` a rebuild creates, `n` being the smallest unused positive integer.

Code: [client/ddl.rs](../../../../../crates/client/src/ddl.rs) (`check_layout` → `Layout`, `FRAMEWORK_DDL`, `reconcile`); [client/schema_store.rs](../../../../../crates/client/src/schema_store.rs) (descriptor read and write, sidecar, next free file, removal of an abandoned file); the open flow, `rebuild` and the pending counts in [client/lib.rs](../../../../../crates/client/src/lib.rs) (`open_at`, `rebuild_beside`, `rebuild`); the compatibility rule in [core/schema.rs](../../../../../crates/core/src/schema.rs) (`Schema::compatibility`, `Compatibility`, `AdditiveStep`).

## 6. Runtime View

The comparison is the compiler's model rule ([Models §9](../../schema/models.md#9-architecture-decisions)), never a hash: a hash can tell that something changed, not whether the change is safe.

| Comparison of stored and incoming schema | Outcome |
| --- | --- |
| Identical (field order ignored) | **open** |
| A new model; a new nullable stored field; a new non-nullable field with a descriptor default | **additive**: the tables and columns are added by `reconcile` in the opening transaction and the stored descriptor is replaced in the same transaction |
| A removed model; a model version change; an identity change; a removed, retyped or nullability-changed field; a changed unique set or relation; changed values of an enum a stored field uses | **incompatible**: the file is not touched; a fresh `<path>.<n>` is created (see below) |
| Legacy layout from the checkpoint era (`axton_push_checkpoint`, `axton_claim`, or `axton_client` without the completion columns) | **incompatible** as well: it stops being a refusal and becomes a rebuild; its checkpoint-era queue cannot be sent by this runtime, so the count of mutations it held is reported as left behind |
| Current layout without a descriptor row (a file from before this rule) | a read-only table check (`ddl::incompatibility`: identity columns, column types, a missing non-nullable column without a default) decides: tables that fit open in place and adopt the incoming descriptor; tables that do not are rebuilt with that reason. Any other open failure is an error, never a reason to switch files |
| Current layout whose `axton_mutation` lacks `diverged` (a file from before [#122](https://github.com/zanminwang/axton/issues/122)), or whose `axton_subscription` lacks the `bootstrap_` columns (a file from before [#151](https://github.com/zanminwang/axton/issues/151)) | the columns are **added in place** with their defaults before anything else, so the queue stays sendable and the subscriptions keep their identities and boundaries |

A **rebuild** creates `<path>.<n>`, where `n` is one above every existing numbered file (numbers only grow, even after the application deletes an old generation), runs the framework DDL and `reconcile` for the incoming schema, stores its descriptor, carries the old file's Scope names over as fresh subscriptions with NULL cursors and no load state (a fresh identity's load is `not_requested` at run zero, so no loading coverage is claimed across the rebuild) and carries the `next_subscription` allocator forward where the old layout had one (never the old cursors: an old cursor must not claim old rows are present, and an equal numeric identity in a replaced replica is not the same handle), commits, and only then writes the sidecar. Initialization is reset by that: each carried Scope waits for the next acknowledged head, and no loading completeness is claimed across the rebuild. Reopening follows the sidecar to the new file. An interrupted rebuild leaves the sidecar untouched, so the next open finds the old file again, classifies it again, removes every numbered file **above** the one in use (it was never pointed at and holds nothing durable) and retries with the next number. The file in use and every earlier generation are kept. The old file stays where it was, with every row, pending mutation and direct record it held; the application may delete the numbered files it no longer needs.

**Unsent work in the old file.** Before an incompatible rebuild the client counts the old file's queued mutations (`pending`) and its direct records (`direct`: rows with no stamp in `axton_record` and no pending operation, which nothing will ever send). With `pending > 0` and `discard_pending = false`, the old file is opened as it is, **with its stored schema** (its frozen bytes and declaration were compiled for that schema and the server serves that mutation version), and `schema_state().pending` reports `{old_file, reason, pending, direct}`. The push lane runs as usual; reads answer from the old schema. When the queue is empty the application reopens, or calls `rebuild(false)`, which performs the switch in place: the same handle now serves the new file, its watchers are moved, every model table is notified, and a connected Downlink lane is reset for the new file without another `connect` ([Downlink worker](../connection/controller/downlink-worker.md)). There is no automatic switch inside a running process. `rebuild(true)`, or `discard_pending = true` at open, rebuilds at once and the report says what the old file keeps: `RebuildReport { old_file, new_file, reason, left_pending, left_direct }`, also available as `schema_state().last_rebuild`. `rebuild` refuses when nothing is pending, when a client transaction is open, and when unsent mutations remain unless told to leave them.

Consequences worth knowing: a field rename is a removal plus an addition, so it is incompatible and rebuilds; a source `@default` is creation policy (`createDefault`), not the descriptor `default` this table reads, so the "non-nullable with default" additive row stays unreachable from a `.model` file and a required field always rebuilds, matching the model-version rule that already demands a bump for it ([#27](https://github.com/zanminwang/axton/issues/27)). Adding, changing or removing a creation default is not a storage change: the schema compares identical, the file opens in place, and no row, queued argument or frozen request is rewritten. The SQLite reader connection is refreshed after an additive change, and double-quoted string literals are disabled on both connections, so an added column is a column, never a string that looks like one.

## 9. Architecture Decisions

**Detect compatibility and rebuild incompatible replicas — implemented ([#20](https://github.com/zanminwang/axton/issues/20)).** The rule lives in `axton-core` so the compiler's history check and the client's open check cannot drift. Rebuilding is automatic framework behaviour: no migration SQL, no migration command, no registered upgrade callback. A compatible database is reused rather than rebuilt on every start.

**The old file is kept and nothing moves.** Rows, pending mutations and direct records stay in the file they were written to. Carrying them into another schema would mean inventing values or rewriting frozen request bytes, both of which the framework refuses to do. The `migration` option the SDK `open` still accepts is ignored: there is no defaults or replay mechanism, and documenting one would promise a seamless upgrade the runtime does not perform.

**Unsent work is sent first, or left behind on the application's say-so.** The client never decides a timeout. It keeps the old file open for its work and reports the state; the application decides whether to wait or to call `rebuild({ discardPending: true })` and tell the user what stayed behind.

**Selection is a sidecar, not a rename.** Renaming the live file under an open connection is not atomic on every platform; a one-line pointer written by temp-and-rename is. The sidecar is written last, so a crash at any earlier point leaves a file the next open discards.

**Ruling: a file without a descriptor adopts the schema it opens with** when reconciliation succeeds. Such files predate the rule and were, by construction, reconciled by the same DDL; refusing or rebuilding them would discard working replicas for no gain.

## 10. Quality Requirements

- **Unchanged and additive schemas open in place; the descriptor is updated; an added column reads as null.** Evidence: [sqlite/tests/rebuild.rs](../../../../../crates/sqlite/tests/rebuild.rs) `unchanged_and_additive_schemas_open_in_place`; [sqlite/tests/ddl.rs](../../../../../crates/sqlite/tests/ddl.rs) `adds_missing_columns_to_both_tables_and_keeps_unknown_ones`.
- **A ledger without the `bootstrap_` columns gains them in place and keeps its identities and boundaries; a second open changes nothing.** Evidence: [sqlite/tests/ddl.rs](../../../../../crates/sqlite/tests/ddl.rs) `a_subscription_ledger_without_bootstrap_columns_gains_them_in_place`.
- **An incompatible schema gets `<path>.1`, the sidecar points to it, the old file keeps its rows, the Scope names are carried over with fresh identities, NULL cursors and no load state, the new file is empty until it syncs.** Evidence: `an_incompatible_schema_gets_a_fresh_file_and_keeps_the_old_one`, `a_rebuild_resets_the_bootstrap_state_with_the_fresh_identity`, `a_subscription_table_without_identities_is_rebuilt_beside` (a ledger without identities is itself a rebuild reason, and the allocator is carried forward where the old file had one); the initialization that follows in [sqlite/tests/downlink_worker.rs](../../../../../crates/sqlite/tests/downlink_worker.rs) `scopes_carried_through_a_rebuild_initialize_at_the_next_acknowledged_head`.
- **A checkpoint-era layout is rebuilt beside, not refused; the old file is left as found.** Evidence: `an_earlier_framework_layout_is_rebuilt_beside_not_refused`; through a caller-opened store it is still refused untouched: `a_database_from_the_checkpoint_era_is_refused_untouched` in ddl.rs.
- **An abandoned partial rebuild is removed and the retry takes the next number; earlier generations survive later rebuilds and numbers only grow.** Evidence: `an_abandoned_partial_rebuild_is_removed_and_retried`, `earlier_generations_survive_later_rebuilds`.
- **A file without a descriptor is rebuilt only when its tables do not fit.** Evidence: `a_database_without_a_descriptor_is_rebuilt_only_when_its_tables_do_not_fit`.
- **Unsent work keeps the old file open with its stored schema until sent; the frozen bytes are unchanged; then `rebuild` switches the same handle.** Evidence: `unsent_work_keeps_the_old_file_open_until_it_is_sent_then_rebuild_switches`.
- **Discarding reports the mutations and direct records left behind and keeps the file.** Evidence: `discarding_pending_work_reports_what_the_old_file_keeps`.
- **A descriptor-less current file adopts the schema it opens with.** Evidence: `a_current_layout_file_without_a_descriptor_adopts_the_schema_it_opens_with`.
- **Every rule of the comparison names its reason.** Evidence: [core/tests/compatibility.rs](../../../../../crates/core/tests/compatibility.rs).
- **Across the runtime and SDKs: `syncState().schema`, a refused rebuild while work is unsent, the report, the empty fresh file.** Evidence: [sqlite/tests/runtime.rs](../../../../../crates/sqlite/tests/runtime.rs) `an_incompatible_schema_keeps_its_file_until_the_work_is_settled_and_rebuilt`; [integration/bindings/client-js/rebuild.test.mjs](../../../../../integration/bindings/client-js/rebuild.test.mjs); [packages/dart/test/client_test.dart](../../../../../packages/dart/test/client_test.dart) `an incompatible schema keeps unsent work in the old file until rebuild is asked to leave it`.
- **End to end: the rebuilt client converges like a fresh one; a restart follows the sidecar.** Evidence: [sim/tests/upgrade.rs](../../../../../crates/sim/tests/upgrade.rs).

Executed 2026-09-16: `cargo test -p axton-core -p axton-client -p axton-sqlite -p axton-binding -p axton-sim --locked`, the JS and Dart suites, passed with the tests above. The runtime test that replaced the binding session test ran in `cargo test --workspace --locked` on 2026-09-26, 687 passed (owner, [#165](https://github.com/zanminwang/axton/pull/165)).

## 11. Risks and Technical Debt

**Accepted limitation: a non-nullable field cannot be added without a rebuild.** Creation defaults never supply historical values ([Models](../../schema/models.md#9-architecture-decisions)), so every required field rebuilds the local database. Evidence: [sqlite/tests/defaults.rs](../../../../../crates/sqlite/tests/defaults.rs) `a_creation_default_never_backfills_a_new_required_column`, `a_default_only_change_opens_in_place_and_rewrites_nothing`.

**Accepted limitation.** Old files accumulate until the application deletes them; the runtime removes only an abandoned partial rebuild. A direct record in an old file is reported, never carried. A legacy checkpoint-era queue is counted as left behind, not sent.
