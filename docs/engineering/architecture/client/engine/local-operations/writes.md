# Writes

## 1. Introduction and Goals

Local writes become visible in their transaction and durable at commit. Named Mutations retain input, optimism and call-owned companions; direct writes stay device-only. Later authority replaces the base beneath pending operations, which replay in committed local order.

## 3. Context and Scope

| Write | Application entry point | Sent to the server | Local fate |
| --- | --- | --- | --- |
| Named Mutation | Generated `mutations.<name>` or `tx.mutations.<name>` | Frozen named input only | Refusal removes this Call's optimism and companions |
| Direct Model write | `tx.models.<model>` | Never | Survives earlier Call refusal when its base still exists |
| Companion | The Mutation's owned local callback | Never | Kept on acceptance; removed on refusal |

One application transaction may submit several Calls and direct writes. They commit locally together, but Calls have independent backend outcomes. The callback is never persisted or replayed. See the [typed client](../../../frontend-sdk/api.md) for callback scope and the two Mutation completion boundaries.

## 5. Building Block View

- [mutate.rs](../../../../../../crates/client/src/mutate.rs) normalizes and applies operations, records companion/direct order, extends cascades and rebuilds dirty projections.
- [mutation_queue.rs](../../../../../../crates/client/src/mutation_queue.rs) retains named input, prerequisites and dependency metadata.
- [authority.rs](../../../../../../crates/client/src/authority.rs) installs proved authority beneath pending edits and reports replay divergence.
- [settlement05.rs](../../../../../../crates/client/src/settlement05.rs) reconciles each Call's outcome and owned work.
- [rows.rs](../../../../../../crates/client/src/rows.rs) owns Model row statements and value encoding.

A dirty record has pending operations. Its before image holds the base beneath them; `axton_local_write` retains settled companion and independent write order. Rebuild combines that base, pending operations and journal rows. Journal rows with no earlier pending operation fold into the base. When replay cannot apply a queued operation, the current base remains visible, the durable Call remains queued, and a `diverged` report identifies its ordinal.

## 6. Runtime View

Submitting a named Mutation validates retained input and binding rules, applies optimism, derives dependencies/prerequisites and persists its ordinal in the local transaction. Owned callback writes share that commit. Savepoint or transaction failure rolls back the corresponding work; unawaited commands cannot escape callback lifetime.

A delete cascades through declared local relations once per identity, including cycles. These child effects stay local and follow their owning Call; they are not additional wire operations. Replaying a queued delete extends it to newly delivered children while respecting later independent or accepted recreations. Business handlers must delete and explicitly invalidate their server-side children themselves.

A direct write never queues a Call. On a dirty record, its journal entry keeps its position above earlier pending work. It cannot preserve a row whose only pending creation is refused. Later proved server authority may replace independent or accepted local state; that is distinct from undoing it because an earlier Call failed.

Records have no publication stamp. Stream cursor guards and call-owned evidence decide authority installation; duplicate delivery cannot undo newer authority. Ordinary null-cursor Query/Fetch records can populate unprotected cache, but cannot replace protected Stream state, clear a tombstone or advance progress. [Protocol 5](../../../protocols/sync.md) owns these rules.

## 10. Quality Requirements

Current source fixtures cover atomic local writes, companion fate/order, retained journal replay, cascade, and authority beneath pending edits:

- [runtime transactions](../../../../../../crates/sqlite/tests/protocol05_runtime_transactions.rs)
- [companions](../../../../../../crates/sqlite/tests/protocol05_companions.rs) and [companion cascades](../../../../../../crates/sqlite/tests/protocol05_companion_cascade.rs)
- [settlement](../../../../../../crates/sqlite/tests/protocol05_settlement.rs) and [authority](../../../../../../crates/sqlite/tests/protocol05_authority.rs)
- [local crash boundaries](../../../../../../crates/sqlite/tests/protocol05_local_crash.rs)

These links identify maintained coverage, not a new execution certificate. Installed-package and mobile evidence have separate [adoption gates](../../../../protocol5-adoption.md).

## 11. Risks and Technical Debt

An application callback can hold its own Store transaction until it finishes; independent Clients remain responsive. Replay/cascade work grows with the retained queue and dependency component. Whole authority units cannot be split to reduce writer time; measure actual staging space and capacity on target devices.
