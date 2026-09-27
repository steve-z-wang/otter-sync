# Queue

## 1. Introduction and Goals

- Persist pending mutations so local writes survive restart and can later reach the server.

## 3. Context and Scope

- [Local operations](../local-operations/README.md) stores each mutation alongside its optimistic changes in the same transaction.
- [Dependencies](dependencies.md) supplies ordering and prerequisite metadata; [Batching](batching.md) assigns mutations to a push.
- [Settlement](../settlement.md) owns rejection and removal of completed mutations.

## 5. Building Block View

- Each queued call has a durable ordinal, an optional push number and ordered optimistic operations. A call row - a durable Mutation or a queued Query - also stores a unique call ID and canonical arguments; a valid call can have no Model operations, and a queued Query never has any. The row stores no kind: name and version select the retained descriptor.
- Call pushes send the stored call ID, name, version and arguments. Their inferred Model operations replay locally. Legacy internal mutations still send wire operations; companion operations and derived cascade effects stay local. A companion delete's cascade is stored as a companion of its call. Settled local writes are retained in the [local write journal](../local-operations/writes.md#5-building-block-view), which is never sent.
- `axton_client` holds the counters (`next_ordinal`, `next_push`), `last_completed_push` (the sequence of the last batch a receipt completed; a batch is in flight while its push number is above it), `push_models` (the authority read contracts declared on the wire), and `push_results` (the Model result read contracts used by frozen calls). The frozen metadata is released on completion. There is no per-batch table: the batch is the set of mutations sharing a push number.
- Code: [queue.rs](../../../../../../crates/client/src/queue.rs); table definitions in [ddl.rs](../../../../../../crates/client/src/ddl.rs).

## 10. Quality Requirements

- Restart preserves queued operations and their order: [queue reconstruction test](../../../../../../crates/sqlite/tests/engine.rs) `queue_rows_reconstruct_mutations_and_cascade_on_delete` and [restart test](../../../../../../crates/sqlite/tests/push.rs) `offline_queue_and_frozen_bytes_survive_restart_and_receipt_completes_at_once`; the frozen declaration survives with the batch: [settlement.rs](../../../../../../crates/sqlite/tests/settlement.rs) `frozen_batch_and_its_declaration_survive_restart_until_completed`. [Action tests](../../../../../../crates/sqlite/tests/actions.rs) cover call ID and arguments, Model-free calls, retained versions and byte-identical frozen retries.
- Ordinals and push numbers are allocated transactionally within the protocol's safe integer range; exhaustion returns an error.

## 11. Risks and Technical Debt

- New calls require an operation descriptor in the local schema. Retained old contracts keep queued calls sendable during compatible schema evolution; an incompatible upgrade keeps the old file pending through [Storage](../../storage/reconciliation.md).
