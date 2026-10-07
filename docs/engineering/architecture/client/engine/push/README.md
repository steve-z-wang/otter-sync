# Push

Push means durable protocol-5 Batch execution. Ready named Mutations retain exact input and descriptor; freezing fixes sequence and membership. Local acknowledgment reconciles refusals atomically, while accepted members remain until authority obligations commit. Dependencies block only the work that actually depends on them.

[Protocol 5](../../../protocol/0.5.md) owns the shared contract; [implementation](../../../../../../crates/client/src/sync05/uplink.rs) owns this component. Earlier carrier mechanics below are historical references, not current public contracts.

Prepare durable mutations for delivery. Network scheduling belongs to [Connection](../../connection/README.md); receipts and rollback belong to [Settlement](../settlement.md).

- [Queue](queue.md) — Persist mutations, operations and their ordering.
- [Dependencies](dependencies.md) — Decide which mutations are eligible to send.
- [Batching](batching.md) — Freeze eligible mutations and preserve their bytes for retries.

## How the parts work together

One push starts when the connection asks the engine to *freeze*. Three steps follow; the detailed rules are in [Batching](batching.md).

1. **Retry an existing batch.** [Batching](batching.md) first checks the [queue](queue.md) for a batch that was sent but has no receipt yet. If there is one, it re-encodes that batch from its stored rows and returns the same bytes; this is how a lost response is retried without a second execution, and no new batch is started while one is in flight.
2. **Select a new batch.** Otherwise it walks the unsent mutations in ordinal order and asks [Dependencies](dependencies.md) about each one. A mutation waits while a prerequisite is unready, while a lifecycle parent has not been acknowledged, or while a sequence predecessor is neither sent nor chosen for this batch; an independent later mutation may be taken instead. At most twenty mutations are taken, and a candidate that would push the request over the byte budget is skipped, with one exception: with a non-zero budget the first eligible mutation is always taken, so a single large mutation can still be sent.
3. **Freeze.** The selected mutations receive the next push number in the queue, and their wire operations (never companions or cascade effects) are encoded into one request. From then on the batch is immutable until its receipt arrives and [Settlement](../settlement.md) completes it; only one batch is in flight at a time.
