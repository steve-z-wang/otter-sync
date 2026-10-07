# Batching

A Batch has one Store sequence and immutable member order, digest and input. Each server member commits independently, so local multi-Mutation atomicity is not backend all-or-nothing execution. Lost responses retry identical bytes; saved members run no Handler or preparation. A valid next Batch replaces the older retained replay window transactionally.

[Protocol 5](../../../protocol/0.5.md) owns the shared contract; [implementation](../../../../../../crates/server/src/mutation_batch.rs) owns this component. Earlier carrier mechanics below are historical references, not current public contracts.

## 1. Introduction and Goals

- Freeze eligible mutations into a numbered request whose bytes remain stable across retries.

## 3. Context and Scope

- Input: the [Queue](queue.md), [dependency rules](dependencies.md) and a byte budget.
- Output: canonical [PushRequest](../../../protocol/push.md) bytes, or no batch.
- [Connection](../../connection/README.md) owns delivery and retry timing; [Settlement](../settlement.md) processes receipts.

## 5. Building Block View

- Code: selection and encoding in [push.rs](../../../../../../crates/client/src/push.rs); durable push assignment in [queue.rs](../../../../../../crates/client/src/queue.rs).

## 6. Runtime View

- Return an unacknowledged batch before preparing another one.
- Otherwise scan unsent mutations in ordinal order, skipping blocked candidates, and select at most 20.
- Skip a candidate that exceeds the byte budget, except that the first eligible mutation is allowed through. A zero budget produces no batch.
- Assign a push number transactionally. Encode only wire operations from the stored rows.

## 10. Quality Requirements

- Retrying or reopening the same frozen batch preserves its sequence and bytes: [restart and frozen-byte test](../../../../../../crates/sqlite/tests/push.rs).
- A large mutation cannot starve the queue solely because of the byte budget: `byte_budget_skips_large_candidate_but_always_allows_one` in the same test file.

## 11. Risks and Technical Debt

**Resolved ([#56](https://github.com/zanminwang/axton/issues/56)): there is no more "batch the server keeps failing" from content alone.** A frozen batch either gets a receipt or is never accepted; nothing about a mutation's content can leave it permanently failing. [Server Push §9](../../../server/engine/push.md#9-architecture-decisions) implements every content problem as that mutation's rejection instead of a whole-delivery failure. The outcomes a client actually sees:

| Outcome | Client behavior |
| --- | --- |
| Receipt received | complete the batch; rejected mutations go to the inbox |
| Transient failure (500, network) | resend the same frozen bytes with backoff (unchanged) |
| Lost response | resend; the server replays the stored receipt (unchanged) |
| Identity/order refusal (401/403/409) | report through `onError` with the code; keep the batch frozen; no local mutation is dropped, because the server did not run it. 401 goes through the existing auth refresh first |

Sent mutations still cannot be dropped ([Client::drop_mutation](../../../../../../crates/client/src/lib.rs)), but that is no longer a liveness risk from handler content: an identity/order refusal is the only case left where a frozen batch is neither completed nor retried automatically to a different outcome, and it is reported rather than silent.
- The count cap (20) and the default byte budget (256 KiB) are the protocol's `limits` ([Common](../../../protocol/common.md)). Limit configuration is tracked in [#11](https://github.com/zanminwang/axton/issues/11).
- Each size check re-encodes the candidate batch. Its cost grows with batch size; measurement belongs to [#12](https://github.com/zanminwang/axton/issues/12).
