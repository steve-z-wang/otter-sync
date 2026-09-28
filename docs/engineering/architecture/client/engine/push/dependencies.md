# Dependencies

## 1. Introduction and Goals

- Prevent a mutation from being sent before its required work, while allowing independent mutations to proceed.

## 3. Context and Scope

- Inputs: explicit dependencies, schema relations, mutation sequence policies and prerequisite readiness.
- Metadata is stored in the [Queue](queue.md) and checked by [Batching](batching.md).
- Rejection propagation belongs to [Settlement](../settlement.md).

## 5. Building Block View

- Lifecycle dependencies protect record existence, including references to pending creates.
- Sequence dependencies express the order declared by [mutation policies](../../../schema/mutations.md).
- [Prerequisites](../../../schema/prerequisites.md) wait for application work such as uploads.
- Code: derivation in [policies.rs](../../../../../../crates/client/src/policies.rs), storage in [queue.rs](../../../../../../crates/client/src/queue.rs), eligibility checks in [push.rs](../../../../../../crates/client/src/push.rs).

## 6. Runtime View

- A lifecycle dependent waits for its parent's receipt and cannot share the parent's batch. A rejected parent removes its lifecycle dependents.
- A sequence dependency is derived once, at enqueue, against the calls already queued, and stored with the call, so it holds across restart. An earlier call of the named Mutation is a predecessor when, for every argument, the two sides reach a common record. A side is a slot followed by relation names, and every record of a list slot counts. A path's first step reads the relation fields the call itself carries: its identity, plus a create's or update's values. A field it does not carry comes from the local row, or from the held authoritative base when the row is deleted. Later steps read stored records. A delete therefore resolves through its identity even after the record is gone. A side that reaches no record never matches.
- A sequence dependent can share a batch when its predecessor appears earlier in that batch.
- Pending or failed prerequisites block their mutation. Independent later mutations can overtake blocked work.

## 9. Architecture Decisions

**A sequence path starts from what the call carries (decided 2026-09-28, [#179](https://github.com/zanminwang/axton/issues/179)).** The first step of a path reads the relation fields the call carries before the stored record. What the call carries is what the backend will act on. It also resolves a delete of a record that a pending create made, which leaves neither a local row nor a held base; reading only stored records would miss that predecessor. Later steps have no operation of the call to read, so they use stored records. Cost if wrong: when a later local write changes a relation field that a queued call carries, the call's value decides, not the current row.

**A list slot contributes every element on both sides (decided 2026-09-28, [#179](https://github.com/zanminwang/axton/issues/179)).** Before, a declaring list slot resolved only when it held exactly one record, so an act with two elements waited for nothing. Both sides now resolve to sets that must intersect. This only adds dependencies, which delay a call and never reject it.

## 10. Quality Requirements

- Selection must preserve these dependency rules, rather than enforce a global FIFO queue.
- Evidence: [dependency and prerequisite scenarios](../../../../../../crates/sqlite/tests/push.rs); relation paths on both sides, list slots, a delete resolved from its identity and the dependency after reopen in [actions.rs](../../../../../../crates/sqlite/tests/actions.rs) `sequence_prior_path_waits_only_for_earlier_acts_on_the_same_record_across_restart`. Coverage gaps in the named P3 scenarios are recorded in [coverage review](../../../../testing/review.md).
