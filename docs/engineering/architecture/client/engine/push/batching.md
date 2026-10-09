# Batching

## 1. Introduction and Goals

A Batch has one Store sequence and immutable member order, digest and input. Each server member commits independently, so local multi-Mutation atomicity is not backend all-or-nothing execution. Lost responses retry identical bytes; saved members run no Handler or preparation. A valid next Batch replaces the older retained replay window transactionally.

## 5. Building Block View

[mutation_queue.rs](../../../../../../crates/client/src/mutation_queue.rs) owns client membership and freezing; [sync05/uplink.rs](../../../../../../crates/client/src/sync05/uplink.rs) schedules transmission. [mutation_batch.rs](../../../../../../crates/server/src/mutation_batch.rs) owns server execution. [Protocol 5](../../../protocols/sync.md) owns shared context, delivery and settlement rules.
