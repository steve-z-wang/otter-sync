# Scheduling

## 1. Introduction and Goals

Rust owns coalescing of active delivery lanes, bounded staging and repair scheduling. Store commits and control operations use the Storeworker; network transfers may progress independently. Reconnect/close fence stale effects. A required large unit may spill to disk but remains one atomic apply, so finite capacity is not a latency promise.

## 5. Building Block View

[sync05/delivery_queue.rs](../../../../../../crates/client/src/sync05/delivery_queue.rs) owns this component. [Protocol 5](../../../protocols/sync.md) owns shared context, delivery and settlement rules.
