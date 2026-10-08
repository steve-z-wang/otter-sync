# Storage

## 1. Introduction and Goals

The storage adapter executes SQL and transactions; Rust owns reconciliation policy. Format-5 admission and physical ownership precede schema coordination. Store metadata retains context, S/B/C, frozen Batch, operation input and acknowledgment. Model tables expose the replayed projection; shared legacy SQL views are adapters over current durable truth, not a second queue.

## 5. Building Block View

[Implementation](../../../../../crates/client/src/store05.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

- [protocol5](protocol5.md)
- [reconciliation](reconciliation.md)
- [store](store.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
