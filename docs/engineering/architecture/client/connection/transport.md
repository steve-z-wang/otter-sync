# Transport

## 1. Introduction and Goals

Protocol-5 carriers retain Store/Stream/materialization context and exact request identity across retries. Handshake authenticates the Store; Batch submits durable Mutations; read routes return null-cursor invocation snapshots; delivery routes return immutable plan fragments. Transport cancellation fences late effects. Neither HTTP success nor receipt arrival proves a committed authority unit.

## 5. Building Block View

[Implementation](../../../../../crates/protocols/src/sync.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
