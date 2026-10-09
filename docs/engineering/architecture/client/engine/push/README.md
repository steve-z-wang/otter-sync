# Push

## 1. Introduction and Goals

Push means durable protocol-5 Batch execution. Ready named Mutations retain exact input and descriptor; freezing fixes sequence and membership. Local acknowledgment reconciles refusals atomically, while accepted members remain until authority obligations commit. Dependencies block only the work that actually depends on them.

## 5. Building Block View

[Implementation](../../../../../../crates/client/src/sync05/uplink.rs) owns this component. [Protocol 5](../../../protocols/sync.md) owns shared context, delivery and settlement rules.

- [batching](batching.md)
- [dependencies](dependencies.md)
- [queue](queue.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
