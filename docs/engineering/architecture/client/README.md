# Client

## 1. Introduction and Goals

The client projects settled authority with ordered device work and pending optimism. One Storeworker serializes local changes; Uplink freezes and acknowledges durable Batches, while DeltaApplier commits complete finite authority units. Remote effects run outside the SQLite writer. Observers and terminal outcomes follow their deciding commit.

## 5. Building Block View

[Implementation](../../../../crates/client/src/sync05/mod.rs) owns this component. [Protocol 5](../protocols/sync.md) owns shared context, delivery and settlement rules.

- [connection](connection/README.md)
- [engine](engine/README.md)
- [frontend-interface](frontend-interface.md)
- [runtime](runtime.md)
- [storage](storage/README.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
