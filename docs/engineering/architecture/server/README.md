# Server

## 1. Introduction and Goals

The Rust server validates protocol-5 context and immutable Batch intent, invokes application Handlers/Loaders through the host contract and persists outcomes with business writes. Publication tracks explicit recipients under a persisted fence. Finite delivery plans capture canonical authority and immutable continuations. Node implements the host and HTTP/WebSocket effects, with no alternate sync policy.

## 5. Building Block View

[Implementation](../../../../crates/server/src/protocol_v05.rs) owns this component. [Protocol 5](../protocol/0.5.md) owns shared context, delivery and settlement rules.

- [backend-interface](backend-interface.md)
- [connection](connection/README.md)
- [engine](engine/README.md)
- [persistence](persistence.md)
- [protocol4](protocol4.md)
- [protocol5](protocol5.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
