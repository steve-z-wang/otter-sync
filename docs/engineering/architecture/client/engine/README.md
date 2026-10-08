# Engine

## 1. Introduction and Goals

The engine retains original Model operations and projects them over authoritative base state. Direct work is device-only; named Mutations queue durable intent and optional companions. Acknowledgment and accepted settlement are separate commits. Authoritative apply, rejection and settlement replay surviving work in original local order before notifying observers.

## 5. Building Block View

[Implementation](../../../../../crates/client/src/settlement05.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

- [loads](loads.md)
- [local-operations](local-operations/README.md)
- [pull](pull.md)
- [push](push/README.md)
- [settlement](settlement.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
