# Runtime

## 1. Introduction and Goals

ClientRuntime owns task admission, transactions, prerequisites, connection lifecycles, direct reads and observers. It drives protocol-5 handshake, Uplink and finite delivery effects. Priority close cancels parked remote tasks and releases the actor without waiting for a network response; post-close results cannot install Models. SDKs submit tasks and carry effects rather than making retry or coverage decisions.

## 5. Building Block View

[Implementation](../../../../crates/client/src/runtime/mod.rs) owns this component. [Protocol 5](../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
