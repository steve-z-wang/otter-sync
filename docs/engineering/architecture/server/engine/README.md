# Engine

## 1. Introduction and Goals

The engine admits Store/Stream/context and exact immutable intent before application work. It saves each Mutation result with business effects, publishes under a fence and freezes finite delivery after preparation closure. Ordinary reads return null-cursor snapshots; settlement-owned materialization changes no enrollment or range prefix. Replay does not rerun saved preparation.

## 5. Building Block View

[Implementation](../../../../../crates/server/src/protocol_v05.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

- [loads](../../../history/pre-protocol5/architecture/server/engine/loads.md)
- [publish](publish.md)
- [pull](../../../history/pre-protocol5/architecture/server/engine/pull.md)
- [push](../../../history/pre-protocol5/architecture/server/engine/push.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
