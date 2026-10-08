# Bindings

## 1. Introduction and Goals

One actor owns each Client and physical SQLite file. The shared ABI submits tasks, drains effect/outcome messages and detaches the actor; platform Bridges execute native networking, timers and callbacks. Rust validates protocol-5 delivery and queue state and emits observer/terminal events only after their deciding commit. Pending network or callback effects do not hold another Client’s writer. Close/detach cancel effects and fence late replies.

## 5. Building Block View

[Implementation](../../../../bindings/common/src/actor.rs) owns this component. [Protocol 5](../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
