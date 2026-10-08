# Controller

## 1. Introduction and Goals

The socket controller drains immutable fragments and advances its offered cursor only at complete units. A reconnect authenticates again and obtains the current handshake head; HTTP repairs missing committed coverage. Live headers use the active backend materialization, so a retained client may request repair under its own context instead of applying foreign payload.

## 5. Building Block View

[Implementation](../../../../../crates/server/src/live.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
