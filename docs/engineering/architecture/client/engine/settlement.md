# Settlement

## 1. Introduction and Goals

The queue owns Mutation lifecycle and final typed outcome. A committed acknowledgment saves the execution result; accepted work waits for required Stream target evidence or permitted private fallback. Private fallback cannot replace newer authority or undo later independent writes. Queue retirement, Model replay and terminal notification eligibility share the settlement transaction.

## 5. Building Block View

[Implementation](../../../../../crates/client/src/settlement05.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
