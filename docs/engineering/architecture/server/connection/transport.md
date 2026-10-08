# Transport

## 1. Introduction and Goals

The Node HTTP/WebSocket host authenticates requests, applies application admission and submits bytes to Rust. Rust validates discriminator 5 and context before business work. Handshake, Batch, ordinary reads and finite materialization have separate carriers; WebSocket offers complete immutable units and hints. Authentication, infrastructure and typed business refusal remain distinct failure classes.

## 5. Building Block View

[Implementation](../../../../../packages/server/index.mts) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
