# Transport

## 1. Introduction and Goals

The Node HTTP/WebSocket host authenticates requests, applies application admission and submits bytes to Rust. Rust validates discriminator 5 and context before business work. Handshake, Batch, ordinary reads and finite materialization have separate carriers; WebSocket offers complete immutable units and hints. Authentication, infrastructure and typed business refusal remain distinct failure classes.

## 5. Building Block View

[Implementation](../../../../../packages/backend/server/index.mts) owns this component. [Protocol 5](../../protocols/sync.md) owns shared context, delivery and settlement rules.

