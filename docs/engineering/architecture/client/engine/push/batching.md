# Batching

## 1. Introduction and Goals

A Batch has one Store sequence and immutable member order, digest and input. Each server member commits independently, so local multi-Mutation atomicity is not backend all-or-nothing execution. Lost responses retry identical bytes; saved members run no Handler or preparation. A valid next Batch replaces the older retained replay window transactionally.

## 5. Building Block View

[Implementation](../../../../../../crates/server/src/mutation_batch.rs) owns this component. [Protocol 5](../../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
