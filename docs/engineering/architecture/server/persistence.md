# Persistence

## 1. Introduction and Goals

Persistence implements the strict Rust host operation contract inside the application transaction. Store binds principal/Stream and Batch progress; MutationResult retains member outcomes; StreamRecord retains explicit interest and positions; DeliveryPlan/DeliveryUnit retain finite immutable payloads. Publication locks and reserved cursors are transaction-owned. Shared legacy SQL names do not create independent protocol-5 lifecycle truth.

## 5. Building Block View

[Implementation](../../../../packages/postgres/migration.sql) owns this component. [Protocol 5](../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
