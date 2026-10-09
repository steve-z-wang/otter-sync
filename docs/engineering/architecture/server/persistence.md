# Persistence

## 1. Introduction and Goals

Persistence implements the strict Rust host operation contract inside the application transaction. Store binds principal/Stream and Batch progress; MutationResult retains member outcomes; StreamRecord retains explicit interest and positions; DeliveryPlan/DeliveryUnit retain finite immutable payloads. Publication locks and reserved cursors are transaction-owned. The fresh namespace contains only canonical protocol-5 tables; old framework layouts are refused intact.

## 5. Building Block View

[Implementation](../../../../packages/backend/postgres/migration.sql) owns this component. [Protocol 5](../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.

Bulk identity guards, member application and positional reads use bounded SQL batches inside the same application transaction. Guards retain canonical order across mode changes; positional reads retain duplicate inputs and missing targets. [PostgreSQL batching tests](../../../../integration/persistence/server/persistence-batching.test.mjs) check query growth at 2,055 identities, result correlation, cursor reuse, savepoint rollback and safe counters. Alternating guard modes require separate runs to preserve lock order; cursor reservation remains one statement per published Stream.
