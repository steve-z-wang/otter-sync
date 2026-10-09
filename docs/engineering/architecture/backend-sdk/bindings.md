# Backend Bindings

## 1. Introduction and Goals

Connect the application-owned backend to the Rust Server engine. Bindings encode/decode Server bridge messages, translate native errors and dispatch host requests to handlers, Loaders and persistence. The caller's principal, transaction and mutable session pass through unchanged.

## 5. Building Block View

[Server bindings](../../../../packages/backend/server/bindings) own native invocation, host dispatch and effect collection. [Server bridge](../protocols/server-bridge.md) owns their contract; the Rust [backend interface](../../../../crates/server/src/backend_interface.rs) invokes the host. The [PostgreSQL adapter](../../../../packages/backend/postgres/src) executes persistence operations on the retained driver transaction.

## 10. Quality Requirements

Extraction must not open a second transaction, copy session state or change rollback/publication behavior. Host fixtures, preparation/savepoint tests, large Bootstrap delivery and PostgreSQL batching regressions preserve these boundaries. Installed packages must contain every API/Bindings module their exports reference.
