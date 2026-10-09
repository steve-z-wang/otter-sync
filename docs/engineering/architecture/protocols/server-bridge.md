# Server bridge protocol

## 1. Introduction and Goals

Define the Rust Server engine's host requests and responses: handler/Loader calls, tracking, guards, positions, publication and protocol-5 persistence operations. Error codes, refusal versus infrastructure failure and canonical identity/counter validation remain part of the contract.

## 5. Building Block View

[Server bridge](../../../../crates/protocols/src/server_bridge/mod.rs) owns HostRequest, response DTOs and pure response validation. Member positions/conversions and the structured error carrier live with this contract. [Backend interface](../../../../crates/server/src/backend_interface.rs) owns Host and typed invocation. [Backend Bindings](../backend-sdk/bindings.md) connect that invocation to handlers and persistence using the caller's original transaction, authenticated principal and mutable session.

The TypeScript mirror is [host-contract.mts](../../../../packages/backend/server/bindings/host-contract.mts). [Shared host fixtures](../../../../fixtures/protocol/host-operations.json) retain the same payloads and malformed-response checks.

## 10. Quality Requirements

Contract validation performs no I/O or scheduling. Adapters must not replace the caller's transaction or split its session during extraction. Rust host tests, the native Host fixture suite and retained preparation/savepoint regressions verify the separate contract and invocation boundaries.
