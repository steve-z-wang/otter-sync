# Reconciliation

## 1. Introduction and Goals

Schema artifacts preserve the original Mutation input contract and exact frozen request. Canonical read materialization is separate: a compatible descriptor reorder keeps queued input, while a changed read contract stages a desired context. Enabling that context requires complete authority transfer; Model DDL alone proves no coverage. Format-4 files are refused intact rather than rebuilt or wiped automatically.

## 5. Building Block View

[Implementation](../../../../../crates/client/src/store05.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.

### The table contract

Each public Model table uses its Model name and field columns. Engine tables use the `axton_` prefix and are not application query surfaces. Ordinary application reads see the replayed projection.
