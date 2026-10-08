# Store

## 1. Introduction and Goals

SQLite gives each physical file one exclusive owner across aliases and processes. Unsupported-format admission checks a temporary database/WAL copy before modifying the original. SQL transactions and savepoints commit Model rows, retained input, replay evidence and progress together. Closing releases ownership; normal reopen neither wipes nor abandons pending work.

## 5. Building Block View

[Implementation](../../../../../crates/sqlite/src/lib.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
