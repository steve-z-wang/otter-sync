# Connection

## 1. Introduction and Goals

The Rust connection schedules authentication, finite HTTP work, live hints, retries and cancellation. The platform carrier performs HTTP/WebSocket/timer effects and reports outcomes. Live foreign-context headers request repair under the Store context; they cannot advance local progress. Reconnect retains durable S/B/C and frozen Batch intent.

## 5. Building Block View

[runtime/lanes.rs](../../../../../crates/client/src/runtime/lanes.rs) owns this component. [Protocol 5](../../protocols/sync.md) owns shared context, delivery and settlement rules.

- [controller](controller/README.md)
- [transport](transport.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
