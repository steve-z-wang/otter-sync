# Controller

## 1. Introduction and Goals

The controller drives independent Uplink, direct read and finite delivery work while Storeworker serializes commits. It bounds admission and reserves repair capacity rather than accumulating unbounded live payload. Network waits hold no Store writer. An active local transaction owns its Store until its application callback finishes and commits or rolls back; independent Clients remain responsive. Durable state determines which retry or transfer remains necessary after reconnect.

## 5. Building Block View

[runtime/lanes.rs](../../../../../../crates/client/src/runtime/lanes.rs) owns this component. [Protocol 5](../../../protocols/sync.md) owns shared context, delivery and settlement rules.

- [downlink-worker](downlink-worker.md)
- [push-lane](push-lane.md)
- [scheduling](scheduling.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
