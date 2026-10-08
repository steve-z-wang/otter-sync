# Publish

## 1. Introduction and Goals

Explicit track creates interest with a real discoverable position; repeat live tracking retains its position. Global invalidation reaches current holders and targeted invalidation reaches selected existing holders, without enrolling new ones. The persisted fence and canonical Stream locks keep content and position consistent; one transaction reserves at most one cursor per affected Stream. Wakes occur after commit.

## 5. Building Block View

[Implementation](../../../../../crates/server/src/delivery_plan.rs) owns this component. [Protocol 5](../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
