# Downlink worker

## 1. Introduction and Goals

DeltaApplier validates context, header and part digests, then admits bounded fragments into DeliveryQueue. It applies a complete constraint-safe unit in one SQLite transaction with per-key guards and durable coverage. Incomplete fragments never become a prefix. Expiry, gaps and blocked apply leave progress unclaimed and permit explicit repair.

## 5. Building Block View

[Implementation](../../../../../../crates/client/src/sync05/delta_applier.rs) owns this component. [Protocol 5](../../../protocol/0.5.md) owns shared context, delivery and settlement rules.

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
