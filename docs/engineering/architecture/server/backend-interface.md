# Backend interface

## 1. Introduction and Goals

Handlers receive authenticated context, typed arguments and the retained transaction. Mutation contexts declare tracking/invalidation; Query and Bootstrap contexts are track-only. Loaders answer current viewer-specific records or null and cannot infer permission from Stream names. Returning Model outputs or read snapshots never tracks them; publication must name recipients explicitly.

## 5. Building Block View

[Implementation](../../../../crates/protocols/src/server_bridge/mod.rs) owns this component. [Protocol 5](../protocols/sync.md) owns shared context, delivery and settlement rules.
