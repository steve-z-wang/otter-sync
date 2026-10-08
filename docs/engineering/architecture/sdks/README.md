# SDKs

## 1. Introduction and Goals

Generated TypeScript/Dart facades encode Model identities, Mutation input and fresh named Query/Fetch calls. Per-client native actors own Rust state and scheduling. Node, Dart and React Native Bridges decode outcomes and perform requested network, timer and prerequisite effects. Language scope guards enforce transaction lifetime; they do not implement another authority or retry engine.

## 5. Building Block View

[Implementation](../../../../bindings/common/src/actor.rs) owns this component. [Protocol 5](../protocol/0.5.md) owns shared context, delivery and settlement rules.

- [bindings](bindings.md)
- [typed-api](typed-api/README.md)

## 10. Quality Requirements

Changes must preserve the component boundary and the protocol’s commit/failure rules. The joined native gate `integration/v05-sdk/run-host.sh` exercises the generated client, real HTTP/WebSocket backend and SQLite. Installed-package and mobile evidence are separate adoption gates.
