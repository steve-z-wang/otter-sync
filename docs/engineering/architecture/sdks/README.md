# SDKs

Generated TypeScript/Dart facades encode Model identities, Mutation input and fresh named Query/Fetch calls. Per-client native actors own Rust state and scheduling. Node, Dart and React Native Bridges decode outcomes and perform requested network, timer and prerequisite effects. Language scope guards enforce transaction lifetime; they do not implement another authority or retry engine.

[Protocol 5](../protocol/0.5.md) owns the shared contract; [implementation](../../../../bindings/common/src/actor.rs) owns this component. Earlier carrier mechanics below are historical references, not current public contracts.

The SDKs are what application code imports. Their job is translation and platform work only: typed calls become tasks for the Rust [client runtime](../client/runtime.md), outcomes and errors come back typed, and the network, timer, credential and callback work the runtime asks for runs on the platform's own tools. Every rule about data, scheduling and retries lives in the Rust runtimes.

- [Typed API](typed-api/README.md) — Expose strongly typed APIs to applications.
- [Bindings](bindings.md) — Carry task submissions, events and wakes between the language and one runtime per client.
