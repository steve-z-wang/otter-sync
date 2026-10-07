# SDKs

Protocol-5 policy and failure boundaries are owned by [protocol 5](../protocol/0.5.md). Rust owns synchronization; language bindings execute networking, timers and interface callbacks. Descriptions of protocol-4, epoch/Load jobs, once caches or legacy queue tables below are historical component context, not current public APIs.

The SDKs are what application code imports. Their job is translation and platform work only: typed calls become tasks for the Rust [client runtime](../client/runtime.md), outcomes and errors come back typed, and the network, timer, credential and callback work the runtime asks for runs on the platform's own tools. Every rule about data, scheduling and retries lives in the Rust runtimes.

- [Typed API](typed-api/README.md) — Expose strongly typed APIs to applications.
- [Bindings](bindings.md) — Carry task submissions, events and wakes between the language and one runtime per client.
