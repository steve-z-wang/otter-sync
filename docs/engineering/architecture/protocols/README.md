# Protocols

Sync defines Client ↔ Server messages. Client bridge defines Frontend SDK ↔ Rust client messages. Server bridge defines Backend SDK ↔ Rust server messages. Each owns field semantics, codecs and pure validation; none opens a connection, schedules a task or calls application code.

- [Sync](sync.md)
- [Client bridge](client-bridge.md)
- [Server bridge](server-bridge.md)

The [Rust contracts](../../../../crates/protocols/src/lib.rs) depend on shared core/schema primitives. Runtimes and bindings consume them; contracts never depend on a runtime. Language mirrors remain checked by shared fixtures. Native ABI entry points belong to Bindings.
