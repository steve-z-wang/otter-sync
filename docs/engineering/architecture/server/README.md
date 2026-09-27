# Server

The server runtime executes the sync protocol on top of the application's own database and business logic.

- [Backend interface](backend-interface.md) — Invoke application handlers and loaders.
- [Engine](engine/README.md) — Process mutations, read their results back, serve pulls and produce receipts.
- [Persistence](persistence.md) — Persist sync metadata within the application's transaction; no business logic.
- [Connection](connection/README.md) — HTTP/WebSocket, subscriptions and streaming.

## How the parts work together

Every request runs inside one application database transaction. The [connection](connection/README.md) authenticates it and calls the Rust [engine](engine/README.md); the engine drives the work through a small set of host operations that the [backend interface](backend-interface.md) routes either to application code (handlers, loaders) or to [persistence](persistence.md) (the framework tables). After each handler the engine allocates a stamp per changed record, applies the handler's Channel membership changes, publishes each changed or newly added record to its Channels and reads the input targets back through the loaders; because business writes, stamps, memberships, publications, framework rows and the receipt share that transaction, they commit or roll back together. Live subscribers are woken only after the commit.
