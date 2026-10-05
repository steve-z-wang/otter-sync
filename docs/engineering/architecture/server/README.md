# Server

The server runtime executes the sync protocol on top of the application's own database and business logic.

- [Protocol 4 runtime](protocol4.md) — Bound Store contexts, null reads, identity manifests, receipts and constraint-safe delivery.
- [Backend interface](backend-interface.md) — Invoke application handlers and loaders.
- [Engine](engine/README.md) — Process mutations, read their results back, serve pulls and produce receipts.
- [Persistence](persistence.md) — Persist sync metadata within the application's transaction; no business logic.
- [Connection](connection/README.md) — HTTP/WebSocket, subscriptions and streaming.

## How the parts work together

Every request runs inside one application database transaction. The [connection](connection/README.md) authenticates it and calls the Rust [engine](engine/README.md); the engine drives the work through a small set of host operations that the [backend interface](backend-interface.md) routes either to application code (handlers, loaders) or to [persistence](persistence.md) (the framework tables). The engine applies explicit Stream declarations and resolves canonical state through viewer Loaders. Business writes, memberships, publications, framework rows and the saved outcome share the transaction and commit or roll back together. Protocol 4 uses real per-Stream positions; ordinary reads and call-private snapshots carry null cursors. Live subscribers are woken only after the commit.
