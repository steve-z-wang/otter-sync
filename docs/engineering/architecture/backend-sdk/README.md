# Backend SDK

The API registers handlers, Loaders, Bootstrap preparation and persistence/connection configuration. Bindings call the native Server engine and answer host requests in the retained application transaction. Rust owns protocol admission, mutation processing, publication and delivery planning.

- [API](api.md)
- [Bindings](bindings.md)

Sources: [Server SDK](../../../../packages/backend/server) and [PostgreSQL adapter](../../../../packages/backend/postgres). Package names and exports remain stable. The PostgreSQL adapter retains batching, canonical lock order, namespace validation and serialization retries.
