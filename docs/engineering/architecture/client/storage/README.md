# Storage

Storage gives the engine a SQL executor whose tables are the schema record. It contains no sync logic.

- [Store](store.md) — The SQL contract and its SQLite implementation: transactions, savepoints, reads and writes.
- [Reconciliation](reconciliation.md) — Table layout per model and how an existing database is brought in line with a newer compiled schema, or rebuilt beside.

The layout of the Model tables is a public, stable contract: a Model's table is named exactly the Model name and each column exactly its field name, and every table the engine owns is named `axton_*`, which applications must not read ([Reconciliation](reconciliation.md#the-table-contract)).

## Code map

| Part | Code location |
|---|---|
| Store | [client/store.rs](../../../../../crates/client/src/store.rs), [sqlite/lib.rs](../../../../../crates/sqlite/src/lib.rs) |
| Reconciliation | [client/ddl.rs](../../../../../crates/client/src/ddl.rs), [client/schema_store.rs](../../../../../crates/client/src/schema_store.rs), `open_at` and `rebuild` in [client/lib.rs](../../../../../crates/client/src/lib.rs), `Schema::compatibility` in [core/schema.rs](../../../../../crates/core/src/schema.rs) |

## Stream vocabulary upgrade

Existing SQLite files migrate framework delivery tables/columns and the layout marker in place before network scheduling. The database path, subscriptions/cursors, Model rows/stamps, frozen Load requests/continuations, queued/pending/rejected work, companions and device-only Models are preserved. No application query-cache JSON is rewritten. The opening transaction commits `local_authority_version=1` and removes the obsolete client holding table/index; retained reconciliation fields are inert. Historical Remove changes no Model, while newer Loader null is canonical absence. Unsubscribe retains content and server tracking. See [reconciliation](reconciliation.md#stream-membership-upgrade) and [coordinated cutover](../../../../../website/docs/backend/deployment.md#stream-membership-cutover).
