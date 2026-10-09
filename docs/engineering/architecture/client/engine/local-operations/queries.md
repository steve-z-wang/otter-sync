# Queries

## 1. Introduction and Goals

Queries read the visible tables, which already contain the merged view (server truth plus pending local edits), so a reader never assembles that view itself. The component offers a small typed query language and an escape hatch for read-only SQL.

## 3. Context and Scope

| Operation | Input | Result |
| --- | --- | --- |
| `read` | model and identity | one row or none |
| `query` / `query_spec` | model, equality filter, optional ordering and limit | rows |
| `related` | a record and a reference name | the referenced row or none |
| `referencing` | a record, a referencing model and reference name | rows pointing at it |
| `read_sql` / `session_sql` | SQL text and parameters | rows as objects |
| `sql_tables` | SQL text | the tables a watched statement reads, by their stored names |

Outside a transaction, reads use the committed reader connection and see the last commit. Inside a transaction they use the writer and see the transaction's own writes ([Frontend interface](../../frontend-interface.md)). Generated model classes in [Typed API](../../../frontend-sdk/api.md) translate typed calls onto these operations.

## 5. Building Block View

The rules a caller needs to know:

- A filter is a set of field equalities; `null` matches a null column. List-typed fields cannot be filtered.
- Ordering applies to scalar fields only. Nulls sort first, strings compare by UTF-16 code units (JavaScript order), numbers as floating point, and the identity breaks ties, so results are deterministic across runtimes.
- `limit` truncates after ordering.
- `read_sql` accepts read-only statements that return at least one column, with unique column names; writes and pragmas are refused, and blob columns cannot be returned.
- `sql_tables` is how the runtime's `watchSql` learns what to wait for ([#184](https://github.com/zanminwang/axton/issues/184)): SQLite names the tables while it prepares the statement ([Store](../../storage/store.md)), never the application. It accepts only one read-only `SELECT` or `WITH … SELECT` and refuses one that reads an engine table (`axton_*`). What those names mean is the [table contract](../../storage/reconciliation.md#the-table-contract).

Code: `evaluate`, `related`, `referencing`, `rows_to_objects` in [client/query.rs](../../../../../../crates/client/src/query.rs); the read-only guard in [sqlite/lib.rs](../../../../../../crates/sqlite/src/lib.rs).

## 10. Quality Requirements

- **Filters are normalized like writes, ordering follows the shared comparison rules, and relations resolve from stored foreign keys.** Evidence: [sqlite/tests/query.rs](../../../../../../crates/sqlite/tests/query.rs) `query_normalizes_filters_orders_nulls_and_resolves_relationships`.
- **Read-only SQL sees optimistic rows and refuses writes.** Evidence: `readonly_sql_sees_optimistic_rows_and_refuses_write_statements`.
- **A watched statement's tables are SQLite's answer: each Model table of a join once, whatever case or aggregate names it, and no CTE; writes, pragmas, `EXPLAIN`, several statements and engine tables are refused.** Evidence: `sql_tables_are_the_model_tables_a_select_reads`, executed 2026-09-28 with `cargo test -p axton-sqlite --locked`.

Tests read, not executed.

## 11. Risks and Technical Debt

**Accepted limitation (measured cost pending).** Filtering runs in SQL, but ordering and `limit` run in memory after every matching row is loaded, and only equality filters exist. Adequate for the current scale; listed as a cost center in [#12](https://github.com/zanminwang/axton/issues/12).
