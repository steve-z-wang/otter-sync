# Store

## 1. Introduction and Goals

The store is the only thing in the client that talks to a database. The engine owns every SQL statement; the store owns connections, transactions and value conversion. Keeping the contract this small lets the same engine run over any SQL store, and keeps every rule about sync out of the database layer.

## 3. Context and Scope

The contract, `ClientStore`:

| Group | Operations | Notes |
| --- | --- | --- |
| Transaction | `begin`, `commit`, `rollback` | one writer transaction at a time |
| Savepoints | `savepoint(name)`, `release(name)`, `rollback_to(name)` | nested scopes inside the transaction |
| Writes | `execute(sql, params)`, `execute_batch(sql)` | rows affected |
| Reads | `query(sql, params)` inside the writer's view; `query_committed(sql, params)` last commit only | columns and rows |
| Read tables | `read_tables(sql)`: prepare on the reader without running | the stored names of the tables a watched statement reads, or a refusal; a store that cannot tell refuses by default |

Callers: the engine only ([Engine](../engine/README.md)). The SQLite implementation is the one shipped; the simulation and tests use it too.

## 5. Building Block View

The SQLite store opens two connections to one file: a writer in WAL mode with foreign keys on, and a reader in query-only mode. The reader is what lets the application read the last commit while a long transaction is open on the writer. Transactions begin with `BEGIN IMMEDIATE`, so a second writer waits up to one second and then fails rather than deadlocking.

Values cross as JSON: booleans become integers, arrays and objects become JSON text, numbers become integers or reals. On the way out, blobs and non-UTF-8 text are refused. Both read operations refuse statements that are not read-only or return no columns, which is what makes application SQL safe to expose.

`read_tables` serves watched SQL ([#184](https://github.com/zanminwang/axton/issues/184)). It installs an authorizer on the reader for one `prepare` and removes it before answering, so no other statement is observed. Every `SQLITE_READ` names a table, including once with an empty column for a table read without columns (`count(*)`), under the name as the statement wrote it; each name is resolved to the stored table name without case. A CTE is not reported. Any other authorizer action than `SELECT`, `READ`, `FUNCTION` or `RECURSIVE` (a `PRAGMA`, a write), an `EXPLAIN`, a statement that is not read-only or returns no columns, more than one statement and any `axton_*` table are refused. SQLite's own `sqlite_*` tables (`sqlite_schema`) are allowed, and so is a table-valued pragma function: both are read-only `SELECT`s of tables no commit writes, so the runtime registers no watcher for them and the statement never re-runs.

The store enforces the constraints the framework DDL declares, so an invariant written as a `CHECK` cannot be violated by any engine path: `axton_subscription`'s cursor pair, for instance, is either both NULL or both set ([Reconciliation](reconciliation.md)). A violating write fails the statement and the transaction rolls back like any other error.

Not every invariant can be a `CHECK`. SQLite's `ALTER TABLE` cannot add one, so columns added to an existing table - the five `bootstrap_` columns of `axton_subscription` ([#151](https://github.com/zanminwang/axton/issues/151)) - are validated by the engine instead, on every read and every write of the row, rather than by the file. A store-level rule is also the wrong place for a rule about *which* row a write may touch: a historical page is fenced by the subscription identity, the run and the committed progress it claims to continue, tests the write repeats inside its own transaction ([Pull](../engine/pull.md)).

Code: [client/store.rs](../../../../../crates/client/src/store.rs) (contract), [sqlite/lib.rs](../../../../../crates/sqlite/src/lib.rs) (implementation).

## 10. Quality Requirements

- **The reader sees only committed rows; the writer sees its own.** Evidence: [sqlite/tests/store.rs](../../../../../crates/sqlite/tests/store.rs) `reader_sees_only_committed_rows_and_writer_sees_its_own`.
- **Savepoints nest and roll back independently** (basis of guarantee L3). Evidence: `savepoints_nest_and_rollback_independently`.
- **`read_tables` answers the stored names of the tables a `SELECT` reads and refuses writes, pragmas, `EXPLAIN`, several statements and engine tables; the authorizer lives for one prepare.** Evidence: [sqlite/tests/query.rs](../../../../../crates/sqlite/tests/query.rs) `sql_tables_are_the_model_tables_a_select_reads`, executed 2026-09-28 with `cargo test -p axton-sqlite --locked`.
- **Read paths refuse writes; a second writer fails instead of hanging.** Evidence: `queries_refuse_writes_and_arrays_travel_as_json_text`, `second_writer_waits_then_fails_on_conflicting_immediate_transaction`.
- **A declared `CHECK` refuses the write the engine attempted.** Evidence: [sqlite/tests/subscriptions.rs](../../../../../crates/sqlite/tests/subscriptions.rs) `the_table_refuses_a_half_initialized_cursor_pair`.
- **An invariant the file cannot declare is refused by the engine on the way in and on the way out.** Evidence: [sqlite/tests/bootstrap.rs](../../../../../crates/sqlite/tests/bootstrap.rs) `a_stored_failure_is_bounded`, `bootstrap_state_is_serializable`, `a_failed_commit_leaves_neither_authority_nor_progress`.

The store's own tests were read, not executed. The bootstrap ledger tests above were executed on 2026-09-25 with `cargo test -p axton-sqlite --locked`.

## 11. Risks and Technical Debt

**Potential risk: two writers on one file.** *Condition:* two handles (two processes, or a stale handle) try to write within the same second. *Consequence:* the second sees `database is locked` after the one-second busy timeout; correctness is protected by the generation fence in [Frontend interface](../frontend-interface.md), availability is not. *Evidence:* the busy-timeout pragma and the test above. Multiple-writer support and the lock timeout are [#57](https://github.com/zanminwang/axton/issues/57).
