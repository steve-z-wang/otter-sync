# Reactive SQL over several Models, and a stable table contract (#184)

Status: decided 2026-09-28 by the maintainer; implemented on branch `codex/184-watch-sql` ([plan](../plans/2026-09-28-184-watch-sql-plan.md)).

## Problem

`watch(model, where)` observes one Model under an equality filter, and `readSql` is one-shot. A screen whose answer joins several Models has no engine-owned way to re-run when any of them commits. Examples:

- a Journal page: placements ⋈ entries ⋈ media ⋈ people;
- a feed;
- an inbox.

Most Days works around this by watching every dependent Model unfiltered, purely as a trigger, and re-running `readSql` on each emission. That costs a whole-Model read per trigger per commit. The replaced framework, LocalSync, offered this natively as `readOnlySql.watch(sql, tables:)`.

## Decision

1. **The client exposes `watchSql(sql, parameters)`** in every SDK (Dart, TypeScript, React Native). The Rust runtime owns it, like `watch`. It works as follows:
   - **Read-only.** Any statement that is not a read-only `SELECT` or `WITH … SELECT` is refused. So is a statement that reads an engine table (`axton_*`).
   - **The runtime finds the tables itself.** It determines the tables the statement reads from SQLite, for example through the authorizer or statement metadata. The application never lists them.
   - **When it re-runs.** After every commit that writes any of those tables: settlement, optimistic apply and replay, rejection rollback, direct local writes, Channel delivery, Load and Fetch.
   - **What it emits.** The first result when listened to, then each result that differs from the last one.
   - **Errors and lifecycle** work exactly like `watch`:
     - a first failure ends the stream;
     - a later failed re-run is reported and the watch stays;
     - cancelling unwatches, and closing the client completes the stream.
   - **Not inside a transaction.** It is refused inside a transaction, as `watch` is.
2. **The local table layout of Models is a public, stable contract:**
   - a Model's table is named exactly the Model name (`Space`, `MomentPlacement`), and each column exactly its field name;
   - every table the engine owns is named `axton_*`, and applications must not read it;
   - changing either rule is a breaking change.

   Document the contract in the client storage docs and the SQL guide. Check that no engine-owned table escapes the `axton_` prefix, including any per-Model support table; rename any that does.

## Tests

- **Multi-Model join.** A join over three Models re-emits after a commit to each of them.
- **No emission when nothing changed.**
  - A commit to an unrelated Model does not re-run the watch.
  - A commit that leaves the answer unchanged emits nothing.
- **Every commit path re-emits:** settlement, rejection rollback, replay, Channel delivery, Load and Fetch.
- **Refusals:** a write statement and an `axton_*` read are refused.
- **The naming contract** is pinned by a test that opens a schema and asserts the table and column names.
