# watchSql and the Stable Table Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close [#184](https://github.com/zanminwang/axton/issues/184) as the [design](../specs/2026-09-28-184-watch-sql-design.md) decides: `watchSql(sql, parameters)` in every SDK, owned by the Rust runtime, re-run only after commits that write a table the statement reads; and the local table layout documented and pinned as a stable contract.

**Architecture:** The SQLite store prepares the statement on the committed reader under a temporary authorizer and answers the tables it reads, refusing anything but one read-only `SELECT` / `WITH … SELECT` and any `axton_*` table. The runtime registers a SQL watch as an observer beside the Model watches: it runs the statement, keeps a `Client::watch(tables)` receiver - the engine's existing per-commit written-table notification, fed by every commit path through `Client::write`, `commit_session` and `rebuild` - and, at the end of each unit with no callback transaction open, re-runs only the SQL watches whose receiver was signalled, publishing a `{"kind":"watch","rows"}` snapshot when the rows differ. `unwatch`, close and rebuild behave as for `watch`. The SDKs submit a `watchSql` task and reuse the `watch` delivery path unchanged.

**Tech Stack:** Rust (`axton-client` runtime, `axton-sqlite` store with rusqlite `hooks`), TypeScript client (Node and React Native share it), Dart SDK, compiler emitter (generated `GeneratedClient` passthroughs), docs and website.

## Constraints

- Implement the design as written; a decision that cannot work is a `[blocked]` comment, never a substitute.
- Keep `watch`, `readSql` and every Most Days surface unchanged.
- Keep changes self-contained (a parallel branch touches SDK index files and runtime watch machinery): new code in its own functions, small edits at shared seams.
- Do not run the full `scripts/test.sh` locally; run focused suites, CI runs the host gate.

## Task 1: Store - tables a statement reads

**Files:** `crates/client/src/store.rs` (trait method with a refusing default), `crates/sqlite/Cargo.toml` (`hooks`), `crates/sqlite/src/lib.rs`; test `crates/sqlite/tests/query.rs`.

- [ ] Failing tests: a join over three Model tables answers exactly those three (a `count(*)` and a differently-cased name resolve to the stored table name; a CTE name is not a table); an `INSERT`, `UPDATE`, `DELETE`, a `PRAGMA`, a `pragma_table_info` select, an `EXPLAIN`, two statements and a statement reading `axton_record` or `axton_before_Entry` are refused; the authorizer is gone afterwards (`readSql` still works, a write on the writer is unaffected).
- [ ] `ClientStore::read_tables(&mut self, sql) -> Result<BTreeSet<String>>`; `SqliteStore` installs the authorizer on the reader for one `prepare`, removes it, checks read-only, columns and `is_explain`, resolves each name through `sqlite_schema` without case, refuses `axton_*`.
- [ ] `Client::sql_tables(sql)` forwards it.

## Task 2: Runtime - `watchSql` observer

**Files:** `crates/client/src/runtime/protocol.rs` (`Command::WatchSql`), `tasks.rs`, `commands.rs`, `observers.rs`; `fixtures/bridge/envelopes.json`; tests in `crates/sqlite/tests/runtime_lanes.rs` and `runtime_loads.rs`.

- [ ] Failing tests (runtime host, three Models `Entry`, `Media`, `Person` plus an unrelated `Note`):
  - a join over the three re-emits after a direct commit to each;
  - a commit to `Note`, a scope registration and a push freeze do not re-run it (a `random()` column proves it), a commit that leaves the joined answer unchanged emits nothing;
  - every commit path re-emits: settlement (push receipt), rejection rollback, replay under a pending Mutation, Channel delivery (socket page), Fetch; Load in `runtime_loads.rs`;
  - refusals: a write statement, an `axton_*` read and an unknown table fail the task and register nothing;
  - a failed re-run is reported and the watch stays; `unwatch` stops it; a callback transaction's writes are invisible until commit; close ends it with `closed: true`; a rebuild keeps and re-runs it.
- [ ] Implement: command decode, the observer (initial rows published after the task's completion, re-run on its receiver), `unwatch` and close shared with `watch`.
- [ ] Add the envelope to the fixture shared by the Rust, TypeScript and Dart bridge tests.

## Task 3: Naming contract test and engine-table audit

**Files:** `crates/sqlite/tests/ddl.rs`.

- [ ] Test: open a schema with two Models, one with a unique constraint; every table in `sqlite_schema` is a Model name or starts with `axton_`; each Model table's columns are exactly its field names in declaration order.
- [ ] Audit every `CREATE` in `crates/client/src`; record the result (no rename expected: `axton_before_<Model>` is already prefixed; unique indexes are indexes).

## Task 4: SDKs

**Files:** `packages/client-js/runtime.mts`; `packages/dart/lib/src/client.dart`; tests `integration/bindings/client-js/runtime.test.mjs`, `integration/bindings/client-react-native/runtime.test.mjs`, `packages/dart/test/client_test.dart`.

- [ ] TypeScript `watchSql(sql, parameters = [], listener, onError?)`: same body as `watch`, submitting `watchSql`. Tests on the native runtime: a join re-emits after commits to each Model and not after an unrelated one; refusals (write, `axton_*`) reach `onError`; `transaction_active` inside a callback; stop unwatches; close ends it. React Native: the shared client over the native library, with its documented narrower transaction guard.
- [ ] Dart `watchSql(sql, {parameters})`: the `watch` stream, submitting `watchSql`. Same tests.

## Task 5: Generated clients

**Files:** `crates/compiler/src/emit.rs`; compiler test; every checked-in generated client.

- [ ] Failing compiler test: generated TypeScript and Dart `GeneratedClient` expose `watchSql` beside `readSql`.
- [ ] Emit the passthroughs; regenerate with each runner's command; confirm only the added members changed.

## Task 6: Documentation

- [ ] Engineering: storage README and reconciliation (the stable table contract), local-operations queries (`read_tables`, `watchSql`), runtime observers and risks, frontend interface, SDK typed-API client.
- [ ] Website: SQL escape-hatch section of `frontend/runtime.md` (`watchSql`, the table contract), `frontend/storage.md`, `api-index.md`, React Native README if it lists methods.
- [ ] Check links and anchors; `python3 website/scripts/check_examples.py`.

## Task 7: Verify and ship

- [ ] `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test -p axton-client -p axton-sqlite -p axton-compiler -p axton-binding --locked`.
- [ ] `node --test integration/bindings/client-js/*.test.mjs`, `node --test integration/bindings/client-react-native/*.test.mjs`, prettier, `tsc -p packages/client-react-native`, `npm run typecheck`.
- [ ] `packages/dart` analyze and test; `bash integration/generated-api/verify.sh`; `bash integration/action-runtime-ts/verify.sh`; action-contract checks.
- [ ] PR with `Closes #184`, the API per SDK, evidence executed versus inspected, limits, Most Days statement; labels and `[pr]` comment; watch CI.
