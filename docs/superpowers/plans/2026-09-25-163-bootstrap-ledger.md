# Bootstrap Ledger Containment and Settlement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep healthy Bootstrap runs moving when another active ledger row is undecodable, report that row once per unchanged defect, and settle large or duplicate barrier batches within SQLite's parameter limit.

**Architecture:** The ledger produces strict named reads and tolerant internal scans that carry decoded rows plus row diagnostics. The Downlink worker emits those diagnostics through the existing host error callback while suppressing repeats. Barrier lookup deduplicates and chunks inputs, decodes candidates before writing, and retains the fenced completion transaction.

**Tech Stack:** Rust client and SQLite integration tests; serialized Downlink actions; TypeScript and Dart connection bridges.

## Global Constraints

- Preserve existing public method signatures, storage schema, Bootstrap phases, run fences, and D10 completion semantics.
- Keep malformed rows unchanged and never manufacture completion or clear progress.
- New prose says Channel; leave existing Scope identifiers and the wider #152 rename alone.
- Chunk SQL `IN` lists at 900 bind values or fewer; do not rely on a newer SQLite variable limit.
- Contain row decode errors only; propagate SQL/store/write errors.
- Report defects via existing application `onError` callbacks with channel and bounded reason in the message, with no repeated pump flood.
- Ship independently of runtime cleanup issue #134 against the current Downlink action bridge; no shared executor refactor.

---

## File map

| File | Responsibility |
| --- | --- |
| `crates/client/src/bootstrap_ledger.rs` | Strict named decode, tolerant internal scans, deduplicated/chunked settlement candidates |
| `crates/client/src/bootstrap.rs` | Public strict behavior and internal schedule/barrier results carrying diagnostics |
| `crates/client/src/downlink_worker.rs` | Report diagnostics and suppress repeats within the worker lifetime |
| `packages/client-js/connection.mts` and its action type source | Route the internal diagnostic action to the existing JS `onError` |
| `packages/dart/lib/src/connection.dart` | Route the same action to Dart `onError` |
| `crates/sqlite/tests/bootstrap.rs` | Real SQLite row corruption, candidate isolation, deduplication and a recording-store bind-count test |
| `crates/sqlite/tests/bootstrap_worker.rs` | Healthy scheduling/reopen, one diagnostic, repair and no busy loop |
| `integration/bindings/client-js/connection.test.mjs`, `packages/dart/test/connection_test.dart` | Host action dispatch |
| `docs/engineering/architecture/client/connection/controller/downlink-worker.md` | Update implemented scheduling/diagnostic behavior after tests pass |

### Task 1: Isolate active-row decode without weakening named reads

**Interfaces:** In `bootstrap_ledger.rs`, introduce `pub(crate) struct LedgerIssue { pub channel: String, pub detail: String, pub fingerprint: String }` and `pub(crate) struct LedgerScan<T> { pub rows: Vec<T>, pub issues: Vec<LedgerIssue> }`. Add `bootstrap_task_scan(&mut self) -> Result<LedgerScan<Loaded>>`. Keep `bootstrap_task_rows(&mut self) -> Result<Vec<Loaded>>` strict for direct `Client::bootstrap_tasks`.

- [ ] **Step 1: Write failing SQLite tests** in `crates/sqlite/tests/bootstrap.rs`. Use an initialized requested row `bad` and a requested row `good`; corrupt `bad.bootstrap_cursor` with SQL text or set an invalid `bootstrap_error` JSON value through the test's real SQLite store. Assert strict `bootstrap_state("bad", id)` errors, `bootstrap_tasks()` errors, `bootstrap_schedule(None)` returns `good`, and the raw corrupt column remains unchanged. Add a second scenario with a malformed active `bad` row, a healthy `waiting` row in `catching_up` whose barrier was reached, and a separate healthy `good` row in `requested`: `bootstrap_barriers` must discover `waiting`, while `bootstrap_schedule` selects `good`. Reuse the existing `origin` and `bootstrap` helpers and the repository's test store setup.
- [ ] **Step 2: Run** `cargo test -p axton-sqlite --test bootstrap --locked`. Expected: the new isolation assertions fail because the whole active scan returns the bad decode error.
- [ ] **Step 3: Implement** the tolerant scan. Query the existing active-row predicate and `COLUMNS` once. Iterate `rows.rows`; for each row, require `row[0].as_str()` as the isolating key, call existing `decode(&row)`, append success to `rows`, or append a `LedgerIssue` with channel, bounded error text, and a deterministic fingerprint of the channel, subscription identity, raw Bootstrap fields, and only invalid subscription fields. Normalize valid `starting_cursor` and ordinary `cursor` values out of the fingerprint; include their raw values only if those values fail decoding. Bound the detail text before it enters an action. Keep SQL and unkeyable rows as `Err`. Retain the existing strict scan for public `bootstrap_tasks`; do not convert a bad row to `BootstrapState`.
- [ ] **Step 4: In** `bootstrap.rs`, add internal `bootstrap_schedule_scan(rotation) -> Result<(Option<BootstrapTask>, Vec<LedgerIssue>)>` and `bootstrap_barriers_scan() -> Result<(Vec<String>, Vec<LedgerIssue>)>`, preserving the current rotation algorithm on `scan.rows`. Keep public `bootstrap_schedule` and `bootstrap_barriers` signatures by returning the healthy value from these helpers. Keep public `bootstrap_tasks` strict. If public methods otherwise cannot surface a diagnostic, document that the worker uses the `*_scan` result and direct named reads remain strict.
- [ ] **Step 5: Run** `cargo test -p axton-sqlite --test bootstrap --locked` and `cargo test -p axton-sqlite --test bootstrap_worker --locked`. Expected: existing tests and Task 1 tests pass.
- [ ] **Step 6: Commit** the focused ledger scan and tests with `git commit -m "fix: isolate undecodable bootstrap task rows"`.

### Task 2: Bound and isolate barrier candidate settlement

**Interfaces:** Add `settleable_scan(&mut self, channels: &[String]) -> Result<LedgerScan<String>>` in `bootstrap_ledger.rs`. Add internal `settle_bootstrap_barriers_scan(&mut self, channels: &[String]) -> Result<(Vec<BootstrapState>, Vec<LedgerIssue>)>` in `bootstrap.rs`; public `settle_bootstrap_barriers` keeps its existing signature.

- [ ] **Step 1: Write failing SQLite tests** in `crates/sqlite/tests/bootstrap.rs`. Give two healthy catching-up channels reached barriers; put duplicate names in the input and append at least 1,001 unique nonmatching names. Assert both complete exactly once, result order is by channel, and a second call writes nothing. Add one malformed catching-up candidate next to a healthy candidate; assert the healthy one completes, the malformed raw row and cursor remain unchanged, and named Bootstrap read of the malformed one errors. Include an empty-input assertion. Add a test-only recording `ClientStore` wrapper around `SqliteStore` that forwards the trait methods and records the parameter count for each barrier-candidate `query`/`query_committed` call. On an input with 1,001 unique names plus duplicates, assert every candidate call has at most 900 parameters, at least two candidate calls occur, and the sum of candidate parameter counts equals the number of distinct names. Keep the real many-channel settlement outcome assertions; the recording assertion is the deterministic proof on SQLite builds whose variable limit exceeds 999.
- [ ] **Step 2: Run** `cargo test -p axton-sqlite --test bootstrap --locked`. Expected: the malformed candidate aborts the write; the recording-store assertion fails because the old query binds the entire input, regardless of the host SQLite limit.
- [ ] **Step 3: Implement** `settleable_scan`: build a sorted unique name vector (for example `BTreeSet<&str>`), return immediately for empty input, iterate `unique.chunks(900)`, and execute the current `catching_up`/barrier predicate with `SELECT {COLUMNS}` and only the chunk's bind values. Apply the same keyed decode helper used in Task 1 to each returned row. Collect healthy channel names and issues; sort/deduplicate names before returning. Do not catch `self.rows` errors.
- [ ] **Step 4: Implement** the internal `Client::settle_bootstrap_barriers_scan` as one committed read of `settleable_scan`, followed by the existing single write transaction over healthy names only. Each `settle_barrier` re-reads and fences its row. The public method returns only settled states and keeps its signature. A zero-candidate result opens no writer.
- [ ] **Step 5: Run** `cargo test -p axton-sqlite --test bootstrap --locked`. Expected: all focused ledger tests pass, including >999 names, deterministic per-query bind counts, duplicate removal, and corrupt candidate isolation.
- [ ] **Step 6: Commit** with `git commit -m "fix: bound bootstrap barrier settlement"`.

### Task 3: Report contained rows through the Downlink hosts

**Interfaces:** Add `DownlinkAction::LedgerIssue { channel: String, message: String }` (serialized as `ledgerIssue`). The action is internal to the Downlink command bridge and does not change the public subscription API. The worker holds a `BTreeMap<String, String>` of last reported fingerprints.

- [ ] **Step 1: Write worker tests** in `crates/sqlite/tests/bootstrap_worker.rs`. Inject one malformed active row and one healthy schedulable row. After `Start`/reopen and `Next`, assert one `LedgerIssue` names the bad channel and a healthy `Request` is still issued. Pump and wake repeatedly without changing the bad Bootstrap fields, including an ordinary delivery that advances its otherwise valid live cursor: assert no more issue actions and no repeated zero-millisecond `Wait`. Change the malformed value and wake: assert one new issue. Repair or remove the row, wake, then corrupt it again: assert one new issue. Add a reached healthy barrier beside a bad active row on reopen and assert settlement still occurs.
- [ ] **Step 2: Run** `cargo test -p axton-sqlite --test bootstrap_worker --locked`. Expected: new reporting assertions fail before the action exists.
- [ ] **Step 3: Add** a worker helper that accepts `Vec<LedgerIssue>`, compares `fingerprint` with the worker map, and emits one `LedgerIssue` action per newly seen/changed defect. Complete active scans also remove map entries for channels absent or healthy; candidate-only scans update only observed malformed candidates. Call it from `resume`, `schedule`, and the settlement path after both moved-channel and reopen scans. Preserve the one-commit-per-pump scheduling rule and the existing `loading.dirty = false` behavior when no healthy task exists.
- [ ] **Step 4: Add host dispatch tests** using the existing scripted action fixtures: JS `connection.test.mjs` should observe one `Error` at `onError` for a `ledgerIssue` action, and Dart `connection_test.dart` should observe one `StateError`; assert each error message contains the affected channel and the bounded reason, and no status transition is emitted. In `packages/client-js/connection.mts`, add the action to the typed `DownlinkAction` union and dispatch it to `options.onError?.(Error("bootstrap ledger " + action.channel + ": " + action.message))`. In Dart, add the `ledgerIssue` switch case with `onError?.call(StateError('bootstrap ledger ${action['channel']}: ${action['message']}'))`. Preserve status projection: the action does not impersonate a Bootstrap transition.
- [ ] **Step 5: Run** `cargo test -p axton-sqlite --test bootstrap_worker --locked`, `node --test integration/bindings/client-js/connection.test.mjs`, and `(cd packages/dart && dart test test/connection_test.dart)` after the documented native build prerequisites. Expected: new and existing assertions pass.
- [ ] **Step 6: Update** `docs/engineering/architecture/client/connection/controller/downlink-worker.md` with the observed containment and report behavior, keeping it concise and linking the tests. Commit code, host tests, and architecture text with `git commit -m "fix: report contained bootstrap ledger rows"`.

### Final verification

- [ ] Run `cargo fmt --all -- --check`, `cargo test -p axton-sqlite --test bootstrap --locked`, `cargo test -p axton-sqlite --test bootstrap_worker --locked`, and `cargo test --workspace --locked`. Record actual results.
- [ ] Run `bash scripts/test.sh` when the documented Node, Dart, Python, PostgreSQL and native build prerequisites are present. If unavailable, name each omitted gate rather than claiming it passed.
- [ ] Review the diff for unintended schema, public API, or Scope-to-Channel renames. Check the tests assert raw corrupt row preservation and D10's `B=S, L>=H` completion guard.

## Plan self-review

Every target requirement maps to a task: active-row isolation (1), strict named reads (1), chunked and deduplicated candidate reads with deterministic bind-count evidence (2), corrupt candidate isolation (2), worker diagnostic visibility and duplicate suppression even when a valid live cursor advances (3), host callbacks with channel and reason (3), and D10 regression checks (2 and final verification). The interfaces use `LedgerIssue` and `LedgerScan` consistently. No implementation, build, or test result is claimed by this planning document.
