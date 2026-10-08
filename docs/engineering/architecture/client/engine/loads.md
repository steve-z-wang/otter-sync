# Loads

Historical carrier reference. Current responsibilities are owned by [protocol 5](../../protocol/0.5.md). This page describes the old release, not a supported current API.

## 1. Introduction and Goals

The Load engine owns the durable state of native [Loads](../../schema/loads.md) on the client ([#173](https://github.com/zanminwang/axton/issues/173)): the job ledger, the once mappings that let a call site reuse a job, the frozen page request, and the transaction that applies a page. Its rules make a Load resumable after exit, replayable by call ID and atomic per page ([guarantees N1–N7](../../../guarantees.md#n-native-loads)). When pages are requested and batched is the [Load worker](../connection/controller/load-worker.md).

## 3. Context and Scope

`Client` operations: `start_load(name, version, args, LoadOptions {once, refresh})` answering `LoadStarted {job, kind: Created | Joined | Reused}`, `get_load`, `list_loads`, `cancel_load`, `retry_load`, `forget_load` and `invalidate_load`, plus, for the worker, `replica_generation`, `load_ready_pages`, `load_page_step`, `record_load_failure` and page application as `StoreDelivery::Load`. The runtime exposes them as the `loadStart`, `loadGet`, `loadStatus`, `loadList`, `loadWait`, `loadCancel`, `loadRetry`, `loadForget`, `loadInvalidate` and `loadDispose` commands ([Runtime](../runtime.md)). Every operation is local and works offline. None runs inside a client transaction: the engine refuses it ("client transaction active"), and the runtime's commands are tasks that a callback transaction or `onStore` cannot issue.

## 5. Building Block View

**`axton_load`.** One row per job: `load_id` (primary key), `seq` (start order), `ready` (oldest-ready order), `name`, `version`, canonical `args`, canonical `models` (the output Models' local read contracts), `continuation` (NULL for the first page, else the canonical `{"state":…}` wrapper, so `{state: null}` differs from NULL), `run`, `phase` (`pending`, `complete`, `failed` or `cancelled`), `pages`, the unique `call_id` and its canonical frozen `intent` (a `CHECK` keeps the two present or absent together), `retry` (`transport`, `backend` or `local`), `attempts` and a bounded `error`. A pending job always has exactly one frozen page. A job consumes no Mutation ordinal, push sequence, subscription ID or cursor.

**`axton_load_once`.** One row per reuse key: `key` (primary key), `name`, `version`, `args`, `models` and a unique `load_id`. There is deliberately no foreign key, so a mapping whose job is missing is detected and reported as `load.ledger_invalid`, never taken as a completed hit. Terminal completion lives only in `axton_load` and commits with the final page, so a mapping cannot claim success ahead of storage. Both tables are separate from `axton_query_cache`, which stores typed Query results ([Reconciliation](../storage/reconciliation.md#5-building-block-view)).

**Row validation.** Rows decode one by one and are checked for coherence: phase against frozen page and error, and the frozen intent against the job's own fields. A damaged row fails a named `get` with an error naming the job, appears in `list` as a failed status with `load.ledger_invalid`, and is reported once by the worker and skipped; it never blocks a healthy job. Scheduler reads are bounded.

Code: [client/loads.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/loads.rs) (operations, once key, `LoadFailure`, `LoadStatus`), [client/load_ledger.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/load_ledger.rs) (rows and statements), DDL in [client/ddl.rs](../../../../../crates/client/src/ddl.rs), page application in [client/store_delivery.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/store_delivery.rs).

## 6. Runtime View

**Start and reuse.** A start without `once` creates a fresh job, never reads or writes a mapping and never registers its outcome. `refresh` without `once` is `load.invalid_options`, raised before anything else; an unknown name or version is `load.unknown`.

Arguments that do not normalize against the Load's inputs fail the start (plain, once or refresh) or the invalidation with `load.invalid_args` before anything is written.

The first request is checked against the 1 MiB request bound before it is stored; arguments that push it over the bound fail the start with `load.request_too_large`, the code dispatch gives such a page, and nothing is written. A start with `once` decides from a committed read, so a join or reuse commits nothing, then decides again inside the write transaction, so concurrent same-key starts resolve to one job. The key is SHA-256 of canonical `{format: 1, name, version, args, models}`: invocation normalization of the arguments (object key order, UUID case and equivalent date offsets normalize; list order and explicit `null` do not; a no-argument Load uses `{}`) and the output Models' local read-contract versions. It never contains a continuation, call ID, job or run, the options or credentials.

| Current mapping | `once: true` | `once: true, refresh: true` |
| --- | --- | --- |
| None | Create and register a fresh job | Same |
| Active (pending) | Return that job (`Joined`) | Join that job; no restart and no duplicate refresh |
| Complete | Return the completed job (`Reused`); no request, page, `onStore` or Model change | Replace the mapping with a fresh job from the first page |
| Failed | Return the failed job; no automatic retry | Replace the mapping with a fresh job from the first page |
| Cancelled | Create and register a new job | Same |
| Missing: the mapping names no job | `load.ledger_invalid`, until `invalidate_load` removes the mapping | Same |

A refresh replaces the mapping on local acceptance, not on success: if it fails, later `once` callers see that failure, never an earlier completion. The replaced job keeps its history.

**Invalidation.** `invalidate_load(name, args)` normalizes the arguments against every retained version of that Load and deletes the exact mappings by name, version and arguments, whatever their Model contracts; it answers how many it removed. An omitted input is never treated as `null`, an unknown name is `load.unknown`, and arguments no retained version accepts are refused. It deletes no Model and no job, cancels nothing and sends nothing. An invalidated job that is still running may complete and store its pages under the stamp rules, but only a `once` start or refresh ever writes a mapping, so it cannot restore reuse. Cancel and forget remove a mapping only when they changed the job and the mapping still names it ([guarantee N6](../../../guarantees.md#n-native-loads)).

**Frozen page.** A page's fresh call ID and exact `LoadIntent` commit with the job: at start, with every page that continues, and at an explicit retry. Automatic recovery - reopen, a transport failure, a `retryable` item, a local preparation or commit error - resends those bytes and counts `attempts` with its class. There is no second durable response inbox: a received page waits in bounded memory for its application, and after an exit the persisted call ID fetches the server's replay.

**Page application.** `load_page_step` checks the fence and classifies the reply: stale, a failure to record, or a `StoreDelivery::Load` to store. The delivery runs through the same owned session as other incoming authority: preparation, the refusal check, `onStore` callbacks, application and commit. Inside the session the fence is checked again (replica generation and no pending rebuild; job, run, `pending` phase and call ID at the row), the records are staged by stamp, and a fenced update advances the continuation and page count, requeues the job behind ready peers with a new frozen page, or completes it for `next: null`.

| Situation | Result |
| --- | --- |
| Older, or equal with equal content, known stamp | No-op for that record; the page still commits |
| `Diverged` pending replay | Reported; the page still commits |
| A `readFailed`, `skipped` or `conflict` diagnostic in preparation | Refused before any callback: terminal `load.store_failed`; the runtime reports those records in a records report, as for other deliveries |
| A callback throws | The whole page rolls back, callback writes included: terminal `load.hook_failed` with the Model and its identities |
| A preparation, replay or commit error | Nothing advances; retried under the same call ID with class `local` |
| A malformed correlated page, too many identities or bytes, an invalid `next` | Terminal `load.protocol_invalid`, `load.page_too_large` or `load.invalid_continuation` |
| A frozen page that cannot be sent even alone, or a single-page request the backend refuses with a 4xx other than 401, 408 and 429 | Terminal `load.request_too_large` or `load.protocol_invalid`, decided by the [Load worker](../connection/controller/load-worker.md#6-runtime-view) |
| A saved backend rejection | Terminal with the backend's `{code, message}`; the rejected call stays frozen |

A terminal failure is recorded in its own short transaction after the rollback. Stored errors are bounded to 1,024 bytes and at most 20 `{model, id, code}` diagnostics; a retryable failure stores only its class and attempt count. Other jobs answered by the same HTTP response commit separately.

**Explicit retry.** `retry_load` on a failed job increments the run, freezes a fresh call ID and a new intent at the last committed continuation in one transaction; committed Models, callback writes and the page count stay, and replies to the old call are inert. It applies to a backend rejection as well as a local failure and promises nothing: a persisting failure fails the new run again. On a pending job it is idempotent. A complete or cancelled job is `load.not_retryable`, and a version that is no longer retained is `load.contract_unavailable`.

**Cancel and forget.** Cancel moves a pending or failed job to `cancelled` with the error `load.cancelled`, clears its frozen page and fences late replies; committed pages stay, and cancelling a complete or cancelled job changes nothing. Forget removes a terminal job (`load.not_terminal` otherwise); `get` then answers null, and later management calls on that ID fail with `load.not_found` (the runtime answers `load.schema_changed` instead for a job a rebuild abandoned, see [Reconciliation](../storage/reconciliation.md)). `get` of a string that is not a UUID answers null. `list_loads` accepts a limit from 1 to 100 (`load.invalid_options` otherwise), newest first. No terminal job is removed automatically.

**Schema changes.** Opening a client fails, in its opening transaction, every pending job whose frozen Load version is no longer retained or whose frozen output Model versions no longer match, with `load.contract_unavailable`; retained versions keep the exact row and frozen page. A pending incompatible rebuild and the rebuild itself follow [Reconciliation](../storage/reconciliation.md): jobs park, then the rebuild abandons them.

## 9. Architecture Decisions

**A whole page, not per record.** A Load page is one typed read whose complete declared output must apply, like a Query result; skipping a record while advancing the continuation would lose it silently. Bootstrap keeps its per-record rule under D7 ([Pull](pull.md#5-building-block-view)).

**Replay, not an inbox.** Persisting the call ID and relying on the backend's saved outcome is enough to survive an exit between response and commit, which is also why a failed `onStore` must not refetch under a new call ID.

**Once reuses a job, not a result.** A mapping points at a durable job; there is no aggregate result to cache, and the Query result cache is not used. The once key reuses the Query once hashing helper only.

## 10. Quality Requirements

- **A start is durable offline; pages advance the committed continuation; a damaged row is visible and blocks nothing** ([guarantee N1](../../../guarantees.md#n-native-loads)). Evidence: [sqlite/tests/loads.rs](../../../../../crates/sqlite/tests/loads.rs) `a_start_offline_survives_reopen_with_its_frozen_request`, `ordinary_starts_are_independent_and_reattach_by_id`, `pages_advance_the_committed_continuation_until_the_backend_ends`, `a_committed_page_requeues_its_job_behind_ready_peers`, `a_damaged_job_fails_visibly_without_blocking_healthy_jobs`, `one_scheduler_read_is_bounded_however_many_rows_are_damaged`, `the_ledger_tables_are_added_beside_existing_work`.
- **A page commits whole or not at all** (N2). Evidence: `a_failing_hook_leaves_no_authority_hook_write_or_progress`, `one_invalid_record_refuses_the_whole_page_before_any_hook`, `a_local_constraint_failure_has_the_same_no_progress_outcome`, `a_preparation_error_keeps_the_call_id_for_backoff`, `a_failed_page_commit_retries_the_same_call_locally`, `authority_that_moved_before_replay_retries_the_same_call_locally`, `failure_diagnostics_are_bounded`, `stamps_decide_content_and_only_divergent_equal_stamps_fail`, `loads_coexist_with_optimism_and_scope_delivery_by_stamp`, `a_diverged_replay_does_not_fail_the_page`.
- **Explicit retry uses a new call from the committed continuation; a held page is inert once its job moved** (N3, N4). Evidence: `retry_rereads_from_the_committed_continuation_under_a_new_call`, `backend_terminal_and_retryable_outcomes_are_recorded_apart`, `cancel_is_idempotent_fences_the_frozen_page_and_keeps_committed_pages`, `forget_removes_only_terminal_jobs`, `a_held_page_is_inert_once_cancel_retry_forget_or_a_replica_change_moved_its_job`.
- **Once shares a job per key, and no old job can restore or erase a newer mapping** (N6, N7). Evidence: `once_starts_of_one_key_share_one_job_and_ordinary_starts_ignore_it`, `a_completed_once_hit_works_offline_across_reopen_without_applying_anything`, `the_once_key_normalizes_arguments_and_separates_versions_and_contracts`, `a_failed_once_job_is_returned_without_an_automatic_retry`, `refresh_joins_active_work_and_replaces_terminal_work_on_acceptance`, `invalidation_removes_mappings_across_versions_and_nothing_else`, `completion_after_invalidation_or_replacement_never_restores_a_mapping`, `cancel_removes_its_own_mapping_and_complete_cancel_keeps_it`, `a_mapping_to_a_missing_job_is_a_visible_ledger_error`.
- **A compatible reopen keeps retained jobs and fails removed versions.** Evidence: `a_compatible_reopen_keeps_retained_jobs_and_fails_removed_versions`.

Verified 2026-09-27 by the host gate (`bash scripts/test.sh`, which runs `cargo test --workspace --locked`); there is no multi-process race test for same-key starts, because one runtime actor serializes them. The same rules over real HTTP, PostgreSQL and process kills are in [End-to-end](../../../testing/end-to-end.md).

## 11. Risks and Technical Debt

**Accepted limitation.** Once is an application assertion that an earlier enumeration is reusable. Local deletion, Stream changes and elapsed time do not invalidate it; the application refreshes or invalidates when needed.

**Accepted limitation.** Terminal jobs stay until forgotten; there is no automatic retention policy.
