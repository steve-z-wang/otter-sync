# Load worker

Historical carrier reference. Current responsibilities are owned by [protocol 5](../../../protocol/0.5.md). This page describes the old release, not a supported current API.

## 1. Introduction and Goals

The Load worker decides when native [Load](../../../schema/loads.md) pages are requested ([#173](https://github.com/zanminwang/axton/issues/173)). One worker per client runtime reads jobs with a ready page from SQLite, groups them into bounded `POST /sync/loads` batches, and queues each answer for the [Load engine](../../engine/loads.md) to apply. It is a pure Rust state machine driven by the [runtime](../../runtime.md), not a thread per job and not a JavaScript or Dart loop. Mutation ordering, push sequences and Stream cursors are untouched.

## 3. Context and Scope

Inputs: ready pages from the ledger (`load_ready_pages`), connection controls, effect results, the clock and entropy. Outputs: `http` effects on the `load` route, a per-attempt deadline `timer` for each batch, one backoff `timer` for the earliest due job, `refreshAuth` requests shared with the other lanes, and queued outcomes that the runtime applies as lane units ([Runtime](../../runtime.md)).

## 5. Building Block View

`LoadWorker` in [client/load_worker.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/load_worker.rs) keeps in memory only the batches in flight, the outcomes waiting to apply and each job's backoff (`{call_id, attempts, due}`). Every durable fact - the frozen request, the attempt count, the phase - is in `axton_load`. Its reads are bounded: one scan reads at most the batch size plus the pages it must skip plus 20 damaged rows. The runtime glue is [client/runtime/loads.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/runtime/loads.rs); lane readiness and alternation are in [client/runtime/tasks.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/runtime/tasks.rs).

| Limit (internal default) | Value |
| --- | --- |
| Items per batch | 8 (`LOAD_BATCH_ITEMS`), within the 1 MiB request bound |
| HTTP batches in flight | 2 |
| Outstanding pages per job (requested, answered or applying) | 1 |
| Outcomes waiting for the writer | at most 16, because a batch keeps its slot until every outcome is consumed |
| Backoff (`load_backoff`) | 1 s doubling, ±20 % jitter, capped at 30 s after jitter |
| Attempt deadline | the connection's `directTimeoutMs` (30 s by default) |

These are not public settings.

## 6. Runtime View

**Batching and fairness.** A dispatch reads the oldest ready pages, skipping jobs in flight, jobs backing off and damaged rows, and sends up to 8 at once; a page that would pass the 1 MiB request bound waits for the next batch, and nothing waits for a batch to fill. A frozen page that cannot be sent even alone - over the request bound - fails its job with `load.request_too_large` and holds back no other job. After a page commits, its job gets a fresh ready position behind its peers. A job in backoff holds back no other job. A page with persisted attempts that the worker has no backoff for, after a reopen, first waits a fresh delay from those attempts.

**Lane units.** The runtime runs the worker when an outcome is queued, or when it wants to dispatch and the connection is running, not paused, with no pending rebuild. A Load turn applies one queued outcome, else dispatches one batch. When Load and the other lanes both have work, turns alternate; the Downlink-before-push order is unchanged. Network waits hold no writer; an application waits in the shared scheduler while any transaction, an `onStore` callback included, holds it. A Load page commit wakes neither the push nor the Downlink lane; a Load task commit does, like any task commit.

**Failures.** A deadline, a transport failure, an envelope that does not correlate (reported, with every page counted as a transport failure) and a `retryable` item keep the frozen call ID and back the job off from its persisted attempts. A `401` joins the one shared credential refresh and resends the same body once; a second `401` backs off; a `401` without `refreshAuth` backs off, as on the push lane. A refresh that fails with status 401 or 403 is an explicit refusal and fails the batch's jobs with `load.unauthorized`; any other refresh failure backs off. A `failed` item fails only its own job. A ledger read error is reported and keeps its own 1 s retry timer until a dispatch runs. `Retry-After` is not honored.

**A rejected request.** A whole-request 4xx other than 401, 408 and 429 means the backend refused the request itself, which one page may have caused. The worker sends each page of that request alone next time, so one bad page cannot fail its siblings; a single-page request refused again with such a 4xx fails that job with `load.protocol_invalid`. A `408`, `429` or `5xx` backs the batch's jobs off without splitting it. Retrying a job that is backing off keeps its delay.

**Controls and close.** `pause` and `stop` cancel unanswered batches and the backoff timer without counting an attempt ([Scheduling](scheduling.md)); `resume`, `connect` and `wake` look again, and answers already admitted still apply. A stored pending job is projected as `loading` while a page is requested, answered or applying, `waiting` while offline, paused, backing off or behind a pending rebuild, and `pending` otherwise. Close rejects process-local waiters with `client_closed` and leaves every job durable; after reopen it resumes with its persisted attempts.

**Rebuild.** While an incompatible rebuild waits for old Mutations to drain, the ledger yields no ready page and the worker is offline for Loads, so it dispatches and applies nothing; Load never delays that drain. A rebuild resets the worker and fences every answer by the new replica generation ([Reconciliation](../../storage/reconciliation.md)).

## 9. Architecture Decisions

**One shared worker, opportunistic batching.** One worker bounds concurrency for every job together and batches whatever is ready at dispatch time. A per-job loop would multiply requests and duplicate retry policy per SDK; an artificial batching delay would add latency to every lone Load.

**Slots are released on consumption.** Holding a batch's slot until each of its outcomes was applied, recorded or found stale bounds how many answers can wait behind a long `onStore` callback, without a separate queue limit.

## 10. Quality Requirements

- **Batches are bounded and never wait to fill; a job never has two pages in flight; unconsumed outcomes stop request growth.** Evidence: [sqlite/tests/load_worker.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/load_worker.rs) `nine_ready_jobs_make_a_batch_of_eight_and_one_without_waiting_to_fill`, `a_job_never_has_two_pages_in_flight`, `sixteen_unconsumed_outcomes_hold_both_slots_until_each_batch_is_consumed`, `a_batch_stops_at_the_request_byte_bound`, `damaged_rows_are_reported_once_and_skipped`.
- **Backoff is bounded, holds back no other job and survives reopen.** Evidence: `backoff_doubles_from_one_second_is_jittered_and_capped`, `a_job_backing_off_holds_back_no_ready_job_and_goes_again_when_due`, `persisted_attempts_wait_a_fresh_bounded_delay_after_reopen`, `an_uncorrelated_response_keeps_every_frozen_call_and_a_pause_counts_no_attempt`.
- **An unsendable page and a rejected request fail only the job they belong to.** Evidence: [load_worker.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/load_worker.rs) `a_page_that_cannot_be_sent_even_alone_fails_its_job_and_blocks_no_other`, `a_rejected_request_splits_into_requests_of_one_and_a_rejected_one_fails`, `a_failed_scheduler_read_keeps_its_retry_until_a_dispatch_runs`; [runtime_loads.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/runtime_loads.rs) `an_unsendable_page_fails_its_job_while_the_others_complete`, `a_whole_request_4xx_splits_the_batch_and_a_page_refused_alone_fails`, `a_408_429_or_5xx_backs_off_without_splitting`, `retrying_a_job_that_backs_off_keeps_its_delay`, `a_failed_scheduler_read_is_retried_on_its_own_timer`.
- **Through the runtime, a slow batch holds back no lane, answers wait behind a callback within the bound, Load and Downlink pages alternate, 401 shares one refresh, and pause, stop and stale answers change nothing.** Evidence: [sqlite/tests/runtime_loads.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/runtime_loads.rs) `nine_ready_jobs_go_out_as_eight_and_one_and_a_slow_batch_holds_back_no_lane`, `sixteen_answers_waiting_behind_a_hook_stop_further_requests`, `load_pages_and_downlink_pages_alternate_one_application_per_turn`, `a_401_shares_one_refresh_and_only_an_explicit_refusal_is_unauthorized`, `a_lost_response_or_a_deadline_resends_the_same_call_and_late_answers_are_inert`, `pause_abandons_without_backoff_resume_resends_and_stop_keeps_the_job`, `stale_timer_callback_and_refresh_answers_change_nothing`.

Verified 2026-09-27 by the host gate (`bash scripts/test.sh`, which runs `cargo test --workspace --locked`). Batching, backoff and replay over real HTTP, PostgreSQL and process kills are in [load.test.mts](https://github.com/zanminwang/axton/blob/v0.4.2/integration/load-e2e/load.test.mts) ([End-to-end](../../../../testing/end-to-end.md)); its network scenario cuts the proxy, not the backend.

## 11. Risks and Technical Debt

**Accepted limitation: tail latency.** A response returns after all of its items finish, so one slow page delays up to seven siblings; streaming responses and adaptive batching are deferred ([Server / Engine / Loads](../../../server/engine/loads.md#11-risks-and-technical-debt)).

**Accepted limitation: no background execution.** The worker runs while the client runtime runs. A mobile app that is suspended makes no progress until it resumes; jobs stay durable.
