<!-- load-draft: verify against implementation -->
# Load worker

## 1. Introduction and Goals

The Load worker decides when native [Load](../../../schema/loads.md) pages are requested. One worker per client runtime discovers jobs with a ready page in SQLite, groups them into bounded `POST /sync/loads` batches, and hands each response to the [Load engine](../../engine/loads.md) for application. It is a Rust state machine driven by the [runtime](../../runtime.md), not a thread per job and not a JavaScript or Dart loop. Mutation ordering, push sequences and Channel cursors are untouched.

## 3. Context and Scope

Inputs: ready jobs and their frozen page requests from the ledger, connection controls (`start`, `pause`, `resume`, `stop`), effect results and timers. Outputs: `http` effects on the `load` route, `timer` effects for backoff and per-attempt deadlines, `refreshAuth` requests shared with the other lanes, and page applications as runtime units ([Runtime](../../runtime.md#6-runtime-view)).

## 5. Building Block View

<!-- load-draft: TODO confirm name -->
The worker (planned `LoadWorker` in `crates/client/src/load_worker.rs`) keeps in memory only the batches in flight, the pages received and waiting to apply, and each job's due time; every durable fact - the frozen request, the attempt count, the phase - is in `axton_load`. Its scheduler reads are bounded; it never materializes every job body.

| Limit (initial private default) | Value |
| --- | --- |
| Items per batch | 8 |
| HTTP batches in flight | 2 |
| Outstanding pages per job (request, awaiting response or awaiting apply) | 1 |
| Backoff | 1 s base, 30 s cap, with the existing jitter |

These constants are not public settings.

## 6. Runtime View

**Batching and fairness.** When a batch slot is free, the worker takes ready jobs in oldest-ready order, up to 8, and sends them at once; it adds no artificial delay to fill a batch, so a lone job goes alone. After a page commits, its job goes back behind the ready peers. A job in backoff keeps no other job from being dispatched. A batch slot is released only after all of its outcomes have been applied or moved into per-job retry or failure state, which bounds how many received pages can wait behind the local writer.

**Lane units.** Load dispatch and page application are lane units in the runtime's admission order, alternating with push and Downlink turns: at most one page application per turn before yielding. Network waits hold no SQLite writer; an application waits in the shared scheduler while another transaction, an `onStore` callback included, holds the writer. A slow Load batch does not hold up the second batch, a push receipt, live delivery or foreground reads.

**Failures.** Transport failures, `429`, server availability errors and `retryable` items back the affected job off (1 s base, 30 s cap, jitter) and resend the same frozen call ID. Each attempt has a finite deadline, scheduled by the worker as its own `timer` because no push or pull deadline applies; there is no overall offline timeout, because waiting for connectivity is not failure. Attempts are persisted, deadlines are not: after reopen a bounded delay is recomputed. A `401` joins the shared credential refresh; an explicit refresh refusal fails the job with `load.unauthorized`, while a transient refresh error backs off like a transport failure. A `failed` item fails only its own job, never the siblings that shared its request. `Retry-After` is not honored yet.

**Controls and close.** `pause` and `stop` abandon Load I/O with `cancelEffect`, like the other lanes ([Scheduling](scheduling.md#6-runtime-view)); `resume` asks again. The job's phase is `waiting` while offline, paused or backing off, and `loading` while a request is out or a received page is admitted for application. Client close rejects process-local waiters with `client_closed` and leaves every job durable for reopen.

**Rebuild.** While an incompatible rebuild waits for old Mutations to drain, the worker dispatches and applies nothing and treats responses that arrive as inert; Load never delays that drain. A rebuild fences every page in flight by the new replica generation ([Reconciliation](../../storage/reconciliation.md#6-runtime-view)).

## 9. Architecture Decisions

**One shared worker, opportunistic batching.** One worker bounds concurrency for every job together and batches whatever is ready at dispatch time. A per-job loop would multiply requests and duplicate retry policy per SDK; an artificial batching delay would add latency to every lone Load.

## 10. Quality Requirements

- **Nine ready jobs produce a batch of eight and an independent batch of one; no job has two pages in flight; unconsumed outcomes stop request growth.**
- **A job in backoff cannot delay other ready jobs; a slow batch does not block a push receipt, live delivery or reads.**

Evidence: to be recorded from the worker and runtime tests of [#173](https://github.com/zanminwang/axton/issues/173).

## 11. Risks and Technical Debt

**Accepted limitation: tail latency.** A response returns after all of its items finish, so one slow page delays up to seven siblings; streaming responses and adaptive batching are deferred ([Server / Engine / Loads](../../../server/engine/loads.md#11-risks-and-technical-debt)).

**Accepted limitation: no background execution.** The worker runs while the client runtime runs. A mobile app that is suspended makes no progress until it resumes; jobs stay durable.
