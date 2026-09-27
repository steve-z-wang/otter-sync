<!-- load-draft: verify against implementation -->
# Loads

## 1. Introduction and Goals

The Load engine owns the durable state of native [Loads](../../schema/loads.md) on the client: the job ledger, the once mappings that let a call site reuse a job, the frozen page request, and the transaction that applies a page. Its rules make a Load resumable after exit, replayable by call ID, and atomic per page ([guarantees N1–N7](../../../guarantees.md#n-native-loads)). When pages are requested and batched is the [Load worker](../connection/controller/load-worker.md).

## 3. Context and Scope

<!-- load-draft: TODO confirm name -->
Local operations: start (with the `once` and `refresh` options), invalidate, get, list, cancel, retry and forget, plus page application (`StoreDelivery::Load`). Their runtime commands are forwarded by the SDK handles ([Typed API / Client](../../sdks/typed-api/client.md)); command names are to be confirmed. Every operation is local and works offline. None is available inside an application transaction or an `onStore` callback, which keep their local-only capability.

## 5. Building Block View

**`axton_load`.** One row per job: the UUID, operation name and version, canonical immutable args, the retained Model contracts, the committed continuation wrapper, the run generation, the phase, the committed-page count, the current page call ID with its canonical frozen request, the retry class, the attempt count and a bounded terminal error. A job holds at most one uncommitted page intent. It consumes no Mutation ordinal, push sequence, subscription ID or cursor. Each row is validated independently, so a corrupt job fails visibly without blocking healthy ones.

**`axton_load_once`.** One row per reuse key: the unique canonical key, the `load_id` it maps to, and the retained key-contract metadata that invalidation needs to normalize arguments across versions. Terminal completion is not stored here; it lives in `axton_load` and commits with the final page, so a mapping can never claim success ahead of storage. A mapping whose job is missing is a visible ledger error, never a completed hit. Both tables are separate from `axton_query_cache`, which stores typed Query results ([Reconciliation](../storage/reconciliation.md#5-building-block-view)).

<!-- load-draft: TODO confirm name -->
Column names are to be confirmed. Code: planned `crates/client/src/loads.rs` and `load_ledger.rs`, DDL in [client/ddl.rs](../../../../../crates/client/src/ddl.rs), page application in [client/store_delivery.rs](../../../../../crates/client/src/store_delivery.rs).

## 6. Runtime View

**Start and reuse.** A start without `once` creates a fresh job, bypasses the mappings and never registers its outcome for reuse. A start with `once: true` looks up the canonical key `(name, version, normalized business args, retained output Model read-contract versions)` and inserts a job and its mapping in the same SQLite transaction, so concurrent same-key starts resolve to one job. The args use invocation normalization (object key order, UUID case and equivalent date offsets normalize; list order and explicit `null` do not), a no-argument Load uses `{}`, and the key never contains a continuation, call ID, job or run ID, the options or credentials.

| Current mapping | `once: true` | `once: true, refresh: true` |
| --- | --- | --- |
| None | Create and register a fresh job | Same |
| Active (`pending`, `loading`, `waiting`) | Return that job | Join that job; no restart and no duplicate refresh |
| Complete | Return the completed job; no request, no page, no `onStore`, no Model change | Replace the mapping with a fresh job starting at the first page |
| Failed | Return the failed job; no automatic retry | Replace the mapping with a fresh job starting at the first page |
| Cancelled or forgotten | The mapping is gone; create a new job | Same |

`refresh` without `once` is `load.invalid_options`, raised before any job or network effect. A refresh replaces the mapping on local acceptance, not on success: if it fails, later `once` callers see that failure, never an earlier completion. Handles to the replaced job keep its history.

**Invalidation.** `invalidate(name, args)` commits locally, works offline and deletes every mapping for that name and those arguments across retained Load and Model read-contract versions, normalizing against each retained version that accepts the arguments without conflating omitted and `null`. It deletes no Model and no job, cancels nothing and sends nothing. An invalidated job that is still running may complete and store Models under the normal stamp rules, but can never reinsert or replace a mapping. Cancel removes a mapping only while it still names the cancelled job; forget deletes a terminal job and only a mapping still naming it; dispose touches neither ([guarantee N6](../../../guarantees.md#n-native-loads)).

**Frozen page.** Before a page is sent, a fresh call ID and the exact normalized intent (kind `load`, load ID, name, version, args, continuation and Model contracts) commit with the job. Automatic recovery - reopen, transport failure, a `retryable` item, an uncertain backend commit, a local preparation or commit error - resends those exact bytes. There is no second durable response inbox: a received page may wait in bounded memory for its application, and after an exit the persisted call ID fetches the server's replay.

**Page application.** One guarded local transaction prepares incoming authority, runs eligible `onStore` callbacks in the order [Pull](pull.md#6-runtime-view) defines, applies the authority, advances the continuation and page count, clears the frozen intent and sets the job to `pending` or, for `next: null`, `complete`. It is fenced by replica generation, job, run generation and call ID, re-read inside the transaction.

| Situation | Result |
| --- | --- |
| Older or equal known stamp with equal content | No-op for that record; the page still commits |
| `Diverged` pending replay | Reported; the page still commits |
| A prepared record the client cannot apply, or equal-stamp different content | The page is refused before any callback: `load.store_failed` |
| A callback throws | The whole page rolls back, hook writes included: `load.hook_failed` |
| A preparation or commit `Err` | Nothing advances; retried with backoff under the same call ID |

A terminal page failure rolls the page back, then records the failed job in a separate short transaction, with a diagnostic list of at most 20 `{model, id, code}` entries. Other jobs answered by the same HTTP response commit separately.

**Explicit retry.** `retry()` on a failed job increments the run generation, replaces the frozen call ID with a fresh UUID and records the new intent at the last committed continuation, in one transaction; committed Models, hook writes and page count stay, and responses to the old call ID are inert. It applies to a backend rejection as well as a local failure, and it does not promise success: a persisting failure fails the new run again. Retry on active work is idempotent and sends nothing; complete and cancelled jobs need a new start. Old-run waiters are rejected with `load.superseded`.

**Cancel and forget.** Cancel is idempotent, fences late responses and keeps committed pages; cancelling a completed job leaves it complete. Forget is allowed only for terminal jobs and removes the row; `get` then answers null, and live handles fail later management calls with `load.not_found`. No terminal job is removed automatically.

**Schema changes.** A compatible reopen keeps jobs and their frozen contracts; a job whose Load or Model contract is no longer retained fails with `load.contract_unavailable` instead of reading old state with a new Handler. During a pending incompatible rebuild and at the rebuild itself, the rules are those of [Reconciliation](../storage/reconciliation.md#6-runtime-view): jobs park, and the rebuild abandons them.

## 9. Architecture Decisions

**A whole page, not per record.** A Load page is one typed read whose complete declared output must apply, like a Query result; skipping a record while advancing the continuation would lose it silently. Bootstrap keeps its per-record rule under D7 ([Pull](pull.md#5-building-block-view)).

**Replay, not an inbox.** Persisting the call ID and relying on the backend's saved outcome is enough to survive an exit between response and commit, which is also why a failed `onStore` must not refetch under a new call ID.

**Once reuses a job, not a result.** A mapping points at a durable job; there is no aggregate result to cache, and the Query result cache is not used.

## 10. Quality Requirements

- **A failing callback after its own writes leaves no authority, hook writes, page count or continuation committed.** Required behavior: [guarantee N2](../../../guarantees.md#n-native-loads).
- **Concurrent same-key once starts produce one job; ordinary starts never touch a mapping; invalidation, cancel, forget and refresh never let an old job restore or erase a newer mapping.** Required behavior: [guarantee N6](../../../guarantees.md#n-native-loads).
- **Explicit retry uses a new call ID from the committed continuation; automatic recovery keeps the frozen one.** Required behavior: [guarantees N3, N4](../../../guarantees.md#n-native-loads).

Evidence: to be recorded from the SQLite tests of [#173](https://github.com/zanminwang/axton/issues/173).

## 11. Risks and Technical Debt

**Accepted limitation.** Once is an application assertion that an earlier enumeration is reusable. Local deletion, Channel changes and elapsed time do not invalidate it; the application refreshes or invalidates when needed.

**Accepted limitation.** Terminal jobs stay until forgotten; there is no automatic retention policy.
