<!-- load-draft: verify against implementation -->
# Loads

## 1. Introduction and Goals

The server executes each page of a native [Load](../../schema/loads.md) as one independent, replayable call: claim its call ID, run the retained Load Handler, resolve the returned identities through Loaders and stamps, and save the outcome in the same application transaction. A batch of pages shares one HTTP request and nothing else ([guarantees N3, N5](../../../guarantees.md#n-native-loads)).

## 3. Context and Scope

Input: one validated `POST /sync/loads` item ([Protocol / Loads](../../protocol/loads.md)), the authenticated owner and a host bound to that item's application transaction. Output: the item's `succeeded`, `failed` or `retryable` outcome. Rust owns shape validation, dispatch constraints, normalization, claim and save, Loader authority and error classification; the TypeScript host owns the HTTP carrier and application transaction execution, as it does for other operations ([Backend interface](../backend-interface.md)).

## 5. Building Block View

<!-- load-draft: TODO confirm name -->
| Piece | Owns |
| --- | --- |
| Rust Load executor (`process_load`, native `processLoad`) | One item: fingerprint, claim, Handler dispatch, continuation and output validation, batched page resolution, save or replay, classification |
| Rust batch validator | The whole envelope before any transaction opens: unique load and call IDs, counts and byte bounds |
| HTTP carrier in [server/index.mts](../../../../../packages/server/index.mts) | Authenticates once, runs at most 4 item transactions concurrently, each through the application's `run` transaction boundary, and assembles one response after every item has committed or rolled back |
| Host `handleLoad` | Invokes the generated Load Handler with a read-only context and validates its continuation inside the Handler's error boundary |

## 6. Runtime View

**Claim and replay.** An item's call is claimed with a Load-specific request fingerprint (explicit kind `load`, load and call IDs, name, version, args, continuation and Model contracts), through the same atomic claim/save storage as other calls ([Persistence](../persistence.md)). The fingerprint and a Load-specific response decoder prevent cross-kind replay, and the owner-scoped claim means another user's request IDs cannot retrieve a saved outcome. A repeated call ID returns the saved `data`, authority `records` and `next` without running the Handler or Loader and without allocating a stamp. Saved outcomes have no TTL or automatic pruning, as for other calls ([guarantee Q2](../../../guarantees.md#q-call-outcomes)); a future retention policy ([#61](https://github.com/zanminwang/axton/issues/61)) must keep outstanding page outcomes, or keep enough identity to reject an expired replay as `load.replay_expired`, and must never run an expired page ID again.

**One transaction per item.** Handler execution, Loader reads, stamp initialization and the saved outcome share one application transaction at Repeatable Read or stronger. A batch is never one transaction with savepoints: a failing item rolls back only itself, and its siblings commit independently. No SQL transaction spans pages; each page reads its own snapshot.

**Handler and continuation.** The Handler receives `{ctx, args, continuation}` with `ctx` holding `tx`, `userId`, the page `callId` and the `loadId`, and no `touch` or `channel`. The host validates the returned state before JSON serialization could coerce it (BigInt, `toJSON`, NaN, depth, 64 KiB), and Rust checks portable JSON, safe integers, depth and size again. A violation is saved as `load.invalid_continuation`. The continuation is untrusted client input, never authorization: the principal is checked on every page.

**Batched page resolution.** Every declared output must be present. The page deduplicates its canonical identities across lists and resolves them per Model: one stamp host operation that reads existing stamps and inserts only missing ones, never rewriting an existing stamp, and one batched Loader call. A null or missing record for an enumerated identity fails the page as `load.record_unavailable`; it is never a deletion. Output and page size are checked before a successful outcome is saved; an oversized page is saved as `load.page_too_large`.

<!-- load-draft: TODO confirm name -->
The two batched host operations (names to be confirmed) are defined in [server/host.rs](../../../../../crates/server/src/host.rs), restated in [server/host-contract.mts](../../../../../packages/server/host-contract.mts), exemplified in [fixtures/protocol/host-operations.json](../../../../../fixtures/protocol/host-operations.json) and implemented by the simulation host, which change together ([Backend interface](../backend-interface.md#9-architecture-decisions)).

**Classification.**

| Outcome | When | Saved |
| --- | --- | --- |
| `succeeded` | The Handler returned valid data and continuation and every identity resolved | Yes, with `records` and `next` |
| `failed` (saved) | A Handler or Loader rejection or throw, including statement or lock timeouts: `handler.failed`, `loader.failed`, a `CallRejected` code, `load.invalid_continuation`, `load.record_unavailable`, `load.page_too_large`. Handler work first rolls back to its savepoint, then only the rejected outcome is saved before the outer transaction commits | Yes; replayed until an explicit retry uses a new call ID |
| `failed` (unsaved) | A deterministic Rust defect: `storage.invalid`, `host.invalid`, `internal` | No; returned with its code so a job does not back off forever |
| `failed` (item rejection) | An unknown name or version, or invalid business arguments: attributable to the item, never to its siblings <!-- load-draft: TODO confirm name --> (whether saved, and the codes, to be confirmed) | To be confirmed |
| `retryable` | Any error escaping the application transaction boundary: commit loss, pool timeout, a driver error after its own serialization retries | No; the client resends the same call ID and server replay decides what committed |

No `succeeded` item is sent before its transaction commits, and the host catches transaction and commit failures after Rust returns, reporting them as `retryable`.

**Tail latency.** Ordinary HTTP answers after every item finishes, so one slow item delays its siblings in that response. Bounded batches, per-transaction timeouts where the driver has them and the client's independent in-flight batches limit the impact; streaming responses and adaptive batching are deferred.

## 10. Quality Requirements

- **A repeated page ID replays its original data, authority and continuation after business rows change, without Handler, Loader or stamp work.** Required behavior: [guarantee N3](../../../guarantees.md#n-native-loads).
- **A failing item's transaction writes roll back while a sibling item commits; a different principal cannot retrieve a saved outcome.** Required behavior: [guarantee N5](../../../guarantees.md#n-native-loads).
- **Existing stamps are never rewritten, and a page of 1,000 identities uses a bounded number of host round trips.**

Evidence: to be recorded from the server and PostgreSQL tests of [#173](https://github.com/zanminwang/axton/issues/173).

## 11. Risks and Technical Debt

**Accepted limitation: no transaction timeout with `pg` or `drizzle`.** *Condition:* an item's application transaction hangs. *Consequence:* it holds its claim row, and a resend of the same call ID blocks behind it. The Prisma shim's `timeout` bounds this; the `pg` and `drizzle` shims have none ([Persistence](../persistence.md#11-risks-and-technical-debt)). This feature does not solve it.

**Accepted limitation: an oversized page reproduces.** `load.page_too_large` is saved, and an explicit retry reads the same continuation again, so it fails again. Recovery requires a backend change to page size.

**Accepted limitation: tail latency.** One slow item delays its response's siblings (see above).
