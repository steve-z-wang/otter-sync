# Transactional onStore Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Use a Sol implementation agent when delegated; the coordinating agent owns specification review and final acceptance.

**Goal:** Generated per-Model `onStore` callbacks derive local data and subscription intent before incoming authority is stored, in its existing atomic transaction.

**Architecture:** Rust prepares authority, owns one transaction and suspends its continuation for each host callback. SDKs decode typed inputs and execute callback commands under that transaction capability; Rust then applies authority, commits progress/settlement, and delivers outcomes. Extend the #134 bridge instead of adding a second SDK executor.

**Tech Stack:** Rust client runtime, SQLite, TypeScript/Node and React Native, Dart, generated interfaces in `crates/compiler/src/emit.rs`.

## Global constraints

- The [spec](../specs/2026-09-26-17-on-store-design.md) owns behavior. No schema annotation, backend protocol/version change or database migration is required.
- Baseline inspected: main `1fc412f`. Rebase onto main including #167 before implementing Downlink continuations; preserve #162/#163 fencing.
- Incoming-only payloads: upsert has identity and full row; delete has identity. No previous row and no synthesized cascade events.
- Hook registrations snapshot both names and callback function values at open. Validate against `origin.target` when pending, otherwise the current schema; suspend target callbacks while old-schema work drains, activating them after rebuild.
- Hook reads see the pre-store local view plus transaction writes. Local operations/optimism/replay never trigger hooks.
- All callback effects, incoming authority, receipt settlement, once cache and delivery progress commit or roll back together. Completion and observer events follow commit.
- Models run sequentially in ordinal schema-name order; records retain accepted incoming order, including repeated identities in the internal journal. Existing Page/Receipt/Bootstrap wire validation still rejects duplicate identities within one payload; the decoded Direct test exercises journal ordering. No hook runs for Older, Same, Conflict or preflight failures.
- Hook failure is `store_hook_failed`, not backend rejection. Retries may invoke callbacks again; a permanently failing callback can block a Channel or the frozen uplink batch.
- `store:false` does not suppress mandatory input-target authority. Extra backend touch is not caller authority under #140. Business result snapshots remain independent.
- Admitted push receipts may be retained in memory for local retry. Inadmissible replies go back to the network with frozen request bytes; reopen uses backend replay and rebuild clears saved memory. Do not implement #144, #152, eviction, hook timeouts, a durable response inbox, or public response retry handles here.
- Do not update unrelated generated fixtures or deploy/publish packages. One PR may contain the sequential checkpoints below; no checkpoint is independently advertised as the complete feature.

## File ownership

| Files | Responsibility |
| --- | --- |
| `crates/client/src/store_hooks.rs` (new), `authority.rs`, `push.rs`, `actions.rs`, `downlink.rs`, `bootstrap.rs`, `lib.rs` | Reversible preparation and resumable application inside an owned session; preserve path-specific admission and settlement |
| `crates/client/src/runtime/store_hooks.rs` (new), `transactions.rs`, `protocol.rs`, `mod.rs`, `tasks.rs`, `direct.rs`, `lanes.rs`, `effects.rs` | Internal application continuation, callback capability lifetime, scheduling and post-commit outcomes |
| `crates/client/src/downlink_worker.rs`, `live.rs` | Yield admitted application work to the runtime and resume after its outcome; keep existing no-hook synchronous entry points usable |
| `bindings/common/src/actor.rs` | Register Model names from the open envelope before serving the runtime |
| `packages/client-js/bridge.mts`, `runtime.mts`, `transaction.mts`, `index.mts`; `packages/client-react-native/transaction.mts`, `index.ts` | Host callback dispatch, guarded transaction context, local Channel facade and exports |
| `packages/dart/lib/src/bridge.dart`, `packages/dart/lib/src/client.dart`, `packages/dart/lib/axton.dart` | Equivalent callback registration, Zone/error handling and Channel facade |
| `crates/compiler/src/emit.rs` | Optional typed registration, generated transaction wrapper and Model decoding in TS/Dart |
| `crates/sqlite/tests/store_hooks.rs` (new), `runtime.rs`, `runtime_lanes.rs`, `bootstrap_worker.rs`; SDK/generated integration tests | Persistence, bridge and typed-surface evidence |

Read [runtime](../../engineering/architecture/client/runtime.md), [frontend interface](../../engineering/architecture/client/frontend-interface.md), [guarantees](../../engineering/guarantees.md), and the [testing strategy](../../engineering/testing/strategy.md) before changing behavior. Check current branch/worktree and issue comments. Keep implementation in an isolated `codex/` branch; this planning worktree can be reused after rebasing.

## Checkpoint 1: Authority preparation without visible writes

**Produces:** an internal `PreparedStore` containing the validated delivery, ordered accepted record indices, preflight diagnostics and `StoreChange` batches by Model. Indices identify occurrences, not just identities. The owned delivery also carries its original receipt/subscription/run/generation guards; do not hold borrowed Engine references across a callback.

```rust
// Internal payload; serde uses the existing identity/row JSON conventions.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum StoreChange {
    Upsert { identity: serde_json::Value, row: serde_json::Value },
    Delete { identity: serde_json::Value },
}
```

- [ ] Add real-SQLite scenarios in `crates/sqlite/tests/store_hooks.rs`: a pending optimistic edit over an older base; a newer incoming row; repeated identities with increasing/equal/decreasing stamps through an already decoded Direct payload (wire Page/Receipt/Bootstrap duplicates remain invalid); a malformed/constraint-failing row beside a valid row; a parent cascade. Preparation must report the accepted incoming entries while a transaction read still returns the original visible row.
- [ ] Run `cargo test -p axton-sqlite --test store_hooks --locked`; record the expected failure before implementation.
- [ ] Extract delivery bodies from closure-only `Client::write` wrappers so they can execute under `Client::begin_session` without nesting a transaction. Keep the current synchronous wrappers for no-hook callers and existing simulations.
- [ ] Instrument successful `stage_isolated` acceptance to collect original occurrence indices and normalized server inputs. Use one internal savepoint to run the delivery application preflight, including the existing receipt companion/base and settlement ordering. Do not infer inputs from a final database diff.
- [ ] Roll back that savepoint before dispatch. Explicitly restore in-memory changed tables, Held maps, reports and completion bookkeeping; no worker receipt/page consumption or notification may happen in preflight. Test a failing per-record savepoint cannot leak Held keys.
- [ ] Replay only the admitted occurrences after callbacks, retaining preflight failures as diagnostics. If a promised occurrence now fails, abort the whole unit instead of record-isolating that failure. Do not newly admit a previously rejected row because the callback made it valid. Preserve divergence as the existing diagnostic rather than inventing a hook error.
- [ ] Keep the no-matching-registration path single-pass. Compare its observable reports, settled queue, cursors and rows with the existing tests for each delivery path.
- [ ] Run the new tests plus `cargo test -p axton-sqlite --test actions --test bootstrap --test client --locked`; review and commit this checkpoint.

**Review gate:** a prepare/apply split must preserve receipt ordering and current Bootstrap partial-record failure rules. A generic callback before `apply_records` does not satisfy this checkpoint.

## Checkpoint 2: Rust-owned hook callback continuation

**Consumes:** prepared inputs and delivery guards from checkpoint 1. **Produces:** an internal authority transaction owner, alongside the existing application transaction owner, and a `storeCallback` effect.

```json
{
  "type": "effect",
  "effectId": "effect-17",
  "operation": {
    "kind": "storeCallback",
    "transactionId": "tx-18",
    "model": "Todo",
    "changes": [{"kind": "upsert", "identity": {"id": "t1"}, "row": {"id": "t1", "title": "server"}}]
  }
}
```

The SDK answers the existing `callbackResult` envelope with matching effect/transaction IDs. Unlike an ordinary application callback, success resumes the enclosing application continuation; it does not immediately commit the session.

- [ ] Add runtime event-trace tests in `crates/sqlite/tests/runtime.rs`: one hook stalls, an owned read runs, an ordinary task stays queued, callback success advances to the next Model, and task completion appears only after final commit. A forged/expired capability and duplicate callback response must not change anything.
- [ ] Run `cargo test -p axton-sqlite --test runtime --locked` and record failing assertions.
- [ ] Add validated optional `storeHooks: ["Todo"]` metadata to the native open envelope in `bindings/common/src/actor.rs`. Functions stay in the SDK; Rust receives only schema Model names. Reject invalid/unknown names, default absence to none, and freeze registration for this runtime lifetime. Preserve registration across replica rebuild.
- [ ] Replace transaction ownership assumptions tied only to a user request ID with an owner variant for authority application. Keep its prepared delivery and outcome continuation in Rust-owned state. Rotate callback effect/capability IDs between Model handlers so a saved handle from an earlier handler cannot join the next handler.
- [ ] Reuse existing scoped commands/savepoints and poisoning checks. Explicitly deny low-level `TransactionCommand::Enqueue` under the store-hook owner, even if it remains internally reachable for compatibility. The allowed Channel command changes local intent only.
- [ ] Retain the database session until every handler finishes and real application succeeds. No ordinary task, observer query or competing application enters it. Network results may arrive and queue; close must still be serviced.
- [ ] Extend close/cancel/error paths to roll back an authority-owned session, invalidate the callback, and settle the appropriate direct/joined waiters or lane bookkeeping. Late callback replies must be ignored safely. Rebuild waits for the active transaction under existing scheduling; it must not commit half a callback.
- [ ] Run runtime tests, `cargo test -p axton-client --locked` and `cargo test -p axton-binding --locked`. Review protocol serialization and owner cleanup; commit.

## Checkpoint 3: Delivery lanes and failure recovery

**Consumes:** authority continuation from checkpoint 2. **Produces:** every runtime authority path uses it, with unchanged no-hook behavior.

- [ ] Add `runtime_lanes.rs`/`store_hooks.rs` scenarios for direct Query, direct Mutation, once fetch with joined callers, push receipt, Channel live/catch-up and Bootstrap. Assert both state and event order, not merely callback counts.
- [ ] For direct calls, retain request/flight/joined ownership until local commit or failure; do not remove it at the start of `apply_direct`. A received backend response ends network timeout handling; a local hook error rejects with `store_hook_failed` and never resends the business operation.
- [ ] For durable receipts, preserve frozen batch/client/call IDs until authority and settlement commit. Hook failure uses existing lane backoff and saved backend receipt replay; no synthetic business failure or `call.wait()` success escapes. Exercise a second call in the same batch to establish the documented shared transaction outcome.
- [ ] Make Downlink application yield a prepared work item to Rust runtime scheduling, then resume with the commit/failure outcome. Preserve socket/frame ordering, pending pull association and #167 pending-action recovery; do not consume a delivery twice or release successful notifications before commit.
- [ ] Capture original subscription identity/run before callback dispatch. After admitted authority is applied, write cursor/history progress only if that identity/run still survives. Unsubscribe commits without resurrecting it; same-name resubscribe keeps its fresh unset origin. Compute subscription/Bootstrap notifications from final committed state.
- [ ] Test a hook that removes its own Channel, one that removes/recreates it, and one that throws after either operation. Repeat for a terminal Bootstrap page. Preserve #162/#163 generation fences and run identity across rebuild/reopen.
- [ ] Preserve known Bootstrap record failures: successful authority and its hooks may commit while historical progress stays put and the run fails. In contrast, hook failure rolls back all page writes, then records run failure through a separate identity/run-guarded transaction. A failure to record that failure must not lose the pending failure outcome; retain and retry bookkeeping without advancing coverage.
- [ ] Verify live/catch-up failures back off at the old cursor. Retry may call the hook again; equal-stamp duplicate authority after a successful commit does not. Query once cache hits do not call hooks; failed refresh keeps the prior cache entry and rejects all joined callers.
- [ ] Run `cargo test -p axton-sqlite --test runtime_lanes --test bootstrap_worker --test store_hooks --locked` and `cargo test -p axton-sim --locked`; review and commit.

**Review gate:** an indefinitely unresolved callback blocks this client's database work, as a public transaction does. A completed-but-failed callback releases the transaction before backoff; unrelated runnable database tasks can proceed. Do not claim hooks eliminate all blocking.

## Checkpoint 4: Thin host bridges and transaction Channel methods

**Produces:** raw SDK registration and transaction methods used by generated clients in checkpoint 5. In TS, `StoreHook` is `(tx: Transaction, changes: readonly RawStoreChange[]) => void | Promise<void>`; its map uses schema Model names. Registration is installed before open/connect can dispatch any store callback. Generated wrappers supply Model-specific decoding.

```ts
// The public generated transaction facade delegates to these local commands.
await tx.channels.subscribe("project:p1");  // Promise<void>
await tx.channels.unsubscribe("project:p1"); // Promise<void>
```

- [ ] Add asynchronous callback bridge tests in `integration/bindings/client-js/runtime-bridge.test.mjs`, `integration/bindings/client-react-native/runtime.test.mjs` and `packages/dart/test/runtime_bridge_test.dart`. Exercise a delivery with no pending public transaction request; the callback must still dispatch through the registration map.
- [ ] Wire `storeCallback` using the normal Transaction implementation, including `finish`, savepoint scope and unawaited-command checks. Extend the active-callback guard to hook execution so captured outer-client Queries/Mutations cannot deadlock behind their own transaction.
- [ ] Add raw transaction subscribe/unsubscribe helpers and generated facade support without creating live Subscription objects. Both resolve locally with void and preserve repeated-subscribe identity. Hook and ordinary application transactions use exactly the same helper.
- [ ] Keep callbacks/functions out of serialized open JSON. Send only the Model-name list; do not accidentally pass closures through `strictJson`. Dart invokes in the registration Zone, retains language causes when possible, and sends a bounded error message to Rust.
- [ ] Track cancellation for store effects independently of pending user-task callbacks. Close clears dispatch entries and invalidates transaction handles even if user code remains unresolved. Returned/cancelled/failed callbacks must not retain their decoded payload forever.
- [ ] Preserve Node async-context guards and React Native's documented conservative guard behavior. Test escaped handle use, unawaited commands, nested savepoint failure, a captured-client remote call, and callback cancellation on all supported SDKs.
- [ ] Build prerequisites per the running guide, then run `npm run typecheck`, `node --test integration/bindings/client-js/*.test.mjs`, `node --test integration/bindings/client-react-native/*.test.mjs`, and Dart analyze/test with the native library configured. Review SDKs for duplicated scheduling/storage decisions; commit.

## Checkpoint 5: Generated API and real application coverage

**Consumes:** raw registration/transaction APIs above. **Produces:** `GeneratedClient.open({ onStore })`, exported `StoreHooks`, `StoreChange`, and the typed Model callbacks in the spec; Dart uses `StoreHooks` and sealed `StoreUpsert`/`StoreDelete` variants.

```ts
const hooks: StoreHooks = {
  async todo(tx, changes) {
    for (const change of changes) {
      if (change.kind === "upsert") {
        const existing = await tx.models.todo.get(change.identity);
        // Incoming title comes from change.row; existing is the old local view.
        await tx.channels.subscribe(`todo:${change.identity.id}`);
      }
    }
  },
};
```

The existing generated Model read method is `get(identity)`; use the same CRUD interface in hooks. This example assumes a fixture Todo Model with a string id.

- [ ] Extend compiler tests and `integration/generated-api` positive/negative TS/Dart fixtures: contextual typing, delete narrowing, composite identity, enum/date decoding, omitted callbacks, external `StoreHooks`, Model-only schema and generated-name collisions. Incorrect fields and remote methods on tx must fail compilation/analysis.
- [ ] Generate registration adapters in `emit.rs` that wrap the raw transaction with the ordinary GeneratedTransaction and decode rows/identities with existing Model decoders. Do not decode from current local rows or substitute optimistic values.
- [ ] Add an end-to-end application hook fixture to `integration/action-e2e/action.test.mts` and its Dart client: return B while updating A; assert A's mandatory authority invokes its hook with `store:false`, B is suppressed when not stored, and result B remains the Loader snapshot. Extra touch alone must not produce a caller hook.
- [ ] Add live/Bootstrap end-to-end coverage with local derived writes and subscription intent. Verify the local watcher and Query/Call success see both authoritative and derived rows only after commit; record failure and retry assertions separately.
- [ ] Run `bash integration/generated-api/verify.sh`, `bash integration/action-runtime-ts/verify.sh`, the documented Dart Action suite, `bash integration/action-e2e/run.sh` and `bash integration/e2e/run.sh`. Review generated diff, keep only intentional fixtures; commit.

## Checkpoint 6: Contract documentation, review and handoff evidence

- [ ] Update `docs/engineering/architecture/client/runtime.md`, `client/frontend-interface.md`, `client/engine/pull.md`, `client/engine/settlement.md`, `sdks/typed-api/client.md` and `sdks/bindings.md` with ownership, payload, timing and failure rules. Link these owning pages rather than repeating the full contract everywhere.
- [ ] Update the failure-isolation introduction and relevant A/D guarantees in `docs/engineering/guarantees.md`: application hooks explicitly share the enclosing storage transaction; known loader failures remain isolated. State that a callback may run again, can block its batch if permanently failing, and does not observe every local change.
- [ ] Add a concise generated-client example to the appropriate guide; check relative links and `python3 website/scripts/check_examples.py`. Mark the former omission in any still-current audit by linking the implementation and tests rather than copying legacy Oasis semantics.
- [ ] Run `bash scripts/test.sh` on the final branch. Record actual results and environment gaps; do not report device smoke coverage from a host test. Require current macOS/Linux CI checks before merge.
- [ ] Review against every spec section: API/types (2–3 → checkpoints 4–5), payload/grouping (4–5 → 1–2), atomicity/subscriptions (6 → 1–3), recovery (7 → 3–4), scope (8 → all), evidence (9 → 1–6). Inspect no-hook regressions, prepared-state rollback, result timing and cancellation particularly closely.
- [ ] Open one PR with `Closes #17`, checkpoint summary, tests and material limits. Attach it to the task and update the issue per the ship-issue workflow. Request coordinating-agent acceptance; this handoff grants no new automatic merge permission.

## Design-review evidence

Reviewed against main `1fc412f`; no implementation tests were run while writing this plan. Resolved source-level gaps: current callback completion commits too early for hook ownership; direct calls currently relinquish flight state before application; generated transactions lack the Channel facade; Bootstrap updates assume the registration survives; `TransactionCommand::Enqueue` needs hook-owner denial. Existing #167 pump recovery remains a separate prerequisite. The implementation must prove the new paths with the tests above, not treat this source review as execution evidence.
