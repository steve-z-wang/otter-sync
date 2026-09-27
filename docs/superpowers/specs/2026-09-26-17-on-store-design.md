# Transactional onStore Model hooks

Status: reviewed implementation design for [#17](https://github.com/zanminwang/axton/issues/17). The user agreed the registration API, local capabilities, incoming-only payloads and before-storage ordering. This revision selects the engineering defaults for grouping, validation and recovery, and incorporates merged #134, #140, #162 and #163. No hook implementation is included. See the [implementation plan](../plans/2026-09-26-17-on-store-plan.md).

## 1. Goal and dependencies

Let an application react to incoming server Model authority by deriving local data and changing Channel subscription intent in the same local transaction. Reuse the public transaction interface and the Rust-owned callback continuation from [#134](https://github.com/zanminwang/axton/issues/134). That cleanup is merged. Integrate after the separate worker recovery fix [#167](https://github.com/zanminwang/axton/issues/167), which preserves pending actions across pump failure; do not duplicate that fix.

The hook participates in the commit; it is not a post-commit notification. Preserve the [guarantees](../../engineering/guarantees.md), especially Model authority, optimistic replay, completion timing and subscription identity. [#16](https://github.com/zanminwang/axton/issues/16) owns notification API changes, and [#144](https://github.com/zanminwang/axton/issues/144) owns same-WebSocket subscription reconciliation.

## 2. Public API

Add an optional generated `onStore` option to the existing generated client open configuration. Keep all existing required open options. Each key uses the same generated Model accessor spelling as `client.models`; each handler is optional. Registration is fixed for that open client lifetime in both Model names and callback function values: raw and generated adapters snapshot the supplied maps. Validate names against `origin.target` when an incompatible old replica is pending, otherwise against the current schema. Suspend target-schema callbacks while old-schema work drains; successful rebuild activates the fixed registrations. No schema annotation, decorator, reflection-based discovery, runtime handler replacement or second global hook is required.

TypeScript usage (the application supplies its existing open options):

```ts
const client = await GeneratedClient.open({
  ...openOptions,
  onStore: {
    async project(tx, changes) {
      for (const change of changes) {
        if (change.kind === "upsert") {
          await tx.channels.subscribe(`project:${change.row.id}`);
        } else {
          await tx.channels.unsubscribe(`project:${change.identity.id}`);
        }
      }
    },
    async todo(tx, changes) {
      for (const change of changes) {
        if (change.kind === "upsert") {
          // Derive local data using the usual tx.models interface.
          console.log(change.row.title);
        }
      }
    },
  },
});
```

Logging in this illustration is not transactional; application effects outside `tx` cannot be rolled back. Production handlers should use transaction operations for required effects.

The generated TypeScript shape is equivalent to the following, using the generated application's actual transaction, identity and Model types:

```ts
export type StoreChange<Identity, Model> =
  | {
      readonly kind: "upsert";
      readonly identity: Identity;
      readonly row: Model;
    }
  | {
      readonly kind: "delete";
      readonly identity: Identity;
    };

export type StoreHandler<Identity, Model> = (
  tx: GeneratedTransaction,
  changes: ReadonlyArray<StoreChange<Identity, Model>>,
) => void | Promise<void>;

export interface StoreHooks {
  readonly project?: StoreHandler<ProjectIdentity, Project>;
  readonly todo?: StoreHandler<TodoIdentity, Todo>;
}
```

Contextual typing must infer `tx`, identities and Model fields in inline handlers. Export the generated hook types so applications can put implementations in separate files. A handler returns no data. Returning normally permits commit; throwing rejects the local application transaction. Mutating a callback payload does not update stored records: all writes go through `tx`.

Dart uses a generated `StoreHooks` object with optional named Model callbacks, e.g. `onStore: StoreHooks(todo: (tx, changes) async { ... })`. Generate typed `StoreChange<I, M>` sealed variants `StoreUpsert<I, M>` and `StoreDelete<I, M>` with the same identity/row fields; callbacks return `FutureOr<void>`. Follow existing generated name-collision handling and Model/identity decoding conventions. React Native shares the TypeScript API and #134 bridge. Do not claim browser support before #59.

## 3. Allowed operations

The handler receives the same local transaction capability surface as an ordinary application transaction:

- Model reads and local create/update/delete, including existing relation/cascade and savepoint behavior.
- Channel subscribe/unsubscribe as transactional changes to local desired subscriptions.
- Reads observe the transaction's current local state and prior writes.

It does not own begin/commit/rollback of the enclosing transaction. It cannot invoke remote Queries/Mutations, enqueue backend work, start Bootstrap or wait for subscription initialization/network acknowledgment. Capturing the outer client does not bypass transaction guards. Its handle expires when the callback scope finishes; unawaited/escaped work follows the normal transaction contract.

Subscription methods must be usable in public local transactions as well as hooks. The generated transactional facade provides `tx.channels.subscribe(name)` and `tx.channels.unsubscribe(name)` using the existing Rust transactional Channel command. Both resolve with no value after changing local intent; neither returns a live Subscription or waits for a connection. Do not reuse the standalone facade, whose subscribe returns a Subscription handle. No `tx.scopes` alias is needed for this new surface; repository-wide naming cleanup remains #152.

Handler Model writes do not call `onStore` again. All handlers participating in one application transaction share its fate: a failure rolls back earlier handler writes and subscription changes as well. Application code may catch and recover within its handler subject to existing transaction/savepoint rules; swallowing a poisoned transaction operation cannot turn it into a successful commit.

## 4. Incoming changes, before storage

`changes` contains the incoming server Model data selected for this storage application, not a diff against the local database. There is no `previous` field. Applications explicitly read local data through `tx.models` if they need it.

- Upsert: `{ kind: "upsert", identity, row }`, where row is the full validated incoming server Model snapshot.
- Delete: `{ kind: "delete", identity }`. It does not require a local record to exist.

Identity always uses the generated identity object, including composite keys. The upsert row is not the current optimistic local view or an arbitrary invocation result. Direct invocation results retain their own Loader snapshot contract.

The handler runs before the selected incoming authority is stored. Incoming changes and handler operations commit together; the incoming batch is not yet visible to local queries during the callback. At entry, tx reads see the existing local view, including pending optimism and direct local writes. During execution they also see the handler's own writes and any earlier handler writes in the same transaction. There is no promise that all local reads remain frozen at callback-entry time.

Payloads are read-only inputs, not a transform/filter return value. Mutating a decoded row does not edit the Rust delivery. Applications may derive other local records or change subscription intent through tx; incoming authority is subsequently applied under the normal stamp/replay rules. A local edit to the same record is not a guaranteed override of the incoming server value: that authority is applied after the callback. Supporting transformation or veto of individual payloads is outside this API.

Do not add a server-history copy or a local pre-image field solely for the hook. The earlier question about what previous meant is superseded by the user's decision to omit it entirely.

## 5. Trigger and grouping contract

Prepare changes from incoming server authority eligible for storage, using existing stamp comparison and validation before callback dispatch. Do not use the generic local write notification stream as the hook trigger.

| Path | Hook behavior |
| --- | --- |
| Channel live/catch-up page | Changed Models in that page's existing atomic application transaction |
| Bootstrap historical page | Changed Models in that historical page's transaction |
| Direct Query or Mutation response | Authority actually stored in that response's transaction |
| Durable Mutation or enqueued Query receipt | Authority actually stored in the existing receipt application transaction |
| Query once cache hit | No hook: no Model authority is reapplied |
| Query once fetch/refresh | Normal stored-authority rules; writing only a result cache entry does not trigger |
| Local CRUD, initial optimism, optimistic replay or hook writes | No hook of their own |
| Receipt settlement without newly applied authority | No hook merely for queue deletion, rejection rollback or companion settlement |

`store: false` is not a blanket hook-disable switch. Under Q7 it suppresses extra authority from explicit Model outputs, but Mutation-input authority is mandatory and may still invoke hooks. Under merged #140, an extra `ctx.touch` is not automatically returned to the caller: it triggers a hook only if authority subsequently reaches this client through a stored result or a subscribed Channel. Removing Channel membership does not manufacture a Model deletion. A Query with no stored authority does not invoke hooks, including when its once result snapshot is persisted. Hook registration does not force a result to be stored.

For directly delivered authority, a candidate that is newer than the stored stamp produces an upsert/delete input. Older, Same, equal-stamp Conflict and known loader/validation failures produce no hook input; preserve their diagnostics. A newer stamp with equal field content still produces an input. Publishing an unchanged existing version normally reuses its stamp and therefore produces none. This is not an exactly-once application-event API, and a hook input is not evidence that a commit has occurred.

Only incoming server records contribute public changes. A cascade executed locally by the schema does not fabricate an incoming child change; an explicitly delivered child deletion does. Hook-originated local writes/deletes and receipt optimism settlement do not append changes or recursively dispatch handlers. Applications must not assume onStore observes every local change or maintains a complete local index by itself.

Group prepared entries by Model and invoke each registered handler once per existing outer application transaction with a nonempty changes array. Preserve accepted incoming application order within each Model, including repeated identities in the internal preparation journal; simulate version selection across repeated keys rather than comparing every entry independently with the original stamp. Existing wire Page, Receipt and Bootstrap validation rejects duplicate identities within one payload and remains unchanged. The repeated-identity preparation test uses an already decoded Direct payload; real-wire coverage proves rejection and separate valid deliveries. Do not silently coalesce accepted inputs into final-only changes. Invoke Models sequentially in ordinal lexicographic schema Model-name order, independent of registration object order or language map behavior. Prepare all payloads before invoking any handler; earlier handlers do not rewrite later handlers' incoming data.

No Channel name, cursor or stamp is required in the public payload. Queries and receipts may have no single source Channel. Source tracing can remain internal diagnostics; delivery provenance and new public metadata are outside this version.

## 6. Transaction and subscription ordering

Rust owns the complete transaction and pauses its continuation to request SDK callback execution through #134. The SDK invokes the registered typed function and returns completion/error; only commands bearing the active transaction capability enter that transaction. No second SDK queue or executor is added.

The unit proceeds as follows:

1. Validate delivery lifetime, registration/run identities, input shape and stamp eligibility; prepare typed incoming changes without exposing incoming records in the local Model view.
2. Open/retain the Rust-owned application transaction and invoke the nonempty registered handlers sequentially. No competing database task may change the view between candidate preparation and hook execution.
3. Apply the selected incoming authority under existing storage, cascade and optimistic-replay rules; stage receipt settlement, once cache writes and delivery progress in the same transaction.
4. Commit once if callbacks and application succeed; only then publish observer changes, successful Query results, durable Call outcomes and subscription work signals.

The preparation/apply split must preserve per-record failure isolation for failures known before dispatch. Use a reversible internal savepoint to preflight the existing delivery application when registered Models could be affected. Capture the ordered successfully staged records, then restore rows, stamps, queue state, changed-table sets, Held keys, reports and completion bookkeeping before invoking callbacks. Capture acceptance at `stage_isolated`, not by comparing final rows: repeated identities and cascades make final-row diffs insufficient. Preflight must execute the same receipt companion/settlement ordering as real application without publishing notifications or consuming lane state. Keep the current single-pass path when there are no matching registrations. It must not expose a prepared server row to a callback's tx reads. A storage failure discovered after callbacks (including a constraint introduced by a callback) aborts the complete local unit; never commit callback effects while silently skipping their promised incoming application. Preserve known per-record failures and their normal path-specific outcome: for example, Bootstrap may commit successful records with their hooks while recording a failed coverage run with unchanged historical cursor. A later hook or post-hook application failure instead rolls back the entire unit. Never retry a preflight-rejected record after callbacks and commit it without the handler input it would have required. Test this distinction against the failure-isolation guarantees; simply adding a callback before `apply_records` is insufficient.

A handler may unsubscribe the Channel whose already-admitted data is being processed. The current admitted data still applies; subscription changes govern future delivery. Progress writes must be conditional on the original subscription identity still existing. Never recreate a removed registration or update a newly created same-name registration with the old cursor. Unsubscribe/resubscribe creates a new identity/origin under the existing rules. Compute final subscription/Bootstrap notifications from committed state, not a pre-hook snapshot; an invalidated Bootstrap waiter follows existing closed/superseded behavior. Repeated subscribe without removing a registration remains a no-op.

A failed hook or post-hook application rolls back the full transaction: hook writes, subscription changes, incoming data/stamps, settlement, cache and progress. No successful status or change event escapes rollback. A physical rollback failure is reported once and closes the runtime before any later write; successful logical auto-rollback is not a cleanup failure. Side effects through unrelated databases, network clients or logs cannot be rolled back.

## 7. Failure and recovery

Classify callback failure separately as a local store-hook failure, with stable code `store_hook_failed` and Model/path context in a structured diagnostic. `callbackEffectId` is internal bridge cause correlation, not public `StoreChange` source metadata; direct local apply errors retain their own classification. Preserve a same-process language cause when possible without treating it as a serializable engine value. It must not become a backend business rejection or a skipped record-read report.

| Delivery | Recovery |
| --- | --- |
| Channel live/catch-up | Keep the prior durable cursor, report the local failure and use existing lane backoff/catch-up recovery. Never advance past the failed unit or spin immediately. |
| Bootstrap historical | Keep prior historical progress; fail the affected run through existing Bootstrap failure reporting after rollback. Explicit Bootstrap retry starts recovery from committed progress. Failure-state bookkeeping is a separate guarded transaction, not progress advancement; if it fails, retain and retry the failed-run state. A genuine post-hook SQL/apply failure retains the admitted page for local retry instead of marking callback failure. |
| Durable receipt | Keep the frozen batch and pending calls. Report the local error; retry the same durable request identities through existing backoff. Backend replay returns saved outcomes without re-running business handlers. An admitted receipt may remain in memory for local retry; malformed or inadmissible replies are discarded and the frozen request is retried over the network. Reopen relies on the backend's saved outcome, and rebuild clears the in-memory receipt. No successful `call.wait()` outcome until local settlement succeeds. |
| Direct Query/Mutation | Reject the invocation with the local-application error. Do not automatically re-invoke business logic or enqueue it. The backend outcome may already be successful; starting a fresh Mutation is not a safe retry recipe. No new public response-retry handle or durable response inbox in this issue. |
| Coalesced once fetch | All joined callers observe the local failure; no new cached success commits, and a previous valid cache entry survives the rollback. |

This contract makes hook code part of the local transaction's success condition. A permanently throwing hook can stop its Channel or the currently frozen uplink batch; other already runnable clients/tasks should remain serviceable, but the single-batch uplink cannot advance past an uncommitted receipt. This is wider coupling than a per-record loader error and must be explicit in the final guarantee update. It does not change existing loader/validation failure isolation. Do not promise per-record hook failure isolation while sharing one callback transaction across the delivery.

Strict rollback preserves the agreed atomicity of derived data. Skipping the failing hook would commit incomplete local invariants; durable per-record repair jobs would introduce a separate lifecycle/storage API. This is the selected engineering default, not a promise of per-record isolation for application hooks. No exactly-once claim applies to callbacks: an aborted unit can invoke them again on retry. An unfinished callback holds this client's one database transaction; ordinary database work waits, although host network effects may finish and their responses may queue. Close must cancel the callback capability and roll back without waiting for application code to resolve. There is no automatic callback timeout in this version.

## 8. Existing evidence and implementation boundaries

Inspected current Axton sources: `crates/client/src/authority.rs` (Disposition and record savepoints), `mutate.rs` (truth versus visible row), `actions.rs` (direct authority and once commit), `push.rs` (receipt settlement), and `downlink_worker.rs` (Bootstrap failures). This revision was checked against main `1fc412f`, including `runtime/transactions.rs`, `runtime/direct.rs`, `runtime/lanes.rs`, `runtime/protocol.rs`, `bootstrap.rs` and the TS/Dart callback bridges. Rebase onto the latest main including #167 before implementing; preserve #162/#163 replica and progress fences.

Oasis is a behavioral reference, not the target transaction boundary. Its current `local-sync/client/local_sync/lib/src/downlink/downlink_page_processor.dart` uses one transaction per incoming change; its typed hook includes old/new snapshots and equal-content reapplications. Axton keeps its existing whole-page/receipt boundaries and record-stamp deduplication. Oasis application use in `mobile/lib/data/local_sync/runtime/open_local_sync.dart` demonstrates local derived writes and subscription intent updates. Do not copy its publication ownership/eviction mechanisms or legacy transaction Mutation surface.

No schema annotation, server-history storage, backend protocol version, business handler or backend receipt change is required by this design. Do not implement same-socket reconciliation, Channel terminology cleanup across the repository, retention/eviction, browser runtime or a general-purpose hook event bus here.

## 9. Verification required before shipping

- Generated TS contextual typing and Dart typed variants: optional handlers, composite identities, incorrect Model fields rejected, delete narrowing, separately exported callback types.
- Real SQLite: incoming row versus pre-store local-view distinction, rollback of authority and hook writes, commit failure, sequential handlers and deterministic grouping, repeated identities, local cascades without fabricated server events, preflight savepoint restoration and post-hook application failure.
- Direct Query, direct Mutation and receipt completion only after hook commit; snapshot results stay independent of local hook edits; store:false mandatory-authority exception; once joins/cache hits/failure rollback.
- Channel/Bootstrap progress and subscription intent commit together; current-Channel unsubscribe/resubscribe cannot resurrect an old registration/cursor/run; retry uses persisted progress.
- Retry does not reexecute durable backend handlers; local hook failure never becomes backend rejection; no success or watcher update escapes rollback.
- #134 bridge boundaries: owned callback commands make progress, ordinary tasks cannot enter the active transaction, async callbacks and stale/escaped handles fail correctly, close rolls back without orphaned waiters, supported SDKs agree.

Use the [testing strategy](../../engineering/testing/strategy.md) and [running guide](../../engineering/testing/running.md). Preparation consists of source/test inspection and documentation checks only; no hook implementation tests have been executed. The [implementation plan](../plans/2026-09-26-17-on-store-plan.md) defines the runtime, authority, SDK and verification checkpoints; the [handoff](../plans/2026-09-26-17-on-store-handoff.md) supplies execution scope.
