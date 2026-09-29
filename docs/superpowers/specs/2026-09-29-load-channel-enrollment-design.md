# Enroll loaded records into Channels

Status: implemented on branch `codex/load-channel-enrollment`; the owning contract is guarantee N8 in [Guarantees](../../engineering/guarantees.md#n-native-loads). Based on `08e20cca`, including Serializable backend transactions, durable Loads, device-only Models and persistent Channel membership.

Implementation sequence: [plan](../plans/2026-09-29-load-channel-enrollment-plan.md).

## 1. Introduction and Goals

While implementing a Load handler, an application can add the records it loads to a Channel. Later changes to those records need only the existing Mutation input inference or `touch`; shared settlement distributes the changed record to every Channel it belongs to. Applications can progressively enroll existing data as they load it instead of requiring a complete historical backfill before starting.

No new client call, schema declaration, subscription mechanism or alternative publication API is introduced. Extend the existing `ctx.channel(name).model.add(identity)` vocabulary to Load handlers. A Load continues to read business data; enrollment is an explicit framework metadata effect.

## 3. Context and Scope

### Context capability decision

| No. | Context | Channel capability | touch | Decision |
| --- | --- | --- | --- | --- |
| 1 | Mutation and retained legacy write handler | add and remove | Yes | Existing behavior unchanged. |
| 2 | `backend.transaction` and `backend.publish(tx, body)` | add and remove | Yes | Existing external-write paths unchanged. |
| 3 | Load handler | add only | No | New capability in this feature. |
| 4 | Query handler | None | No | Keep current contract; a similar explicit read-and-enroll use case could justify later work, but it is not inferred here. |
| 5 | Model Loader, including Fetch/Pull/Bootstrap reads | None | No | Remains channel-independent; materializing a record must not implicitly enroll it or generate another publication. |
| 6 | Client transaction and onStore | Local subscribe/unsubscribe intent | No server touch | Existing local capabilities are different from server membership and stay unchanged. |

Use a capability-limited Load Channel handle in generated types and at runtime. Do not expose a full Mutation context and rely on documentation to forbid remove/touch. Query cached results and Load once reuse do not execute a handler; exposing capabilities in a type does not make a cached read perform enrollment.

### Application API

The following is the proposed backend API:

```ts
export const loads: Loads<Tx> = {
  async projectTodos({ ctx, args, continuation }) {
    const page = await readTodoPage(
      ctx.tx, ctx.userId, args.projectId, continuation,
    );
    const channel = ctx.channel(`project:${args.projectId}`);
    for (const identity of page.ids) channel.todo.add(identity);
    return { data: { todos: page.ids }, next: page.next };
  },
};
```

The same mixed-Model form is available: `ctx.channel(name).add([Todo({ id }), Project({ id: projectId })])`. Both forms are synchronous declarations, return `void`, snapshot identities at declaration time and close when the handler exits. Neither executes an immediate database write or changes the handler's `{ data, next }` return shape. Add only the desired subset of returned records; returning a Model does not enroll it automatically.

Engineering boundary: an enrolled `(model, identity)` must occur in this page's validated output identity lists and resolve successfully through its Loader. Reject enrollment of a Model/identity outside that page. This keeps enrollment attached to records actually loaded under this caller's read permissions; it is not a general-purpose membership edit endpoint. An empty page therefore cannot enroll records. The same identity may occur in several outputs or be enrolled in several Channels.

Authorization for the Channel name and its intended audience belongs to the application. Loader authorization still applies on subsequent delivery. A Channel is shared server membership, not a client-owned subscription or access grant; loading into a shared Channel may make records available to other subscribers whose Loaders allow them.

### Membership semantics

Use existing shared settlement without changed records:

- New membership retains an existing stamp, or initializes absent stamp evidence at 1; it allocates one Channel position carrying that stamp.
- An unchanged existing member causes no additional publication or stamp increment. Duplicate declarations of the same Channel/record pair are one effect.
- Loading into A does not republish an unchanged record to existing Channel B. A later touch of the record publishes its new stamp to all its then-current Channels.
- Touch affects the named record, not every record in those Channels.
- Membership persists after the Load finishes, is cancelled, is forgotten or loses its client handle. None of those operations removes it.
- Future records not encountered by a Load are not automatically enrolled. Creation handlers or application jobs must enroll them, or another Load must encounter them.

## 4. Solution Strategy

Reuse the declaration collector's identity validation/snapshot rules with an add-only facade. Carry collected membership intents alongside the successful internal `handleLoad` answer. The application still returns only `{ data, next }`; the host attaches framework metadata. Rust validates membership intent, page coverage and limits, then calls the existing `settle_changes` with an empty change map.

The client request/response format, continuation, returned Model records and job ledger do not change. No Model/Load history version bump is required merely to expose an additive context method. JS host, native host contract and generated backend artifacts must be upgraded together. Missing membership metadata in an older host answer means an empty list; null/malformed metadata is invalid. Existing saved page responses remain readable because they contain no enrollment payload.

Do not call `backend.publish(ctx.tx, ...)` from a Load as a workaround: the transaction is already bound to AXTON, and that external path intentionally refuses re-entry. Shared Rust settlement and the existing bound session own all writes and after-commit wakes.

## 5. Building Block View

Current implementation inspected:

- [Load context and dispatch](../../../packages/server/index.mts) supply `tx`, `userId`, `callId`, `loadId` and currently no declaration handles.
- [Generated Load context](../../../crates/compiler/src/emit_loads.rs) has the same restriction.
- [Host response](../../../crates/server/src/host.rs) currently rejects `memberships` on `HandledLoad`.
- [Load execution](../../../crates/server/src/loads.rs) claims each page, executes inside a savepoint, validates its output, resolves records in batches and saves the outcome in the same application transaction.
- [Shared settlement](../../../crates/server/src/settlement.rs) already implements persistent membership, stable stamps, first-enrollment publication and canonical lock order.
- [Declaration collector](../../../packages/server/effects.mts) already copies typed identities and closes escaped handles.
- [Publication architecture](../../engineering/architecture/server/engine/publish.md) owns savepoint-aware wake collection and publication after commit.

This is a host/context and Rust settlement extension, not a client engine rewrite or a new database table.

## 6. Runtime View

### One page

1. Claim or replay the page under its authenticated owner and call ID.
2. For a fresh page, open the existing page savepoint and invoke the handler with the restricted collector.
3. Close the collector; validate the continuation, output identities and add-only membership intents. Canonicalize identities before checking coverage against the returned page.
4. Resolve all page records through the existing batched stamp/Loader path. Missing, unauthorized, refused, malformed or oversized output fails the whole page under current rules.
5. Once the candidate response has passed its shape/size checks, settle validated membership additions with `changed = empty`, inside that same savepoint and Serializable transaction. Do not call touch or advanceStamp. Preserve the page's stamp/content coherence with membership publication.
6. Release the page savepoint and save the successful page response. Commit the outer transaction, then use its existing published-Channel wake set.

If validation/Loader execution fails, save a terminal failed page after rolling back its savepoint; no new membership, initialized stamp or publication survives. A persistence/commit/serialization fault follows existing transaction retry/classification and cannot save a successful page independently of membership. A failure while saving the response rolls back enrollment with the transaction. Successful siblings in an HTTP Load batch remain independent.

All bundled adapters now use Serializable isolation; preserve that baseline. Regression tests must force overlap between enrollment and a concurrent business update/touch. Valid outcomes must be equivalent to serial execution: either enrollment observes the newer version, or the later touch publishes to the newly enrolled Channel. Any aborted attempt must roll back both response and enrollment and retry with the existing call identity. Do not claim this guarantee for a custom adapter that violates AXTON's transaction contract.

### Replay, refresh and once

- Same page call ID returns the saved outcome without handler, Loader, settlement or new publication. A later explicit removal is not undone by replaying an old saved page.
- A fresh page call ID can execute `.add` again; existing membership makes this idempotent. Explicit retry, refresh and once invalidation retain their current Load meanings.
- `once` reuse of a completed job performs no backend work and therefore cannot enroll records missed by an old handler. When deploying enrollment to previously completed jobs, applications must request a fresh traversal using existing refresh/invalidation controls.
- Transaction retries may rerun the handler; declarations are recreated per attempt, and only the committed attempt can wake subscribers.

### Client synchronization boundary

Server enrollment does not subscribe the caller. For uninterrupted update coverage, the client first registers the Channel and waits until the subscription's persisted `status.initialization` is `ready`, then starts the Load. `await subscribe()` alone only persists local intent and does not prove the first handshake has completed. Use the existing status/watch API rather than introducing an invented `ready()` method.

For an already initialized subscription, reconnect resumes its saved cursor. Once the baseline is established, later Channel authority may arrive before the Load response; existing record stamps prevent the older page from overwriting it. Load and Channel may deliver the same record, which must remain harmless. Existing explicit local-write protection is unchanged.

Loading before that initial subscription boundary remains allowed, but it does not promise gap-free updates between page read and later subscription initialization. This feature does not implicitly join the Load worker and Downlink worker or acquire subscriptions for the caller.

### Validation and bounded work

Only add (`present: true`) memberships are valid. Changed records, remove intents, membership fields on rejected/failed answers and unknown settlement fields remain forbidden. Malformed additions or out-of-page identity references fail with the existing `handler.invalid` family and roll back the page; device-only Models retain `loader.unregistered` behavior wherever the normal boundary detects them.

Cap unique Channel/record additions for one page at 1,000 and their encoded metadata at 1 MiB, aligning with existing page identity/byte ceilings. Exceeding the bound is a saved `load.page_too_large` page failure; smaller pages or fewer Channels resolve it. Deduplicate repeated pair declarations before counting, in both collector storage and Rust validation. Keep these limits as named shared constants/fixtures rather than unrelated magic numbers. This bound is internal host metadata, not an addition to the public Load response.

Pages with no additions retain the existing batched readStamps/Loader path and its fixed-round-trip tests. Enrollment adds membership/publication work per distinct record/Channel; do not promise the same cost as a pure read, and do not replace the shared algorithm with an unverified batch optimization.

## 10. Acceptance Requirements

| No. | Requirement |
| --- | --- |
| 1 | Generated and runtime Load contexts support per-Model and mixed-list add only; other contexts retain the capability matrix above. |
| 2 | Enrollment covers only successfully returned identities; duplicates and existing membership publish once or not at all as appropriate, without advancing an existing record stamp. |
| 3 | Page outcome, stamps, membership, cursor and invalidation commit together; failure and savepoint rollback leave no enrollment or wake. |
| 4 | Same-ID replay performs no enrollment; fresh pages re-add idempotently; replay does not reverse a subsequent remove. |
| 5 | Real Serializable enrollment/touch races cannot produce stale loaded state with a permanently missed update. |
| 6 | A client with an initialized subscription loads an old record, then receives a later touch without another add, including duplicate and out-of-order delivery. |
| 7 | Loader/refusal/size/coverage failures, expired handles, forged remove/touch, bounded additions and batch sibling isolation are tested. |
| 8 | Once/cancellation/subscription semantics are documented accurately; no schema history, client protocol or new persistence table is introduced. |

Evidence for this document: current sources and existing tests were inspected. No implementation or runtime tests were executed as part of writing it; the implementation's executed evidence is listed under guarantee N8 in [Guarantees](../../engineering/guarantees.md#n-native-loads).
