# Fetch one Model by identity

Date: 2026-09-27. Issue: [#153](https://github.com/zanminwang/axton/issues/153).

Status: proposed implementation contract following the API discussion; documentation only. Inspected baseline: `ed566eb77f9b0b47f2c128bd212261d08687e324`. See the [implementation plan](../plans/2026-09-27-153-model-fetch-plan.md).

## 1. Purpose and scope

Fetch reads one complete Model from the application's existing versioned Loader, identified by its generated identity. An application does not declare a Query or write another Handler to fetch that record.

Local Model reads stay local. Query expresses a named business read. Load traverses a backend-defined collection as a persistent job. Fetch is a single remote read with one completion and no persistent client job. It neither requires nor modifies Channel membership, subscriptions, cursors or collection completeness.

This supersedes #153's earlier suggestion to reuse completed local records automatically. Fetch always starts a fresh remote invocation after the previous invocation finishes. Only overlapping identical requests share work. There is no cache validity inference from a local row or an active Channel.

## 2. Public API

Generated TypeScript, also used by React Native:

```ts
const todo = await client.fetch.todo({ id }); // Promise<Todo | null>
const preview = await client.fetch.todo({ id }, { store: false });
const item = await client.fetch.project({ tenantId, id }); // composite identity
```

Generated Dart uses the existing generated identity class and named options:

```dart
final todo = await client.fetch.todo(TodoIdentity(id: id)); // Future<Todo?>
final preview = await client.fetch.todo(TodoIdentity(id: id), store: false);
```

Each Model generates one method, with the existing identity codec and Model decoder. The optional `store` boolean defaults to `true`; output-name maps are invalid. No `once`, `refresh`, status handle, `wait()`, optimistic input or persisted result cache is added. Invalid identity or options fail before network I/O. Fetch is absent from transaction and onStore transaction interfaces.

`client.models.todo.get({ id })` remains a local database read. A locally present row does not satisfy Fetch, and offline Fetch does not fall back to it. The API is generated for schemas with Models even if they contain no Queries or Mutations. A schema with no Models need not expose an empty Fetch facade. Reserve new generated helper names only where generated, following existing compiler collision checks.

## 3. Result and storage

The result is the normalized complete Loader snapshot for this invocation, including its identity, or `null`. It is not a live Model handle and is never replaced by a reread of the local optimistic view. Each joined SDK caller decodes its own result object.

With default storage, the response carries authoritative state for exactly the requested identity. The Rust runtime validates it, applies the shared stamp/reconciliation rules and invokes onStore for accepted changes. The incoming change and callback writes commit together. Fetch resolves only after this process commits. A newer local authoritative stamp may make the response a valid no-op; Fetch still returns its own snapshot and must not overwrite newer data. Pending optimistic writes remain and are replayed by the existing engine rules.

An equal-stamp/content conflict, invalid record or failed onStore/commit rejects Fetch. It cannot become a successful stored result simply because the general downlink machinery can report and isolate record failures. For this single-record delivery, validate before opening the hook and roll back callback writes if application fails. Preserve existing data and diagnostics. No onStore call is made for a stale or identical no-op.

With `store: false`, validate and return the same complete result shape without local authority application, onStore, local deletion or result-cache writes. Server call replay metadata is still allowed. This does not disable storage for an independent Channel delivery of the same record.

### Absence and errors

The canonical Loader's successful aligned `null` row means no readable record for that identity under the existing Loader contract. Fetch returns `null`; with storage enabled, apply stamped null authority through the existing deletion/replay path. This is distinct from a nullable Query output for which no identity was selected: Fetch always knows which identity was requested.

Loader refusal, thrown failure, missing Loader/version, invalid row, HTTP authentication failure and malformed response all reject. None implies deletion. Fetch does not invent an authorization policy: the backend must use the Loader's existing refusal versus null distinction deliberately.

## 4. Ownership and runtime flow

```text
generated fetch method
  -> SDK submits one Rust task (requestId)
  -> Rust validates, joins or starts a direct request (callId)
  -> SDK executes Rust's HTTP effect (effectId)
  -> server resolves the versioned Model Loader
  -> SDK returns response bytes to Rust
  -> Rust validates and, by default, runs the shared store transaction
  -> Rust emits each waiting task's outcome
  -> SDK decodes and resolves each Promise/Future
```

Reuse the direct request lifecycle in [runtime/direct.rs](../../../crates/client/src/runtime/direct.rs): effects, credential refresh, deadlines, fair response admission and task completion. Extract a small common request lifecycle where necessary; keep Query once caching separate. The Fetch-specific decoder and store continuation belong in Rust. No network scheduling, flight registry, retry decision or authority application moves into the SDK.

Fetch does not enter the Mutation queue, create a Load worker or wait for either queue to drain during normal operation. Distinct direct requests may be in flight concurrently. Network waits hold no SQLite transaction. A stored response waits its turn on the existing writer; local reads also wait behind an open onStore transaction. There is no second writer queue owned by the SDK.

### Concurrent requests

Within one client runtime, the flight key is `(replica generation, Model name, Model read version, canonical identity, store boolean)`. Omitted store and `true` normalize identically. Composite-key property order does not matter. Different storage policies do not join.

A flight remains joinable until its terminal task outcome, including time waiting for local application. All joined tasks receive the same invocation result or error; storage and onStore happen once. Remove the registry entry and all waiters on every terminal path. The next call gets a new call ID and reads again. There is no persisted flight table and no cross-client deduplication.

Use the existing client/account isolation contract. Credentials may refresh within that client's principal, but changing principal requires the appropriate client/replica lifecycle; a token refresh is not permission to share results between users.

### Lifecycle and failures

Use the connection's direct-request deadline and shared one-time credential refresh. Resending after that refresh preserves call ID and frozen request bytes. Ordinary transport errors and timeouts reject; there is no background offline retry. A new application invocation creates a new call ID.

Stop mirrors current direct calls: requests waiting for the network fail, while an already admitted reply may finish applying. Close rejects outstanding tasks, removes flights and fences late responses; nothing applies after runtime closure. A schema rebuild changes the replica generation and fences old responses.

During an incompatible schema upgrade waiting for old Mutations to drain, reject new Fetch work and terminate affected in-flight tasks with `fetch.schema_pending`, without applying their replies. Fetch must never store while onStore is disabled. After rebuilding, new Fetch calls can run. There is no durable Fetch job to migrate or abandon. This issue does not change Query's existing upgrade contract.

Use typed failures with cause details: `fetch.invalid_options`, `fetch.unavailable`, `fetch.timeout`, `fetch.transport_failed`, `fetch.invalid_response`, `fetch.store_failed`, `fetch.schema_pending` and `fetch.schema_changed`. Preserve backend terminal rejection codes (including Loader/version errors) and existing closed-client admission errors. A read failure does not need Mutation-style claims about unknown business side effects.

## 5. Server and protocol

Add `POST /sync/fetch` through the same authenticated server transaction wrapper as direct Queries. Fetch introduces no schema operation kind, Handler interface, action version or action history entry. It uses the retained Model read version and existing Loader registration.

Request, with `store` omitted for the default:

```json
{
  "callId": "123e4567-e89b-42d3-a456-426614174000",
  "model": "Todo",
  "version": 1,
  "identity": { "id": "todo-1" },
  "store": false
}
```

Use the existing direct completion envelope and AuthorityRecord encoding. A success result is the Model object itself or null, rather than a named output object:

```json
{
  "completion": {
    "callId": "123e4567-e89b-42d3-a456-426614174000",
    "outcome": { "status": "succeeded", "result": null }
  },
  "records": []
}
```

For success with `store: true`, `records` contains exactly one matching AuthorityRecord, including null authority for absence. For `store: false`, it is empty. A failed completion also has no records. The result and authority must describe the same normalized snapshot. Reject extra identities, duplicate records, record errors, wrong completion ID, invalid stamps, incompatible content or disagreement between result and authority before any local writes. Reuse the direct endpoint's request/response byte bounds and canonical scalar encoding.

The server workflow runs inside the application's transaction:

1. Authenticate the principal and validate the request envelope. Normalize identity using the requested retained Model read contract.
2. Claim `(owner, callId)` using existing ClaimCall/SaveCall infrastructure. The canonical request fingerprint contains an explicit Fetch kind tag, Model/version, identity and normalized store policy. It must not collide with an Action or Load fingerprint.
3. Replay a previously saved matching outcome without invoking the Loader again. Conflicting reuse of the call ID returns `call.identity_conflict`.
4. For a fresh stored read, obtain stamp evidence before reading content, using the existing EnsureStamp/Loader ordering and shared resolver. For `store: false`, no authority stamp is required. Fetch does not touch a record, publish it, add membership or advance Channel cursors.
5. Call the authorized Loader once for the requested identity/version. Validate its aligned row and assemble both the result and authority from that read. Reuse retained-contract normalization and compatible projection/default expansion; do not duplicate Model validation or fill defaults under a different rule.
6. Save the terminal outcome and commit before sending the response. A transaction/storage failure rolls back the claim and must not be saved as a successful or terminal Loader outcome. Deterministic Loader refusals use the existing terminal-call classification; internal failures retain the existing transaction-boundary behavior.

No metadata schema change is required solely for client Fetch state. Server reuse of the call ledger must remain compatible with existing Action replay. Do not modify old Action fingerprints or reinterpret saved Action bytes. Reusing a UUID between operation kinds is a conflict, never a cross-kind replay.

## 6. Integration and alternatives

Prefer extracting a shared authorized Model resolver over creating synthetic Queries: synthetic operations would pollute generated declarations, action history and Handler requirements. Prefer a typed generic Fetch protocol over SDK-built HTTP calls so Rust continues to own deadlines, storage and completion. Prefer fresh remote reads over automatic local reuse because one local record cannot establish remote freshness.

Implementation touches the core protocol, server/native route, Rust runtime and store continuation, generator, TypeScript/React Native and Dart transports. No application Loader signature change is needed. Fetch is independent of #173 Load semantics, but both may edit runtime effect routing, the generator and server routes; rebase and preserve the other feature's dispatch cases rather than replacing whole files.

## 7. Acceptance and verification

- Single and composite identities produce complete typed results with the generated codecs, including dates/enums and null.
- No Query declaration or business Handler is required; local reads remain local.
- Default success follows store/onStore commit; hook failure rolls back and rejects. False storage returns without invoking the hook or modifying local data.
- Missing canonical records produce stamped absence only when storage is enabled. Loader/transport failures do not delete records.
- Stale authority cannot overwrite newer data; optimistic state remains distinct from the returned Loader snapshot; equal-stamp conflict rejects.
- Concurrent identical requests share one Loader call and one store; sequential requests read again. Different identities, versions, policies and client generations do not join.
- Timeout, failed refresh, stop, close and rebuild release all tasks/flights and fence late effects. Mutation drain never permits a Fetch write without onStore.
- Server replay is isolated by owner and operation fingerprint, returns the saved snapshot and performs no publication.
- TypeScript, React Native and Dart carry the same protocol; custom transports receive a documented Fetch route.

This document specifies target behavior. Only source inspection and documentation checks accompany it; implementation tests are listed in the plan and remain to be run by the implementation agent.
