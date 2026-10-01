# Stream tracking and invalidation

Status: agreed API and behavior; implementation specification. This document supersedes the application API and tag facilities of the 2026-09-30 Scope membership design. Historical specifications remain unchanged.

Baseline: `66232e525648188e8a8986870ea7a272741029a9`, branch `codex/stream-tracking-api`. Work in the existing isolated worktree. Do not modify Most Days, merge, or publish as part of this implementation.

## 1. Goal and boundaries

Expose two server declaration verbs: `track` establishes persistent interest in records, and `invalidate` asks existing interested streams to obtain current authority. Both accept multiple records, including mixed Models; a stream handle accepts multiple stream names. Declarations in one enclosing settlement are combined before database effects are applied. Retire tags, tag selectors, the old `add`/`touch` application vocabulary, and the public membership-withdrawal API.

A stream is a resumable ordered notification sequence. The application database owns business state and permissions. A record stamp orders authority; a stream cursor orders delivery. A viewer Loader answers current state, absence, or an error. Tracking never grants permission, and invalidation never creates interest.

Use Stream consistently for delivery framework identifiers, serialized fields, capability names, generated subscription APIs, and persisted framework metadata. Transaction/savepoint scopes retain their existing names and meaning. Opaque application values, Model names and fields, business JSON, and dated historical documents are not renamed by text substitution.

No new Loader cache, per-tag or per-record cursor on the device, SQL execution on clients, automatic business relationship discovery, permission language, automatic tracking garbage collection, or device subscription broker belongs to this change.

## 2. Application API

```ts
const streams = ctx.stream(["User:alice", "User:bob"]);

streams.track.moment(momentId);
streams.track.moment([momentA, momentB]);
streams.track.momentMedia(mediaIds);

ctx.invalidate.moment(momentId);
ctx.invalidate.moment([momentA, momentB]);
ctx.invalidate.momentMedia(mediaIds);

streams.invalidate.moment([momentA, momentB]);
streams.invalidate.momentMedia(mediaIds);

const records = [
  Moment({ id: momentA }),
  MomentMedia({ id: mediaA }),
];
streams.track(records);
ctx.invalidate(records);
streams.invalidate(records);
```

The examples are the new contract, not claims that baseline packages expose it. Generated callable namespaces provide one lower-first Model accessor. Model-specific methods accept one identity or a readonly list. A single-field identity accepts its scalar or complete identity object; a composite identity requires its complete object. Mixed calls accept a generated `RecordRef` or readonly list of them. Generated constructors preserve each Model's identity type. Models without a viewer Loader cannot be declared.

`ctx.stream(nameOrNames)` accepts a nonblank string or a readonly array of nonblank strings. Names remain opaque and case-sensitive under the existing name rule. The handle alone creates no database row, relationship, subscription, or request. Multiple names and multiple records denote their Cartesian product for that declaration. To express different associations, use separate declarations in the same transaction. Empty names or records arrays are no-ops, not a request to target every stream. Copy and canonicalize operands at invocation; later caller mutation cannot retarget an operation. Handles are valid only within the originating callback. Preserve the existing declaration failure boundaries: ordinary invalid declarations throw before appending effects; Load collectors retain their existing sticky failure so a caught error cannot enroll an arbitrary prefix. Do not introduce universal collector poisoning as an unrelated behavior change.

```ts
await backend.transaction(async (ctx) => {
  // Different groups can receive different records in the same settlement.
  ctx.stream(["User:alice", "User:bob"]).track(recordsA);
  ctx.stream("User:carol").track(recordsB);
  ctx.invalidate(changedRecords);
  ctx.stream(["User:bob", "User:carol"])
    .invalidate(permissionAffectedRecords);
});
```

There is no additional `batch`, `combine`, terminal execute call, or network request per declaration. The existing transaction/settlement boundary collects these synchronous declarations. A Mutation handler already has that boundary. `backend.publish(tx, callback)` remains the caller-owned transaction entry, settling once per invocation and returning its after-commit wake. Separate `publish` calls remain separate settlements even within the same database transaction.

Mutation and legacy write contexts and external publication contexts provide `stream` and global `invalidate`. A native Load context provides only `stream(...).track`, restricted to records returned by that page. A Query and a viewer Loader provide neither declaration. The existing Load limits remain 1,000 distinct stream/record pairs and 1,048,576 encoded bytes; a multi-stream Cartesian product counts each distinct pair. Failures roll back the page and enrollment together.

The generated client uses `client.streams.subscribe(name)` and the corresponding `tx.streams.subscribe/unsubscribe` local subscription-intent namespace. Subscription handles, bootstrap, status and lifecycle retain their behavior. Subscription intent does not edit server tracking; unsubscribe stops delivery and does not evict cached Models. The rest of the client Models, transactions, Mutations, Queries, Loads and fetch API is unchanged.

## 3. Tracking

Tracking is a durable unique `(stream, Model, identity)` relationship. First enrollment ensures an authority stamp exists and produces one upsert position with current authority read on delivery. Repeating an existing relationship is idempotent: no new stamp or cursor and no forced refresh. Adding a stream to an already tracked record does not change its authority stamp.

Tracking is not a retention reason, authorization claim, business placement, or permanent copy of a record's payload. It persists across authority absence so offline consumers can learn of deletion and later invalidations can deliver reinstatement. The new public API does not cancel tracking. Storage therefore grows with distinct relationships, not with every repeated invalidation. Reclamation requires a separately specified retention/reconciliation policy; Loader absence is not automatic server membership garbage collection.

## 4. Invalidation

Global `ctx.invalidate` targets every stream finally tracking each identity at settlement. Targeted `ctx.stream(names).invalidate` targets the intersection of those names and the final tracking set. It never creates a tracking relationship. Track and invalidate declarations in the same callback combine regardless of order: a newly tracked targeted stream receives one upsert at the final stamp. Selecting no names produces no declaration and no authority stamp change.

Every distinct invalidated identity advances its global authority stamp once per settlement, including targeted invalidation. A cursor bump at an unchanged stamp is insufficient when a viewer's Loader answer changes from a row to absence or to a different projection. An invalidated identity with no current holders still advances its stamp, as the old touch contract does; selecting an explicitly empty stream set is the exception because it declares nothing.

Repeated targeted invalidations union their recipient name sets. A global invalidation dominates targeted ones for that identity. A Mutation's inferred changed Model inputs always act as global invalidation and cannot be narrowed by an explicit targeted declaration. Output-only readback and tracking an unchanged record do not invalidate it.

Application rule: shared content changes must invalidate all holders. Targeted invalidation serves changes to viewer-specific authority, such as selected viewers losing access, when other viewers' answers remain valid. A notification target does not alter the Loader's authorization or introduce stream-dependent answers: Loaders receive a user, Model identities and transaction, never a stream. One user's record authority remains the same through every delivery source.

Business deletion and access revocation are authority changes: invalidate affected identities, and let their viewer Loader return `null` where appropriate. Do not turn an error into absence. No automatic traversal of business relationships is introduced; the application identifies dependent records and may retain centralized local hooks for cache policy. Existing device-side cascades, pending mutations and companion state keep their existing authority rules.

## 5. Combined settlement and persistence

Keep the record catalog, durable stream tracking, stream heads, and compacted latest log position per `(stream, record)`. Retire server tag dictionaries, member/tag joins, label reads/writes, and predicate evaluation. Stream logs retain existing upsert/removal evidence; do not delete old removal positions, stop decoding saved removals, or discard the client's holding ledger simply because fresh application APIs no longer expose withdrawal.

The engine must use bulk host operations, not one host round trip or SQL statement per identity. Add these host contracts; `MemberKey` uses canonical encoded identity strings:

```ts
type MemberKey = { model: string; identityKey: string };
type TrackingPair = MemberKey & { stream: string };

type ReadTrackingRequest = {
  op: "readTracking";
  records: MemberKey[];       // all tracking pairs for these records
  pairs: TrackingPair[];     // existing explicit candidate pairs
};
type ReadTrackingResponse = TrackingPair[];

type GuardRecordsRequest = {
  op: "guardRecords";
  records: (MemberKey & { mode: "advance" | "ensure" | "lock" })[];
};
type GuardRecordsResponse = (number | null)[]; // request-aligned
```

The read request returns the union of its two candidate sets, without duplicate pairs. Validate names, identities, allowed result pairs and duplicates. Guard results match request cardinality and order; advance and ensure must return positive safe stamps, while lock may return null for absent metadata. Invalid host responses abort settlement.

Settlement sequence:

1. Validate and freeze stream names and references; deduplicate tracking pairs and coalesce global/targeted invalidation selectors per canonical record key.
2. Read tracking in bulk. Global invalidations request all holders; targeted invalidations and explicit tracks request only relevant pairs. Resolve candidate stream locks, including newly tracked pairs, then lock all stream rows in canonical byte order.
3. Guard every affected record in canonical key order in bulk. Advance wins over ensure; unchanged tracks ensure a stamp. Existing lock-only paths retained for framework compatibility never advance a stamp. Do not group modes in a way that reverses record lock order.
4. Re-read the relevant tracking sets under the locks. A global destination outside the locked set is a retryable transaction conflict; retry the whole owning transaction rather than extending the lock set out of order. Explicit tracking and targeted candidates are already covered by declared name locks. Reconcile the final pair set from this view.
5. Emit at most one upsert per newly tracked or invalidated final pair. Apply all final pairs using bounded set-based SQL batches, grouped head reservation, membership upserts and compacted log updates. Validate all returned positions. Application rows, metadata, saved outcomes and wake sets commit or roll back together.

The adapter may chunk requests for statement payload/row-count limits, using the existing 1,000-item batch size. Host round trips and SQL statement counts scale with bounded chunks rather than one statement per record or stream. Actual row, lock, WAL and log work still scales with affected records, streams and tracking pairs; bulk SQL does not eliminate the Cartesian product. Bulk record guards must preserve the old no-op write conflict fence on existing rows: a plain `SELECT FOR UPDATE` is not an equivalent fence for stale Repeatable Read transactions. Canonical ordinality spans the whole request and every SQL chunk; never reorder one chunk by mode or lock an existing higher key before a missing lower key by splitting existing and absent records into disjoint mode passes. SQL implementations must prove their actual lock/write acquisition order, not assume output sorting orders locks. Concurrent missing-row creation and mixed guard modes preserve canonical ordering and whole-transaction retry on serialization failures/deadlocks. Existing framework-owned Serializable retries and caller-owned transaction responsibilities remain unchanged.

## 6. Loaders, cursors and local state

The existing Loader signature already receives `ids[]` and returns one aligned row/null per identity. Normal delivery deduplicates across streams and groups reads by Model and requested version. Keep per-identity error isolation fallback after a batch failure or invalid batch response. Do not promise one Loader invocation regardless of failure or add a cache layer.

A cursor is delivery progress, not proof that every Loader succeeded. The existing error reporting, receipts, authority stamps, request fences and pending-layer protection remain in force. Saved call IDs replay exact stored outcomes without rerunning a Loader or settling declarations again.

### A business relationship as a cleanup signal

When a person leaves a Journal, the application may invalidate the single membership/access record that states their standing, rather than enumerating every Entry and Media record for notification. The backend ends the business relationship in the same transaction, and the Loader supplies explicit absence for that relationship. A local onStore hook responds to that authority inside the existing local transaction. Its application-owned cleanup must preserve the person's own records and content still reachable through other relationships or valid delivery paths. Other holders of the deleted relationship still receive its global invalidation; a genuinely viewer-only projection change may use targeted invalidation.

This is a supported application pattern, not automatic framework inference or a Most Days code change. The hook must consume an explicit authority change, not infer absence from an incomplete Load or a dropped subscription. Hook writes such as `tx.models.delete` follow existing direct local-write and cascade semantics; they are not the replicated-base withdrawal primitive and do not automatically guarantee preservation of arbitrary pending/local work. Direct child deletion does not create the stream-withdrawal epoch fence: a delayed newer-stamp Load/fetch result or a saved server replay can re-materialize the child in storage. Application queries must therefore decide presentation from current standing and independently valid paths, not the mere existence of cached child rows. Hook cleanup reclaims cache; the standing record gates presentation. Prove the chosen application's cleanup through a fixture covering independently reachable, pending and device-only work and delayed authority arriving after standing loss. Do not introduce a new child fence, retention API or framework-derived permission policy to make this example automatic.

The business relationship and cache can end while the engine's historical tracking pair remains. Child business rows that actually changed or were deleted still require their own authority invalidation; cache release alone does not declare those rows deleted on the backend.

The client already stores normalized source holdings. Preserve them across the vocabulary migration so multiple streams sharing a record continue to protect its replicated base. Saved stream removal evidence remains identity-only and does not run a Loader or business onStore/cascade hooks; authoritative Loader absence remains a separate content operation with existing hooks/cascades. Do not infer a business parent deletion from an incomplete Load page.

## 7. Coordinated vocabulary migration

Replace delivery-specific Scope/Scopes/LoadScope and `scope`/`scopes` interfaces with Stream/Streams/LoadStream and `stream`/`streams`. Update framework protocol and capability checks together, using `stream-membership-v1` as the new membership capability. Old runtime wire shapes are rejected through negotiation rather than accepted into a partial rename. Opaque name values such as `User:alice` remain byte-identical.

Forward PostgreSQL migration renames framework delivery tables/columns and preserves heads, catalog IDs, tracking pairs, log positions, receipts and saved calls. Retire tag-only tables after checking they own no business data. Rewrite only the framework claim objects in top-level saved receipt/call envelopes (`memberships[*].scope` to `memberships[*].stream`); do not rewrite business results, states, identities or continuations. Handle the prior Channel layout through its existing forward upgrade path as well as the current Scope layout.

SQLite migration renames framework delivery tables/columns and its layout marker while preserving the database path, call IDs, subscriptions/cursors, holding rows, frozen Load requests, continuation state, queued/pending/rejected work, local companions and device-only Models. SQLite does not store full wire membership-claim envelopes; do not invent a JSON rewrite of application query-cache results or push Model descriptors.

Bridge transaction/savepoint scope tokens, application Model Scope, application fields named scope, and historical migrations/specifications retain their meanings. Migration files may mention retired layouts. Current guidance and executable examples use the new vocabulary.

Source package manifests currently say 0.2.0; the merged Scope facade is not the published 0.2.0 runtime contract. This implementation does not publish, merge, change application release floors, or choose a registry release version. A coordinated versioned release and Most Days upgrade are separate work; do not overwrite an existing registry release.

## 8. Acceptance evidence

- Generated TS compiles model-specific single/list and mixed-reference calls for global and targeted invalidation and multi-stream tracking; wrong Models/identity types and retired tag/add/touch/remove surfaces are refused.
- JS declarations capture caller data, deduplicate names/pairs, accept empty batches as no-ops, reject invalid identities/names and escaped handles, and preserve Load page limits after Cartesian expansion.
- Engine/simulation proves global union, targeted intersection, inferred global dominance, one stamp per invalidated identity and one position per pair; targeted invalidation does not enroll unrelated recipients.
- Viewer permission row-to-null changes arrive at newer stamps; other holders are not notified for a targeted declaration. Loader errors preserve local records.
- A membership/access authority deletion invokes the existing local hook transaction; its application cleanup preserves independently reachable records and pending/local work. After a delayed newer-stamp authority or saved replay re-materializes a child, presentation stays governed by current standing and independently valid paths. The framework introduces no per-child invalidation requirement for cache release and no automatic business-parent inference.
- Real PostgreSQL verifies set-based chunk scaling above 1,000 operands, multi-Model/multi-stream associations, mixed guard modes, existing and missing-row races, Repeatable Read fencing, rollback after a later chunk, saved-call replay and after-commit-only wake.
- PostgreSQL/SQLite upgrades preserve old removal evidence, multiple holdings, progress, pending work, local data and opaque business JSON. Fresh wire shape and capabilities match across Rust, JS and Dart; old negotiation fails before state changes.
- Full Rust, persistence, generated API, Action/Load round trips, JS/Dart SDK, documentation examples, release package checks and full host gate pass before completion is claimed. Device smoke checks are reported separately.

## 9. Accepted limitations

Tracking history has no automatic retention policy. In-process wakes retain their existing cross-process limitation. Targeted invalidation correctness relies on the application choosing all holders when shared content changes. The framework does not derive business dependents, replace local cache policy, or turn subscription into access control.
