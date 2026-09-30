# Channel tags and synchronized removal

Status: proposed implementation contract; no runtime changes accompany this document.
Date: 2026-09-30.
Baseline inspected: `08e20cca` on AXTON `main`.
Implementation: [plan](../plans/2026-09-30-channel-tags-removal.md).

## 1. Goal and scope

A backend can label channel members and remove all members matching one label in its domain transaction. AXTON sends ordinary per-record removals and releases the corresponding replicated data locally. Application code does not maintain client tags or write a cleanup hook for this operation.

Tags are backend selection labels, not access grants or reference counts. The application still decides visibility in its Model Loaders. AXTON remains an application-owned synchronization framework, not a replacement domain database or authorization engine.

The chosen tradeoff is explicit: removing N members performs O(N) database changes and transfers O(N) identities, in batches. There is no tag-removal message, client tag index, or extra tag cursor. This avoids making mutable tag history part of protocol compaction.

This design covers the public backend declarations, persistence contract, PostgreSQL adapter, protocol, Rust client, generated bindings and upgrades as one coherent change. Oasis Journal audience rules and removal of retired Oasis domain tables are outside it.

## 2. Existing behavior and intentional changes

The inspected implementation has six framework tables. `axton_membership` records live membership; `axton_invalidation` retains one latest position per channel and record. Scans join live memberships, so an old position for a removed member is skipped. Membership removal currently neither publishes a removal nor cleans a device. A delivered authority record carries no channel provenance.

Sources: [SQL](../../../packages/postgres/src/sql.mts), [schema](../../../packages/postgres/migration.sql), [settlement](../../../crates/server/src/settlement.rs), [pull protocol](../../engineering/architecture/protocol/pull.md), [guarantees](../../engineering/guarantees.md).

This design deliberately changes channel `remove` into synchronized release. It does not turn `touch` into a global deletion: `touch` still announces that a record's current state should be loaded for its current members. An authoritative Loader `null` remains different from releasing one channel's membership.

[PR #214](https://github.com/zanminwang/axton/pull/214) adds channel enrollment to native Load pages. Its branch was inspected as a dependency, not assumed merged. The implementation must incorporate that capability before enabling Load enrollment here. A saved Load response replay must not execute enrollment again.

## 3. Backend API

Extend the existing declarations; keep their synchronous, transaction-owned behavior:

```ts
const user = ctx.channel(`User:${bobId}`);

user.moment.add({ id: momentId }, { tags: [`Journal:${journalId}`] });
user.add(
  [Moment({ id: momentId }), MomentMedia({ id: mediaId })],
  { tags: [`Journal:${journalId}`] },
);

user.moment.remove({ id: momentId });
user.remove([Moment({ id: momentId }), MomentMedia({ id: mediaId })]);
user.remove({ tag: `Journal:${journalId}` });
```

Proposed signatures, with generated identity types substituted for `Identity`:

```ts
type MembershipOptions = { readonly tags?: readonly string[] };
type TagSelector = { readonly tag: string };

interface ModelMembership<Identity> {
  add(identity: Identity, options?: MembershipOptions): void;
  remove(identity: Identity): void;
}
interface ChannelMembership {
  add(records: readonly RecordRef[], options?: MembershipOptions): void;
  remove(records: readonly RecordRef[]): void;
  remove(selector: TagSelector): void;
}
```

Rules:

- Membership is unique per `(channel, model, identity)`.
- `add` ensures membership and unions the supplied tags with its existing tags. Repeating it is idempotent. Omitting tags or passing `[]` adds no label; it does not create an independent retention reason.
- `remove({tag: X})` removes the **whole membership** of every matching record. A record tagged both X and Y is removed too. Applications needing “keep while another source grants access” must decide that before calling this API; tags do not implement that policy.
- Removing an absent record or a tag with no members changes nothing and allocates no cursor.
- Removing a membership discards all its tag associations. A later add starts with the tags in that new add; old tags do not revive.
- Tags are channel-scoped, case-sensitive opaque strings. Reject empty/whitespace-only names; preserve accepted spelling without trimming or case folding. Bound each name to 256 UTF-8 bytes and distinct tags per add declaration to 64. Copy and deduplicate tags when collecting the declaration; later caller mutations cannot alter it.
- Preserve existing single-model identity and mixed-record array forms. Do not add a new fluent builder, model-array overload, `releaseTag`, rename, or public tag-management API.
- Mutation handlers, `backend.transaction`, and the existing `backend.publish` transaction surface use the same semantics. Native Loads have the add-only subset from #214, restricted to returned Models. Queries and Model Loaders cannot declare membership effects.
- All declarations and domain writes commit or roll back together. Escaped handles refuse calls after their owner closes.

### Ordered declarations

Selectors observe preceding declarations in the same callback, not just the initial database snapshot:

```ts
user.moment.add({ id: 'A' }, { tags: ['X'] });
user.remove({ tag: 'X' });       // A is absent at commit
user.moment.add({ id: 'B' }, { tags: ['X'] }); // B is present
```

Reduce the transaction to its final membership state. Allocate at most one log position per affected `(channel, record)` per settlement. Existing member → remove → re-add produces a final upsert even if content has not changed; never send an intermediate remove. Initially absent → add → remove produces no channel event. Tag-only changes to an already present member need no client event. A touch still produces an upsert for every final member.

A new membership produces an upsert position, including an add without `touch`. Ensure an initial positive content stamp without artificially advancing an existing one. Membership position and content stamp remain separate concepts.

## 4. Server data model

Names below are the target schema, not claims about the current migration.

| Table | Columns and keys | Responsibility |
|---|---|---|
| `axton_client` | Existing `client_id`, `owner_id`, `sequence`, `receipt` | Durable mutation sequence and receipt; unchanged role |
| `axton_call` | Existing `(owner_id, call_id)`, `request`, `response`, `claim_tx` | Idempotent saved call; unchanged role |
| `axton_record` | `id bigint PK`, `model`, `identity_key`, `identity jsonb`, `stamp`; unique `(model, identity_key)` | Global record identity and content stamp |
| `axton_channel` | `channel text PK`, `head bigint` | One delivery cursor namespace |
| `axton_channel_member` | `id bigint PK`, `channel FK`, `record_id FK`; unique `(channel, record_id)` | Live membership only |
| `axton_channel_tag` | `id bigint PK`, `channel FK`, `name`; unique `(channel, name)` | Channel-scoped label identity |
| `axton_channel_member_tag` | `member_id FK`, `tag_id FK`; PK `(member_id, tag_id)` | Current label associations |
| `axton_channel_log` | `channel FK`, `record_id FK`, `cursor`, `kind`; PK `(channel, record_id)`, unique `(channel, cursor)` | Latest deliverable membership state per record |

Additional indexes: member `(record_id, channel)` for touch fan-out; member-tag `(tag_id, member_id)` for selectors. The log's unique `(channel, cursor)` index owns ordered scans. Member/tag foreign keys cascade deletion of join rows; neither removes `axton_record` or its log rows.

`kind` is exactly `upsert | remove`. There is no log payload, tag ID, content stamp, member `present` flag, or backend membership cursor. Membership absence is a deleted live row plus its retained `remove` log row. Record identity is centralized so a removal needs no domain row or Loader.

Enforce that both sides of a member-tag association belong to the same channel. To keep the join's two-column shape, PostgreSQL uses an insert/update constraint trigger comparing the two referenced channels; channel ownership of member/tag IDs is immutable. Custom adapters must enforce the same invariant. IDs are internal storage keys and do not travel to clients.

Content stamps and delivery cursors retain the existing positive safe-integer bounds, up to `9007199254740991`; channel head may start at zero. Internal surrogate bigint IDs are not JavaScript counters and must not be coerced unsafely to `number`.

An unused tag row may be deleted transactionally when it has no associations. Reusing its name can create a new internal ID. No client or log refers to that ID, so this needs no tag tombstone. Record metadata remains while a member or log row references it.

### Example after a bulk removal

Initial state: A is tagged X and Y in `User:bob`; B is tagged X; C is tagged Y. Removing X yields:

```text
axton_channel_member       axton_channel_member_tag
User:bob → C               member(C) → tag(Y)

axton_channel_log
channel    record_id  cursor  kind
User:bob   A          105     remove
User:bob   B          106     remove
User:bob   C          103     upsert

axton_channel
User:bob   head=106
```

A/B are illustrative aliases for numeric record IDs. A is removed despite its Y label. The old A/B upsert positions have been replaced, not appended forever. C's state and position do not change.

## 5. Atomic settlement and bulk work

Use the existing Serializable transaction runner and bounded whole-transaction retry. Adapter calls inside caller-owned transactions must retain the current concurrency guarantees; the library must not secretly commit or weaken isolation.

One logical settlement:

1. Collect and validate ordered declarations. Resolve tag selections against a transaction-local view that includes earlier adds, removes and tag unions.
2. Serialize competing channel membership changes, including tag-only edits and empty-tag selections. Lock affected channels in canonical order before resolving selectors, then guard affected record keys canonically. Every membership writer uses this order. Global touch must use the same ordering/retry discipline when it expands to channels; a discovered lock-set change retries rather than acquiring out of order.
3. Compute final live members, final tags and changed record stamps. Deduplicate touched records and final delivery pairs.
4. Capture removed record IDs before deleting live members. For each channel, reserve a contiguous range for N resulting log changes with one `head = head + N` update. Refuse counter overflow before committing.
5. Assign positions in deterministic record-key order. Bulk upsert `axton_channel_log`, bulk update live members and join rows, and delete removed members. All mutations use the same transaction; the SQL statement order is an implementation detail under that boundary.
6. Commit before waking live delivery. Rollback exposes neither partial removal nor advanced head.

The tag index avoids scanning unrelated members. Parameter limits may require multiple bounded SQL batches **inside the same transaction**. Batch removal performs no Model Loader calls and no per-member network round trips to clients. It still generates row writes, index work, WAL and lock duration proportional to the number of affected members. Benchmark 1, 1,000 and 10,000 matches before setting operational guidance; do not claim unbounded cheap transactions.

For example, reservation and log writes can use the following SQL pattern after affected IDs and final kinds have been determined:

```sql
UPDATE axton_channel SET head = head + $2
WHERE channel = $1 AND head <= 9007199254740991 - $2
RETURNING head - $2 AS start_cursor;

-- $2 is an ordered JSON array of {record_id, kind}; positions start at 1.
INSERT INTO axton_channel_log(channel, record_id, cursor, kind)
SELECT $1, (v->>'record_id')::bigint, $3 + ordinal, v->>'kind'
FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY AS changes(v, ordinal)
ON CONFLICT (channel, record_id)
DO UPDATE SET cursor = EXCLUDED.cursor, kind = EXCLUDED.kind;
```

## 6. Compaction and retention

Compaction is a current-state upsert under `(channel, record_id)`. Both additions and removals get new positions; only that pair's latest position survives. A scan **must not join only live members**, because that would discard removals.

```text
101: add A       → log[A] = upsert@101
102: remove A    → log[A] = remove@102
103: add A again → log[A] = upsert@103
```

A device at 100 receives only the final upsert. A device that applied 101 and reconnects before 103 receives removal at 102. No intermediate history is needed because each message completely states that pair's presence or absence. Dynamic tags do not affect this argument: the server resolves them into concrete pairs before changing the log.

A channel scan uses `cursor > after`, orders by cursor and limits to 50 rows. For a nonterminal page, `to` is the last scanned position; when no retained position remains up to the observed head, `to = head` even across compaction holes. For bootstrap, retain the existing bounded interval and completion barrier; a row compacted beyond the upper bound is delivered by the concurrent delta lane.

**First release retention: do not prune removal log rows or local removal evidence by TTL.** This is one retained row per record ever represented in a channel, not one row per historical operation. Rows can still accumulate as new identities appear. A later retention project must introduce an enforced cursor floor and a complete reconciliation/reset protocol before deleting evidence needed by offline clients. Connected-device acknowledgements alone do not prove safety.

## 7. Protocol

This is a negotiated protocol change. Require capability `channel-membership-v1` before enabling synchronized removal. The SDK supplies the capability on every sync/storage path, including live subscribe and native Loads; the backend refuses incompatible requests before executing handlers or advancing cursors. Unknown control messages must never be silently skipped. The implementation must integrate this gate with the existing admission mechanism and expose a stable `protocol.unsupported` refusal (HTTP 426; live subscribe refused before acknowledgement). Send `capabilities: ["channel-membership-v1"]` as request-envelope metadata, including live subscribe. Capability metadata is transport negotiation, excluded from saved-call logical request equality; upgrading a retry must not turn the same logical call into a conflicting request.

Keep the page envelope and the single cursor per channel. Replace its record-only change union with explicit channel changes:

```json
{
  "cursors": {"User:bob": {"from": 100, "to": 106, "head": 106}},
  "changes": [
    {"channel": "User:bob", "cursor": 103, "kind": "upsert",
     "model": "Moment", "identity": {"id": "C"}, "stamp": 9,
     "state": {"text": "Still here"}},
    {"channel": "User:bob", "cursor": 105, "kind": "remove",
     "model": "Moment", "identity": {"id": "A"}},
    {"channel": "User:bob", "cursor": 106, "kind": "remove",
     "model": "Moment", "identity": {"id": "B"}}
  ]
}
```

The state above illustrates a model payload; actual payloads must match the requested Model version.

- An upsert states channel membership and contains the existing `AuthorityRecord` fields. Resolve its current global stamp and viewer Loader in the same server read transaction. A Loader failure remains an error, not a remove; Loader `null` remains stamped authoritative absence.
- A remove contains only channel, cursor and record identity. It has no body, content stamp, tags or Loader invocation.
- Each event belongs to a named channel and has `from < cursor <= to <= head`. At most 50 events per channel; no duplicate pair within a page. The same record in two channels requires two membership events; memoize its Loader result per read transaction rather than discarding provenance.
- Bootstrap and live delivery use the same event semantics, retaining their existing run/subscription guards and bounded interval rules. Bootstrap replaces its record-only items with this change union.
- An unknown kind, missing channel, conflicting duplicate, out-of-range cursor or remove carrying authority fields is a malformed page: refuse it atomically without progress.
- As today, a valid upsert whose body cannot be loaded/decoded may advance page progress with a diagnostic. Its positive membership evidence can still be stored, but it does not fabricate content or advance a rejected content stamp.

### Enrollment responses

A Load page that enrolls returned records must also persist and return the resulting claims:

```ts
type MembershipClaim = {
  channel: string;
  cursor: number;
  model: string;
  identity: object;
};
// Additional envelope metadata, separate from model fields:
// memberships: readonly MembershipClaim[]
```

A claim uses the pair's current upsert log cursor from the same transaction as the body; it is not another cursor namespace. Only claims for returned authority identities and channels enrolled by that call are included. A repeated add may reuse an existing upsert position. Save the claims with the exact idempotent response; replaying the saved page neither re-adds the member nor refreshes the claim.

Apply the same metadata rule to Mutation/direct-action readback that enrolls a returned identity. Ordinary Fetch/Query responses do not invent claims or enroll records. Model Loaders remain channel-independent. No enrollment claims or tags are exposed as Model fields.

## 8. Client ownership and stale responses

“No client tags” does not mean “no channel bookkeeping.” The engine must distinguish two channels holding one record. Keep a small internal ledger keyed by `(channel, model, identity)` with its latest observed cursor and present/absent state. These per-record positions are ordering evidence, not additional polling cursors. Persist them in SQLite, including absence, alongside the one existing subscription cursor.

Apply the entire delivery in one local transaction:

1. Validate page and subscription/run guards.
2. Merge each pair's membership evidence by cursor, including claims from enrollment responses. Older evidence cannot undo newer evidence. Equal evidence is idempotent; equal cursor with conflicting presence is invalid.
3. Stage accepted content by its existing global stamp. An upsert superseded by stored removal evidence cannot materialize its body without another current hold. Fold all channel changes in the delivery before deciding eviction, so remove in A plus upsert in B does not transiently delete the shared base.
4. Release the replicated base only when no known channel holds it. Commit membership, data changes, reactive notifications and delivery progress together.

Release is cache eviction, not an authoritative Model deletion. It does not advance content stamp, enqueue a mutation, or run `onTargetDelete` cascades. An authoritative stamped `state:null` keeps its existing deletion/cascade semantics. Applications removing an aggregate must label/remove the aggregate's published identities; removing a parent cannot silently remove an independently held child.

Do not make application `onStore` hooks responsible for release. Normal local query observers see the resulting row changes after commit. Keep the existing `onStore` contract for accepted authority; release itself does not masquerade as Loader `null` to that hook.

### Same-stamp restoration and local work

Retain content-stamp evidence after eviction and distinguish `evicted` from `authoritative absent`. A newer membership upsert can restore an evicted base at the same content stamp. It cannot resurrect authoritative absence at that stamp or regress a newer accepted stamp. Once restored, ordinary equal-stamp conflict checks apply again.

Evict only the replicated base. Keep queued mutations, rejection/conflict evidence, and direct local-write layers; rebuild optimistic presentation over the now-absent base under existing replay rules. A pending update may no longer render against an absent base, but its submitted data remains recoverable and its receipt must still settle normally. A direct local row is not deleted merely because its identity matches a released replica. After settlement, release an unheld replicated base instead of letting an old receipt repopulate it. True authoritative deletion still follows existing authority policy.

### Fence delayed Load, Fetch, Query and receipt bodies

Channel cursor ordering alone cannot protect an unversioned stored-read response. Define a durable local **store epoch**, incremented when a newly accepted removal leaves the record with no known holds, including previously cached records with no recorded hold. Duplicate or stale removals do not increment it. Store that epoch as the record's `evicted_at`. Freeze the current epoch when a logical storage-producing request is created, not on each retry. Persist that token with durable Load pages and mutation work; receipts use the original queued call token. Transient requests retain it for their lifetime. Restart does not refresh old tokens.

For positive bodies, admission is separate from content ordering:

```text
merge valid membership claims first
if record has any current channel hold:
    use normal content-stamp admission
else if request.store_epoch < record.evicted_at:
    settle the call, but do not materialize its positive replicated body
else:
    this is an explicitly fresh read; use normal content-stamp admission
```

A newer channel upsert/claim can restore a hold and admit its body; a stale claim cannot. Process an authoritative `null` by normal stamp rules even if its request was older. Do not discard mutation acknowledgements, Load continuation or error reporting merely because a positive body is suppressed.

This is not a permanent client authorization ban. An explicitly fresh request after release may read/cache a record again if its current Loader permits it. A record returned by a one-shot read without enrollment has no promise of future channel-driven cleanup. Enrollment claims and the epoch fence prevent delayed, previously issued work from accidentally doing that fresh read's job.

Unsubscribe keeps its existing meaning of stopping delivery; it does not synthesize removal or clear all data. Retained membership evidence is reconciled before a resumed subscription is considered caught up. A stopped channel cannot promise timely revocation.

## 9. Upgrade and deployment

Use forward migrations and a coordinated protocol gate. Never reset a client's database to introduce this feature; pending writes and saved calls survive.

Server migration:

1. Add record surrogate IDs and normalized identities. Current `identity_key` is canonical JSON (`RecordKey::encoded_identity`), so backfill `identity` by validated JSON decode; compare any retained invalidation identity and fail on disagreement.
2. Populate live members from old memberships. Copy retained invalidations into log rows, using `upsert` when a live member exists and `remove` otherwise. Preserve their cursors and channel heads.
3. For existing members with no retained invalidation, allocate new upsert positions above the old head. This ensures every live member is representable during reconciliation.
4. Start tags empty. Verify uniqueness, foreign keys, channel ownership and safe counters before switching runtime reads/writes. Keep old tables during the forward rollout; remove them only in a separately authorized cleanup.
5. Stop old framework writers during cutover. Both server versions must not independently advance the same channel heads against different tables. Do not rewrite opaque saved responses into claims they never contained.

Client migration adds membership/eviction evidence and request epochs transactionally. Existing bodies are initially legacy cache, not fabricated channel holds. Preserve existing live delivery positions, and run an engine-owned membership reconciliation from cursor 0 to a fixed observed head for each retained subscription. It uses the compacted log and the bootstrap completion barrier, not a claim that native Load is a complete snapshot. Mark the subscription reconciled only after its concurrent delta stream reaches the barrier. Apply tombstones even for records with no prior local hold; they may be cached from before upgrade. Seed old queued work with epoch zero and never reinterpret a saved pre-capability response as new enrollment.

Legacy records never represented by a retained member/log pair remain untracked cache; migration cannot infer ownership that was never recorded. Explain this limit rather than wiping unrelated data. For migrated absent log rows that have never reached the device, the reconciliation walk supplies the required removal evidence.

Old saved Load/call responses can settle under an explicit legacy-response compatibility decoder, without fabricated claims and subject to epoch fencing. New client requests and new server operations require the capability. Upgrade tests must cover saved retries crossing the server cutover. The first implementation must ship backend and JS/Dart runtime support together before an application enables removal.

## 10. Acceptance scenarios

| Scenario | Required result |
|---|---|
| Add A twice with X then Y | One member; both labels; no tag-only publication |
| Remove X from A tagged X/Y and B tagged X | Both whole memberships removed; one tombstone each |
| Remove absent selector twice | No new position, no Loader call |
| Add A/X then remove X in one transaction | No lasting member and no event for initially absent A |
| Remove existing A then add A/Y | One final upsert; only Y survives |
| Touch and tag removal concurrently | A serial outcome; no upsert with absent member at the same committed position |
| Remove 10,000 matches, fail before commit | No partial domain/member/tag/log/head changes |
| Client has A via two channels | Removing one keeps it; removing the last evicts its replica |
| Client has cached A but never recorded a hold | Removal still evicts that cache and creates absence evidence |
| Remove and re-add A without editing it | Equal-stamp upsert restores the evicted base |
| Loader answers null / throws | Authoritative absence / diagnostic, never confused with channel release |
| Delayed enrolled Load after remove | Saved old claim and body cannot restore A |
| Delayed Fetch/Query or receipt after remove, including restart | Old positive body suppressed; continuation/settlement preserved |
| New Fetch after remove, Loader allows it | Explicit fresh cache is allowed; no implicit enrollment |
| Pending edit/direct local work during release | Work survives; replica is not resurrected by stale settlement |
| Device offline across repeated add/remove cycles | Latest pair state is sufficient after reconnect |
| Old client attempts delivery | Refused before handler execution or progress |
| Legacy upgrade with pending writes and old saved Load | No database wipe, no fake claims, reconciliation converges |
| Parent released, child held elsewhere | Release does not run a global deletion cascade |

## 11. Delivery boundaries and known costs

Backend tags, synchronized membership and client admission must land as a complete vertical capability before applications depend on automatic cleanup. Internal tasks can be committed separately behind the capability gate. Do not ship a backend-only remove that an old client silently ignores.

Costs deliberately accepted: per-record removal identities; retained compacted tombstones; a client channel-holding ledger and small stale-response evidence. Deferred work: retention floors/snapshots, generic source-reference semantics, tag sync, public tag editing APIs, and Oasis integration/legacy-domain cleanup.

Design review must specifically prove concurrent lock ordering, cross-path stale-response admission, migration replay and local-work preservation. Passing only a happy-path tag deletion test is insufficient.
