# Local authority delivery

Date: 2026-10-01
Status: Approved for implementation by the user on 2026-10-01.
Baseline: `134294d0e7b0d6161e9c9a8dfed85eee02ca1d1b`.

## Decision

AXTON delivers current viewer-authorized Model state into one local store. It
does not assign cached records a lifetime through ownership by Streams.

A local record is identified by Model and identity. Its authority stamp orders
canonical content and absence across delivery paths. A Stream has its own
delivery cursor; it is a notification route, not a local data partition.

Remove the client's per-Stream record holding ledger and automatic final-holder
eviction. Keep the server's durable tracking pairs: they determine where
invalidation notifications go. Applications own local cache reclamation through
their local transactions and `onStore` hooks.

This decision does not promise that a request which is refused, fails, or loses
connectivity delivers data. Existing admission, Loader diagnostics, retries and
transaction guarantees still apply.

## Responsibilities

AXTON continues to own:

- Model identity, authority stamps and stamped Loader absence.
- Per-Stream subscription identity, delivery progress, gap recovery and explicit
  bootstrap progress.
- Durable Mutation intent, frozen logical calls, receipts, rejection recovery,
  pending replay, companion semantics and direct-write ordering.
- Local transaction rollback, declared cascades, and incoming `onStore` hooks
  sharing their enclosing transaction.
- Server tracking, invalidation settlement and current viewer Loaders.

The application owns:

- Which previously delivered cached content to retain or reclaim.
- The meaning of business relationships and access facts delivered as Models.
- Local cleanup helpers, preservation of its own work, and presentation gates
  based on current access.

Neither a Stream unsubscribe nor loss of a historical Stream holding is a Model
delete. A newer stamped Loader `null` remains canonical absence and still uses
existing Model hooks and cascade semantics.

## Normal delivery

Stream pull/live pages, bootstrap, Push receipts, direct Actions, Loads and
Fetch use the shared Model/identity/stamp authority path. Incoming authority
does not require a local positive holding claim.

If Streams A and B deliver the same Model/identity, they update one local
record. Lower-stamp authority cannot replace newer authority. The existing
equal-stamp conflict, per-record diagnostic and operation-specific atomicity
rules remain in force.

An explicit application cleanup does not create a permanent source-withdrawal
fence. Later valid canonical authority may repopulate the record under ordinary
stamp and local-write rules. Hooks and Queries must not treat cache presence as
permission. This change does not revise equal-stamp restoration behavior.

## Public API and protocol

Keep the existing server `track` and global/selected `invalidate` declarations,
batched operands, viewer Loaders and client `streams` subscription API.

Do not add Events, tags, source-release operations, cache eviction APIs, automatic
business graph traversal or per-Journal subscriptions in this change. A derived
information Model with a Loader and an `onStore` hook is an application choice,
not a new framework protocol primitive.

Introduce the capability `stream-authority-v1`. A coordinated server/client
cutover requires it before handlers, claims, durable progress or live subscription
acknowledgement. Do not advertise `stream-membership-v1` while omitting the
ownership semantics that capability promises.

Capabilities remain transport decoration, excluded from logical call identity.
Preserve frozen logical bytes, call IDs, sequence numbers and continuations.
Update capability size allowances without increasing business payload limits.

Fresh Push, Action and Load envelopes do not produce membership claims. Retained
saved outcomes may still contain valid historical top-level `memberships`
metadata. Decode it for compatibility, but never merge it into client ownership.
Preserve business results and identically named nested business fields. Saved
call replay remains replay: do not rerun a handler or Loader to modernize its
response.

## Local storage migration

Perform an idempotent, transactional in-place framework migration before
scheduling network work. Fresh databases have no `axton_stream_member` table
or record-holding index. Existing compatible files remove that holding table
and index without deleting Model rows.

Use `axton_client.local_authority_version = 1` to distinguish a completed
migration from a malformed former layout. Support the known original Channel, Scope and
Stream layouts; refuse corrupted modern layouts atomically. Preserve the
existing nondestructive rebuild policy for unsupported older layouts.

Preserve client identity, cached Models, record stamps, subscription identities
and cursors, explicit bootstrap progress, queued/failed calls, companions,
direct local work, rejections, Query results, Load identities/continuations and
frozen logical request bytes.

That preservation describes the storage upgrade itself. Subsequent newer cloud
authority still replaces direct local edits under the existing local-write
contract; the migration does not make those edits immune to synchronization.

Retire automatic membership-reconstruction scheduling and its active progress
handling. It must not restart completed explicit bootstrap, rewind ordinary
cursors or run solely to reconstruct discarded holdings.

Do not equate all machinery named `Held` or `local_replica_layer` with Stream
ownership. `authority.rs::Held` is a pending-replay accumulator. Existing local
operation history must survive this migration. Preserve the legacy `evicted`
base distinction and frozen request/eviction epochs where needed to settle
pre-cutover work safely. New Stream delivery creates neither holding rows nor
ownership eviction epochs. Retire compatibility fields only through a separately
proven migration; their presence is not an active per-Stream ownership ledger.

## Historical removal cutover

A historical `Remove` contains Stream, cursor and record identity. It contains
neither canonical authority nor an authority stamp. It must never become a
global Model deletion, hook invocation or cascade.

With old writers and live sessions stopped, perform one server-side repair of
the latest retained removal positions before admitting authority-only traffic:

1. Read the distinct historical withdrawn Stream/record pairs.
2. Re-establish durable tracking for those pairs and globally invalidate the
   affected identities through the existing bulk settlement rules.
3. Allocate newer record stamps and ordinary upsert positions. Preserve
   unrelated records, Stream heads, saved receipts, saved call outcomes and
   business tables.
4. Let ordinary viewer Loaders provide current content or `null` when those
   positions are delivered. Never synthesize null from withdrawal evidence.

Latest-position compaction replaces the repaired removal positions. Repeating
the repair finds no remaining latest removals and allocates nothing. Existing
clients retain their delivery cursors; fresh positions make repair observable
without a database reset or a rewind.

Historical removal frames retained in fixtures or compatibility input can be
validated and covered by their cursor range without writing a Model or changing
its stamp. That is compatibility decoding, not permission cleanup. The server
repair is what provides current authority for deployed historical withdrawals.

## Required evidence

Before implementation is accepted, verify:

- Multi-Stream delivery updates one record; unsubscribe never removes it.
- Newer null applies across paths; older content cannot resurrect it.
- Fresh Stream, Load, Action and Push delivery requires no holding rows/claims.
- Hooks, incoming authority and cursor progress roll back together on failure.
- Pending optimism, companions, direct writes, rejection recovery and frozen
  calls survive upgrade and reopen.
- Original Channel, Scope and Stream files upgrade without resetting cached
  rows, stamps or cursors; malformed layouts remain untouched.
- Legacy evicted state and frozen work settle without converting authoritative
  absence into restorable positive authority.
- Historical removal repair is idempotent, assigns newer authority, preserves
  other viewers' valid access and cannot translate one source loss into global
  deletion.
- Old capability rejection occurs before effects; saved outcome replay keeps
  logical identity and nested business JSON unchanged.
- Explicit bootstrap and subscription/run cancellation guards still work.

Use Rust component/simulation coverage for state transitions, real SQLite
reopen tests for migration, PostgreSQL integration for repair and rollback,
and JS/Dart round trips for published-facing generated/runtime boundaries.

## Consumer boundary

Most Days remains on its published pin until this coordinated contract is
released. Its separate CAP-799 migration will use one User Stream, canonical
access information and reusable application cleanup helpers. It must retain
the viewer's own Entries, other valid Journal paths and current Reply visibility
rules. This AXTON change does not implement that product cleanup.

No merge or registry publication is part of this design-writing step.
