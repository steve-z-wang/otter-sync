# Protocol 4 server runtime

## 1. Introduction and Goals

Protocol 4 serves one Stream bound to a persistent Store. Ordinary Query and Fetch snapshots carry `cursor: null`; only Stream and manifest records carry that Stream's real per-identity cursor. Mutation receipts acknowledge an immutable accepted or refused call and identify its server-visible optimistic targets. Device-only companions are settled locally and never appear in a receipt.

## 3. Context and Scope

[`protocol_v04.rs`](../../../../crates/server/src/protocol_v04.rs) is dispatched from the existing Action, Fetch and Pull entrypoints. [`live.rs`](../../../../crates/server/src/live.rs) negotiates the shared `SubscribeIntent` and drains strict Delta pages through the same Pull carrier. The Node host owns the application transaction and current Stream authorization; PostgreSQL persists calls, membership, publication groups and bounded identity manifests.

`backendId` and `contractId` are stable trusted configuration. The materialization ID uses the shared normalized Model-read descriptor and `projectionGeneration` (default `"1"`), not a hash of the entire client/server Schema. Current Model names, versions, fields, identities, relevant enums and Bootstrap selection participate. Credential refresh does not change these values. A Store's incarnation survives ordinary reopen; explicit reset changes it.

Retained materializations store the complete original Schema and projection generation. An old version map alone cannot reconstruct a saved Model set or Bootstrap selection after a rollout. Normal direct reads use the current context. Durable Mutation execution/replay can use an explicitly served retained descriptor. Current authorization is evaluated before every saved response replay.

## 6. Runtime View

Relevant business work is fenced by a persisted namespace-wide PostgreSQL row UPDATE. Mutation and framework-owned background callbacks acquire it before application work. Loader preparation acquires it before preparation writes. Preparation hooks receive explicit `streams([...])` and global `invalidate` declarations within that transaction; `backend.publish(tx)` must not rebind a framework-owned transaction. Authority planners prepare the bounded key closure, re-plan after preparation publications, then read final pair positions and canonical content without rerunning preparation. Remove requires neither a Loader nor preparation. Mutation output and target identities share this preparation phase. A genuinely read-only Query runs concurrently; explicit tracking upgrades through the UPDATE, and a stale Serializable snapshot forces the database adapter to retry the entire transaction, including its handler. No stale result is saved for reuse after that retry.

Caller-owned transactions must await `acquirePublicationFence(tx)` before relevant reads/writes, or place that work inside `publish(tx, callback)` whose fence precedes its callback. Publishing after earlier unfenced business work cannot establish this guarantee retroactively. All writers changing a published viewer projection must follow this contract and publish the complete affected identity set. External or time-dependent Loader changes without a corresponding publication violate the content/position contract.

New tracking publishes current authority; repeated tracking keeps the pair cursor. Invalidation never enrolls. Mutation input targets do not implicitly invalidate or track: an accepted no-op keeps its historical cursor, and an untracked or removed target receives a call-private null-cursor snapshot. A live initiating-Stream target receives its real current cursor and the same-transaction private fallback. Receipts, publications and application writes commit atomically. Refusal rolls the call's savepoint back and saves no accepted targets.

Bootstrap Start snapshots an immutable bounded identity manifest at N: live identities of marked Models, plus explicitly held Stream-authority identities for rematerialization. Held identities must have current or retained historical evidence in the initiating Stream; unknown keys are refused, never enrolled. Pages address exact identity ordinals and return current canonical authority or an explicit Remove. Served page ranges and immutable page payloads survive process restart. Tail capture requires complete manifest coverage and freezes H; it does not advance client delivery C. Completion requires actual Delta coverage through H. A fresh Store may initialize its previously unset delivery boundary to N; rematerialization preserves an existing boundary.

A missing historical Mutation target can sit below C, so future Delta delivery cannot necessarily reach it. The separate `Materialize` intent proves an exact saved accepted receipt, matching stable binding and incarnation, and requests only its Stream targets. It skips the application Bootstrap handler and general Bootstrap range. It materializes under the active current context after verifying the original retained descriptor and identity contracts, without allocating any cursor. Installing current compatible G at or above the required cursor can settle the old call; requiring old-context G after current rematerialization would strand it.

## 9. Architecture Decisions

Each publication transaction retains its original identity set and Stream span; several settlements inside one outer transaction merge into one group. Compacted members resolve at their current pair positions, which can be ahead of the group's original committed prefix. Later overlapping groups close dependencies without making the whole transport page indivisible. Retained group evidence has no age-based garbage collection in this release.

Manifest ordinal items and additive companions form one local commit unit. Companions install authority but cover no identity ordinal. A transferable secondary unique constraint requires bounded same-Model current authority, including held Remove identities: a release in an earlier independent transaction may otherwise conflict with an acquisition encountered first in identity order. A unique key containing the complete primary identity cannot transfer between distinct identities and does not require this closure. Different historical unmarked types do not become Bootstrap-selected merely by sharing a coarse publication transaction; actual post-N publications may accompany a page as ordinary current Stream authority.

References provide navigation and declared cascade/dependency metadata, not cached-parent existence enforcement. Missing or unloaded parents do not block partial-cache reads. Stream authority for each child remains explicit; no child cursor or absence is invented from a parent declaration.

Publication/constraint groups allow at most 10,000 identities and 1 MiB of retained identity metadata. `protocol4.maxUnitBytes` bounds serialized authoritative changes in an atomic unit (default 1 MiB). Capacity overflow fails the transaction explicitly; it never splits a required group or saves partial coverage.

## 10. Quality Requirements

[`protocol-v04.test.mjs`](../../../../integration/persistence/server/protocol-v04.test.mjs) exercises real Node/Rust/PostgreSQL and HTTP/WebSocket boundaries: null reads, tracking upgrade retries, immutable replay, original-prefix groups, unique transfers across pages and separate transactions, finite manifest resume and Remove coverage, type filtering, retained-context receipt recovery, no-op/private Mutation targets, bounded failure and live delivery. Client-side authority admission, projection reconstruction, ownership and durable C/G commits remain the client engine's responsibility; server evidence alone does not prove local completion.
