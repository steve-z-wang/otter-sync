# Publish

Protocol-5 policy and failure boundaries are owned by [protocol 5](../../protocol/0.5.md). Rust owns synchronization; language bindings execute networking, timers and interface callbacks. Descriptions of protocol-4, epoch/Load jobs, once caches or legacy queue tables below are historical component context, not current public APIs.

## 1. Introduction and Goals

Publish settles durable Stream tracking and authority invalidation inside the application's transaction, then wakes subscribers after commit. Tracking is interest, not permission: viewer Loaders own current content or absence.

## 3. Context and Scope

Mutation and legacy handlers, `backend.transaction` and `backend.publish` provide synchronous `stream(nameOrNames).track`, global `invalidate` and selected `stream(nameOrNames).invalidate`. Model-specific single/list identities and mixed generated references use the same collector. Multiple names and records form a Cartesian product. Empty operands are no-ops. Names are opaque, nonblank and case-sensitive; operands are copied before appending and handles expire with the callback. Ordinary rejected declarations append nothing. Load collectors remain sticky-failed and expose only tracking of the page's returned records; Queries and viewer Loaders expose neither verb.

A first pair establishes unique durable interest and one upsert position, ensuring a stamp without advancing existing authority. Repeated tracking moves neither stamp nor cursor. Global invalidation reaches every finally tracking Stream; selected invalidation intersects names with final tracking and never enrolls. Selected sets union and global wins. Inferred changed Mutation inputs always invalidate globally. Each invalidated identity advances once per settlement, even without holders; each final pair receives at most one upsert. Explicitly empty selected names declare nothing.

Shared content changes require global invalidation. Selected invalidation serves viewer-specific authority changes; row-to-null answers need newer stamps. Tracking survives Loader absence and provides no automatic retention policy. Fresh APIs have no withdrawal or labels. Retained historical removal positions remain decodable cursor evidence and change no client Model. Client cache retention is independent of server tracking; newer viewer Loader null supplies canonical absence.

## 5. Building Block View

[settlement.rs](../../../../../crates/server/src/settlement.rs) validates and canonicalizes declarations, combines tracking/invalidation effects and reconciles final pairs. [stream_members.rs](../../../../../crates/server/src/stream_members.rs) defines the delta and position types. The host contract uses `readTracking`, `lockStreams`, `guardRecords` and `applyStreamMembers` ([Persistence](../persistence.md)).

1. Read candidate tracking in bulk: all holders for globally invalidated records, explicit candidate pairs for tracking and selected invalidation.
2. Lock candidate Streams, including newly tracked names, in canonical UTF-8 byte order before record guards.
3. Guard canonical record keys in one bulk request: `advance` wins over `ensure`; compatibility `lock` preserves authority and can return null for absent metadata. Modes never reorder keys.
4. Re-read tracking under locks. A global destination outside the lock set requires whole-transaction retry, rather than extending locks out of order.
5. Apply final pairs in bounded SQL chunks, reserve grouped head ranges and compact logs. Validate request-aligned stamps and returned positions. Rows, metadata, saved outcomes and wakes share commit or rollback.

Host calls are bulk; SQL statements scale with bounded 1,000-item chunks. Row, lock, WAL and log work still scale with affected records and pairs. Mixed guard acquisition order spans chunks and must preserve no-op write fencing on existing records; sorted output or plain `SELECT FOR UPDATE` alone is not equivalent evidence.

The TypeScript [effects collector](../../../../../packages/server/effects.mts) freezes declarations. The [server session](../../../../../packages/server/index.mts) snapshots the wake set at savepoints and restores it after rejection. No rejected unit wakes anyone.

## 6. Runtime View

Handler → combined settlement → grouped viewer Loader readback → saved outcome → commit → wake. Saved call replay runs no handler, Loader or declarations and allocates no new stamp or position. Loader reads remain grouped by Model/version and deduplicated across Streams, with per-identity fallback after batch failures. Errors are not absence and cursors are not proof that every Loader succeeded.

`backend.transaction` opens the transaction, settles after the body returns and wakes after commit. `backend.publish(tx, callback)` uses the caller's transaction, settles before returning its wake, and requires the caller to invoke that wake after commit. Separate publish invocations are separate settlements even in one transaction. A publication inside a savepoint rolls back with it. Publish refuses transactions AXTON already owns.

## 9. Architecture Decisions

Tracking and invalidation are distinct: new interest does not change authority, and invalidation does not create interest. No Loader cache, network request per declaration, automatic business traversal or tracking-retention policy is introduced. Subscription intent remains local and unsubscribe preserves cache and server tracking. [Public API](../../../../../website/docs/backend/api.md#streams) owns application signatures; [standing cleanup](../../../../../website/docs/frontend/sync.md#authentication-and-account-changes) owns the application pattern and its limits.

## 10. Quality Requirements

Bulk contracts, global/selected precedence, one-stamp/one-position settlement, rollback, replay and canonical acquisition are covered in [server tests](../../../../../crates/server/tests/membership.rs) and [PostgreSQL integration](../../../../../integration/persistence/server/membership.test.mjs). This page names coverage, not a new test execution claim.

## 11. Risks and Technical Debt

Wakes are process-local ([#62](https://github.com/zanminwang/axton/issues/62)). Hot Streams and records serialize and can retry whole transactions. Tracking/log retention requires separate policy ([#61](https://github.com/zanminwang/axton/issues/61)); Loader absence never prunes tracking automatically.

After prior layout upgrades, the stopped-writer [authority repair](../../../../../packages/postgres/migrations/2026-10-01-local-authority.sql) re-tracks historical removed pairs, advances each affected identity once and publishes upserts to all its current tracking pairs. Viewers then resolve current state or null through their Loader. Reapplication allocates nothing; saved calls/receipts and business bytes remain unchanged ([Deployment](../../../../../website/docs/backend/deployment.md#stream-membership-cutover)).
