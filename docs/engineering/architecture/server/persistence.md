# Persistence

## 1. Introduction and Goals

Persistence answers the engine's storage requests inside the application's transaction. Business writes, authority stamps, Stream tracking/positions and saved outcomes share one commit or rollback. The PostgreSQL adapter owns SQL; application shims supply a transaction runner and query executor.

## 3. Context and Scope

[host.rs](../../../../crates/server/src/host.rs) and [host-contract.mts](../../../../packages/server/host-contract.mts) define requests and response validation. [Backend interface](backend-interface.md#9-architecture-decisions) owns signatures; [database guide](../../../../website/docs/backend/database.md) owns shim setup.

| Storage | Responsibility |
| --- | --- |
| `axton_client` | Client claim, batch progress and saved receipt. |
| `axton_call` | Immutable call-ID outcomes, including Load pages and Fetch snapshots. |
| `axton_stream` | Opaque names and delivery heads. |
| `axton_record` | Catalog identity and authority stamp. |
| `axton_stream_member` | Durable unique tracking pairs. |
| `axton_stream_log` | Latest retained position per Stream/record, including historical removals. |

Fresh DDL has six tables and no tag dictionaries/member-label joins. Tracking survives authority absence; no automatic retention policy is provided. Log removals remain identity-only protocol evidence, distinct from Loader null and unrelated to a public withdrawal API.

## 5. Building Block View

[PostgreSQL persistence](../../../../packages/postgres/src/persistence.mts) uses [SQL statements](../../../../packages/postgres/src/sql.mts) over the chosen driver. `claim` serializes client retries; `claimCall` and `saveCall` persist exact outcomes. Saved replay runs no handler, Loader or declarations. Savepoint operations isolate one call and preserve outer transaction ownership.

A fresh `claimCall` uses `INSERT … RETURNING` without reading its new row back. Only a duplicate reads and locks the stored outcome; a committed incomplete response is refused. `saveCall` uses the last fresh claim's `ctid` as a short-lived row-position hint, guarded by owner, call ID, null response and the full creating transaction ID (`claim_tx`). A stale or missing hint falls back to the same guarded logical-key update. Savepoint rollback and reused connections cannot transfer claim ownership.

`readStamps` returns stamps in request order, inserting missing records at 1 without rewriting or locking existing records. `COALESCE` uses inserted stamps first and probes the `(model, identity_key)` unique key only for existing records. An insert or re-stamp committed after the transaction snapshot still fails serialization and retries.

Bulk `readTracking({records, pairs})` returns the union of all holders of named record keys and existing explicit pair candidates, without duplicates. `guardRecords` takes canonical `{model, identityKey, mode}` records and returns aligned safe stamps: advance once, ensure without advancing existing authority, or compatibility lock (null for absent metadata). Invalid cardinality, order or response pairs abort settlement.

`lockStreams` locks candidate Stream rows before record guards in canonical UTF-8 byte order. Settlement re-reads tracking under locks; an unlocked global destination triggers whole-transaction retry. `applyStreamMembers` writes final pairs, reserves grouped head ranges and compacts upsert positions. `scan` resolves current authority for upserts and preserves identity-only removals without Loader calls. Existing single-record stamp operations remain for non-settlement read paths.

SQL uses bounded set-based chunks of 1,000 items. Host round trips and SQL statement counts scale with chunks; row, lock, WAL and log work still scale with records, Streams and tracking pairs. Guard order spans all mixed modes and chunks, retaining existing-row no-op write fencing. Sorted output or plain `SELECT FOR UPDATE` is not proof of acquisition order or an equivalent Repeatable Read conflict fence. Retry serialization failures/deadlocks as whole owning transactions; caller-owned publication retains the caller's transaction/retry responsibility.

## 9. Architecture Decisions

[Publish](engine/publish.md) owns global/selected invalidation and combined settlement. Inferred changed Mutation inputs are global. Tracking another Stream does not change authority; targeted invalidation does not enroll. PostgreSQL shims run framework-owned transactions at Serializable with bounded retries. Side effects that cannot repeat belong in an application outbox.

## 10. Quality Requirements

[Driver conformance](../../../../integration/persistence/server/driver-conformance.test.mjs), [membership integration](../../../../integration/persistence/server/membership.test.mjs) and [host conformance](../../../../fixtures/protocol/host-operations.json) cover aligned responses, rollback, replay, chunking and concurrency. Task evidence records executed checks separately from this coverage description.

The read-footprint conformance tests inspect `pg_locks`: fresh claims, their saves and new stamps leave no predicate locks on the call/record tables or their indexes. They also cover duplicate replay, multiple claims, stale row positions, savepoint rollback, reused connections, existing-stamp reads and five interleavings of disjoint deliveries. These assertions run alongside Stream tracking and migration tests; they do not establish a mixed-workload contention bound.

## 11. Risks and Technical Debt

Hot Stream heads and record stamps serialize; retries can occur even across disjoint rows under Serializable predicate locking. Saved calls, tracking and logs have no automatic TTL/pruning ([#61](https://github.com/zanminwang/axton/issues/61)). In-process wakes do not cross backend processes ([#62](https://github.com/zanminwang/axton/issues/62)).

Existing-stamp probes can still use a sequential scan and take a relation predicate lock on tiny analyzed tables. Stream tracking/settlement, foreign-key checks and application reads also remain sources of Serializable retries. The claim/stamp optimization ([#210](https://github.com/zanminwang/axton/issues/210)) changes neither isolation nor retry settings; earlier Channel/Scope measurements do not establish current Stream performance.

## Stream forward migration

Existing layouts upgrade forward with old writers stopped: legacy v0.1 → [Channel membership](../../../../packages/postgres/migrations/2026-09-30-channel-members.sql) → [Scope](../../../../packages/postgres/migrations/2026-09-30-scopes.sql) → [Stream](../../../../packages/postgres/migrations/2026-10-01-streams.sql), then current DDL. Start from the migration appropriate to the installed layout; never reapply an older-layout migration after cutover. Fresh databases use [migration.sql](../../../../packages/postgres/migration.sql).

The Stream migration preserves heads, catalog IDs, pairs, positions/removals, receipts and saved calls, retires tag-only tables and rewrites only framework top-level `memberships[*].scope` to `memberships[*].stream`. Business results, identities, continuation JSON and opaque names remain byte-identical. Failed migration rolls back wholly; repeat migration is idempotent. See [coordinated negotiation](../../../../website/docs/backend/deployment.md#stream-membership-cutover). Historical migration files remain unchanged.
