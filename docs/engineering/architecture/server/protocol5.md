# Protocol 5 Mutation Batches

## 1. Introduction and Goals

The additive Batch path commits each named Mutation independently and resumes a fixed request after a crash. Protocol 4 remains operational; protocol-5 delivery planning and client adoption are separate work.

## 3. Context and Scope

`BackendOptions.protocol5` supplies known current/retained read schemas, projection generation and `authorizeStream(principal, stream, tx)`. The server derives the read context through core's protocol-5 materialization helper. Authentication identifies the principal; authorization runs under the retained database transaction and publication fence, including replay.

The SDK carries one validated immutable request through sequential `processBatchMember` native calls. Each call opens its own Serializable transaction. Rust validates the complete request, selects the trusted Mutation by name/version and normalizes its input. The caller's descriptor fingerprint is immutable intent bound by the Batch digest; it is not an equality gate against today's artifact. Compatible input widening or field reordering therefore does not refuse frozen work.

## 5. Building Block View

- [protocol_v05.rs](../../../../crates/server/src/protocol_v05.rs) validates carriers, assembles acknowledgements and adapts existing settlement/Loader machinery.
- [mutation_batch.rs](../../../../crates/server/src/mutation_batch.rs) owns Store admission, replay, per-member execution and outcome persistence.
- [host.rs](../../../../crates/server/src/host.rs) defines strict `Protocol05Operation` requests, mirrored in [host-contract.mts](../../../../packages/server/host-contract.mts).
- [persistence.mts](../../../../packages/postgres/src/persistence.mts) dispatches those operations through the shared pg/Prisma/Drizzle driver; [migration.sql](../../../../packages/postgres/migration.sql) adds Store, MutationResult and StreamRecord tables without removing legacy data.

## 6. Runtime View

A member locks its Store before checking immutable principal/Stream binding, Batch sequence, digest, count and progress. Only the current Batch or the last completed Batch can replay. A valid next Batch prunes the older result set in the same transaction. Saved members run neither handler nor preparation.

Fresh work acquires the persisted namespace publication fence before authorization and business work. The business savepoint isolates an explicit typed refusal. A handler or Loader business refusal rolls back its effects, then saves rejection/progress outside that savepoint. Infrastructure, malformed Loader output or database failure aborts the outer transaction and leaves progress unchanged. The final saved member promotes current metadata to the last completed Batch.

Explicit tracking and invalidation use the retained discovery/recheck and canonical Stream locks. New/live pairs always have a non-null discoverable position. Each actual transaction reserves at most one cursor per affected Stream, reused by every preparation publication. Reservation state is checked against `pg_current_xact_id()` so a reused caller-owned connection cannot carry it across COMMIT. Existing live tracking retains its cursor; invalidation reaches existing holders without enrolling others.

Input targets are read under the same fence after preparation. A tracked initiating-Stream target records its position and a call-owned null-cursor fallback. Other targets stay private. Returning an identity or snapshot never tracks it. Business rows, final positions, target evidence, result and progress commit together; subscriber wakes follow commit.

## 10. Quality Requirements

[protocol-v05-batch.test.mjs](../../../../integration/persistence/server/protocol-v05-batch.test.mjs) inspects actual PostgreSQL rows after partial execution, process exit, refusal, infrastructure failure, duplicate replay, publication races and overflow. The explicit server runner includes it alongside retained protocol-4 gates. Large delivery-plan capacity and end-to-end client settlement remain later adoption gates.
