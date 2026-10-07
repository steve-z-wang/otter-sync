# Frontend interface

Protocol-5 policy and failure boundaries are owned by [protocol 5](../protocol/0.5.md). Rust owns synchronization; language bindings execute networking, timers and interface callbacks. Descriptions of protocol-4, epoch/Load jobs, once caches or legacy queue tables below are historical component context, not current public APIs.

## 1. Introduction and Goals

The frontend interface connects application tasks to the client Engine, storage and its owned transaction scopes. The public [typed client](../sdks/typed-api/client.md) translates those tasks; [protocol-4 client](protocol4.md) owns authority, admission, coverage and settlement.

## 3. Context and Scope

Bound opening supplies a schema and stable backend/viewer/Stream/contract identity for one exclusively owned physical SQLite file. Validation and ownership precede schema coordination. A fresh Store gets an incarnation; ordinary reconnect/reopen preserves it. Different physical files have independent queues, requests, evidence, progress and observers, even when they follow the same Stream.

Local Model reads/writes, read-only SQL, declared relations/cascades, observations, prerequisites and recovery remain available. Named typed Mutations queue durable work. Query/Fetch execute direct reads with boolean Store mode. `bootstrap` awaits finite initialization. Public anonymous mutations, incoming hooks, Load jobs and multistream registration are absent.

## 5. Building Block View

[Client](../../../../crates/client/src/lib.rs) and [Engine](../../../../crates/client/src/engine.rs) retain local operation/queue responsibilities. [protocol04.rs](../../../../crates/client/src/protocol04.rs) owns bound opening, retained descriptors and reset; [progress04.rs](../../../../crates/client/src/progress04.rs) owns commit-unit/manifest coverage; [settlement04.rs](../../../../crates/client/src/settlement04.rs) owns retained receipt reconciliation. [Runtime](runtime.md) executes task/effect scheduling; [bindings](../sdks/bindings.md) own the per-client actor and physical ownership locks.

## 6. Runtime View

A transaction's commands carry its capability and see its earlier writes. Typed Mutation callback-local commands execute before its input optimism and are retained as that Call's companions. Invalid input, expired/foreign capabilities, unawaited operations or callback failure prevent commit. A Call returned inside the transaction remains provisional; waiting before commit fails promptly. Several Calls share one local commit but have independent backend fates. Retry never reruns the application callback.

Only Stream materialization establishes authoritative record positions. Current content/deletion protection, retained per-identity evidence and the completely committed Stream prefix are separate persisted facts. Device-only writes clear live-content protection without inventing a position or discarding true deletion history. Ordinary Query/Fetch cache writes carry null positions and check current protection at commit; their returned snapshots can legitimately differ from the Store.

Delivery plans persist immutable commit units and exact progress. Each unit atomically commits rows/evidence, surviving local layers and proven prefix. Loader/constraint/COMMIT failures retain the unresolved unit; smaller requests may recover independent prefixes, never split a required atomic group. Bootstrap coverage and captured tail are independent of that prefix; its await completes only when both coverage and handoff commit.

Accepted outcomes persist before fallible local settlement. Input targets use real Stream evidence or permitted call-owned private canonical data at the original operation order. Later direct edits and newer Stream content cannot be undone by late receipts. Queue retirement, optimism removal and terminal completion commit together; accepted local failure is not backend rejection.

Priority close ends pending tasks and releases runtime ownership after rollback. Explicit reset changes incarnation, reports abandoned Calls and fences old effects; it refuses pending work unless explicitly discarded. Compatible schema reconciliation retains full original descriptors and rematerializes authoritative identities without clearing local work. A 0.3 file is not silently adopted as a bound 0.4 Store.

## 10. Quality Requirements

[Current guarantees](../../guarantees.md) own observable behavior. [Native protocol/ownership tests](../../../../crates/sqlite/tests), [language binding suites](../../../../integration/bindings) and [actual protocol transport](../../testing/0.4.md) establish different boundaries. On 2026-10-05, client/SQLite tests passed 684 plus eleven common-binding tests; this includes retained internal compatibility tests and is not a claim that every case uses the bound path. [Protocol verification](../../testing/0.4.md) separates symbolic, real native, PostgreSQL and SDK evidence.

## 11. Risks and Technical Debt

Retained unbound/stamp/Load entry points support internal compatibility fixtures only; they are not extra public bound-client usage modes. Legacy runtime/engine leaf pages should be read under that boundary. No custom hook is required for bound incoming authority. Applications must explicitly publish backend child deletions and Loader visibility dependencies; device cascades alone cannot supply that evidence.
