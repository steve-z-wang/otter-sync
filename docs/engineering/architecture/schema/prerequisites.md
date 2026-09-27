# Prerequisites

## 1. Introduction and Goals

Some mutations must not reach the server until the application has finished other work, such as uploading a file the record refers to. A prerequisite expresses that wait in the schema, so the client holds the mutation back while the optimistic write stays visible, and nothing about uploads leaks into the sync engine.

## 3. Context and Scope

```
prerequisite Uploaded(key String)
model Attachment {
  id  UUID
  key String @requires(Uploaded(key: self))
  @@id(id)
}
```

The descriptor carries `prerequisites: [{name, fields}]` and `requirements: [{model, field, name, arguments}]`. The client derives *tasks* from them when a mutation is enqueued; the SDK exposes `pendingTasks()`, `setReadiness(key, state)` and `runPrerequisites(handlers)`. The server never sees prerequisites.

## 5. Building Block View

A declaration has a unique name and typed fields (`String`, `UUID`, `DateTime`, `Int`, `Float`, `Bool`). A requirement invokes one declaration, supplies every field, and today every argument must be `self`, meaning the value of the annotated field.

A **task key** is what ties them to the queue. When a wire operation carries a non-null value for an annotated field, the client forms the key `{"name": Uploaded, "arguments": {"key": <value>}}` in canonical JSON and stores it against the mutation. Two mutations that need the same upload share one key; marking it ready releases both. A mutation is not frozen while any of its keys is pending or failed ([Dependencies](../client/engine/push/dependencies.md)).

The runner's decisions are Rust's. `next_task(handlers)` returns the next pending task whose `name` the host offers a handler for; a pending task no handler covers is failed with the reason `missing prerequisite handler` and the walk goes on. `outcome(key, error)` resolves the task when `error` is absent and otherwise fails it and keeps the reason, which `pending_tasks` and `record_status` report as `error`. The client [runtime](../client/runtime.md) runs the loop for a `runPrerequisites {handlers}` task: it asks `next_task`, issues one `prerequisite` effect for the task, records the result with `outcome` and asks again; no transaction is held while a handler runs. The SDK only calls `handlers[name](arguments)` when the effect arrives and answers with its result. A failed task is retried only after the application resets it to pending, so a permanent failure does not spin.

Code: compiler checks in [compiler/validate.rs](../../../../crates/compiler/src/validate.rs); key derivation in [client/policies.rs](../../../../crates/client/src/policies.rs); rows in [client/queue.rs](../../../../crates/client/src/queue.rs); the runner's decisions in [client/lib.rs](../../../../crates/client/src/lib.rs) `next_task` / `outcome`; the loop in [client/runtime/prerequisites.rs](../../../../crates/client/src/runtime/prerequisites.rs); the handler effects in [client-js/connection.mts](../../../../packages/client-js/connection.mts) (`prerequisites`) and [dart/connection.dart](../../../../packages/dart/lib/src/connection.dart).

## 10. Quality Requirements

- **A mutation with an unready prerequisite is not frozen, stays optimistic and survives restart, while independent mutations may be sent ahead of it** (guarantee P3). Evidence: [sqlite/tests/push.rs](../../../../crates/sqlite/tests/push.rs) `schema_requirements_create_durable_tasks_and_gate_only_dependent_mutation`, `failed_prerequisite_stays_optimistic_independent_work_can_overtake`.
- **Readiness arriving after the mutation was dropped leaves nothing behind.** Evidence: `late_task_completion_does_not_resurrect_unused_readiness`.
- **Rust picks the next task, fails an unhandled one with a reason, keeps a reported failure's reason and does not retry a failed task.** Evidence: `next_task_walks_pending_tasks_fails_unhandled_ones_and_records_reasons` in the same file.
- **The runtime runs each handler as an effect and records its outcome, and every outcome wakes the push lane.** Evidence: [sqlite/tests/runtime_lanes.rs](../../../../crates/sqlite/tests/runtime_lanes.rs) `prerequisites_run_as_effects_and_every_outcome_wakes_the_push_lane`.
- **Through the SDKs, a callback failure is recorded with its reason, an explicit reset unlocks the push, and a run without the handler fails the task instead of stopping.** Evidence: [prerequisite.test.mjs](../../../../integration/bindings/client-js/prerequisite.test.mjs); [prerequisite_test.dart](../../../../packages/dart/test/prerequisite_test.dart).

Verified 2026-09-15: `cargo test -p axton-sqlite --test push --locked`, `node --test integration/bindings/client-js/prerequisite.test.mjs`, `dart test test/prerequisite_test.dart` in `packages/dart`. Executed 2026-09-26 for the runtime test: `cargo test --workspace --locked`, 687 passed (owner, [#165](https://github.com/zanminwang/axton/pull/165)).

## 11. Risks and Technical Debt

**Accepted limitation.** `self` is the only argument expression. The compiler message says "currently"; no issue tracks an extension. Stated for authors in the [schema reference](../../../../website/docs/schema/define.md#relations-prerequisites-and-ordering).

**Accepted limitation.** The Rust API accepts opaque prerequisite keys; a non-JSON key has no `name`, so a run of `runPrerequisites` fails it with `missing prerequisite handler` rather than skipping it. Affects only Rust callers that also use the SDK runner; they settle such keys through `set_readiness`.
