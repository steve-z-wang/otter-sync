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

The descriptor carries `prerequisites: [{name, fields}]` and `requirements: [{model, field, name, arguments}]`. The client derives *tasks* from them when a mutation is enqueued. The application registers its handlers once, with the client's open option `prerequisites` keyed by prerequisite name, and the runtime runs them; the SDK also exposes `pendingTasks()` and `setReadiness(key, state)`. The server never sees prerequisites.

## 5. Building Block View

A declaration has a unique name and typed fields (`String`, `UUID`, `DateTime`, `Int`, `Float`, `Bool`). A requirement invokes one declaration, supplies every field, and today every argument must be `self`, meaning the value of the annotated field.

A **task key** is what ties them to the queue. When a wire operation carries a non-null value for an annotated field, the client forms the key `{"name": Uploaded, "arguments": {"key": <value>}}` in canonical JSON and stores it against the mutation. Two mutations that need the same upload share one key; marking it ready releases both. A mutation is not frozen while any of its keys is pending or failed ([Dependencies](../client/engine/push/dependencies.md)).

The runner is Rust's ([#185](https://github.com/zanminwang/axton/issues/185)). The open request names the prerequisites the application has handlers for; each must be declared by the requested schema, or the open fails with `invalid prerequisite handler <name>`. The client [runtime](../client/runtime.md#6-runtime-view) runs a handler whenever a task becomes pending - a commit that queued it, an open that finds it after a restart, a reset to `pending` - with no call from the host: it issues one `prerequisite` effect for the first pending task, in key order, whose `name` has a handler and that is not backing off, and no transaction is held while the handler runs. One handler runs at a time per client. A task whose prerequisite has no handler, or an opaque key with no `name`, is never run; the application settles it through `setReadiness`.

What the handler comes to decides the task:

| Handler | Task |
| --- | --- |
| Resolves | Resolved; its mutations may be sent. |
| Throws `PrerequisiteRetry` (the effect answers `error.retry: true`) | Stays pending and runs again after the backoff: 1 s after the first consecutive transient failure, doubling per failure, at most 30 s, with ±20 % jitter - the [Load worker](../client/connection/controller/load-worker.md)'s policy. The count is in memory; a reopen runs the task at once and starts again. |
| Throws anything else | Failed with the error's text, which `pending_tasks` and `record_status` report as `error`; never retried until the application resets it to `pending`, so a permanent failure does not spin. |

**A failed task keeps its failure while any call waits on it ([#204](https://github.com/zanminwang/axton/issues/204)).** A call committed later that requires a task which already failed inherits the failure: its row takes the task's error when it is written, so the call is listed in `client.failures` at once, with that task, instead of waiting silently. The task is not reset: retrying stays the application's explicit act, and one `failures.retry([key])` (or `setReadiness(key, "pending")`) makes it pending for every call waiting on it. Once no call waits on a key - every one was sent, dropped or refused - the key has no rows left, and a later call that requires it starts a fresh pending task.

`setReadiness` of a task forgets its backoff, so a reset to `pending` runs it at once. Close cancels the handler's effect: TypeScript aborts the handler's `AbortSignal`, Dart completes its `cancelled` future, and a later settlement is dropped. Nothing waits on a handler, so no call hangs behind a slow upload and nothing is reported after close. A rebuild cancels the run, forgets every backoff and scans the new replica.

Code: compiler checks in [compiler/validate.rs](../../../../crates/compiler/src/validate.rs); key derivation in [client/policies.rs](../../../../crates/client/src/policies.rs); rows in [client/queue.rs](../../../../crates/client/src/queue.rs); outcomes in [client/lib.rs](../../../../crates/client/src/lib.rs) `outcome`; the scheduler in [client/runtime/prerequisites.rs](../../../../crates/client/src/runtime/prerequisites.rs); registration at open in [bindings/common/src/actor.rs](../../../../bindings/common/src/actor.rs); the handler effects in [client-js/connection.mts](../../../../packages/client-js/connection.mts) (`prerequisites`, `PrerequisiteRetry`) and [dart/connection.dart](../../../../packages/dart/lib/src/connection.dart) (`prerequisiteHandler`, `PrerequisiteRetry`).

## 9. Architecture Decisions

**Handlers at open, no manual runner ([#185](https://github.com/zanminwang/axton/issues/185)).** `runPrerequisites(handlers)` made the host decide when tasks ran: every Command that queued a task had to kick the runner, and a transient failure needed the host's own timer and `setReadiness`. The handlers are now fixed at open and the runtime runs them. A manual trigger kept beside it would have meant two sources of handlers and rules for joining a run; the one thing it could add, "retry now", is `setReadiness(key, "pending")`, which also forgets the backoff. `runPrerequisites` was removed.

**A new requirement inherits a failed task; it does not reset it (decided 2026-09-28, [#204](https://github.com/zanminwang/axton/issues/204)).** Resetting the task for the new call would retry an upload the application may have given up on, and run it again for every call already waiting on it, behind the author's back. Inheriting keeps one state per task and makes the new call visible in the unsent-work list at once. Cost: a call that would have succeeded on a retry waits for the author to retry.

**The host's timer is the clock of record for a retry.** The runtime waits out a backoff with a `timer` effect; when it fires, every task due by then may run, even if the runtime's own clock is a little behind.

## 10. Quality Requirements

- **A mutation with an unready prerequisite is not frozen, stays optimistic and survives restart, while independent mutations may be sent ahead of it** (guarantee P3). Evidence: [sqlite/tests/push.rs](../../../../crates/sqlite/tests/push.rs) `schema_requirements_create_durable_tasks_and_gate_only_dependent_mutation`, `failed_prerequisite_stays_optimistic_independent_work_can_overtake`.
- **A call that requires a task that already failed inherits the failure in its own row, is listed with the task at once and is not sent; one retry makes the task pending for every call on it; a key no call waits on any more starts pending.** Evidence: [sqlite/tests/unsent.rs](../../../../crates/sqlite/tests/unsent.rs) `a_new_requirement_on_a_failed_task_inherits_its_failure_and_one_retry_covers_both`; through the runtime, [runtime_unsent.rs](../../../../crates/sqlite/tests/runtime_unsent.rs) `a_terminal_handler_failure_lists_the_act_and_a_retry_runs_the_handler_again`; through the SDKs, `a new requirement on a failed task is listed at once and one retry unblocks both (#204)` in [unsent-harness.mjs](../../../../integration/bindings/client-js/unsent-harness.mjs) and [unsent_test.dart](../../../../packages/dart/test/unsent_test.dart).
- **Readiness arriving after the mutation was dropped leaves nothing behind.** Evidence: `late_task_completion_does_not_resurrect_unused_readiness`.
- **A reported failure keeps its reason, and a reset makes the task pending again.** Evidence: `outcomes_keep_their_reason_and_a_reset_makes_the_task_pending_again` in the same file.
- **The runtime runs a handler when a commit queues its task, and after a reopen, with no host call; a transient failure backs off 1 s, 2 s, 4 s and a reset runs it at once; a task backing off holds back no other; a terminal failure stays failed and visible; close cancels the run and ignores a late answer; a rebuild cancels it and scans the new replica; only declared names register, and a prerequisite without a handler is left pending; every outcome wakes the push lane.** Evidence: [sqlite/tests/runtime_prerequisites.rs](../../../../crates/sqlite/tests/runtime_prerequisites.rs).
- **Through the SDKs, the same: a commit runs the handler, a task pending at restart runs after reopen, a `PrerequisiteRetry` retries with growing delays, a terminal failure stays failed until a reset, close during a run cancels it without hanging or reporting, and an undeclared name fails open.** Evidence: the shared [prerequisite-harness.mjs](../../../../integration/bindings/client-js/prerequisite-harness.mjs) run by the Node [prerequisite.test.mjs](../../../../integration/bindings/client-js/prerequisite.test.mjs) and the React Native [prerequisite.test.mjs](../../../../integration/bindings/client-react-native/prerequisite.test.mjs); [prerequisite_test.dart](../../../../packages/dart/test/prerequisite_test.dart). The SDK tests observe the backoff by recording and shortening the runtime's `timer` effects (a wrapped carrier in TypeScript, a zone timer override in Dart).

Executed 2026-09-28 for [#185](https://github.com/zanminwang/axton/issues/185): `cargo test --workspace --locked`, `node --test integration/bindings/client-js/*.test.mjs`, `node --test integration/bindings/client-react-native/*.test.mjs`, `dart test` in `packages/dart`.

## 11. Risks and Technical Debt

**Accepted limitation.** `self` is the only argument expression. The compiler message says "currently"; no issue tracks an extension. Stated for authors in the [schema reference](../../../../website/docs/schema/define.md#relations-prerequisites-and-ordering).

**Accepted limitation.** The Rust API accepts opaque prerequisite keys; a non-JSON key has no `name`, so no handler runs it. Rust callers settle such keys through `set_readiness`.

**Accepted limitation: one handler at a time, backoff in memory.** A slow handler holds back the other tasks of its client until it settles; a transient failure's attempt count does not survive a reopen, so a task that keeps failing transiently runs once at each open before backing off again. No issue tracks parallel handlers or a durable count.
