# Unsent work: refused and failed acts, and their resolutions in a transaction (#186, #205, #204)

Status: decided 2026-09-28 by the maintainer. Not yet implemented.

## Problem

An app's unsent-work screen needs, account-wide:

- the acts the server refused, with what was sent, so the author's words can be recovered;
- the acts stuck on a failed prerequisite;
- a change notification for both;
- ways to dismiss, retry or drop an act.

The data exists: `axton_rejection.detail.mutation` retains the whole act. No API enumerates it, however, and nothing announces changes. Most Days reads `axton_mutation`, `axton_mutation_prerequisite`, `axton_mutation_operation` and `axton_rejection` through `readSql` and polls every second (#186).

`drop` and `dismissRejection` also cannot run inside `client.transaction`. A repair therefore commits its replacement act first, planned over the failed original's optimistic state, and only then drops the original (#205).

A new act that requires an already-failed prerequisite waits silently (#204).

The replaced framework, LocalSync, had exactly this shape:

- `mutations.watchRejections()`, `getRejection` and `acknowledgeRejection`;
- `prerequisites.watchFailures()`, `retry` and `discard`;
- every resolution also available on the transaction (`TransactionMutationsInbox`, `TransactionPrerequisites`).

## Decision

1. **Refused acts.** `client.rejections.watch()` emits the list of retained refusals. Each carries:
   - its id (the ordinal);
   - the Mutation name and version;
   - the refusal code;
   - **the full act as submitted**: its call arguments and its operations with their values.

   `client.rejections.get(id)` reads one refusal and `client.rejections.dismiss(id)` removes it. The retained act is a public, stable part of the API, because recovering an author's words depends on it.
2. **Failed acts.** `client.failures.watch()` emits the queued acts that are blocked on a terminally failed prerequisite task. Each carries:
   - the ordinal, the Mutation name and the full act as submitted;
   - the failed tasks: key, prerequisite name, arguments and error.

   It also offers two resolutions:
   - `client.failures.retry(taskKeys)` makes the tasks pending again, and the registered handlers run them (#185);
   - `client.failures.drop(ordinal)` removes the act and its optimism. Dropping is the author's own decision, so it records no `dropped` refusal that must then be dismissed. Acts that depended on the dropped act's records are refused in turn, and appear in `rejections`.
3. **Pending count.** `client.outbound.watchPending()` emits the number of queued, unsettled acts. It changes on each enqueue, settlement, refusal and drop.
4. **No polling.** All three streams are driven by the commits that change them, including the resolutions above, and emit only distinct values.
5. **Every resolution works on the transaction** (#205): `tx.rejections.dismiss`, `tx.failures.retry` and `tx.failures.drop`, inside `client.transaction`. A resolution made in a transaction:
   - takes effect before later reads and later Mutations in the same transaction, so a dropped act's optimism is gone and a replacement is planned and sequenced without it;
   - commits or rolls back together with the rest of the transaction.
6. **A new requirement on a failed task inherits the failure** (#204). The new act appears in `failures` at once, listed with that task. The task is not reset automatically: retrying stays the author's explicit act, and one retry covers every act waiting on the task. Document this.

## Tests

- **Rejections.** A refused act appears in `rejections` with its full arguments and operations, and `dismiss` removes it.
- **Failures.**
  - A terminal handler failure puts the act in `failures`. `retry` runs the handler again, and success removes the act from the stream.
  - `drop` removes the act without leaving a refusal. A dependent act is refused.
- **The #204 reproducer.** Enqueue act 1 requiring blob X, fail X terminally, then enqueue act 2 requiring X. Act 2 is listed in `failures` immediately, and one `retry(X)` unblocks both acts.
- **In-transaction resolutions.**
  - Inside one transaction, drop a failed act, then enqueue a replacement that edits the same record. The replacement is planned without the dropped act's optimism, is not sequenced after it, and is accepted.
  - A throw after the drop rolls both back, and the original is intact.
- **Stream behaviour.** Every stream emits on each commit that changes its answer and never on a timer.
