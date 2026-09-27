# Transactional Mutation enqueue and local companions

Status: agreed product contract, recorded for design review; not implemented. `local` is the selected callback option spelling for this proposal. This document extends the local-only transaction boundary established by [#145's design](2026-09-23-145-local-transactions-design.md); it does not reopen remote execution inside transactions.

Implementation sequence: [plan](../plans/2026-09-27-transaction-mutations-plan.md).

## 1. Introduction and Goals

An application must be able to read a local Composition, enqueue one Mutation that creates an Entry, media and Journal placement, and remove the local Composition in one durable local commit. The Composition deletion is never sent to the backend. Backend acceptance retains that deletion; rejection removes its effect so editing can resume. Later independent edits must survive settlement.

The same contract supports local shelf ordering that follows a placement Mutation, and reading a media list, computing a difference and enqueueing the resulting Mutation without an incoming sync write interleaving with those reads and writes.

Two boundaries are distinct: a local transaction commits all its work together; each Mutation subsequently has its own backend outcome. A local transaction is not a distributed transaction.

## 3. Context and Scope

### Public API

The following TypeScript is the proposed shape, not a currently available API:

```ts
const call = await client.transaction(async tx => {
  const composition = await tx.models.composition.get({ id });
  if (!composition) throw new Error("Composition not found");

  return await tx.mutations.publishEntry(buildEntryInput(composition), {
    local: async local => {
      await local.models.composition.delete({ id });
    },
  });
});

const outcome = await call.wait();
```

`tx.mutations.<name>(args, options?)` exposes generated, typed, durable Mutation submissions only. It returns the existing `Call<Output>` observation handle after the Mutation's inferred optimism and optional local callback have run successfully in the open transaction. Existing applicable store options remain available; `local` is a client callback and is never part of business arguments or the wire request. Omitting it adds no companion writes. Dart exposes the equivalent typed callback and Call semantics.

`tx.mutations.call` is absent. Remote Query, Fetch, Load and Bootstrap operations, and Query enqueue, are not added to the transaction surface by this work. Capturing an outer client does not bypass the transaction's existing remote-call guard.

`client.transaction` retains its existing return convention: its callback may return any value, including nothing, a local row, one Call or several Calls. The transaction itself has no Call, server status or backend result. Its promise/future succeeds only after local commit.

### Explicit companion ownership

Normal `tx.models` writes are independent local writes. Being in a transaction containing a Mutation does not make them companions. A companion belongs only to the Mutation whose `local` callback issued it.

The `local` callback provides typed local Model reads and create/update/delete methods. Reads see preceding transaction writes and the owning Mutation's inferred optimism. Writes execute immediately in that same transaction and are recorded as local companions of that Mutation. The callback exposes no Mutation submission, remote operation, watch, Channel modification or nested transaction/savepoint API. It is not a second transaction.

Captured outer transaction writes or enqueue commands during this callback must be refused, not silently classified as independent or attached to another Mutation. The active callback capability determines admission in Rust; SDK lifetime/async-context checks complement it. Its handle expires on callback completion. Existing transaction failure and unawaited-operation rules apply. A callback failure fails the submission and poisons the enclosing transaction under those rules; it must not leave a partially usable Call.

The outer application transaction keeps its existing local Model and Channel operations. Incoming-authority `onStore` callbacks remain local-only and cannot enqueue Mutations, even though both callback kinds use the runtime's writer. Their generated interfaces and runtime capabilities must stay distinct.

### Multiple Mutations

```ts
const calls = await client.transaction(async tx => {
  const first = await tx.mutations.firstAction(firstArgs, {
    local: async local => { /* first's companion writes */ },
  });
  const second = await tx.mutations.secondAction(secondArgs, {
    local: async local => { /* second's companion writes */ },
  });
  return { first, second };
});

const firstOutcome = await calls.first.wait();
const secondOutcome = await calls.second.wait();
```

| Event | Required outcome |
| --- | --- |
| Local transaction fails | Neither Mutation becomes sendable; all its local writes and queue changes roll back. |
| Local transaction commits | Both Mutations and their companions are durable. |
| First accepted, second rejected | First's companion effect is retained; second's effect is removed. |
| Caller ignores either Call | Durable execution and companion settlement continue. |

Sharing a transaction adds no backend atomic group or implicit execution dependency. Existing dependencies inferred from Model operations still apply. A business operation requiring atomic backend creation of Entry, media and placement should be one Mutation handled in one backend transaction. This API does not provide rollback of already accepted backend work when another Mutation fails.

## 4. Solution Strategy

Extend the existing application transaction session with a typed Mutation enqueue command and a narrowly scoped companion callback. Keep scheduling, ownership, persistence, validation and settlement in Rust. SDKs generate typed facades, run application callbacks and decode observation handles.

Persist normalized companion operations and recovery metadata alongside their owning queued call in the same SQLite transaction. Retain concrete operations, never executable callback code. A restart, retry, optimistic replay or server receipt must not re-run the callback. Companions never enter backend args, wire operations, handler inputs, declared results or Channel publication.

The implementation must preserve canonical validation of the schema-derived Mutation input operations while validating companions separately as local operations. Relaxing all canonical-intent checks or forwarding companions as backend changes is not acceptable.

## 5. Building Block View

Current code inspected at base commit `39f629c`:

- [SDK transaction](../../../packages/client-js/transaction.mts) exposes local reads, direct writes and Channel intent, with callback lifetime and failure checks; it exposes no typed Mutation API.
- [Rust transaction interface](../../../crates/client/src/lib.rs) retains low-level `ClientTransaction::enqueue` and `Mutation.companion`.
- [Runtime protocol](../../../crates/client/src/runtime/protocol.rs) retains a low-level transaction `Enqueue`; this is not the proposed generated interface.
- [Mutation engine](../../../crates/client/src/mutate.rs) handles low-level companion operations, but canonical call-ID-based Mutation validation currently rejects nonempty companions.
- [Typed client architecture](../../engineering/architecture/sdks/typed-api/client.md) currently forbids all Mutation/Query routes in application transactions.

These are reusable foundations, not evidence that the new contract already works. Compiler generation, runtime admission, durable companion representation, Call lifecycle and settlement must be integrated together. The implementation must update owning architecture and frontend guides and replace negative API fixtures that encode the superseded application-transaction restriction; retain the corresponding onStore restrictions.

## 6. Runtime View

### Local commit and sending

The runtime holds the existing application writer transaction while local reads, inferred optimism, callback writes and queue insertion execute. Incoming authority and other writers cannot commit into that unit. Network messages may arrive and wait; no database transaction is held while awaiting a backend result.

Until commit, a submitted Mutation is provisional and cannot be sent. Watchers see only committed state. On commit, the runtime makes the work eligible for its existing uplink scheduling and returns the transaction's callback value. On rollback, no queue row, optimism, companion or recovery metadata remains.

Existing savepoints may confine failures where already supported; this feature adds no new general nested transaction mechanism. Rolling back an existing savepoint also invalidates Calls created in that scope while leaving earlier surviving scopes intact.

### Call observation

Each Mutation returns its own existing Call type. It has no additional public transaction status enum. Before commit it is not evidence of durable submission; existing pending status must not be documented as proof of persistence for a provisional handle.

`wait()` invoked before the owning transaction commits rejects immediately with `transaction_uncommitted`; this observation misuse alone does not cancel the Mutation. It must never wait on a backend result while holding the local transaction. After successful commit, the same handle observes normal execution. A handle leaked from a rolled-back scope or transaction fails with `transaction_rolled_back` and never hangs or becomes sendable. These are local observation/lifecycle errors, not backend rejections or durable server outcomes. An uncaught wait error in the application callback follows normal callback-failure rules.

Handle ownership is transient; queue and companion ownership are durable. Dropping a handle never cancels work or disables recovery.

### Settlement and later edits

Acceptance and rejection settle the owning Mutation and its companions atomically through the existing local writer. A retryable transport failure does not undo companions. Backend acceptance retains companion effects; terminal backend rejection removes only that Mutation's optimistic and companion effects. Completion is observable only after that settlement commits.

Settlement reconstructs state from the retained base and applicable later operations; it must not overwrite a row with a saved whole-row snapshot. Multiple companion writes to the same row preserve their local operation order regardless of arrival order of server outcomes. A later independent direct write survives both acceptance and rejection of an earlier companion when the record has an independently existing base, consistent with [L4](../../engineering/guarantees.md).

For example, deleting an existing local Composition as a companion and subsequently recreating that identity with new content must not restore the old content on rejection or delete the new content on acceptance. A later independent deletion must likewise not be undone by rejection. Existing rules for edits to a record whose very creation is still pending remain in force; an edit cannot preserve a record whose only creation was rejected. Existing replay-conflict reporting remains in force for operations that become inapplicable.

These local guarantees do not bypass server authority for a record that also receives authoritative data. Preserve existing receipt/readback and stamp precedence: a companion is not a way to force local content over a server-owned value. Composition recovery in the motivating scenario concerns local-only data.

Reopen resumes the same durable call and settlement without running callbacks again. Process interruption before commit leaves neither durable enqueue nor deletion; after commit, both survive. Crash-durability claims require interruption tests beyond clean close/reopen.

## 9. Architecture Decisions

- Keep ordinary transactions and Calls separate. A transaction returns its callback's value; each Mutation has its own Call.
- Explicit per-Mutation companion callbacks avoid ambiguous ownership when one transaction includes several Mutations or independent writes.
- Expose enqueue only inside application transactions. Direct remote execution and onStore enqueue remain forbidden.
- Keep callback execution in the current transaction rather than introducing a general nested transaction abstraction.
- Do not infer backend atomicity or an execution order from a shared local transaction.
- Do not change business schema syntax or backend operation versions merely to add a client-local callback. Any storage/protocol representation changes must follow existing compatibility rules.
- Standalone Mutation companion callbacks are outside this proposal; applications use `client.transaction` for this combined workflow.

## 10. Quality Requirements

Implementation acceptance must cover:

1. Read Composition, enqueue Entry/media/placement, delete Composition; injected failure at each local step leaves all or none, including queue and recovery metadata.
2. Offline commit and reopen retain the queued intent and deletion; acceptance retains deletion and rejection restores the original Composition when no later independent edit exists.
3. Later independent edits, recreate-after-delete and independent deletion survive earlier Mutation settlement. Exercise both acceptance and rejection, including overlapping companions and different outcome orders.
4. Multiple Mutations have one local commit and separate Calls/outcomes; mixed acceptance/rejection does not roll back unrelated accepted work or independent direct writes.
5. Local media read/diff/enqueue holds a stable writer boundary while incoming authority waits. Watchers never observe partial transaction state.
6. Companions never appear in wire requests or reach backend handlers; canonical business input validation remains enforced.
7. Before-commit wait fails immediately; escaped Calls from rollback terminate; ignored Calls and restart retain execution and companion settlement. Existing savepoint rollback fences its provisional Calls.
8. Generated TS/Dart APIs expose only queued Mutations on application transactions. Direct calls, remote reads, callback nested enqueue and onStore enqueue fail at both typed and runtime boundaries. Preserve applicable React Native adapter checks.
9. Callback throw, caught failed command, unawaited work and expired handles obey the existing transaction failure contract.
10. Process-interruption tests cover local commit and receipt-settlement boundaries before claiming crash recovery.

Verification for this document: existing source and architecture were inspected; no implementation or runtime tests were executed. This spec does not certify the current implementation against these requirements.
