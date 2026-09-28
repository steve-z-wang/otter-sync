# Serializable backend transactions (#202)

Status: decided 2026-09-28 by the maintainer. Not yet implemented.

## Problem

Every Postgres driver opens transactions as REPEATABLE READ: `packages/postgres/src/prisma.mts`, `pg.mts` and `drizzle.mts`. A handler that locks a parent row with `SELECT … FOR UPDATE` and then re-checks related rows reads the snapshot taken before its lock wait. It therefore acts on rows another transaction has already deleted or created.

Most Days reproduces three such races:

- an Archive written for an Author who had already left;
- a Co-author request created after the requester was admitted;
- a delete's cleanup missing a Star created at the same moment.

READ COMMITTED is not an option. AXTON's own bookkeeping relies on one coherent snapshot per transaction: the `ENSURE_STAMP`, `READ_STAMPS` and `LOCK_RECORD` comments in `sql.mts`, and the `Database.transaction` contract in `packages/server/index.mts`.

## Decision

1. **Every backend transaction is SERIALIZABLE.** This holds for all three drivers. Nothing lets an application choose another level, because each extra option is another way to be wrong.
   - SERIALIZABLE keeps REPEATABLE READ's snapshot, so every AXTON invariant that holds today still holds.
   - Postgres additionally aborts any transaction whose outcome would differ from some serial order. The abort surfaces as `40001`, which the runner already retries (`driver.mts`, `packages/server/retryable.mts`).
2. **Handlers need no locks for correctness.** "Lock the parent, then re-check" stays harmless but is no longer required. The guarantees document says so.
3. **Handlers must be safe to run more than once.** A transaction body can run several times. Nothing inside it may have an external effect such as email, push or an HTTP call. Those happen after commit; `backend.publish`'s wake, called after the host's commit (#180), is the model. Put this in `docs/engineering/guarantees.md` as a stated guarantee with its precondition.
4. **Retries stay configurable** through the driver's existing `retries` option (default 3).
   - When retries run out, the Mutation is reported as a retryable server failure, never a refusal.
   - The durable call stays queued and the client submits it again later.
   - Verify that this is today's behaviour, and pin it with a test.
5. **Update the documentation** for the new level:
   - the `Database.transaction` contract ("must provide serializable isolation");
   - the `sql.mts` comments that say "Repeatable Read";
   - the storage docs.

## Tests

- **A lock-then-recheck race.** Transaction A deletes a child row. Transaction B reads that child, then writes something that depends on it. Under the old level B commits a stale decision; now exactly one of them retries and the final state is the serial one.
  - Cover the delete case and the insert case (a phantom row).
  - Run it on each driver: Prisma, pg and drizzle.
- **Exhausted retries.** When retries run out, the call ends as a retryable failure and the Mutation stays queued.
- **Existing suites.** Stamp, membership and Load pages stay green under the new level (`bash scripts/test.sh`).
