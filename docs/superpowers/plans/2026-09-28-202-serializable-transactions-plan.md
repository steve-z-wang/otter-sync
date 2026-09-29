# Serializable Backend Transactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close [#202](https://github.com/zanminwang/axton/issues/202): every transaction a `@axton/postgres` shim opens runs at SERIALIZABLE, so a lock-then-recheck handler can no longer commit a decision made on rows another transaction already deleted or created.

**Architecture:** The isolation level lives in three shims (`packages/postgres/src/pg.mts`, `prisma.mts`, `drizzle.mts`); the retry loop (`withRetries` with `isRetryableTransactionError`) and the `retries` option already exist and do not change. SERIALIZABLE keeps Repeatable Read's snapshot, so AXTON's own bookkeeping (`ENSURE_STAMP`, `READ_STAMPS`, `LOCK_RECORD`) keeps its invariants; PostgreSQL adds `40001` aborts for non-serializable outcomes, which the runner retries. Exhausted retries already escape the transaction and reach the HTTP listener as `500 {code:"server"}` (push, direct call) or a `retryable` item (Load page); the client keeps the frozen batch and resends it with backoff. That existing behaviour is pinned, not changed.

**Design:** [2026-09-28-202-serializable-transactions-design.md](../specs/2026-09-28-202-serializable-transactions-design.md). Implement as written; if the code shows a decision cannot work, comment `[blocked]` on the issue and stop.

**Tech Stack:** TypeScript shims, Node `node:test` suites against disposable PostgreSQL, the action-e2e harness (generated TypeScript client, native SQLite, generated backend over HTTP), documentation.

## Constraints

- No option to choose another isolation level. The shims' `retries` and `timeout` options are unchanged.
- The public tool-facing types `PrismaClientLike` and `DrizzleDatabase` narrow the `isolationLevel` literal they pass; a real `PrismaClient` and Drizzle `NodePgDatabase` still satisfy them. Call it out in the PR body.
- `backend.publish(tx, body)` runs in a transaction the application opens: AXTON cannot set its level. Document what the host must do; do not change the API.
- Race tests coordinate the two transactions with promises, never sleeps.
- Do not run `bash scripts/test.sh` locally (shared machine); CI runs it on macOS and Linux.

## Task 1: Lock-then-recheck races (red)

**Files:** `integration/persistence/server/driver-conformance.test.mjs`.

Two scenarios, each run through every shim's `driver.transaction`, plus one control run through a local Repeatable Read `pg` runner that proves the scenario reproduces the anomaly at the old level. Tables without foreign keys (an FK would turn the old-level race into a serialization failure on its own).

- **Delete case** (an Archive written for an Author who had already left). A "leave" transaction A locks the parent `FOR UPDATE`, deletes the child and deletes the child's archives. B reads the parent (fixing its snapshot) and signals; A commits; B locks the parent `FOR UPDATE`, re-checks the child and, seeing it, inserts an archive for it. Serial outcome: no child and no archive. Assert A ran once, B ran twice, and no archive exists. Control at Repeatable Read: B ran once and the archive for the departed child exists.
- **Insert case, a phantom** (a delete's cleanup missing a Star created at the same moment). A "star" transaction A locks the parent `FOR UPDATE`, re-checks it exists and inserts a child. B reads the parent (fixing its snapshot) and signals; A commits; B locks the parent `FOR UPDATE`, deletes the parent's children and then the parent. Serial outcome: no parent and no child. Assert A once, B twice, nothing left. Control: B ran once and the orphan child exists.

- [ ] Write both scenarios and the control; run `bash integration/persistence/server/run.sh` (or just the conformance file against a scratch cluster) and record the failure at the current level: B runs once and the stale row commits on every shim.

## Task 2: SERIALIZABLE in every shim (green)

**Files:** `packages/postgres/src/pg.mts`, `prisma.mts`, `drizzle.mts`, `driver.mts`; `integration/persistence/server/runtime.test.mjs` (the fake Prisma client asserts the options passed).

- [ ] `pg`: `BEGIN ISOLATION LEVEL SERIALIZABLE`. `prisma`: `isolationLevel: "Serializable"`. `drizzle`: `isolationLevel: "serializable"`. Update `PrismaClientLike` / `DrizzleDatabase` literals and the `PostgresDriver.transaction` doc comment.
- [ ] Update the Prisma fake-client assertion to `{isolationLevel:'Serializable',timeout:20000}`.
- [ ] Re-run the conformance suite: both races pass on all three shims; the existing serialization-conflict and membership-guard tests still pass (adjust only a control whose premise was the old level, and say so).

## Task 3: Exhausted retries stay retryable and queued (pin)

**Files:** `integration/action-e2e/backend-fixture.ts`, `integration/action-e2e/action.test.mts`.

- [ ] Add a fixture switch that makes `updateTodo` hit a real serialization failure on each of its next N attempts: inside the handler, read the row, commit a concurrent update to it through the pool, then update it in the transaction.
- [ ] Test: with N = 4 (default `retries: 3` plus the first attempt), a durable `updateTodo` gets `500 {code:"server"}` on its first push, stays pending, saves no call outcome, is resent with the same frozen batch and succeeds; `wait()` resolves without an error and the handler ran five times. Confirm it passes without code changes (today's behaviour) and record that.

## Task 4: The contract and the comments

**Files:** `packages/server/index.mts` (`Database.transaction`), `packages/server/host-contract.mts`, `crates/server/src/host.rs`, `packages/postgres/src/sql.mts`, `integration/bindings/node/transaction-bridge.test.mjs` (probe runner), `integration/persistence/server/runtime.test.mjs` (test names and comments).

- [ ] `Database.transaction`: must provide serializable isolation, roll back rejected callbacks and retry serialization failures.
- [ ] Replace "Repeatable Read" in the `sql.mts`, `host-contract.mts` and `host.rs` comments with the snapshot wording that still holds at SERIALIZABLE.
- [ ] Rename `repeatable-read runner keeps head, scan, and loader coherent across concurrent publication` and `a RepeatableRead conflict on the real database retries …` to their serializable names; the probe fixture's runner uses `Serializable`.
- [ ] Add a check that `backend.publish` settles inside a caller-owned SERIALIZABLE transaction (the existing tests cover Repeatable Read).

## Task 5: Documentation

- [ ] `docs/engineering/guarantees.md`: a new section stating the guarantee (every transaction AXTON opens is serializable; no locks needed; exhausted retries are retryable, never a rejection, and a durable call stays queued) with its precondition (bodies safe to run more than once, no external effects inside the transaction) and evidence.
- [ ] `docs/engineering/architecture/server/persistence.md`: interface comment, SQL properties, shim paragraph, quality requirements (race evidence), §11.
- [ ] `docs/engineering/architecture/server/engine/pull.md`, `publish.md` if affected, `loads.md` if affected.
- [ ] Testing docs: `docs/engineering/testing/integration/persistence.md`, `components/server.md`, `integration/bindings.md`, `review.md`, end-to-end/action docs for the new e2e test.
- [ ] Website: `website/docs/backend/database.md`, `setup.md`, `api.md` (the `backend.publish` isolation row: what the host must do for its own transaction).
- [ ] `git grep -n -i "repeatable"` over `docs/` (excluding dated specs and plans) and `website/`: no stale mention remains. Check relative links and anchors; run `python3 website/scripts/check_examples.py`.

## Task 6: Verify and open the PR

- [ ] `bash integration/persistence/server/run.sh`, `bash integration/persistence/transaction-probe/run.sh`, postgres/server package tests, `bash integration/e2e/run.sh`, `bash integration/action-e2e/run.sh`, `bash integration/load-e2e/run.sh`, prettier on touched `.mts`, `python3 website/scripts/check_examples.py`.
- [ ] PR with `Closes #202`, the exact behaviour change, executed vs inspected evidence, limits and the Most Days surface statement; swap labels, comment `[pr]`, watch CI.
