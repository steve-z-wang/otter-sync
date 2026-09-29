# Serializable Read Footprint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address [#210](https://github.com/zanminwang/axton/issues/210): on near-empty framework tables at SERIALIZABLE, concurrent deliveries on disjoint rows abort each other with `40001`. Shrink AXTON's own read footprint so they stop doing so, without changing the isolation level or the `retries` default.

**Architecture:** Only reads take SIREAD (predicate) locks. On a near-empty table a primary key has one leaf page, so a page lock from one point read covers every other key's insert. Two reads are avoidable, both in `@axton/postgres`, shared by the `pg`, `prisma` and `drizzle` shims:

- `claimCall` in `packages/postgres/src/persistence.mts` re-reads (`CLAIM_CALL_LOCK`) the row its fresh insert has just returned.
- `READ_STAMPS` in `packages/postgres/src/sql.mts` joins the whole model, even for keys its own insert just created.

The Rust engine only reads `fresh`, `request` and `response` from a claim (`crates/server/src/calls.rs`, `loads.rs`). It assumes no lock.

**Design:** the `[design]` and `[cause]` comments on #210. If a decision cannot work, for example if a concurrent duplicate claim no longer waits, comment `[blocked]` with a reproducer and stop.

**Tech Stack:** TypeScript persistence and SQL, Node `node:test` against disposable PostgreSQL, a scratch measurement harness (not committed).

## Constraints

- No isolation change, no `retries` change, no host-contract change, no schema change (`migration.sql` untouched).
- `READ_STAMPS` keeps its semantics: request order; a missing row inserted at 1; an existing row read, never rewritten or locked; a key another transaction inserted or re-stamped after the snapshot fails the insert with a serialization error.
- `claimCall` keeps its semantics: a concurrent duplicate waits for the first transaction to commit; a committed null response is refused ("incomplete stored response").
- Do not run `bash scripts/test.sh` locally; CI runs it.

## Task 1: Red tests

**Files:** `integration/persistence/server/driver-conformance.test.mjs`.

- [ ] Per shim: a fresh claim leaves no SIREAD lock on `axton_call` or `axton_call_pkey` (inspected in `pg_locks` by the transaction's backend pid), and the fresh claim answers `{fresh:true, request, response:null}`. A duplicate of a saved call still reads, so it leaves a SIREAD lock there, and it answers the stored request and response. The existing test `call claims commit with business writes and replay without overwriting the original` already proves that a concurrent duplicate waits on the first transaction.
- [ ] Per shim: `readStamps` of new keys leaves no SIREAD lock on `axton_record` or its primary key.
- [ ] `pg`, in a fresh schema whose framework tables are near-empty and never analyzed: `readStamps` of existing keys leaves no relation-level or heap-page SIREAD lock on `axton_record`, only tuple locks and primary-key pages. It still answers in request order and leaves the existing rows unrewritten (`xmin` unchanged).
- [ ] `pg`: a key inserted, or an existing key re-stamped, by a transaction that committed after this one's snapshot makes `readStamps` fail with `40001` (a semantics guard, green before and after).
- [ ] `pg`, same fresh schema: two disjoint deliveries (`claimCall`, `readStamps` of two new keys, `saveCall`, `COMMIT`) interleaved step by step in several orders both commit on the first attempt. Before the change 68 of the 70 orders abort.
- [ ] Run the conformance file against a scratch cluster and record the failures.

## Task 2: `claimCall` without a read on a fresh claim

**Files:** `packages/postgres/src/persistence.mts`, `packages/postgres/src/sql.mts` (comments).

- [ ] When `CLAIM_CALL_INSERT` returns its row, answer `{fresh:true, request:r.request, response:null}` without `CLAIM_CALL_LOCK`. Otherwise read and lock as today, and refuse a null response.
- [ ] Re-run: the claim tests pass on all three shims, including the concurrent-duplicate wait.

## Task 3: `READ_STAMPS` reads only what its insert did not create

**Files:** `packages/postgres/src/sql.mts`.

- [ ] Replace the `LEFT JOIN axton_record` with `COALESCE(inserted.stamp, (SELECT r.stamp FROM axton_record r WHERE r.model=$1 AND r.identity_key=keys.identity_key))`. `COALESCE` evaluates the correlated primary-key probe only for a key the insert did not return.
- [ ] `EXPLAIN` on empty, near-empty, analyzed-small and 100k-row tables; record the plans.
- [ ] Re-run the conformance file and `loads.test.mjs`.

## Task 4: Measure

- [ ] Run the scratch harness before and after through the real persistence path (`claimCall`, `readStamps` of two new keys and `saveCall` via `persistence(driver)`). Use the default `retries: 3` and the jittered wait, 50 rounds, at concurrency 4, 8 and 16 on near-empty tables, 8 on analyzed 200-row tables, and 8 at 100k rows. Run it on `pg`, `prisma` and `drizzle`, and on PostgreSQL 16 if installed.
- [ ] Measure `backend.push` Mutation deliveries with and without a Channel enrollment, and attribute what remains.

## Task 5: Docs and verification

**Files:** `docs/engineering/architecture/server/persistence.md` §5, §10 and §11; `docs/engineering/testing/integration/persistence.md`; `packages/postgres/src/driver.mts` (the backoff comment cites §11); `docs/engineering/architecture/server/engine/publish.md` §11 if its risk text changes.

- [ ] Update the claim and `READ_STAMPS` properties, the new quality requirements with their evidence, and the §11 known risk with the new numbers and what remains. Update the testing inventory rows. Check links and anchors.
- [ ] Run `bash integration/persistence/server/run.sh`, `bash integration/persistence/transaction-probe/run.sh`, `bash integration/action-e2e/run.sh`, `bash integration/load-e2e/run.sh` and `bash integration/e2e/run.sh`. Also run prettier on the touched `.mts` files and `python3 website/scripts/check_examples.py`.
