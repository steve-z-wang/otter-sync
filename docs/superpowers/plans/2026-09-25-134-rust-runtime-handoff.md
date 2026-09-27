# Issue 134 implementation handoff

Implement [#134](https://github.com/zanminwang/axton/issues/134) through one reviewed pull request and one final merge.

## Starting point

Planning branch: `codex/134-rust-runtime-plan`. The spec and plan were committed in `6a8f08e`; no implementation or baseline tests were run during preparation.

Read the [spec](../specs/2026-09-25-134-rust-runtime-design.md), [implementation plan](2026-09-25-134-rust-runtime.md), repository AGENTS.md, and issue body/comments. Fetch current main and incorporate these planning commits into an isolated implementation worktree. Preserve concurrent changes, particularly #162 and #163. Inspect their current state instead of assuming they have merged.

## Required outcome

Rust owns complete task progression, local scheduling and transaction ownership, retry/lifecycle decisions, Query flight coordination, subscription/Bootstrap state and watch refresh policy. SDKs retain typed interfaces, centralized waiter routing, language object lifetimes, user callback execution and platform network/timer/auth adapters.

Keep public APIs and backend protocol unchanged. Required local commits precede Query success and final Call success. Mutation local acceptance remains separate from its final durable outcome. Network waits do not hold SQLite transactions; callback transaction commands use ownership capabilities and cannot queue behind their own parent. Independent clients must not share a global database execution lock.

## Execution and review

Complete all four checkpoints in order within one draft PR:

1. Rust executor, centralized bridges, native carriers and transaction callbacks.
2. Complete Query/Mutation lifecycles and existing Channel/Bootstrap authority paths.
3. Shared lifecycle, retry/auth, once, subscription and watch policies.
4. Remove obsolete SDK execution paths, update documentation and verify the combined implementation.

Use coherent commits and record each checkpoint's tests, review findings and resolutions in the PR. Review each checkpoint before continuing and review the final combined diff before merge. Continue without repeated user approval within the agreed scope. If delegating implementation, use Sol as requested; the coordinating agent owns design fidelity, review and merge acceptance. Do not stop after one checkpoint or merely open a PR.

Follow the repository issue workflow and testing guides. Use deterministic runtime tests and real carrier tests for wake/drain, shutdown and callback lifetime. Verify commit-before-completion, correlation races, callback ownership, network concurrency, close/rebuild fencing, once and Bootstrap parity. Run the required final repository gate and affected platform checks; record actual commands, failures and material environment limitations. Unavailable required verification is a blocker, not a passed check.

Before merge, update from main, preserve independent fixes, resolve correctness findings and required CI checks, then merge the complete PR once. Confirm #134 closes and report the PR URL, merge commit and evidence. Attach the PR to the Codex task if that tool is available.

## Boundaries

Do not implement the downlink hook (#17), same-socket subscription reconciliation (#144), Channel naming cleanup (#152), new watch APIs (#16), browser support (#59), automatic data eviction or a persistent response inbox. #17 follows the completed runtime cleanup. Performance comparisons are evidence for this change, not an expansion into all of #12.

The spec's internal message/module names may be refined while preserving guarantees. Escalate material public behavior or scope changes, rather than silently changing the contract. Keep the issue updated with meaningful design changes or blockers.
