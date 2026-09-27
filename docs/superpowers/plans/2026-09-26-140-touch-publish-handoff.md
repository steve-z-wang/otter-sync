# Issue 140 implementation handoff

Issue: [#140](https://github.com/zanminwang/axton/issues/140)
Planning branch: `codex/140-touch-publish-plan`
Preparation worktree: `/Users/stevewang/.codex/worktrees/140-touch-publish-plan/axton`
Inspected main: `9cfb0b88506b001fa78e744af132245a7d0d0230` on 2026-09-26.

## Assignment

Implement #140 through all six checkpoints of the [plan](2026-09-26-140-touch-publish.md), using the [spec](../specs/2026-09-26-140-touch-publish-design.md) as the behavior contract. Work in an isolated checkout based on current main and include the latest planning documents from the planning branch. Read AGENTS.md and the issue workflow. The user will assign this to another agent; no implementation agent was started by the planning session.

Do not stop after the SDK renames or compiler changes. Complete server persistence/settlement, generated contracts, first-party callers, runtime/integration tests and lasting docs. Self-review each checkpoint, then review the complete PR and address all findings. Open a PR referencing `Closes #140`, report executed checks and any real limitations, and attach it to the assigned task. Follow merge authorization supplied in that task; this preparation handoff does not grant new merge authority. Do not close the issue merely because planning or a partial checkpoint is complete.

## Final decisions; do not restore superseded proposals

1. Proposed concrete API is `ctx.channel(name).todo.add/remove(identity)`, mixed `channel.add/remove(RecordRef[])`, and `ctx.touch.todo(identity)`. Channel handles only collect transaction-scoped intents. Preserve generated typed identities without hidden-tag requirements for mixed references.
2. Channel membership is durable. Add once, then subsequent inferred or explicit changes automatically fan out to every Channel containing that record. Repeated add/remove is a no-op. Remove stops future distribution and does not evict local data.
3. `todo Todo.update` automatically registers the input target as changed and automatically returns synchronization authority to reconcile it. This occurs even when there are no named outputs, `store` disables optional output storage, or the record belongs to no Channel.
4. Business outputs are explicit and fully independent from inputs, even when names and Models match. The handler supplies every explicit Model output identity. Input Todo A and output Todo B are allowed: receipt authority reconciles A, `result.todo` resolves B. There is NO same-name identity inference or missing-output fallback.
5. Extra touch advances the record stamp and fans out. It does NOT automatically return that record in caller authority or business result, and must not require the initiating client to declare/read an unrelated touched Model. Actual explicit output and input-authority reads still enforce existing Loader/auth/version rules.
6. A record gets one new stamp per settlement and separate cursor positions in each affected Channel. Rust owns this algorithm. All business writes, memberships, stamps, positions and saved outcomes commit together; wakes occur only after commit. Saved-call replay allocates nothing again.
7. Removal filters retained invalidation rows BEFORE pagination limit in delta and Bootstrap; it does not erase history, rewind cursors or fabricate deletion. Loader-null business deletion keeps identity membership; same-identity recreation resumes it. These defaults are specified, not left for guessing.
8. Same source/handler contracts apply to direct and durable delivery. External transactions share settlement without input authority; Queries expose no effect declarations. Frontend runtime ownership and completion-after-apply remain intact.

## Start and review gates

- Fetch the planning branch and current main. Reuse an isolated checkout if one is already provided; otherwise create one under `codex/`. Do not modify the user's main checkout.
- Read the current spec/plan, not an earlier commit: they replace both the old API-only plan and the temporary superseded banners.
- Baseline and prerequisites: follow `docs/engineering/testing/running.md`. Preparation ran no implementation tests and installed no dependencies.
- Implement checkpoints in order, keeping reviewable commits. Use meaningful failing regression tests before behavior changes. If delegation is authorized by the assigned task, the user prefers Sol for implementation.
- The core review examples are: no-output input mutation; input A/result B sharing the name todo; touch-only extra Model unreadable/undeclared by caller; enrollment once then update without republishing; remove/history filtering; concurrent membership-only changes under Repeatable Read; and immutable saved-call replay.
- Keep operation-history validation strong. This is a coordinated prelaunch change: regenerate reviewed repository fixture histories/evolution cases, not a production migration or automatic output rewrite. Host SDK/native artifacts change together.
- After targeted tests pass, run `bash scripts/test.sh`, inspect the entire diff and required CI on the final head. Do not describe source inspection as executed test evidence.

## Prepared artifacts

- [Spec](../specs/2026-09-26-140-touch-publish-design.md)
- [Implementation plan](2026-09-26-140-touch-publish.md)
- [Issue](https://github.com/zanminwang/axton/issues/140)

Preparation deliverable: reviewed design, plan and assignment. Implementation, runtime verification and PR creation are the receiving agent's work.
