# Load Once: In-flight Implementation Amendment

Issue: [#173](https://github.com/zanminwang/axton/issues/173). Planning branch: `codex/load-operations`. Original planning commit: `8df1cc3`.

The user has authorized this amendment while another agent is implementing Load. Continue the current implementation and incorporate these changes; do not restart it, reset its branch, or discard completed code. This document and the updated [spec](../specs/2026-09-27-load-design.md#once-reuse-refresh-and-invalidation-2026-09-27-amendment) and [plan](2026-09-27-load-plan.md) supersede the original statements that every start is fresh and once is out of scope.

## Public API

```ts
const job = await client.loads.projectTodos({ projectId }, { once: true });
await job.wait();

const refresh = await client.loads.projectTodos(
  { projectId }, { once: true, refresh: true },
);
await client.loads.invalidate.projectTodos({ projectId });
```

Ordinary calls keep creating fresh independent jobs. Once is a call option, never a schema annotation. Reserve `invalidate` on the Load facade. Dart mirrors the options and invalidation namespace, using Query's callOnce/callRefresh fallback when business argument names collide. Reject refresh:true without once:true before creating work.

## Behavior to implement

- Persist an `axton_load_once` key-to-job mapping in SQLite. The key is the active replica's Load name/version, normalized business arguments and retained output Model read versions. No-argument inputs use `{}`. Token credentials, continuation and job IDs are not key material.
- A same-key active job is shared; completed jobs are returned offline with no network/store/onStore or Model-watch change. Failed jobs are returned as failed, with explicit `retry()` needed. Reused handles share the job ID, not necessarily the language object.
- Refresh joins a currently active mapped job. Otherwise it atomically registers a new first-page job, replacing the old mapping. A failed refresh remains visible as failure; there is no stale completed fallback. Retry resumes the old job's uncommitted page using the existing page replay rules.
- Invalidation is a local committed operation that removes matching mappings across retained versions. It does not cancel jobs, delete Models, or fetch anything. Old work can still finish/store, but must never reinsert its mapping. Only explicit once start/refresh writes mappings.
- Cancel/forget remove a mapping only when it still points at that job ID; old-handle cleanup cannot erase a refreshed mapping. Cancelling an already complete job remains a no-op. Dispose releases handle resources only. An incompatible replica rebuild carries neither jobs nor mappings into the new empty replica.
- Once means previous enumeration is reusable by application choice; it is not a freshness or completeness oracle. Local deletion/permission/Channel changes do not automatically invalidate it. Application code explicitly refreshes/invalidates where needed.

## Integrate with work already completed

1. Fetch `origin/codex/load-operations` and inspect the new documentation-only commit. Merge normally when working on a descendant branch, or cherry-pick that documentation commit onto a different branch. Resolve documentation conflicts by retaining this amendment and your verified progress. Do not force-push/reset implementation work.
2. Checkpoint 1: reserve `invalidate` and keep once/refresh out of descriptor/history. Existing backend page/wire implementation is unchanged by this amendment.
3. Checkpoint 3: add the mapping table, canonical normalization and atomic start/refresh/invalidate/cancel/forget behavior. Reuse shared Query canonicalization helpers, not its result cache.
4. Checkpoint 4: handle options/invalidation in Rust commands; joins/hits must not enqueue duplicate worker tasks or storage replay. Add race and restart tests before relying on completed reuse.
5. Checkpoint 5: generate TS/Dart options and invalidation facade; test Node, React Native host and Dart handle behavior, no-argument calls and option-name collisions.
6. Checkpoint 6: prove no-network hits after reopen, in-flight invalidation fencing, reverse completion order, explicit failed retry, failed refresh behavior, and mapping cleanup ownership over real HTTP/SQLite. Update guides and API docs.

The updated plan embeds these items in their existing checkpoints and coverage map. Tests for this amendment have not been run because this task edits design documents only. Existing baseline evidence must not be presented as feature validation. Follow the repository's issue workflow to record adoption, complete independent review and required checks, and include once in the final PR scope.
