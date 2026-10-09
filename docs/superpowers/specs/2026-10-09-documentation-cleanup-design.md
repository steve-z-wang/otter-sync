# Current documentation cleanup

**Status:** Proposed. Exploration and planning only; implementation requires approval.

**Baseline:** `71b14195b897bc8a42f19e0a83d59dfe8fc76677`, the merged Protocol/SDK refactor. Packages remain at 0.5.4 and the sync discriminator is 5.

## Goal

Make the current documentation describe the current framework, with one owner for each topic and a separate place for historical references.

## Findings

- The component tree has Protocols, Frontend SDK and Backend SDK, but current API pages still live under `architecture/sdks/typed-api/` and the complete Sync contract under `architecture/protocol/0.5.md`.
- Current navigation still links retired Load, Pull and protocol-4 pages as component children. Their historical banners help, but readers still have to separate current behavior from old carriers.
- Several current code-map labels name deleted files even when the link now targets a replacement. Client batching points to the server member engine; publication points to delivery planning instead of its settlement owner.
- Testing pages still describe deleted stamp, Load and simulation suites. A read-only scan found 186 missing local Markdown-link occurrences in 15 files across tracked engineering, website, package and entry-point Markdown. The scanner checked paths, not anchors or remote URLs; this is an exploration inventory, not a complete link audit.
- `testing/review.md` identifies itself as a historical protocol-3 review but still directs readers to protocol-4 evidence as current. `engineering/README.md` says testing is to be defined even though the strategy and runners exist.

No tests or builds were run for this exploration. The prior refactor's acceptance is separate evidence.

## Approach

Keep current topics under their component owners and move obsolete reference pages into `docs/engineering/history/pre-protocol5/`, preserving their original relative hierarchy. Rewrite mixed current/historical guidance from today's source and assertions.

Deleting all history would lose useful decisions and executed evidence. Keeping history in the current tree with more warnings would retain the navigation problem. Archiving it gives current readers a direct route while preserving the record.

The archive is a documentation snapshot, not a claim that all its pages describe one release. Its index records the original path, stated period and baseline commit. Historical bodies retain their claims and dates; link maintenance must not turn old evidence into current guarantees.

## Current owners

| Topic | Final owner | Change |
| --- | --- | --- |
| Sync messages and semantics | `docs/engineering/architecture/protocols/sync.md` | Merge the current 0.5 contract with the existing Sync overview; remove the duplicate current page. |
| Client and Server bridge contracts | Existing `protocols/client-bridge.md`, `server-bridge.md` | Keep their ownership and update incoming links. |
| Frontend API | `docs/engineering/architecture/frontend-sdk/api.md` | Move `sdks/typed-api/client.md`. |
| Backend API | `docs/engineering/architecture/backend-sdk/api.md` | Move `sdks/typed-api/server.md`. |
| Frontend bindings and shared client carrier | `docs/engineering/architecture/frontend-sdk/bindings.md` | Merge useful material from `sdks/bindings.md`; link the native actor and ABI without duplicating their implementation. |
| Backend bindings | Existing `backend-sdk/bindings.md` | Keep retained transaction/session responsibilities explicit. |
| Runtime, storage and engines | Existing component subtrees | Correct navigation, labels and implementation ownership. |
| Current test responsibilities and evidence | Existing `docs/engineering/testing/` | Replace obsolete evidence with assertions present in the current checkout. |
| Historical references | `docs/engineering/history/pre-protocol5/` | One archive index, linked from the engineering index. |

Remove the redundant `architecture/sdks/` navigation after moving its contents. Remove the old `architecture/protocol/` directory after moving the current Sync contract and archiving its historical pages. Do not leave forwarding Markdown files at the old paths.

## Archive inventory

Paths below are relative to `docs/engineering/`. Their archive destinations preserve those paths beneath `history/pre-protocol5/`.

- `architecture/protocol/{0.4,actions,common,loads,pull,push,subscriptions}.md`
- `architecture/schema/loads.md`
- `architecture/client/protocol4.md`
- `architecture/client/engine/{loads,pull}.md`
- `architecture/client/connection/controller/{live-session,load-worker}.md`
- `architecture/server/protocol4.md`
- `architecture/server/engine/{loads,pull,push}.md`
- `guarantees-0.3.md`
- `testing/{0.4,review}.md`
- `channel-membership-release.md`
- `brand-rename.md`, after current package/native naming guidance has been checked against the current entry points.

Mixed testing pages are not moved wholesale out of the current tree. Preserve their pre-edit text as documentation snapshots in the same archive hierarchy, then rewrite the current pages. A snapshot is marked as mixed-period documentation at the baseline, not certified historical coverage.

Current architecture/guarantee pages may link the archive when explaining a historical decision. Component navigation must not advertise retired APIs as available work.

## Rewrite rules

- Preserve the agreed component tree. A documentation page is not a request to introduce a new class or crate.
- Preserve Sync 5 behavior, S/B/C, per-identity authority evidence, cursor-null reads, explicit tracking, independent Mutation execution and the execution/settlement distinction.
- Link each rule to its owning explanation and actual source; remove repeated generic acceptance paragraphs where they add no component-specific information.
- Correct current code-map labels as well as targets. Client batching belongs to `crates/client/src/mutation_queue.rs` and `sync05/uplink.rs`; server execution belongs to `crates/server/src/mutation_batch.rs`; publication belongs to `crates/server/src/settlement.rs` with its current adapter and persistence collaborators.
- Keep local CRUD, retained schema-history validation, prerequisites, native carriers and current status APIs documented even when their names originated in an older release.
- Read assertions before citing a test. Mark evidence as source inspected unless the relevant command was actually executed. Do not replace a missing test with a similarly named file.
- Keep historical source links pinned to a verified release/commit. For a deleted historical target without verified source, describe the reference as unavailable; do not link to an unrelated current replacement.
- Update tracked live incoming links, affected heading anchors, the website's contributor links and package entry points. Frozen plans/specs and original design notes remain unchanged.

## Scope boundary

This work changes documentation only. It does not alter Rust/SDK behavior, generated schemas, storage, published exports, native symbols, package versions, Capso, releases or deployments.

Preserve `docs/superpowers/`, `.superpowers/` and any original design record byte-for-byte except the new documents for this task. Local worktree deletion and build-cache cleanup are separate work. SYN-24 and SYN-25 remain separate tooling issues.

## Acceptance

- Current navigation follows Schema, Protocols, Compiler, Frontend SDK, Backend SDK, Client runtime and Server runtime.
- Retired carrier pages have one historical home; current API and Sync topics have one owner.
- Every current implementation/evidence link names an existing, relevant target. Current local file links have no missing targets; changed heading anchors are verified.
- Run a read-only repository Markdown path audit over current engineering/website/package entry points, excluding frozen records/history. It does not fetch remote URLs or claim to validate historical coverage. Use the command in the plan; add no new checker framework or CI job.
- The website extraction tests, strict build and built-site link/anchor check pass. Run snippet typechecking only if executable snippets change.
- Review confirms no source/runtime changes and no rewritten original design records. Report inspected evidence separately from executed checks.

## Delivery

One documentation PR. It can be prepared independently of the [internal cleanup](2026-10-09-internal-cleanup-design.md), then merged first so the code PR only updates the few final source-owner references it changes.

Implementation steps: [plan](../plans/2026-10-09-documentation-cleanup.md).
