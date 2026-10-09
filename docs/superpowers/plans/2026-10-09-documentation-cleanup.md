# Documentation Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking. This plan is proposed; do not start implementation before user approval.

**Goal:** Give current documentation one coherent navigation tree, accurate source/evidence links and a separate historical archive.

**Architecture:** Current topics follow the agreed component owners. Historical pages retain their claims and dates in one archive; mixed evidence pages are rewritten from inspected current assertions. A read-only local path audit and existing website checks verify the result.

**Tech Stack:** Markdown, Python standard library, existing MkDocs Material and website checks.

**Spec:** [Current documentation cleanup](../specs/2026-10-09-documentation-cleanup-design.md).

## Global Constraints

- Baseline: `71b14195b897bc8a42f19e0a83d59dfe8fc76677`.
- Packages remain at 0.5.4 and the sync discriminator is 5.
- Preserve the agreed component tree and implemented guarantees.
- No Rust/SDK behavior, generated schemas, storage, published exports, native symbols, package versions or Capso changes. Publish no package and deploy no application/backend; the existing Documentation workflow publishes the website after merge to main.
- Keep `docs/superpowers/`, `.superpowers/` and original design records unchanged except the new documents for this task.
- Preserve historical claims/dates; distinguish source inspection from executed evidence.
- Local worktrees, caches, SYN-24 and SYN-25 are outside this PR.

## Task 1: Separate historical reference material

**Files:**
- Create: `docs/engineering/history/pre-protocol5/README.md` and the mirrored destinations for the archive inventory in the spec.
- Move: the spec's 17 historical architecture pages, `guarantees-0.3.md`, `testing/0.4.md`, `testing/review.md`, `channel-membership-release.md`, `brand-rename.md`.
- Modify: `docs/README.md`, `docs/engineering/README.md`, current incoming links under `docs/engineering/`.

**Interface:** The archive index owns original path/period/baseline metadata. Current entry points link this index; frozen records remain untouched.

- [ ] Confirm the implementation branch is isolated and clean; record its actual base commit. Recheck each inventory path and current incoming links with `rg`, excluding frozen records.
- [ ] Move the historical architecture pages, old guarantees and reviews to mirrored archive paths. Use `git mv`; do not delete their contents.
- [ ] Preserve the rename's still-current package/native naming in its appropriate live entry point, then archive the old storage/cutover guidance.
- [ ] Write a short archive index with the baseline, original paths and each page's stated period. State that mixed-period snapshots are records, not current coverage.
- [ ] Rebase archive-relative document links within the archive. Verify pinned historical source targets with `git cat-file -e <verified-ref>:<path>` before citing them; mark unavailable evidence explicitly rather than substituting current code.
- [ ] Update live incoming links and move old release/rename entries from general current navigation to the history index. Leave dated design/planning records unchanged.
- [ ] Inspect `git diff --summary` and `git diff -- docs/superpowers .superpowers`; expect only intended documentation moves and no change to existing frozen records. Commit this task.

## Task 2: Give current API and Sync topics their final owners

**Files:**
- Merge into: `docs/engineering/architecture/protocols/sync.md` from `architecture/protocol/0.5.md`.
- Move: `architecture/sdks/typed-api/client.md` → `architecture/frontend-sdk/api.md`; `server.md` → `architecture/backend-sdk/api.md`.
- Merge into: `architecture/frontend-sdk/bindings.md` from `architecture/sdks/bindings.md`.
- Remove: redundant `architecture/protocol/README.md`, `architecture/sdks/README.md`, `architecture/sdks/typed-api/README.md` after their useful links are moved.
- Modify: `docs/engineering/architecture.md`, `architecture/README.md`, component READMEs, `docs/engineering/guarantees.md`, `protocol5-adoption.md`, current website/package incoming links.

**Interface:** `protocols/sync.md`, `frontend-sdk/api.md` and `backend-sdk/api.md` become canonical targets. No Markdown forwarding files remain at the former paths.

- [ ] Merge the complete current Sync contract into `protocols/sync.md`, keeping all actual admission, replay, S/B/C, read protection, materialization, capacity and failure boundaries.
- [ ] Move the two API pages, repair their relative links and update every live incoming reference. Preserve existing public API examples.
- [ ] Merge client-native actor/ABI explanation into Frontend Bindings; link Protocols and Rust Runtime as the owners of messages and decisions. Keep Backend Bindings' retained Host context separate.
- [ ] Remove the empty old group directories and their redundant indices. Current navigation lists no retired Load or protocol-4 component child.
- [ ] Correct source labels and ownership: `mutation_queue.rs`/`sync05/uplink.rs` for client batching; `mutation_batch.rs` for server execution; `settlement.rs` plus its current adapter for publication. Name `runtime/lanes.rs`, `sync05/downlink.rs` and `delivery_queue.rs` by their actual filenames.
- [ ] Remove repeated generic acceptance paragraphs where they add nothing beyond the parent; preserve each component's concrete rules and evidence boundaries. Do not create new component nodes or new classes.
- [ ] Check changed anchors in repository links and website contributor links. Verify the final tree against the architecture spec, inspect the diff and commit this task.

## Task 3: Replace stale test maps with inspected current evidence

**Files:**
- Modify: `docs/engineering/testing.md`, `testing/components/{client,compiler,protocol,schema,server}.md`, `testing/integration/{connection,persistence}.md`, `testing/simulation/{scenarios,invariants,recovery}.md` and their current indices.
- Create: a concise current `docs/engineering/testing/review.md` and archive snapshots of mixed testing pages before rewriting them.
- Modify: affected current compiler/runtime/storage documentation where evidence or owner links change.

**Interface:** Current evidence tables cite existing assertions with their scope and limits. The archive retains past tables and execution dates.

- [ ] Save each mixed page's pre-edit body in the mirrored archive hierarchy, clearly labelled as the baseline documentation snapshot. Preserve its old execution statements there. If the destination already contains the baseline page from Task 1, retain it; never replace an archived original with an edited current page.
- [ ] Read current protocol fixtures/tests in `crates/protocols/tests/`, admission/delivery tests in `crates/server/tests/`, and the actual assertion sections of the SQLite scenarios being cited.
- [ ] Replace missing SQLite names with relevant inspected `protocol05_*` scenarios, not by automated filename substitution. Keep current `query.rs`, `runtime_prerequisites.rs`, `store.rs` and `stream_upgrade.rs` evidence where relevant.
- [ ] Describe current simulation from `crates/sim/src/{lib,rng,scenario05}.rs` and `tests/{scenario05,protocol05_coverage,mutation_versions05}.rs`. Remove claims about deleted random runners/shrinkers unless kept explicitly in history. Do not imply coverage from a filename alone.
- [ ] Read current persistence assertions in `integration/persistence/server/`: Batch/delivery, retained Fetch, publication/savepoint semantics, read liveness, namespace/locks and persistence batching. Map them to their actual boundary and driver.
- [ ] Rewrite the current review as an evidence index with explicit limits. Link adoption and the current joined host/capacity gates; source inspection alone supplies no new execution result or complete-coverage claim.
- [ ] Correct the engineering index's testing description and the current component/protocol code map. Keep current retired-input rejection tests as active negative admission evidence.
- [ ] Inspect every changed assertion/coverage claim against its cited source, review the archive/current split and commit this task.

## Task 4: Verify current paths and the final documentation

**Files:**
- Verify: all changed current Markdown plus the built website.

**Interface:** The read-only audit below checks current local file/directory targets. Existing `website/scripts/check_links.py` owns built-site anchors. Add no new checker framework or CI job.

- [ ] Run this path audit from the repository root, inspect each diagnostic and correct the current reference. Expected: zero missing targets. It checks paths, not Markdown reference-style syntax or repository heading anchors; inspect those changed references separately.

```sh
python3 - <<'PY'
from pathlib import Path
from urllib.parse import unquote, urlsplit
import re
import subprocess

names = subprocess.check_output(
    ['git', 'ls-files', '--cached', '--others', '--exclude-standard'],
    text=True,
).splitlines()
errors = []
for name in sorted(set(names)):
    current = name in ('README.md', 'AGENTS.md') or name.startswith(
        ('docs/engineering/', 'website/docs/')
    ) or (name.startswith('packages/') and name.endswith('/README.md'))
    if not current or not name.endswith('.md') or name.startswith(
        'docs/engineering/history/'
    ):
        continue
    page = Path(name)
    text = re.sub(r'(?ms)^(`{3,}|~{3,}).*?^\1[^\n]*', '', page.read_text())
    for match in re.finditer(r'!?\[[^\]\n]*\]\(\s*(<[^>\n]+>|[^\s)]+)', text):
        target = match.group(1).strip('<>')
        url = urlsplit(target)
        if url.scheme or url.netloc or not url.path:
            continue
        path = page.parent / unquote(url.path)
        if not path.exists():
            errors.append(f'{name}: missing {target}')
if errors:
    raise SystemExit('\n'.join(errors))
print('Current local Markdown paths exist.')
PY
```

- [ ] Verify current GitHub `blob/main`/`tree/main` source links against the corresponding local targets. Historical release links remain pinned and separately identified. No network crawler or per-file warning baseline is needed.
- [ ] Run the existing checks from the repository root:

```sh
python3 -m unittest website/scripts/test_examples.py
python3 -m mkdocs build --strict --config-file website/mkdocs.yml
python3 website/scripts/check_links.py
git diff --check
```

Expected: extraction tests, strict build and built-site links/anchors pass; no whitespace errors. Use the existing configured Python environment rather than installing tools unnecessarily.

- [ ] If executable snippets changed, run `python3 website/scripts/check_examples.py` with the documented built native/language dependencies. If snippets did not change, record that check as unnecessary for this documentation-only change. Do not rerun the full runtime gate for prose/path edits.
- [ ] Confirm the diff contains only documentation and no rewritten frozen records. Review the built navigation and a route through Sync, frontend API, backend API and the archive. Commit and prepare one documentation PR with actual verification evidence.

## Coordination

This lane changes no runtime source. The internal cleanup can proceed in a different worktree without waiting; its final source-owner documentation update rebases onto this PR. Prefer merging this documentation PR first. Each PR remains independently reviewable.

No implementation checks have been executed while writing this plan.
