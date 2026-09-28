# Direct writes to synced Models: implementation plan

> **For agentic workers:** Use `superpowers:executing-plans` with `superpowers:test-driven-development`. Tests come first; a failing test is an engine bug, diagnosed with `superpowers:systematic-debugging` and recorded on the issue as `[change]`.

**Goal:** Guarantee, document and pin how a direct local write (`tx.models.<model>`, not a Mutation) to a Model that Channels also deliver interacts with canonical data and with refused Mutations.

**Issue:** [#188](https://github.com/zanminwang/axton/issues/188). **Design:** the issue's `[design]` comment.

## Decided behaviour

1. A direct write is device-only: never uplinked, never a queued operation.
2. Newer canonical data wins: a Channel page, Load page or Fetch response that delivers the same identity at a stamp newer than the device holds replaces the direct write. Nothing replays the direct write afterwards.
3. A refusal doesn't undo it: refusing a pending Mutation that holds the same row removes only that Mutation's optimism (inferred operations and `local` companions). The direct write stays until newer canonical data replaces it. The existing L4 exception stays: a direct write on a row whose only creation is a pending create goes with the refused create.

"Later" means a newer stamp (D2). An equal-stamp redelivery after a direct write is a conflict today. That case is pinned as current behaviour and handed to a follow-up issue.

## Constraints

- The engine code touched, if any: `crates/client/src/{authority,mutate,fetch,loads}.rs`. Expected: none; the tests decide.
- Do not change public APIs or the Most Days surfaces (`@sequence(after:)`, `backend.publish`, `createBackend({ admit })`, Dart `SyncServer(headers:)`, `AdmissionRefused`).
- The shared machine rules out a local `bash scripts/test.sh` run; CI runs the full gate.

## Checkpoint 1: conformance tests (red first)

**File:** create `crates/sqlite/tests/direct_writes.rs` using `common` helpers and `load_schema()` (it has `Entry` and the `Entries` Load).

- A `Source` enum (`Channel`, `Load`, `Fetch`) and a device harness that delivers `Entry e` at a given text and stamp through each source. The harness asserts that the delivery applies: a Channel page with no conflict, a completed Load job, and a stored Fetch.
- `a_direct_write_is_never_queued_and_newer_canonical_data_replaces_it`: for each source, cover a direct create on an absent row (no stamp), a direct update of a stamped row and a direct delete of a stamped row. After the direct write, nothing is pending and `freeze()` is `None`. After a newer delivery, the row is the delivered row at its stamp, nothing is retained (no before image, no journal) and a reopen keeps it.
- `a_refused_mutation_leaves_a_direct_write_on_its_row`: cover a direct write before the pending edit, a direct write after it, and a direct write after a Mutation whose `local` companion edits the same field. After the refusal, the row shows the direct write, one rejection is retained and nothing is pending.
- `after_a_refusal_newer_canonical_data_replaces_the_surviving_direct_write`: the same cases, followed by a newer delivery from each source. The delivered row wins, nothing is retained and a reopen keeps it.
- `canonical_data_during_a_pending_mutation_retires_the_direct_write_for_good`: a direct write on a pending row, then a newer delivery while the Mutation is still pending, then a refusal. The delivered row shows, and the direct write never comes back.
- `an_equal_stamp_redelivery_is_not_newer_canonical_data`: pins current behaviour. On a Channel page the conflict is reported and the direct write is kept. A Load page and a Fetch are refused whole, and the direct write is kept.

Run `cargo test -p axton-sqlite --test direct_writes --locked`. If any test fails, record the difference, fix the engine and comment `[change]` on the issue.

## Checkpoint 2: documentation

- `docs/engineering/guarantees.md` L4: name the canonical sources (Channel, Load, Fetch) and the newer-stamp rule, and add evidence and the equal-stamp limitation below the table.
- `docs/engineering/architecture/client/engine/local-operations/writes.md` section 10: add the new evidence.
- `website/docs/frontend/client-api.md` Local-only writes: explain writing canonical data from another transport, including what replaces it, what a refusal does, and the equal-stamp caveat.
- Open a follow-up issue labelled `decision` for the equal-stamp case, and link it from the docs and the issue.

## Checkpoint 3: verification

- `source scripts/env.sh`, then `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test -p axton-sqlite --locked`, `cargo test -p axton-client --locked` and `cargo test -p axton-sim --locked`.
- Check relative links and anchors in the edited Markdown files.
