# Channel tags and synchronized removal — implementation handoff

Date: 2026-09-30. Written for the agent that continues this work.

Spec (binding authority): [2026-09-30-channel-tags-removal-design.md](../specs/2026-09-30-channel-tags-removal-design.md).
Plan (execution order, ten tasks): [2026-09-30-channel-tags-removal.md](2026-09-30-channel-tags-removal.md).
Read [AGENTS.md](../../../AGENTS.md) first, then the spec, then the plan.

## 1. Where the work stands

| Item | Value |
|---|---|
| Repository | `/Users/stevewang/Github/axton` |
| Worktree (work here only) | `/Users/stevewang/Github/axton/.worktrees/channel-tags-removal-design` |
| Branch | `codex/channel-tags-removal-design`, rebased onto `main` at `ba14ee59` (release 0.1.2; PR #214 merged as `f90e10f7`) |
| HEAD | `8261c703`, working tree clean |
| Target release | AXTON `0.2.0` via release-please; both `bump-minor-pre-major: true` and `bump-patch-for-minor-pre-major: true` are configured, so an ordinary `feat:` yields a patch. Use `feat!:` or `BREAKING CHANGE` for the intentional 0.2 boundary; do not hand-edit versions |
| Done | Tasks 1–4 of 10 implemented, reviewed and fixed; Task 4's last fix round is not re-reviewed |
| Not started | Tasks 5–10 |

Commits on the branch, oldest first (all on top of `ba14ee59`):

```text
91af1fb2 docs: design channel tags and synchronized removal
a252c0a8 docs: target channel membership changes for 0.2.0
ddaeb9b3 feat(core): freeze the channel membership wire contract            (Task 1)
c54216d8 feat(core): carry membership claims in enrollment-capable responses (Task 1 fix)
2be39643 feat(server): collect channel tags and ordered tag selectors        (Task 2)
25662b0b feat(server): reduce ordered channel declarations through channel host operations (Task 3)
ade3b298 docs: describe ordered membership reduction and the channel host operations (Task 3)
a76e90da fix(server): lock a Load page's enrollment Channels before its record reads (Task 3 fix)
223cc1fa feat(postgres): eight-table Channel schema and forward upgrade      (Task 4)
64ea9c30 feat(postgres): answer lockChannels, readChannelMembers and applyChannelMembers (Task 4)
6fd3d626 test(postgres): Channel tags, removal, concurrency and upgrade on real PostgreSQL (Task 4)
ac1aec2c docs: describe the Channel tables, operations and upgrade of the PostgreSQL adapter (Task 4)
8261c703 fix(postgres): split migration.sql for Prisma consumers without breaking on comments (Task 4 fix)
```

Detailed per-task reports and the execution ledger are git-ignored local files in the worktree under
`.superpowers/sdd/2026-09-30-channel-tags-removal/` (`progress.md`, `task-N-brief.md`, `task-N-report.md`,
`review-*.diff`). They are worth reading; if that directory is gone, git history is the record.

## 2. What each finished task delivered

### Task 1 — wire contract (`crates/core`)

All re-exported from `axton_core`:

- `CHANNEL_MEMBERSHIP_CAPABILITY = "channel-membership-v1"`, `PROTOCOL_UNSUPPORTED = "protocol.unsupported"`.
- `read_capabilities`, `logical_request`, `with_capabilities(envelope, &[caps])`, `require_capability(envelope, cap) -> Result<(), NegotiationRefusal>` where `NegotiationRefusal::{Unsupported, Malformed}` map to `protocol.unsupported` / `request.invalid`.
- `ChannelChange::{Upsert{channel, cursor, record: AuthorityRecord(flattened)}, Remove{channel, cursor, key: RecordKey(flattened)}}`, `ChannelPullPage`, `ChannelBootstrapPage`, `ChannelLiveMessage` — new page types **alongside** the legacy `PullPage`/`BootstrapPage`/`LiveMessage`; no runtime path selects them yet (that switch is Tasks 5/6/8).
- `MembershipClaim {channel, cursor, model, identity}` and `memberships: Vec<MembershipClaim>` on `LoadPageResponse`, `PushReceipt`, `DirectActionResponse` (`#[serde(default, skip_serializing_if = Vec::is_empty)]`, validated against the envelope's returned records; failed/retryable items carry none). Nothing produces claims yet.
- Every request decoder accepts and strips a `capabilities` array; it is excluded from saved-call logical equality. Legacy decoders now refuse a record carrying `channel`/`cursor`/`kind`.
- Fixture: `fixtures/protocol/channel-membership.json`; tests in `crates/core/tests/contracts.rs`.

### Task 2 — typed tags and selectors (TypeScript collector, compiler)

- `packages/server/effects.mts`: `MembershipOptions = { readonly tags?: readonly string[] }`, `TagSelector = { readonly tag: string }`; per-Model `add(identity, options?)`, mixed `add(records, options?)`, `remove(records)`, `remove({tag})`. Load handles are add-only and accept `{tags}` (ruling R2).
- `packages/server/host-contract.mts`: ordered `ChannelIntent = add{channel, record, tags} | remove{channel, record} | removeTag{channel, tag}` replaces the old `present` shape in `SettlementEffects.memberships` and `HandledLoad.memberships`.
- Rust mirror `ChannelIntent` in `crates/server/src/host.rs` (`MembershipIntent` is gone). Tag validation in TS: non-blank (JS `trim()`), ≤256 UTF-8 bytes, ≤64 distinct per add, copied and deduplicated at collection.
- Generated API (`crates/compiler/src/emit.rs`, `emit_loads.rs`) exposes exactly those overloads; compile-negative examples cover wrong identity, Load `remove`, and selector misuse. Regenerated fixtures under `integration/generated-api/`.

### Task 3 — server reducer and host operations (`crates/server`)

- New `crates/server/src/channel_members.rs`: pure reducer `reduce(channel, initial: Vec<MemberState>, declarations, touched) -> Vec<MemberDelta>`, `check_tag`, `declared_tags`, constants `TAG_BYTES = 256`, `TAGS_PER_ADD = 64`. Rust "blank" = union of JS `trim()` whitespace and Rust whitespace; a tag JS accepts but Rust refuses fails loudly as `handler.invalid`.
- Three host operations (TS types in `host-contract.mts`, Rust in `host.rs`, shared fixture `fixtures/protocol/host-operations.json`), replacing the retired `publish` and `setMembership`:
  - `lockChannels {channels: string[]}` → null. Channels ≥1, distinct, strictly increasing byte order; locks **existing** rows only, creates none.
  - `readChannelMembers {channel, explicitKeys: MemberKey[], tags: string[]}` → `MemberState[]` (`{model, identityKey, tags}`; union of named keys and tag-selected members, complete tag sets).
  - `applyChannelMembers {deltas: MemberDelta[]}` → `MemberPosition[]` (`{channel, model, identityKey, cursor, kind: upsert|remove}`). Deltas `{channel, model, identity, identityKey, present, tags, publish}` sorted by channel then canonical key; one `head += N` reservation per channel; published deltas get consecutive cursors in delta order; `publish=false` deltas answer the member's **existing** upsert position; one position per delta in delta order.
- `settle_changes` order on every path (push, Action, external transaction/publish, Load enrollment, global touch): validate tags → resolve channels (explicit + touch recipients) → `lockChannels` → `ensureStamp`/`advanceStamp`/`lockRecord` in canonical key order → re-read recipients and check the lock set (mismatch raises `transaction.conflict`, which `isRetryableTransactionError` treats as retryable so the whole transaction reruns) → `readChannelMembers` per channel → reduce → one `applyChannelMembers`. Engine conformance checks (`check_members`, `check_positions`) refuse any mismatched or non-consecutive host answer with `host.invalid`.
- Loads: `lockChannels` for enrollment channels is taken before `readStamps` (`settle_locked`); a page that enrolls nothing takes no lock.
- The positions answer is validated and then **discarded** — Task 5 must thread it out for `ChannelChange`s and claims. Scans still skip `remove` rows.

### Task 4 — PostgreSQL persistence and migration (`packages/postgres`)

- `migration.sql` is the fresh-install eight-table DDL (`axton_record` with `id`, `identity jsonb`; `axton_channel`, `axton_channel_member`, `axton_channel_tag`, `axton_channel_member_tag`, `axton_channel_log`; constraint trigger for same-channel associations; immutable channel ownership). **It contains dollar-quoted functions and must be applied whole**; the Prisma-style consumers (`examples/todo/server.mts`, `integration/e2e/fixtures/round-trip/server.mts`, `integration/platform/react-native/server.mts`) use the shared comment-aware splitter `packages/postgres/src/statements.mts` (`sqlStatements`).
- Forward upgrade `packages/postgres/migrations/2026-09-30-channel-members.sql` (steps 1–5 of spec §9; old tables retained; repeated run is a verified no-op; the copy step is gated on `axton_record.id` being absent).
- Adapter: `packages/postgres/src/sql.mts`, `persistence.mts`, new `channel-ops.mts`. Set-based statements, 1,000-entry batches inside the caller's transaction, overflow guard (`head <= MAX - N`), insert-or-update on `axton_channel` so two first writers to a new channel serialize (ruling R7), `lockChannels` also writes a no-op row version (`FOR NO KEY UPDATE`) so Repeatable Read callers retry deterministically. Kept deltas read their position in the same statement.
- Tests: `integration/persistence/server/channel-tags.test.mjs` (19 cases: pair uniqueness, cross-channel trigger via direct SQL, whole-member removal, idempotent removal, rollback, compaction, the brief's exact SQL expectations, add vs removeTag / union vs empty selector / touch vs removal races with barriers, failure injection after log write, 10,000-member removal = 24 statements ≈650 ms with 0 Loader calls on an M1 Pro with PostgreSQL 14.23, fresh-vs-upgraded catalog convergence, repeated upgrade no-op, splitter check). `run.sh` now includes it and uses `--test-timeout`/`--test-force-exit` so it cannot hang.
- `integration/e2e/bootstrap.test.mjs` expectation updated: a removal now takes its own position (re-add at 122, not 121).

Verified green at `8261c703`: `cargo test --workspace --locked` (server/client/sqlite/sim/core), `bash integration/persistence/server/run.sh`, `bash integration/e2e/run.sh`, `bash integration/e2e/todo-run.sh`, `bash integration/generated-api/verify.sh` (at Task 2), prettier, `npm run typecheck`, clippy `-D warnings`. Not run since Task 4: `integration/platform/react-native` (uses the splitter), `integration/action-e2e`, `integration/load-e2e`, `integration/release`, and the full `bash scripts/test.sh`.

## 3. Rulings made so far (verify or overturn, then keep going)

- R1 Task 1 owns capability types/constants/core validation and saved-call exclusion; Task 8 owns runtime enforcement on every transport (HTTP 426 + `protocol.unsupported`, live subscribe refused before ack).
- R2 Load add-only handles accept `{tags}`.
- R3 No manual version bumps. Both pre-major bump flags are enabled; plain `feat:` yields a patch. The coordinated `0.2.0` release needs `feat!:` or a `BREAKING CHANGE` footer, with produced versions/dependency references verified before release.
- R4 Implementation targets `main` at `ba14ee59` with PR #214 merged.
- R5/R6 Interim refusals and a red persistence runner were allowed between Tasks 2–4; both are resolved now.
- R7 `lockChannels` creates no rows; `applyChannelMembers` serializes new-channel head reservation on the channel row.
- R8 Canonical lock order holds per settlement; a multi-mutation push can only surface as a detected deadlock plus whole-transaction retry (document in Task 10).
- R9 Tag-selected records are not record-guarded; the channel lock covers every membership writer.

## 4. Remaining work: Tasks 5–10

Execute the plan's Tasks 5–10 in order; the plan text is the requirements, the spec resolves conflicts. Points the plan cannot know:

- **Task 5 (delivery, claims):** thread `MemberPosition`s out of `settle_changes` (they are currently discarded, `crates/server/src/settlement.rs`); make scans (`crates/server/src/lib.rs`, `live.rs`, `loading.rs`, `packages/postgres/src/sql.mts` SCAN) read `axton_channel_log` **without** joining live members so `remove` rows are delivered; a removal carries only channel/cursor/identity and never calls a Loader; the `to` continuation rule from the plan; fill `memberships` claims on Load pages, receipts and direct-action responses and save them with the idempotent response (replay must not re-enroll). Watch: server `current()`/`current_authority` renormalize saved records but not claims; if identity renormalization can differ, replayed claims fail `validate_memberships`. No fixture pins whether a claim may tie to a Loader-error record.
- **Task 6 (client holds):** build on `ChannelPullPage`/`ChannelBootstrapPage`/`ChannelLiveMessage`; new `crates/client/src/channel_members.rs`; release is cache eviction, not the authoritative-null cascade.
- **Task 7 (epoch fence):** `StoreToken{epoch}` frozen at logical request creation; predicate `held || request_epoch >= evicted_at` before stamp checks.
- **Task 8 (gate + client migration):** enforce `require_capability` on every server route and native entry point (`packages/server/index.mts`, `crates/server/src/lib.rs`, bindings); SDKs advertise the capability. Hazard from Task 1: `PushRequest::encode()` and `SubscribeRequest::encode()` drop `capabilities`, so client paths that decode→encode (`crates/client/src/push.rs:75-77`, `crates/client/src/transport.rs:41-43`) must call `with_capabilities` after the last re-encode. `logical_request` in `protocol.rs` currently has no non-test caller.
- **Task 9 (simulation, e2e):** `crates/sim/src/host.rs` already implements the three host operations minimally (Task 3); expectations that said "a removal allocates no position" were updated. Extend `integration/load-e2e`.
- **Task 10 (docs, measurement, release boundary):** update `website/docs/backend/database.md` fully (Task 4 only fixed the operation/table list), `docs/engineering/architecture/server/engine/loads.md` (lock step), document R8, the Load-holds-channel-locks-across-Loader tradeoff, that a repeated "no-op" upgrade still takes `ACCESS EXCLUSIVE` on `axton_record`, that bulk adds cost one `ensureStamp` per record, and the Oasis adoption handoff (pin `0.1.1` → exactly `0.2.0`). Then `cargo test --workspace --locked`, `bash integration/generated-api/verify.sh`, `bash scripts/test.sh` once.

## 5. Deferred minor findings (triage before merge)

Task 1: upsert decoding is lenient (drops unknown members such as `tags`) while remove is strict, no fixture pins the rule; refusal fixture cases assert only `is_err()`; `protocol.rs` ~1,500 lines (channel section is a clean seam for `channels.rs`); `check_changes` re-validates decoded changes; stale doc comments in `crates/core/src/loads.rs:367-386`; `read_memberships` duplicates the typed route.

Task 2: `effects.mts` exact-`{tag}` check passes an object with a non-enumerable own `tag` plus one enumerable member; options accept inherited `tags` through a prototype getter; frozen-record construction duplicated; extra second argument to `remove()` silently ignored; Load enrollment byte measure changed 77→96 bytes per pair.

Task 3: `check_members` does not refuse a read omitting a known touch recipient; late `ensureStamp` path untested; `explicitKeys` order/distinctness not enforced by the decoder; `encoded_identity` uses `unwrap_or_default`; `publish=false` deltas emitted for unchanged named members on Mutation paths; second `memberships` re-read runs even with no channels; `settle_locked` re-locks the whole set when not a subset of `held` (latent); `host.rs` ~970 lines.

Task 4: upgrade verification cannot detect a skipped copy if the file is misapplied statement by statement; upgrade file duplicates `migration.sql` DDL; untested refusals (kept delta whose log row is `remove`; non-object JSON identity key); `--test-force-exit` can hide leaked handles; `fieldsOf`/`nonEmpty` overlap `checkMembershipRequest`.

## 6. Operating notes

- Commands run from the worktree root. `npm ci` and `bash scripts/build.sh` have been run there; after changing Rust, rebuild the `@axtonjs/native` addon (the napi step in `scripts/build.sh`) before JS runners, because `integration/persistence/server/run.sh` rebuilds only `bindings/node`.
- PostgreSQL 14.23 CLI tools are on PATH; the runners create temporary clusters.
- Subagent commits so far end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; use conventional commit subjects.
- Long single responses were repeatedly cut off on this machine when it slept; keep the Mac awake (`caffeinate -i`) during long runs.
- Do not merge, publish packages, deploy, or change Oasis's pin. Stop at a reviewable branch and a PR description covering intentional wire/host compatibility changes and measured limits.
