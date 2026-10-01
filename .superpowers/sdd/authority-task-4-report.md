# Authority Task 4 implementation and focused evidence

## Scope and baseline

- Worktree: `/Users/stevewang/Github/axton/.worktrees/scope-membership-api-design`.
- Branch: `codex/local-authority-delivery`.
- Task 4 baseline: `c2e1da2fe49685c9fc1eb97f246601765d8370cc`.
- Governing inputs: `.superpowers/sdd/authority-task-4-brief.md`, approved `docs/superpowers/specs/2026-10-01-local-authority-delivery-design.md`, current AGENTS.md, architecture/guarantees, testing strategy/running guide, writing conventions and TDD skill. `/private/tmp/authority-boundary-inventory.md` was preparation only; current files were inspected before editing.
- Core/client authority and original-layout migration (Task 2), protocol/server and stopped-writer PostgreSQL repair (Tasks 1/3) were accepted inputs. This task does not replace their source or historical fixtures. Root built current native artifacts successfully beforehand; this worker used them and performed no native rebuild.
- No Most Days/Oasis, package/version/pin, merge/push, publication or application Model/action contract changes. Server tracking, pending Held state and local-replica history remain.

## RED and oracle adaptation

The first added historical assertion proves row and stamp retention after the final historical Remove, before the previous final-hold `None` assertion. The initial focused run used the baseline executable expectations; it observed authority 9/9 passing and historical removals 1/8 passing, exit 101 (`/private/tmp/authority-task4-red.log`). Seven failures showed retained row/pending/direct state against obsolete final-source-loss or tracking-based expectations; one failure separately showed the old capability expectation. The new positive retention/stamp assertion passed, and the immediately following old `None` assertion failed. Core authority was already implemented by Task 2; RED therefore diagnoses the simulation's old oracle, not missing core behavior.

Active sim HTTP capability helpers were updated to `stream-authority-v1` before the subsequent behavioral runs. Capability refusal was not used as a substitute for the retained-row/oracle mismatch. Distribution's first GREEN attempt exposed two additional old expectations: retained `v1` and retained `again` versus `None`; these were corrected to delivered authority while keeping routing/head assertions.

The generated loss invariant is stronger: a previously visible row can disappear only with delivered canonical deletion evidence at its retained stamp. The obsolete client holding-table exemption was removed rather than suppressing the invariant. Generated saved Load replay expectations use the previously delivered content/stamp and the saved page's authority; empty server tracking never implies empty client cache. Seed loops, generated operations, duplication/reordering/drops, crashes, pending/direct work, replay-byte/Handler checks, equal-stamp diagnostics, failures and ordinary bootstrap cancellation remain.

## Behavioral evidence added or strengthened

- Historical final Remove preserves row/stamp, and repaired current Loader null later supplies absence. Duplicate/reordered Remove and offline reopen retain the affected row and unrelated A/C rows; subsequent newer null and another reopen preserve canonical absence.
- New Fetch/Stream null cross-path test exercises both directions with older positive delivery and crash/reopen, retaining the null stamp and proving no client holding table.
- Additional Action/real-host receipt/explicit Bootstrap canonical-null test and malformed null Load refusal pass in the final focused run recorded below. The typed N2 Load contract requires state-bearing outputs; changing that contract to permit successful null Load outputs is outside this cutover. Old positive Load pages may complete without resurrecting already newer null.
- JS standing cleanup now deliberately throws after deleting children, then checks rollback of incoming standing absence and child deletion before explicit successful retry. The rejected Fetch matches `fetch.store_failed` and its intentional `cleanup refused` cause. Pending/companion/Composition words, independent reachability, Query permission gates and later child rematerialization remain asserted.
- JS native Stream hook test proves fresh delivery without holding SQL, cursor/authority/child-cleanup rollback, explicit retry commit, unsubscribe retention and no extra hook invocation.
- Dart Load hook case proves canonical authority/child cleanup/page count rollback, exact `load.hook_failed`, successful retry, device-local Composition preservation, no holding table and unsubscribe retention. Dart live cases retain Remove-delivered content until newer null and retain previously delivered initial/live rows after resubscribe with no implicit bootstrap; obsolete HTTP completion remains fenced.
- JS/Dart and React Native active transport expectations now advertise authority capability, including existing frozen resend/reopen assertions. Native runtime boundaries are unchanged; there is no SDK ownership implementation.
- Native and HTTP bootstrap requests with explicit old capability are refused before any host effect.
- PostgreSQL saved historical Load replay compares complete nested continuation data, top-level original claims, unchanged heads/tracking/log rows, Handler count and Loader count. Saved Action replay compares complete nested business result and top-level metadata, no Handler/Loader rerun, and exact saved request/response text.
- Original-v0.2 generated JS reopen verifies local authority marker, absent holding table, preserved cursor/application fields, exact original logical frozen request and byte-stable decorated retries. The original binary/SQL/logical fixtures are unchanged.

## Commands and results

All commands were run from the shared worktree unless noted. Logs are temporary evidence, not release artifacts.

| Command | Result |
| --- | --- |
| `cargo test -p axton-sim --locked --test authority --test historical_removals --test upgrade` | Initial RED exit 101: authority 9 pass; historical 1 pass/7 fail; Cargo stopped before upgrade. `/private/tmp/authority-task4-red.log` |
| `cargo test -p axton-sim --locked --test authority --test historical_removals --test upgrade --test distribution --test bootstrap --test stream_tracking --test push` | Final original focused group exit 0, 57 pass: authority 10, bootstrap 4, distribution 11, historical 8, push 11, stream_tracking 10, upgrade 3. `/private/tmp/authority-task4-sim-final.log` |
| `node --test integration/bindings/client-js/subscriptions.test.mjs integration/bindings/client-js/loads.test.mjs integration/bindings/client-js/actions.test.mjs integration/bindings/client-js/live.test.mjs integration/bindings/client-react-native/network.test.mjs integration/bindings/client-react-native/live.test.mjs` | Exit 0, 95 pass at initial active-negotiation boundary. `/private/tmp/authority-task4-js.log` |
| `node --test integration/bindings/client-js/subscriptions.test.mjs` | Exit 0, 25 pass including added hook/authority/cursor rollback. `/private/tmp/authority-task4-js-cursor.log` |
| `node --test integration/bindings/client-js/loads.test.mjs` | Exit 0, 12 pass including exact intentional Fetch cleanup-refusal cause. `/private/tmp/authority-task4-js-hook.log` |
| `node --test integration/persistence/server/protocol-admission.test.mjs` | Exit 0, 5 pass including explicit bootstrap old-capability native/HTTP no-effect case. `/private/tmp/authority-task4-admission.log` |
| `AXTON_LIBRARY=<worktree>/target/debug/libaxton_dart.dylib AXTON_DART_LIBRARY=<same> dart test test/subscriptions_test.dart test/loads_test.dart test/actions_test.dart test/live_test.dart test/admission_test.dart test/fetch_test.dart` from `packages/dart` | Exit 0, 86 pass including live changes and new cleanup case. `/private/tmp/authority-task4-dart.log` |
| Same library environment, `dart test test/loads_test.dart` from `packages/dart` | Exit 0, 9 pass after tightening intentional hook-error match. `/private/tmp/authority-task4-dart-hook.log` |
| `dart format test/actions_test.dart test/admission_test.dart test/fetch_test.dart test/live_test.dart test/loads_test.dart` from `packages/dart` | Exit 0; 5 files checked, 2 formatted. `/private/tmp/authority-task4-dart-format.log` |
| `dart analyze` from `packages/dart` | Exit 0, no issues. `/private/tmp/authority-task4-dart-analyze.log` |
| `bash /private/tmp/authority-task4-pg.sh` | Exit 0: Actions 11 pass, Loads 20 pass on disposable real PostgreSQL, prebuilt native. `/private/tmp/authority-task4-pg.log` |
| `cargo fmt --package axton-sim` | Exit 0; only owned sim formatting changed. |
| `git diff --check` | Exit 0 before root final assembled gate. |

The focused PostgreSQL script uses existing generated Prisma client/dependencies and prebuilt native. It sources `scripts/env.sh`, creates a disposable cluster with `initdb`/`pg_ctl` on an available loopback port, exports its DATABASE_URL, then executes exactly:

```sh
node --test --test-timeout=300000 --test-force-exit integration/persistence/server/actions.test.mjs
node --test --test-timeout=300000 --test-force-exit integration/persistence/server/loads.test.mjs
```

It stops and deletes the cluster on exit. It deliberately does not run the standard server runner's native build/setup prelude because root already provided current artifacts. Root owns `bash integration/persistence/server/run.sh` within the complete host gate.

## Failures, noise and practical limits

- First JS socket run in the sandbox failed `listen EPERM`; authorized local-server escalation ran the suites successfully. These were infrastructure refusals, not assertion RED.
- One edit command was initially launched in `packages/dart` while using root-relative paths and failed before editing; corrected from root. That attempt's Dart invocation also used the wrong library path. Neither is behavioral evidence.
- Initial new Dart hook test had incorrect matcher arguments/LoadStatus property names; corrected before the passing run. One adapted live expectation incorrectly called retained e56 `initial`; actual pre-resubscribe live payload was `live`, so the corrected expectation verifies exact prior content.
- Initial new Rust cross-path helper used Option state and a record-only PullPage decoder; corrected to canonical JSON state and StreamPullPage before its passing run. The later matrix also initially used a nonexistent FetchResponse Deserialize implementation, a non-null void Action result and a handcrafted receipt the sim host had not answered. Each was corrected as a harness error: typed FetchResponse, null void result and real host delivery/receipt; the final sim invariant check remains enabled and passes. Initial JS retry attempted to reuse a faulted socket; rollback assertions passed, but progress did not advance. The successful explicit apply retry now verifies the transaction directly rather than assuming connection retry timing.
- PG focused output includes the existing diagnostic recording serializable race commit orders; no unexpected errors were hidden. Existing negative Loader tests and dependency setup/npm allow-script warnings in root's broader host log are expected only where their tests/setup explain them; this worker did not clean or suppress those outputs. Focused SDK/rust GREEN logs contain no unexpected failures.
- Earlier dated test observations and `docs/engineering/channel-membership-release.md` remain explicitly historical. Original migration/SQL/binary/protocol fixtures and saved historical bytes were not rewritten. Active prose was updated; dated design/task records were not refreshed.
- `website/docs/frontend/sync.md` had no actual typed hook snippet; the existing compilable hook infrastructure is `client-api.md#react-to-incoming-records`. The guide reuses/links it and explains the application-owned access Model policy, without adding schema/compiler features or implying automatic pending preservation from arbitrary deletion.

## Source freeze and remaining root gates

Executable fixture/source edits were frozen after focused PG/SDK/sim checks. Root authorized one later `authority.rs` test-only exception to add Action/receipt/Bootstrap and delayed Load coverage; the exact final result is appended below. No native implementation changed. Root owns regeneration and review of generated diffs: no action/Model version or application payload changes are authorized.

Remaining whole-feature gates belong to root: assembled `bash scripts/test.sh`, generated API and Action/Load/e2e verification inside it, final docs examples/link/site checks, final formatting/clippy/diff/status, independent final review against approved required evidence, and integration. Task 4 focused checks do not mark the whole feature complete or authorize merge/publication.

## Owned paths

- `crates/sim/src/host.rs`
- `crates/sim/src/invariants.rs`
- `crates/sim/tests/authority.rs`
- `crates/sim/tests/bootstrap.rs`
- `crates/sim/tests/distribution.rs`
- `crates/sim/tests/historical_removals.rs`
- `crates/sim/tests/push.rs`
- `crates/sim/tests/stream_tracking.rs`
- `docs/engineering/architecture/client/connection/controller/downlink-worker.md`
- `docs/engineering/architecture/client/engine/README.md`
- `docs/engineering/architecture/client/engine/pull.md`
- `docs/engineering/architecture/client/storage/README.md`
- `docs/engineering/architecture/client/storage/reconciliation.md`
- `docs/engineering/architecture/protocol/actions.md`
- `docs/engineering/architecture/protocol/common.md`
- `docs/engineering/architecture/protocol/loads.md`
- `docs/engineering/architecture/protocol/pull.md`
- `docs/engineering/architecture/protocol/push.md`
- `docs/engineering/architecture/protocol/subscriptions.md`
- `docs/engineering/architecture/server/engine/publish.md`
- `docs/engineering/architecture/server/persistence.md`
- `docs/engineering/guarantees.md`
- `docs/engineering/testing/components/client.md`
- `docs/engineering/testing/components/server.md`
- `integration/action-e2e/action.test.mts`
- `integration/action-runtime-ts/backend.test.mts`
- `integration/bindings/client-js/live.test.mjs`
- `integration/bindings/client-js/loads.test.mjs`
- `integration/bindings/client-js/subscriptions.test.mjs`
- `integration/bindings/client-react-native/live.test.mjs`
- `integration/bindings/client-react-native/network.test.mjs`
- `integration/e2e/protocol-fixture.mjs`
- `integration/load-e2e/load.test.mts`
- `integration/persistence/client/reopen.mts`
- `integration/persistence/server/actions.test.mjs`
- `integration/persistence/server/loads.test.mjs`
- `integration/persistence/server/protocol-admission.test.mjs`
- `packages/dart/test/actions_test.dart`
- `packages/dart/test/admission_test.dart`
- `packages/dart/test/fetch_test.dart`
- `packages/dart/test/live_test.dart`
- `packages/dart/test/loads_test.dart`
- `website/docs/backend/api.md`
- `website/docs/backend/database.md`
- `website/docs/backend/deployment.md`
- `website/docs/concepts.md`
- `website/docs/frontend/sync.md`
- `.superpowers/sdd/authority-task-4-report.md`

## Final test-only exception and self-review

Root authorized one final `crates/sim/tests/authority.rs` addition after executable freeze. Exact command `cargo test -p axton-sim --locked --test authority` exited 0, **11 passed**, in `/private/tmp/authority-task4-canonical-matrix-final.log`. `cargo fmt --package axton-sim` and `git diff --check` exited 0 after the exact final edit. The added test loops Action, real host receipt and explicit Bootstrap null sources, then delivers an older positive Load (successful completion without resurrection) and Fetch, rejects a null Load as `load.protocol_invalid` with zero page progress and unchanged stamp, and crashes/reopens before settling and checking the existing sim invariants.

Root's concurrently running assembled workspace gate had compiled the earlier 10-test authority target before this final addition. The focused final 11-test target covers the new exact source; root's later current-source clippy/format gate covers compilation/lint. This boundary is explicit and does not claim the new test ran in the earlier compiled host binary. Total distinct focused sim tests are now **58** (previous 57 plus one); existing source/generated invariants were not removed.

Self-review read owned executable and documentation diffs, verified old capability literals remain only explicit unsupported ingress or saved historical inputs, checked unchanged original fixture files and checked capability headroom against source (39 bytes). Historical `channel-membership-release.md` intentionally still describes its pre-Scope snapshot. Standing cleanup failure matches intentional codes/cause, and the new Load malformed-null case preserves the existing state-bearing output contract rather than widening it. Server tracking SQL remains; no active client holding SQL is left in the changed boundaries. No unresolved functional concern from focused evidence; final assembled host/generated review and independent final review remain root gates.

## Root-provided documentation verification

Root ran `mkdocs build --strict`, exit 0, and verified 18 HTML pages / 1,084 internal links/assets. Root's changed Markdown local path/anchor checker examined 616 links, exit 0. These are root-provided results, not commands executed by this worker. The assembled host and final independent review remain pending at this commit.
