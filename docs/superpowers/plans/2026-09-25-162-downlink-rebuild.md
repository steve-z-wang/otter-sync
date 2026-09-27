# Downlink lifecycle across replica rebuild Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep a connected Downlink lane active through an in-process replica rebuild while fencing all old socket and HTTP results from the new replica.

**Architecture:** `RuntimeHost` resets its existing Rust `DownlinkWorker` after successful `Client::rebuild`. The worker preserves correlation allocators and running/paused intent while discarding replica-bound transient state; its next pump tells each SDK host to abandon old I/O before new actions. SDK rebuild completion wakes the existing lane.

**Tech Stack:** Rust client and shared binding, SQLite test harness, TypeScript client runtime, Dart client runtime and scripted host tests.

## Global Constraints

- Follow the [design](../specs/2026-09-25-162-downlink-rebuild-design.md), [client connection architecture](../../engineering/architecture/client/connection/controller/downlink-worker.md), [guarantees](../../engineering/guarantees.md) D9/D10, and [testing strategy](../../engineering/testing/strategy.md).
- Work in an isolated `codex/` branch. Keep repository documentation in English.
- Preserve the Bootstrap request and barrier contracts. Rebuild keeps Channel names but gives them fresh identities and no boundary. The terminology rename is #152.
- Keep the public `connect`/`rebuild` signatures and wire protocol unchanged. Reset only after a successful native rebuild.
- Socket epochs and ordinary/Bootstrap HTTP IDs must not be reused across resets. Old events must be inert even when they arrive after new I/O starts.
- Test executed commands separately from source inspected. Initial design preparation ran no build or test.

## File map

| File | Responsibility |
| --- | --- |
| `crates/client/src/connection.rs` | Expose a narrow running/paused lifecycle snapshot or reset helper. |
| `crates/client/src/downlink_worker.rs` | Reset replica-bound state in place; preserve ID allocators; emit one ordered host reset action. |
| `bindings/common/src/lib.rs` | Call worker reset after successful native rebuild. |
| `bindings/common/tests/session.rs` | Exercise rebuild through the real binding handle and stale events. |
| `crates/sqlite/tests/downlink_worker.rs` | Assert worker scheduling, queue clearing, and fencing. |
| `packages/client-js/connection.mts`, `packages/dart/lib/src/connection.dart` | Execute the reset action by aborting old host I/O. |
| `packages/client-js/runtime.mts`, `packages/dart/lib/src/client.dart` | Wake the connected Downlink lane on successful rebuild. |
| `integration/bindings/client-js/connection.test.mjs`, `packages/dart/test/connection_test.dart` | Verify host abort and lost-wake behavior. |
| `docs/engineering/architecture/client/connection/controller/downlink-worker.md` | Record the implemented rebuild lifecycle and evidence. |

## Task 1: Failing native regression and worker reset

**Interfaces:** Add `DownlinkWorker::reset_for_rebuild(&mut self)` and `DownlinkAction::Reset` (serialized `{"type":"reset"}`). The method does no database I/O and returns no action directly; the first subsequent `next` emits `Reset` exactly once, before any `Open` or `Request`. Keep `DownlinkEvent` unchanged. Add only the narrow `ConnectionDriver` lifecycle accessor/reset needed by the worker.

- [ ] In `bindings/common/tests/session.rs`, add a test using the incompatible-schema fixture pattern already in `incompatible_schema_reports_pending_work_and_rebuild_switches_files`. Register `book`, `downlink(start)`, record the old `open.epoch`, and force an ordinary catch-up request. After a successful `rebuild`, call `downlink(next)` without another `start`; require `reset` followed by a new `open` with a greater epoch. Repeat with a Bootstrap request in flight and require its next ID to be greater than the old ID. Use the existing `downlink`/`pump` helpers to assert actions and `status` to inspect new replica cursors. Expected before implementation: the pump is idle because the worker was defaulted.
- [ ] Add worker-level cases in `crates/sqlite/tests/downlink_worker.rs` that enqueue an old page, acknowledgement and HTTP answer before reset; verify none applies after reset, and `Reset` precedes new I/O. Test a second rebuild in the same process so monotonicity is not accidentally limited to one reset.
- [ ] Run `cargo test -p axton-binding --test session --locked` and `cargo test -p axton-sqlite --test downlink_worker --locked`; confirm the new assertions fail for the current behavior. `bindings/common/Cargo.toml` names the crate `axton-binding`.
- [ ] Implement the reset in `DownlinkWorker`: snapshot running/paused intent; clear `session`, `control`, `pages`, `active`, `bootstrap`, `loaded`, `loading`, `reopened`, `again`, `closing`, and `expected`; preserve `LiveSession` epoch and `requests` counters; initialize the driver dirty/due for running intent and restore pause if needed; set a one-shot `reset_host` flag. At the top of `pump`, take that flag and push `DownlinkAction::Reset` before any other action. Do not reuse `Start`, which assumes a closed old host lane, and do not emit a `Close` that can race a new session.
- [ ] Replace `e.downlink = DownlinkWorker::default()` in `bindings/common/src/lib.rs` with `e.downlink.reset_for_rebuild()` after `e.client.rebuild(discard)?` returns. Keep `e.cycle = SyncCycle::default()` as currently specified outside this issue. Do not reset on error. Ensure emitted epoch/request counters stay within the binding's safe JSON integer range and fail explicitly at exhaustion instead of wrapping.
- [ ] Run the two focused Rust test commands again; require the newly added tests and existing worker/session suites to pass. Commit this independently testable native change.

## Task 2: Host I/O reset and rebuild wake

**Interfaces:** Both Downlink host executors recognize `{"type":"reset"}`. It cancels the current socket, ordinary pull and Bootstrap pulls, clears host status for the old session, rotates Bootstrap cancellation to a fresh token, and leaves the lane loop running. SDK rebuild completion publishes the existing Channel wake notification after old subscription handles are invalidated.

- [ ] Add scripted TypeScript and Dart host tests in `integration/bindings/client-js/connection.test.mjs` and `packages/dart/test/connection_test.dart`: dispatch `reset` before `open`; assert the old socket and both HTTP cancellations fire, no old callback is reported as an application error, and the new `open` stays active. Also hold `next` at its idle/sleep boundary, complete a rebuild through each SDK's existing rebuild test fixture, and assert the Downlink lane pumps without another `connect` or `start`.
- [ ] Run `node --test integration/bindings/client-js/connection.test.mjs integration/bindings/client-js/rebuild.test.mjs` and `(cd packages/dart && dart test test/connection_test.dart test/client_test.dart)`; confirm the new cases fail. Follow [running tests](../../engineering/testing/running.md): `npm ci` and `bash scripts/build.sh` before JS tests if artifacts are absent; `(cd packages/dart && dart pub get)` and `AXTON_DART_LIBRARY` setup before Dart tests. Do not reinstall if dependencies and native artifacts are already current.
- [ ] In `packages/client-js/connection.mts`, handle `Reset` before any new `Open`: abandon and clear the current session, abort the old `loading` controller, replace it, reset `outstanding`, and emit the ended/requests status needed by the existing subscription projection. Reuse `abandon` so callback behavior matches a normal local abort. In Dart `DownlinkLane._execute`, do the same with `_abandon`, `_abandonLoads`, a new `Completer<void>()`, and `_outstanding`.
- [ ] In `packages/client-js/runtime.mts`, emit the existing `channels` wake after `#subscriptions.rebuilt()` and successful native rebuild. In `packages/dart/lib/src/client.dart`, add `_channels.add(null)` after `_subscriptions.rebuilt()` and successful native rebuild. These notifications must be absent from the error path. If a connection is absent, the notifications are harmless; a later `connect` sends its normal `start`.
- [ ] Run the same focused JS and Dart commands again. Inspect action order and make sure a late cancelled callback is ignored by the native fence. Commit the host change.

## Task 3: Full regression matrix and lasting documentation

- [ ] Extend `bindings/common/tests/session.rs` with running, paused, stopped, no-Channel, and failed-rebuild cases. For running: old `message`, `closed`, `overflow`, `response`, and `failed` events both before and after the new handshake cause no cursor or status change; the new handshake initializes carried `book` at its acknowledged head and can advance afterward. For paused: `reset` aborts old work, `next` emits no `open`/`request`, and `resume` opens once. For stopped: rebuild remains inert until an explicit `start`. For failed rebuild: old epoch/request work remains valid and no `reset` is emitted. Include old Bootstrap response and failure so neither affects the fresh replica.
- [ ] Update `docs/engineering/architecture/client/connection/controller/downlink-worker.md` with the implemented in-process rebuild transition and link the regression evidence. Keep the existing statement that ordinary socket replacement does not cancel an in-flight Bootstrap request; distinguish replica replacement, which does.
- [ ] Run `cargo fmt --all --check`, `cargo test -p axton-binding --test session --locked`, `cargo test -p axton-sqlite --test downlink_worker --locked`, `node --test integration/bindings/client-js/connection.test.mjs integration/bindings/client-js/rebuild.test.mjs`, and `(cd packages/dart && dart test test/connection_test.dart test/client_test.dart)` from the repository root. If all required host tools are available, run `bash scripts/test.sh` as the repository gate. Record every command, result and any unavailable prerequisite in the handoff. Check documentation relative links and headings. Commit tests and documentation.

## Self-review before implementation handoff

- [ ] Confirm each design requirement maps to a test: successful running liveness, pause/stop preservation, old socket and both HTTP classes fenced, failed rebuild inert, host abort, and fresh Channel origin/Bootstrap semantics.
- [ ] Check the serialized `Reset` action is handled in both hosts and always precedes `Open`/`Request`, including when a persisted Bootstrap barrier commits during the first pump.
- [ ] Scan for `TBD`, `TODO`, mismatched method/action names, wrong crate names, and a test command that assumes absent tools are installed.
