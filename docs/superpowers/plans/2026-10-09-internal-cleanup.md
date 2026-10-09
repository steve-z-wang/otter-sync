# Internal Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking. This plan is proposed; do not start implementation before user approval.

**Goal:** Delete redundant internal protocol routes and unreachable current-compiler Load code without changing application behavior.

**Architecture:** Runtime and bindings consume messages from Protocols; the Rust backend interface owns invocation wiring. Current compiler validation rejects retired Load source declarations; retained JSON history validation remains separate and intact.

**Tech Stack:** Rust workspace, existing JSON fixtures, Node/Dart bindings and joined PostgreSQL/SQLite host gate.

**Spec:** [Internal ownership and retired compiler cleanup](../specs/2026-10-09-internal-cleanup-design.md).

## Global Constraints

- Baseline: `71b14195b897bc8a42f19e0a83d59dfe8fc76677`.
- Packages remain at 0.5.4 and the sync discriminator is 5.
- No SDK API/export, generated API, serialization/error, native ABI, table/SQL, dependency-version, storage migration, Capso, publication or deployment changes.
- Preserve current retirement diagnostics and historical JSON contract validation/reconciliation.
- Preserve intentional server/client root exports and `axton_client::v05`; remove the four unnecessary file/module routes listed in the spec.
- Keep original design notes, prior specs/plans, SYN-24 and SYN-25 outside the implementation diff.
- Make no broad `05`/subscription naming sweep or engine algorithm change.

## Task 1: Consume extracted protocols directly

**Files:**
- Delete: `crates/client/src/runtime/protocol.rs`, `crates/server/src/{host,error,stream_members}.rs`.
- Modify: `crates/client/src/runtime/{mod,commands}.rs`.
- Modify: `crates/server/src/{lib,backend_interface,action_results,delivery_plan,mutation_batch,protocol_v05,settlement}.rs`.
- Verify: `crates/protocols/tests/`, `crates/server/tests/`, SQLite/runtime/binding callers and `bindings/node/src/server.rs`.

**Interfaces:**
- `axton_protocols::client_bridge::{Input, Command, Event, ...}` remains the message owner.
- `axton_protocols::server_bridge::{HostRequest, Error, code, ...}` remains the Host message/error owner.
- `backend_interface::{Host, HostResult, HostExt}` remains the invocation interface.
- Public npm/pub.dev/ABI contracts and intentional Rust entry points keep their current shape.

- [ ] Confirm an isolated branch from the actual current main, record the base and check for unrelated changes. Record existing facade contents and in-repository import consumers using `rg`.
- [ ] In `runtime/mod.rs`, replace `pub mod protocol; pub use protocol::*;` with the direct entry-point export:

```rust
pub use axton_protocols::client_bridge::*;
```

In `runtime/commands.rs`, import `Command` and `TransactionCommand` directly from `axton_protocols::client_bridge`. Delete the forwarding file. Existing `runtime::{ClientRuntime, Input, Event, ...}` callers still compile.

- [ ] In `backend_interface.rs`, replace the glob bridge export with explicit imports required by its existing implementation:

```rust
use axton_protocols::server_bridge::{Error, HostRequest, Result, code};
```

Keep the `Host`, `HostResult` and `HostExt` implementations byte-equivalent apart from imports. In each server engine module, import `HostExt` from `crate::backend_interface` and the required DTOs from `axton_protocols::server_bridge` or its `members` module.

- [ ] In `server/lib.rs`, remove the three facade module declarations. Preserve root invocation and error exports:

```rust
pub use axton_protocols::server_bridge::{Error, code};
pub use backend_interface::{Host, HostResult};
```

Import `HostExt` from the backend interface and `Handled`, `Head`, `HostRequest` from Server bridge. Replace explicit `crate::host::Loaded`, `crate::host::HandledAction` and `crate::stream_members::*` paths with owner imports. Delete the three forwarding files.

- [ ] Search Rust source/tests and current docs for `runtime::protocol`, `crate::host`, `crate::error`, `crate::stream_members` and the four deleted file paths. Update actual current consumers; leave frozen records as historical context. Do not replace TypeScript `bindings/host.mts`, which contains actual dispatch logic.
- [ ] Run:

```sh
cargo fmt --all --check
cargo test -p axton-protocols -p axton-server -p axton-client -p axton-sqlite -p axton-binding --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

Expected: fixtures, admission, delivery, authority/settlement and runtime tests pass; no warnings. Compare `fixtures/protocol/` and dependency versions against the base: no changes. Commit this task.

## Task 2: Remove unreachable current Load compilation

**Files:**
- Modify: `crates/compiler/src/{validate,generate}.rs`.
- Verify unchanged: `crates/compiler/src/{parse,main,history}.rs`, `crates/core/src/{loads,schema}.rs`.
- Test: existing `crates/compiler/tests/{loads,cli,history,compiler,v04_facade}.rs`, `crates/core/tests/loads.rs`, existing generated API fixtures.

**Interface:** `compile(source)` produces the same current descriptors and SDK code. Retired source syntax still fails with the current diagnostic; retained Load JSON remains readable and version-fenced.

- [ ] Reconfirm `validate()` rejects `!d.loads.is_empty()` before successful validation. Read the `saved_load_descriptor` test helper in `compiler/tests/history.rs`: it constructs historical JSON through Query compilation, not through the validated Load emitter.
- [ ] Run the existing checks before deletion:

```sh
cargo test -p axton-compiler --test loads --test cli --test history --test compiler --test v04_facade --locked
cargo test -p axton-core --test loads --locked
```

Expected: current Load/CLI rejection and historical descriptor/history behavior pass. These are existing contract checks; do not invent tests that merely count removed symbols.

- [ ] In `validate.rs`, delete `Validated.loads`, the `Load` struct, the unused `LoadDecl` import, `validate_load`, and the later collection loop/initializer field. Preserve the early gate exactly:

```rust
if !d.loads.is_empty() {
    return Err("Load declarations were removed in 0.4; use Bootstrap or a named Query".into());
}
```

Update the current method-namespace comment to describe Mutations and Queries. Keep `loads: vec![]` when constructing `axton_core::Schema`; that retained type still has a real history field.

- [ ] In `generate.rs`, delete its validated `Load` import, both `if !v.loads.is_empty()` emission blocks and private `load`/`loads` helpers. Remove any resulting unnecessary `mut` on the local schema value. Keep current descriptor members and their ordering unchanged.
- [ ] Search the compiler for remaining `Validated.loads`, `validate::Load` and `validate_load` references; expect none. Preserve Parse's `LoadDecl`/recognition, CLI flag refusals, Core history descriptors and `reconcile_load_history` unchanged.
- [ ] Rerun the same existing checks and `cargo test -p axton-compiler --locked`; expect all pass. Run `bash integration/generated-api/verify.sh` with the documented dependencies; inspect regenerated fixtures and expect no API/descriptor changes. If supported output changes, investigate before committing; do not refresh expected snapshots to hide it.
- [ ] Inspect and commit the removal as its own reviewable change.

## Task 3: Remove orphan comments and correct current ownership references

**Files:**
- Modify comments only: `crates/client/src/runtime/mod.rs`, `frontend_interface.rs`, `queue.rs`, `bootstrap.rs`, `subscriptions.rs`; any stale Load/store-policy comments in the exact touched Host contract files only when their replacement meaning is verified.
- Modify current owner references: `docs/engineering/architecture.md`, the Runtime/Backend interface/protocol pages affected by Task 1, and compiler Validate/Generate pages affected by Task 2. Use final canonical paths after the documentation PR.

**Interface:** No struct field, enum variant, status message, SQL, wire tag or algorithm changes.

- [ ] Remove orphan blocks in `runtime/mod.rs` about the deleted Load worker and removed open/store-hook entry points. Describe observers as current bound Store status/Bootstrap/watch waiters.
- [ ] Remove queue comments left above deleted frozen-Push methods. Keep the actual format-5 queue, operation order, accepted companions and direct-write journal descriptions.
- [ ] Correct `frontend_interface.rs` comments for Mutation operations, session callbacks and completions by tracing `runtime/transactions.rs`, `settlement05.rs` and their existing tests. Avoid claiming that completions are never persisted or that incoming custom store hooks remain available.
- [ ] In Bootstrap/status DTO comments, distinguish retained serialized field names from current S/B/C-derived status. Keep all variants and `subscription_id` fields; their presence in current observers/SDKs is verified usage.
- [ ] Update current source-owner links to Protocols and Backend interface; remove facade filenames from the live code map. Explain that retained Load JSON history and retired source rejection are separate compiler responsibilities. Preserve frozen design notes.
- [ ] Review `git diff` to ensure this task changes comments/docs only. Run `git diff --check` and the read-only path audit in the documentation plan; verify changed heading anchors. Commit this task.

## Task 4: Validate assembled behavior and prepare the cleanup PR

**Files:** Verify the complete branch; no new runtime component or fixture expectation is added.

**Interface:** Accepted refactor preserves contracts across Rust, SDKs, native adapters, real PostgreSQL, SQLite and installed artifacts.

- [ ] Rebase onto the current main after the documentation PR if it has merged. Resolve owner-link changes to the new canonical pages; do not reintroduce moved documents.
- [ ] Run `bash scripts/test.sh` once on the final source. It rebuilds native carriers, runs Rust tests/strict Clippy, protocol/SDK fixtures, retained Host tests, real PostgreSQL persistence, joined host/capacity, Dart and installed-package checks. Record the final tested commit and exit code.
- [ ] If the unchanged release inventory check trips over older nested worktrees (SYN-25), use a clean isolated checkout for the same gate. Preserve those worktrees and report the tooling limit; do not broaden this PR or skip a genuine root version mismatch.
- [ ] Compare against the base: `fixtures/protocol/`, published manifests/exports, ABI symbols, SQL/DDL and generated fixture content must retain their contracts. Review any generated diff individually; no expected runtime behavior changes are accepted.
- [ ] Obtain review of the final diff for both owner boundaries and behavior preservation. Verify Protocols has no reverse runtime/storage dependency and Host invocation retains the caller's transaction/session.
- [ ] Prepare one internal cleanup PR with the deletion inventory, current-vs-historical Load distinction and actual validation evidence. Wait for required final CI on the exact reviewed head; disclose any platform check not executed. Publish no package and migrate no application as part of this PR.

## Coordination

Facade cleanup and compiler cleanup touch different modules and can be implemented independently in separate worktrees if parallel execution is later authorized. Integrate them before the final gate. The documentation lane can run independently; merge its canonical moves first, then update only the affected source references here.

No implementation checks have been executed while writing this plan.
