# Protocol and SDK boundaries implementation plan

> **For agentic workers:** Execute this plan with superpowers:subagent-driven-development, using GPT-6.1 Sol at medium reasoning for implementation agents. The parent owns integration and acceptance. Steps use checkbox syntax for tracking.

**Goal:** Give Protocols, Frontend SDK and Backend SDK concrete source ownership while preserving released 0.5.4 behavior (`4252ae3ef041321f4b79b00e944e522e15be4d1e`).

**Architecture:** A runtime-independent `axton-protocols` crate owns Sync and bridge DTOs/validation. Client/server interfaces consume it; language-facing SDKs and native adapters retain execution and transport responsibilities.

**Tech Stack:** Rust workspace, serde JSON, Node/TypeScript, Dart, native Node/Dart/mobile bindings, PostgreSQL and SQLite.

## Global constraints

- Preserve all wire and ABI spellings and public SDK package names/exports.
- The protocol crate must not depend on client, server, storage or bindings.
- Preserve transaction, retry, apply, correlation, cancellation and settlement behavior.
- Preserve historical design documents; update living documentation only.
- Preserve the 0.5.3 Server SDK Bootstrap collection fix and the 0.5.4 PostgreSQL batching implementation and regression tests.
- Use the established release process after integration; do not reserve a version or publish as part of this refactor.

## Execution ownership

After scope approval, use independent worktrees based on the same 0.5.4 baseline:

| Lane | Owns | Completion evidence |
| --- | --- | --- |
| Rust contracts and interfaces | `crates/`, Rust binding imports, Cargo manifests and locks | Protocol-only compilation, unchanged contract fixtures, Client/Server tests |
| Frontend SDK | `packages/frontend/` moves and package-local API/Bindings modules, tests and manifests | Preserved public exports, TypeScript/Dart/RN checks against rebuilt native artifacts |
| Backend SDK | `packages/backend/` moves and package-local API/Bindings modules, tests and manifests | Retained host transaction/session, Bootstrap and PostgreSQL batching regressions |
| Parent integration | Root npm lock/workspaces, shared scripts, CI, release metadata, compiler/generated fixture paths and living docs | Complete host gate, installed packages, independent review and PR |

The three source lanes can run in parallel. Each keeps its existing package-root facade and wire contracts while changing internal ownership. Shared path/tooling edits belong to the parent, so agents do not compete over them. Integration may proceed as each lane lands; final acceptance waits for all three. No lane publishes or merges independently.

## 1. Extract the contracts

- [x] Historical baseline: `cargo test -p axton-client -p axton-server --locked` passed on 0.5.2 on 2026-10-08 (`/private/tmp/axton-protocol-bridges-baseline.log`). This does not verify the updated branch or refactor.
- [x] Before source edits, Client/Server baseline passed on `c87188ae` (identical 0.5.4 source), exit 0; `/private/tmp/axton-protocol-bridges-v054-baseline.log`.
- [x] Move `crates/core/src/protocol_v05.rs` and `protocol_v05/{delivery,mutation}.rs` to `crates/protocols/src/sync.rs` and `sync/`, and their existing tests to the new crate.
- [x] Move `crates/client/src/runtime/protocol.rs` to `crates/protocols/src/client_bridge/mod.rs`. Move Operation, Readiness, Report and QuerySpec DTOs with it; keep query execution and mutation state in Client.
- [x] Move the contract portion of `crates/server/src/host.rs`, HandlerContext, stream-member DTOs and structured error carriers into `server_bridge`. Keep `HostExt::call_typed` in `crates/server/src/backend_interface.rs`.
- [x] Add workspace/path dependencies, update imports and lockfiles; retain existing runtime facade paths through re-exports.
- [x] Run `cargo test -p axton-protocols --locked` and the focused Client/Server tests; the final host gate runs workspace tests and strict Clippy.

**Concrete edits:** Add `crates/protocols/Cargo.toml` and `src/lib.rs` with `pub mod sync`, `client_bridge`, `server_bridge`. Depend only on `axton-core`, serde/serde_json and the existing hashing library. Register the crate in the workspace. Update client/server and every workspace consumer of `v05`, including SQLite, simulation and binding tests. Update the separately managed `bindings/node/Cargo.lock` as well as the workspace lock.

Move `OperationKind` and `ReportKind` with their DTOs. Keep pure constructors such as `Report::new` with their type and make their cross-crate visibility explicit; do not add inherent implementations for external types in Client. Move the Server member wire conversions with the member types and retain pure response validation in Protocols. Core must not re-export Protocols: `Protocols → core` is the only dependency direction.

**Interface edits:** Move Client entry implementation to `crates/client/src/frontend_interface.rs`, preserving root exports. Replace the former runtime protocol module with a re-export of `axton_protocols::client_bridge`. Put Host/HostExt and typed invocation in `crates/server/src/backend_interface.rs`; preserve `axton_server::Host` and the existing `host` access path as facades over the interface and extracted types. Keep caller context construction and Engine decisions in Server.

**Contract evidence:** Reuse the existing Client bridge envelope/malformed-command tests from `runtime/protocol.rs`, Sync tests from `core/tests/protocol_v05.rs`, and Server host-operation fixture tests from `server/tests/host_contract.rs`. Rehome pure tests with their source owner, retaining runtime-dependent host invocation tests in Server. Keep fixture payloads unchanged. Verify rejection/error codes, null versus omitted fields, numeric limits and canonical identities; compile the protocol crate without either runtime dependency.

## 2. Separate the SDK source owners

- [x] Move frontend packages to `packages/frontend/` and backend packages to `packages/backend/`.
- [x] Group frontend API modules and bridge/transport implementations separately; retain public package entry points.
- [x] Separate the backend native invocation adapter from API construction; keep the host callback bound to the original transaction/session.
- [x] Update import paths, generated fixtures, package manifests, build/CI runners, Dart tooling and release pack paths. Regenerate the npm workspace lock without changing third-party pins.
- [x] Rebuild the Rust workspace and Node addon, then run TypeScript checks, JS/RN binding suites, Dart checks and the host-operation fixture suite.

**Frontend modules:** Move the Node client's public operations and language objects into `api/`, and `bridge.mts`, transport, connection and live-effect execution into `bindings/`. Apply the same ownership to Dart and React Native. Keep public entry files forwarding the same exports. Keep Dart transaction scopes, callback zones, provisional Call behavior and native finalizers intact.

**Backend modules:** Move public backend construction, handler/Loader types and Stream selectors into `api/`. Put native function wrappers/error translation and the Host callback adapter into `bindings/`; pass retained context through explicit parameters so extraction does not open another transaction or copy mutable session state. The existing `host-contract.mts` becomes the Server bridge mirror used by these bindings, not another protocol definition in the Engine.

Preserve the implementations currently in `packages/server/{effects,index}.mts` and `packages/postgres/src/{persistence,sql}.mts`. Update paths in `integration/persistence/server/{effects,protocol-v05-delivery,persistence-batching}.test.mjs` and keep the batching suite in `integration/persistence/server/run.sh`. These regressions must pass against the moved modules, including the large Bootstrap result and batched SQL behavior.

**Path checklist:** Update root npm workspaces and TypeScript project references; recursive TypeScript/Prettier source selection; package repository metadata; Dart imports and local pubspec paths; native-manifest generation; generated positive/negative fixtures; integration/example runners; CI; release-please extra-file paths; version/inventory assertions; packing and installed-package runners. The Dart pack path gains a directory level, so its tar strip depth must change from 2 to 3. Refresh npm workspace links and lockfile entries without upgrading third-party dependencies. Historical work-log documents retain their bytes.

**Focused commands after the layout move:**

```sh
node scripts/release/version.mjs check
node --test integration/release/*.test.mjs
bash scripts/build.sh
npm run typecheck
node --test integration/bindings/client-js/*.test.mjs packages/frontend/client-js/*.test.mjs
node --test integration/bindings/client-react-native/*.test.mjs
node --test integration/persistence/server/host-contract.test.mjs
```

Use the existing Dart setup and native-library environment from `docs/engineering/testing/running.md`, with its new frontend package path. These focused checks are intermediate diagnostics; final acceptance uses the complete host gate below.

## 3. Align documentation and accept

- [x] Update the living architecture tree/graph/code map and child indexes with the agreed names and actual source owners.
- [x] Add concise Client bridge / Server bridge documents and frontend/backend SDK indexes; verify changed links.
- [x] Run the full host gate and installed-package checks. Inspect the diff for changed wire behavior, duplicate implementations, leaked runtime dependencies and historical-document changes.
- [x] Record executed evidence, commit the refactor and open a reviewable PR. Attach the PR to this conversation; report any remaining release work separately.

**Documentation edits:** `docs/engineering/architecture.md` gets the agreed tree, graph and code map. Protocol child docs cover Sync, Client bridge and Server bridge. Frontend and Backend SDK docs each expose API and Bindings. Rust interface docs link to the actual dispatch/Host implementation. Keep existing website SDK guides and package names working.

**Final commands:**

```sh
bash scripts/test.sh
git diff --check
```

`scripts/test.sh` includes Rust formatting, workspace tests and strict Clippy, real PostgreSQL, joined native host cases, generated APIs, Dart, end-to-end and installed npm/Dart acceptance. Run the complete gate once after integration; repeat affected checks only after changes or failures. CI must pass on macOS and Linux, including its optimized-native checks. Record final executed results only; do not convert a baseline or inspected assertion into a completion claim. Review manifests and archive contents for omitted nested API/Bindings modules. Confirm the protocol dependency graph contains no client/server/storage/native imports and that previously frozen design/spec/plan files did not change.

## Current phase

Steve approved implementation and merge after acceptance on 2026-10-09. All three source lanes are integrated and independently reviewed. The complete local host gate passed, including real PostgreSQL, generated APIs, Dart, end-to-end and installed npm/Dart packages. Final review path findings were corrected and accepted. macOS/Linux CI on the final commit remains required before merge. Progress and final CI evidence live in SYN-23 and PR #269. Package publication and Capso migration are outside this task; a later release follows `docs/engineering/releasing.md`.

Readiness review on 2026-10-09 checked the actual 0.5.4 source and an independent read-only review found no blocking design gap. The existing docs-only branch was rebased onto the baseline above. This is planning evidence, not implementation or test acceptance.

Executed acceptance on 2026-10-09: `bash scripts/test.sh` exited 0 with the integrated implementation and review fixes (`402dbe7a`), recorded in `/private/tmp/axton-protocol-host-gate.log`. The documentation checker also passed independently. Public Node export names matched the 0.5.4 baseline; protocol fixture payloads and third-party npm pins were unchanged. Native/installed package round trips used the moved API/Bindings modules. The final cross-platform CI result is recorded on the PR rather than inferred from these local checks.

The initial Linux PR run exposed a preexisting multi-device test ordering race: device A settlement and device B initial Bootstrap do not fence B subsequent Stream delivery. A deterministic held/released-delivery reproduction confirmed the separate boundaries. Test-only commit `218b2909` explicitly starts B before publication and waits for its canonical state; the focused case and complete affected SDK host suite passed (1/1 and 21/21). Production synchronization code is unchanged.
