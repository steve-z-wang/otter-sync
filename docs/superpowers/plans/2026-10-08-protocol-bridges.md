# Protocol and SDK boundaries implementation plan

> **For agentic workers:** Execute this plan inline with superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Give Protocols, Frontend SDK and Backend SDK concrete source ownership while preserving 0.5.2 behavior.

**Architecture:** A runtime-independent `axton-protocols` crate owns Sync and bridge DTOs/validation. Client/server interfaces consume it; language-facing SDKs and native adapters retain execution and transport responsibilities.

**Tech Stack:** Rust workspace, serde JSON, Node/TypeScript, Dart, native Node/Dart/mobile bindings, PostgreSQL and SQLite.

## Global constraints

- Preserve all wire and ABI spellings and public SDK package names/exports.
- The protocol crate must not depend on client, server, storage or bindings.
- Preserve transaction, retry, apply, correlation, cancellation and settlement behavior.
- Preserve historical design documents; update living documentation only.
- Prepare for 0.5.3 through the established release process; do not publish as part of this refactor.

## 1. Extract the contracts

- [x] Run baseline `cargo test -p axton-client -p axton-server --locked` (exit 0 on 2026-10-08; `/private/tmp/axton-protocol-bridges-baseline.log`). This check preceded source changes.
- [ ] Move `crates/core/src/protocol_v05.rs` to `crates/protocols/src/sync.rs`, and its existing tests to the new crate.
- [ ] Move `crates/client/src/runtime/protocol.rs` to `crates/protocols/src/client_bridge/mod.rs`. Move Operation, Readiness, Report and QuerySpec DTOs with it; keep query execution and mutation state in Client.
- [ ] Move the contract portion of `crates/server/src/host.rs`, HandlerContext, stream-member DTOs and structured error carriers into `server_bridge`. Keep `HostExt::call_typed` in `crates/server/src/backend_interface.rs`.
- [ ] Add workspace/path dependencies, update imports and lockfiles; retain existing runtime facade paths through re-exports.
- [ ] Run `cargo test -p axton-protocols --locked`, workspace tests and strict Clippy.

**Concrete edits:** Add `crates/protocols/Cargo.toml` and `src/lib.rs` with `pub mod sync`, `client_bridge`, `server_bridge`. Depend only on `axton-core`, serde/serde_json and the existing hashing library. Register the crate in the workspace. Update client/server and every workspace consumer of `v05`, including SQLite, simulation and binding tests. Update the separately managed `bindings/node/Cargo.lock` as well as the workspace lock.

**Interface edits:** Move Client entry implementation to `crates/client/src/frontend_interface.rs`, preserving root exports. Replace the former runtime protocol module with a re-export of `axton_protocols::client_bridge`. Put Host/HostExt and typed invocation in `crates/server/src/backend_interface.rs`; preserve `axton_server::Host` and the existing `host` access path as facades over the interface and extracted types. Keep caller context construction and Engine decisions in Server.

**Contract evidence:** Reuse the existing Client bridge envelope/malformed-command tests from `runtime/protocol.rs`, Sync tests from `core/tests/protocol_v05.rs`, and Server host-operation fixture tests from `server/tests/host_contract.rs`. Rehome pure tests with their source owner, retaining runtime-dependent host invocation tests in Server. Keep fixture payloads unchanged. Verify rejection/error codes, null versus omitted fields, numeric limits and canonical identities; compile the protocol crate without either runtime dependency.

## 2. Separate the SDK source owners

- [ ] Move frontend packages to `packages/frontend/` and backend packages to `packages/backend/`.
- [ ] Group frontend API modules and bridge/transport implementations separately; retain public package entry points.
- [ ] Separate the backend native invocation adapter from API construction; keep the host callback bound to the original transaction/session.
- [ ] Update import paths, generated fixtures, package manifests, build/CI runners, Dart tooling and release pack paths. Regenerate the npm workspace lock without changing third-party pins.
- [ ] Rebuild the Rust workspace and Node addon, then run TypeScript checks, JS/RN binding suites, Dart checks and the host-operation fixture suite.

**Frontend modules:** Move the Node client's public operations and language objects into `api/`, and `bridge.mts`, transport, connection and live-effect execution into `bindings/`. Apply the same ownership to Dart and React Native. Keep public entry files forwarding the same exports. Keep Dart transaction scopes, callback zones, provisional Call behavior and native finalizers intact.

**Backend modules:** Move public backend construction, handler/Loader types and Stream selectors into `api/`. Put native function wrappers/error translation and the Host callback adapter into `bindings/`; pass retained context through explicit parameters so extraction does not open another transaction or copy mutable session state. The existing `host-contract.mts` becomes the Server bridge mirror used by these bindings, not another protocol definition in the Engine.

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

- [ ] Update the living architecture tree/graph/code map and child indexes with the agreed names and actual source owners.
- [ ] Add concise Client bridge / Server bridge documents and frontend/backend SDK indexes; verify changed links.
- [ ] Run the full host gate and installed-package checks. Inspect the diff for changed wire behavior, duplicate implementations, leaked runtime dependencies and historical-document changes.
- [ ] Record executed evidence, commit the refactor and open a reviewable PR. Attach the PR to this conversation; report any remaining release work separately.

**Documentation edits:** `docs/engineering/architecture.md` gets the agreed tree, graph and code map. Protocol child docs cover Sync, Client bridge and Server bridge. Frontend and Backend SDK docs each expose API and Bindings. Rust interface docs link to the actual dispatch/Host implementation. Keep existing website SDK guides and package names working.

**Final commands:**

```sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
bash scripts/test.sh
git diff --check
```

`scripts/test.sh` includes real PostgreSQL, joined native host cases, generated APIs, Dart, end-to-end and installed npm/Dart acceptance. Record final executed results only; do not convert a baseline or inspected assertion into a completion claim. Review manifests and archive contents for omitted nested API/Bindings modules. Confirm the protocol dependency graph contains no client/server/storage/native imports and that prior design/spec/plan files did not change.

## Current phase

Specification and plan review only. No implementation source has been changed. Implementation starts after Steve approves the written scope and plan. Merging the refactor and merging/publishing a release remain separate operations; a new release follows `docs/engineering/releasing.md`.
