# AXTON Automated Release Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task by task. Checkboxes track execution; none are marked complete by documentation work.

**Decision status:** The user selected the standard release-PR flow on 2026-09-29.

**Goal:** Publish AXTON as one versioned set of installable packages, automate releases after main merges, and remove Oasis's vendored copy.

**Architecture:** AXTON packages compiled JavaScript and prebuilt native code; Dart hooks resolve versioned assets. Release-please maintains one release PR and creates its tag with a GitHub App; a tag workflow builds, verifies and publishes. Oasis consumes an exact version through its own single pin.

**Tech stack:** Rust/Cargo, Node/npm, TypeScript, Dart/Flutter native assets, GitHub Actions/App/OIDC, npmjs.com, pub.dev.

**Spec:** [Automated distribution design](../specs/2026-09-29-automated-release-design.md).

## Global constraints

- Do not change database migrations, wire shapes, stored data or queue semantics.
- One release-version input: AXTON root release-please release unit; one consumer-version input: Oasis `ops/axton/version`.
- Initial proposed V is 0.1.0, only after confirming availability of all package names/versions.
- SDK floor stays Node >=22.18.0 and Dart >=3.12.0; publishing uses npm >=11.5.1 with a supported Node runtime.
- Host targets: darwin-arm64, darwin-x64, linux-x64-gnu, linux-arm64-gnu. Mobile targets are defined in the spec.
- No Rust build toolchain in consuming applications; no source-checkout imports in installed packages.
- Public package publication is immutable and non-atomic across registries.
- pub.dev publishing runs from a tag-push workflow. A manually dispatched workflow is not equivalent.
- User login, 2FA and account ownership are human boundaries; never request credentials in chat.
- This is an implementation sequence, not an assertion that the proposed package API/build configuration already exists.

## Working setup and file ownership

AXTON tasks 1–7 run in an isolated AXTON worktree. Read `AGENTS.md`, architecture/bindings/compiler docs and the testing strategy first. Task 8 runs in its own Oasis worktree and follows `docs/sop/new-feature.md`, `docs/convention/work-ledger.md`, and backend/mobile AGENTS. File sizable implementation work in the appropriate repository ledger; this documentation task does not create or mutate remote issues.

New AXTON files:

- `release-please-config.json` and `.release-please-manifest.json`: standard single-release-unit configuration and version state.
- `CHANGELOG.md`: release-please-managed release notes.
- `scripts/release/{version,pack,manifest}.mjs`: respectively manifest synchronization/checking, package assembly and integrity inventory. Use ordinary registry CLI steps for publication, not a custom release engine.
- `scripts/release/targets.json`: supported platform table including Rust triples, npm OS/CPU/libc and library names.
- `scripts/release/tsconfig.json`: JavaScript/declaration build configuration.
- `packages/native/{package.json,index.cjs}`: Node addon selection.
- `packages/cli/{package.json,bin/axton.cjs}`: compiler selection/exec wrapper.
- `packages/dart/hook/build.dart`: native asset integration.
- `integration/release/{version,manifest}.test.mjs`: version/state failure tests.
- `integration/release/verify-installed.sh`: clean installed-package fixture orchestrator.
- `.github/workflows/{release-please,release-publish}.yml`: release triggers and credential boundaries.
- `docs/engineering/releasing.md`: durable operator guide, including bootstrap and recovery.

Existing files changed: SDK manifests/source loaders, compiler default imports, Cargo manifests/locks, package locks, verification workflow and docs index. Platform npm package directories, Dart hash manifests and distributable SDK trees are generated into `dist/release/V/`, not manually maintained source copies.

## Task 1 — Establish owned names, exact version and target inventory

**Interfaces:** `version.mjs sync` fills only manifest/lock formats not covered by release-please; `version.mjs check` exits nonzero on disagreement. `targets.json` is consumed by packaging and build workflows.

- [ ] Query real registry state before selecting the initial release version:

```sh
npm view @axton/server name version --json
npm view @axton/client name version --json
npm view @axton/postgres name version --json
npm view @axton/cli name version --json
npm view @axton/native name version --json
curl --fail --silent --show-error https://pub.dev/api/packages/axton
```

Repeat the npm ownership/availability check for every generated native/CLI target name. A 404 means unregistered, not guaranteed ownership; authentication/connection errors are not availability evidence. Obtain the user's intended npm scope and pub.dev identity before any upload; if names are unavailable, update the spec and generation defaults consistently.

- [ ] Inspect pinned Flutter/Dart, Rust targets, Node ABI, Linux glibc baseline, iOS deployment target and Android minimum SDK. Record exact supported floors in `targets.json` and the operator guide.
- [ ] Write version tests for mismatched manifests, exact internal pins, invalid SemVer and duplicate/reused release versions. Define first-version and documentation-only behavior.
- [ ] Configure one release-please unit and its standard extra-files updaters; implement only the remaining version synchronization/checking; include standalone `bindings/node/Cargo.toml`, workspace Cargo version, generated platform manifests and all relevant lockfiles. Do not publish unrelated workspace packages.
- [ ] Run `node --test integration/release/version.test.mjs`, then `node scripts/release/version.mjs check`; both must pass. Commit this independently.

## Task 2 — Make Node SDKs and compiler independently installable

**Interfaces:** SDK public exports stay compatible. `@axton/native` loads the current platform's addon. `@axton/cli` exposes executable `axton`; unsupported OS/architecture/libc fails explicitly.

- [ ] Create a temporary-project test that installs packed SDKs and attempts both ESM import and CommonJS loading; initially it must expose the current missing addon/source TypeScript problem.
- [ ] Add the TS package build to emit `.mjs` and `.d.mts`. Update emitted internal imports, package exports, file allowlists and declaration resolution. Include LICENSE/README and postgres SQL.
- [ ] Replace server/client `../../bindings/node/axton-node.node` lookup with the shared loader. Use exact-version optional platform dependencies with npm OS/CPU/libc constraints; distinguish unsupported platform from corrupted/missing install.
- [ ] Build CLI wrapper and platform executable packages. Forward argv, stdio, exit status and signals; never run an implicit Rust build. Add a reliable compiler `--version` if absent.
- [ ] Change compiler default runtime imports to public package specifiers. Preserve explicit runtime overrides used by source fixtures. Regenerate fixture expectations rather than hand-editing generated code.
- [ ] Verify Prisma adapter behavior with Oasis's installed Prisma 7 version before updating peer metadata; test pg and drizzle exports without forcing unused adapters to install.
- [ ] Execute installed fixture with no repository imports. It must compile a schema, typecheck generated TS, load server/client/adapter, create and reopen a local client database, and perform an actual round trip against the existing local test backend.
- [ ] Run `bash scripts/test.sh` and the affected installed-package checks. Commit SDK/compiler distribution changes.

## Task 3 — Package Dart native libraries and prove mobile release loading

**Interfaces:** published Dart package includes `hook/build.dart` and a generated V/target/SHA-256 manifest; ordinary client initialization resolves the installed asset. Source tests may retain explicit library injection.

- [ ] Create an external Dart/Flutter fixture with a package dependency and no `AXTON_LIBRARY`, `AXTON_DART_LIBRARY`, or repository `libraryPath`. Demonstrate the current failure before adding hooks.
- [ ] Build all spec targets with locked Cargo dependencies. Use the existing native scripts as the source for C ABI names, Android triples and iOS symbol requirements.
- [ ] Implement the build hook using Dart's supported code-assets API at the existing SDK floor. Select the exact target, fetch its V archive, verify the package-embedded hash, then cache/link. Reject missing assets and wrong hashes before loading native code.
- [ ] Exercise cold cache, warm cache, failed download, corrupt archive, wrong target and unsupported platform. Warm-cache success must not need the network.
- [ ] Run Dart persistence/transaction tests from the installed-package fixture on supported hosts.
- [ ] Build and launch iOS simulator and Android fixtures. Build an iOS release configuration and verify native symbol retention; a real device launch requires available signing/device access and must be explicitly recorded if unavailable. Do not substitute a debug simulator pass for release evidence.
- [ ] Confirm Android ABI packaging and minimum API alignment; run startup on arm64 and x86_64 emulators/devices, and compile/link verification for armeabi-v7a. Document any runtime coverage limit without claiming that target was executed.
- [ ] Run affected SDK tests and `dart pub publish --dry-run` on the staged SDK. Commit hooks and packaging.

## Task 4 — Assemble immutable release artifacts and test failure recovery

**Interfaces:** `pack.mjs --version V --out dist/release/V` assembles packages. `manifest.mjs --dir dist/release/V --check` validates inventory. Ordinary workflow CLI steps query existing versions and verify integrity before skipping uploads; no registry/state abstraction is introduced.

- [ ] Define the manifest in implementation with schema version, release V, commit SHA and an artifact list containing logical package/target, filename, size, SHA-256 and npm integrity where applicable. Store no credentials or local absolute paths.
- [ ] Write tests with registry doubles for absent version, exact existing archive, conflicting existing bytes, partial npm success, pub.dev failure, native upload failure and older-version promotion. Network errors must not be interpreted as “package absent.”
- [ ] Implement deterministic assembly and hash verification. Generate Dart embedded checksums before packing/testing the SDK; freeze all package bytes before publishing begins.
- [ ] Include dependency-order publication, immutable asset names and retained archives for retries. Make GitHub native assets publicly downloadable before pub.dev publication; use a prerelease, not a private draft.
- [ ] Add `verify-installed.sh` to extract/install the exact staged archives outside the source tree. Verify imports, CLI generation, runtime persistence and a local sync round trip.
- [ ] Run `node --test integration/release/*.test.mjs` and `bash integration/release/verify-installed.sh dist/release/V` with the selected numeric version. Expected: full inventory verified; fixture runs without Rust or source paths.
- [ ] Commit packaging/state tests. Do not publish during this task.

## Task 5 — Configure standard release-please and tag publication

**Interfaces:** A single root release-please release unit owns V. `vV` selects immutable source; `release-publish.yml` is the exact npm trusted-publisher workflow name.

- [ ] Add `release-please-config.json` and `.release-please-manifest.json` using the upstream schema, a single root release unit, `vV` tags, prerelease GitHub Release output and extra-files updates. Verify updater support for JSON, YAML and Cargo; add only minimal synchronization for uncovered fields/locks.
- [ ] Require conventional squash-merge titles and explicitly configure pre-1.0 version behavior. Dry-run a fix, feature and breaking change; ensure one version is applied across every package and only one release PR appears.
- [ ] Add `release-please.yml` on main using the release App's short-lived installation token, scoped to AXTON with Contents/Pull requests write. No registry credentials are available to PR code.
- [ ] Add tag-push `release-publish.yml` with tag/SHA checks, correctness tests, target builds, archive smoke tests and standard CLI publish steps. Confirm tagged commit belongs to main without requiring it to remain main HEAD.
- [ ] Use per-version concurrency and no in-progress cancellation. Check latest before promotion so an old retry cannot overwrite a newer version. Document finishing/recovering the current release before merging the next release PR; do not build a release scheduler.
- [ ] Set environment `release`, minimal per-job permissions and `id-token: write` on publisher jobs. Use GitHub-hosted npm publishing runners and supported Node/npm. Pin action revisions.
- [ ] Retry through the original tag-triggered job; pub.dev OIDC cannot be replaced by workflow_dispatch. Use release-please's existing GitHub Release, uploading assets there rather than racing to create another release.
- [ ] Validate workflows and a release-please dry run before registry writes. Commit the standard configuration and operator guide.

## Task 6 — Bootstrap accounts and complete first real release

**Human prerequisites:** npm scope/name ownership, pub.dev Google login, initial upload authentication. Agent prepares exact archives, links and commands before asking the user to authenticate.

- [ ] Review file lists from `npm pack --dry-run --json` and `dart pub publish --dry-run`, license notices, dependency pins and the full manifest. Ensure no secrets, test databases or source-tree path assumptions are present.
- [ ] Create the first immutable version tag and run the build/verification pipeline. Retain all checked artifacts. Upload and anonymously download/verify native archives before the Dart SDK upload.
- [ ] Have the user complete npm and Dart browser authentication. Publish actual prepared packages in dependency order; npm scoped packages use public access. First pub.dev publication is performed with the authenticated Google account, not assumed OIDC ownership.
- [ ] Fetch the installed versions from npm/pub.dev and run the clean fixtures. Mark GitHub release complete and promote npm only after verification.
- [ ] Configure npm trusted publishing for every SDK/CLI/native platform package. Configure pub.dev `zanminwang/axton`, `v{{version}}`, environment `release` after the first package exists.
- [ ] Register/install the release GitHub App with Contents and Pull requests write on AXTON only. Set repository variable `AXTON_RELEASE_APP_ID` and secret `AXTON_RELEASE_APP_PRIVATE_KEY` through GitHub settings. Align main/tag rules with the bot's required permissions; avoid per-release approval gates.
- [ ] Record the actual release URL, npm/pub.dev versions and artifact checksums in the implementation result. Do not claim automation is proven yet.

## Task 7 — Prove unattended publishing and document recovery

- [ ] For the next genuine fix or addition, merge its conventional commit, inspect the automatically generated release PR and merge that PR. Observe App-created tag → tag-push run → successful npm/pub.dev OIDC upload → final promotion.
- [ ] Verify registry audit/provenance points to the intended workflow and commit. No personal long-lived registry token should be needed for ongoing releases.
- [ ] Test failure reconciliation with doubles and rerun an actual harmless failed pre-publication job if available. Do not intentionally break public packages to test recovery.
- [ ] Write the runbook's exact resume procedure: locate V/tag/manifest, inspect existing registry integrity, rerun original tag job, verify completed registries. Conflicting artifacts require a new version and investigation.
- [ ] Link the release guide from `docs/README.md` and relevant getting-started guides. Distinguish first-release evidence from fully automatic-release evidence.

Task 8 may start after Task 6 succeeds; it need not wait for an unrelated future patch merely to begin adopting the first completed release.

## Task 8 — Switch Oasis and remove vendored source

**Files:** `ops/axton/{version,generate.sh,check-contract.sh}`, new `ops/axton/dependency-version.mjs`, `backend/{package.json,package-lock.json,tsconfig.json,axton/tsconfig.json,src/axton/runtime.ts,src/axton/runtime.cjs}`, generated product/probe contracts, `mobile/pubspec.yaml`, `mobile/pubspec.lock`, `mobile/packages/axton_models/pubspec.yaml`, native test helpers, `Dockerfile`, `.github/actions/axton-native/action.yml`, `.github/workflows/test.yml`, `ops/build-ios-release.sh`, `dev/scripts/axton-smoke.sh`, `mobile/ios/Flutter/{Debug,Release}.xcconfig`, `tool/ci/classify-pr-changes.{mjs,test.mjs}`, `railway.json`, `ops/check_railway_deployment.mjs`, living AGENTS/architecture/setup docs.

- [ ] Create the required Oasis issue and isolated worktree following its SOP. Inventory active references with `rg -n 'third_party/axton|AxtonNative|AXTON_LIBRARY' --glob '!third_party/**'`; distinguish historical provenance from executable dependencies.
- [ ] Add one consumer pin and a dependency update/check command. Exact-pin Node SDKs/CLI and both Dart dependency declarations; regenerate and commit lockfiles. Reject mixed AXTON versions in CI.
- [ ] Replace the backend loader's runtime targets with public package exports. Preserve the real Nest/Jest interop path until tests prove a simpler loader works. Remove vendored declaration aliases and generation only when package declarations satisfy them.
- [ ] Update generation/check scripts to invoke the installed, pinned CLI. Regenerate product and probe output; inspect diffs for import/version changes and reject unintended schema or historical-contract changes.
- [ ] Replace Dart native test paths and iOS/Android application linkage with SDK-owned assets. Remove old generated linker settings and JNI source outputs only after the new release builds demonstrate correct bundling.
- [ ] Simplify Docker and CI to ordinary dependency installation. Remove Rust build stages used only for AXTON. Update Railway watch checks and CI classification tests for the consumer pin and relevant dependency files.
- [ ] Run backend `npm test` and `npm run lint` with local Docker; mobile `flutter test` and `flutter analyze --no-fatal-infos --no-fatal-warnings`; contract check and affected CI/deployment-watch tests. Use each app's AGENTS for exact setup and any additional required checks.
- [ ] Build production Docker, iOS release and Android; launch the app and verify local-stack sync. Test reopening a database with cached entries and queued offline writes created before the dependency switch; this packaging change must not reset them.
- [ ] Remove `third_party/axton/`, `ops/axton/vendor.sh`, obsolete native build script/action and now-unused linkage/helpers in the worktree. Repeat clean package-only tests with no sibling AXTON checkout accessible. No raw deletion occurs before the installed dependency path is proven.
- [ ] Search again for active vendored references. Keep immutable SQL/work-log provenance; update living docs and replace broken links to vendored guides with upstream documentation links.
- [ ] Commit, review diff and submit the Oasis change through its normal PR workflow. Removing the copy is complete only when required checks pass; publishing AXTON alone does not complete this task.

## Plan self-review and evidence boundary

| Spec requirement | Tasks |
| --- | --- |
| One version; owned names; supported platforms | 1 |
| Real npm packages, compiler and Prisma compatibility | 2 |
| Dart native assets and iOS release retention | 3 |
| Artifact integrity, partial retries, no false atomicity | 4 |
| Automatic main/tag flow and credential boundary | 5 |
| Real first publication and account setup | 6 |
| Actual unattended publishing and recovery guide | 7 |
| Exact Oasis adoption, verification and source removal | 8 |

Self-review found and corrected: first pub.dev upload cannot assume an existing automation configuration; default GitHub token will not start the required tag workflow; installed `.mts` delivery needs compiled output; retry cannot trust version equality alone; native download assets cannot stay private; a first manual release is not evidence of OIDC automation; historical SQL and work logs must not be rewritten. The spec and plan use the same workflow, environment, variable names and version-source paths.

The documents are implementation-ready sequencing and acceptance criteria. Exact native hook API behavior, registry ownership and runtime matrix success require the named implementation tests; no test, release or account setup is claimed complete here.

Review update: Removed custom historical-release reconciliation and manual version bumps from feature PRs. The recommended plan now uses release-please for release state and standard registry commands for publishing. The user selected this release-PR trigger.
