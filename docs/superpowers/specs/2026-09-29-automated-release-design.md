# AXTON automated distribution and Oasis adoption

Status: release-PR approach selected by the user; documentation only. Reviewed against local source and registry documentation on 2026-09-29. No package has been published by this work. The maintainer scoped the first release to alpha distribution on 2026-09-29 (see [Alpha distribution](#alpha-distribution)).

## Outcome and scope

AXTON owns building and publishing its compiler, Node SDKs and native addon, and Dart SDK and native libraries. Selected trigger: merge the automatically maintained release PR into `main`; release-please creates the version tag and the tag workflow publishes one coordinated release. Feature merges update the release PR rather than publishing immediately. The user selected this standard release-PR flow on 2026-09-29. Oasis installs that version and removes `third_party/axton/` after installed-package verification passes.

This changes distribution, not synchronization behavior. Database migrations, Model/Mutation history, wire compatibility, and app minimum-build policy remain unchanged. Oasis retains its schema, generated application contracts, and existing database migration history. Do not publish React Native, Windows, browser/WASM, or Rust crates in this first release.

## Evidence from the current repositories

AXTON baseline: `08e20ccafc283455d1ba1fadea81cfe16f73fdd2`, also Oasis's current vendored pin. `packages/server`, `packages/client-js`, and `packages/postgres` are private npm packages at 0.1.0. `packages/dart/pubspec.yaml` has `publish_to: none`. Node loaders reach outside package boundaries to `../../bindings/node/axton-node.node`. Published npm packages cannot retain that layout.

The npm packages export `.mts` directly. Shipping that unchanged is insufficient: Node's TypeScript stripping is not an installation strategy for TypeScript inside `node_modules`. Publish executable JavaScript plus declarations and test the real installed artifact.

Oasis builds native code in `ops/axton/build.sh` and its Dockerfile. iOS relies on generated linker settings retaining `axton_*` symbols. Dart currently accepts an explicit library path or uses process symbols. Generated TS imports, the Nest CommonJS loader, declaration generation, CI classification, Railway watch paths, mobile tests, and release scripts all reference the vendored tree.

## Distribution contract

All release artifacts use exact version V and come from the same tagged commit. npm packages use the `axtonjs` organization, which the maintainer created and owns (2026-09-29) because `@axton` was unavailable. The pub.dev package name `axton` is available, and the CLI command stays `axton`. Verify each exact name/version is still unpublished before implementation publishes anything. Do not silently publish under an unrelated name.

| Artifact | Distribution | Public interface |
| --- | --- | --- |
| `@axtonjs/server` | npm | Existing backend API; compiled `.mjs` and `.d.mts` |
| `@axtonjs/client` | npm | Existing JS client API; compiled `.mjs` and `.d.mts` |
| `@axtonjs/postgres` | npm | Root API plus explicit `./prisma`, `./pg`, `./drizzle`, `./migration.sql` exports |
| `@axtonjs/native` | npm | Internal loader, exact platform-package dependencies |
| `@axtonjs/native-<target>` | npm | One Node addon per supported host target |
| `@axtonjs/cli` | npm | `axton` command, exact compiler platform dependencies |
| `@axtonjs/cli-<target>` | npm | One precompiled compiler executable per supported host target |
| `axton` | pub.dev (unlisted) | Dart client with native asset hook (or FFI-plugin fallback) and matching library hashes |
| Dart native archives | GitHub Release | Versioned archives and SHA-256 checksums |

Keep the Rust implementation private to these distributions; consumers do not need Cargo, rustup, or cargo-ndk. Normal platform toolchains such as Flutter, Xcode and Android SDK remain required.

### Alpha distribution

The first release is an alpha. Versions stay 0.x. npm packages publish under the `alpha` dist-tag and the workflow never sets or moves `latest` (the registry may point `latest` at a brand-new package's first version; verify and record what it does); consumers, Oasis included, pin exact versions. pub.dev packages publish normally; after the first upload the maintainer marks the package unlisted on pub.dev's admin page, a human step. Every package README states that AXTON is alpha: the API is unstable and not for production use.

### Initial supported targets

Node addon and CLI: darwin-arm64 and linux-x64-gnu only. Build the Linux artifact against a documented glibc baseline no newer than Debian 12 (Oasis uses bookworm). darwin-x64 and linux-arm64 are future additions. Verify actual target architecture and shared-library requirements. Other hosts, including Alpine/musl and Windows, must fail clearly as unsupported; never silently compile from source.

Dart host tests: darwin-arm64 and linux-x64. Flutter: iOS device arm64, iOS simulator arm64/x64, Android arm64-v8a, armeabi-v7a and x86_64. Use an explicit checked-in target table. Do not infer supported targets from whichever build happened to finish.

Dart build hooks download libraries at build time, validate a SHA-256 embedded in the published package, cache by version/target/hash and link them into the application. No runtime download. Missing files, hash mismatch and unsupported targets fail with an actionable error. Cold-cache and warm-cache behavior are both tested. SDK hooks must work at the existing Dart >=3.12.0 floor and Oasis's pinned Flutter 3.44.1; validate release-mode retention of C ABI symbols on iOS, not only simulator debug. A feasibility spike settles this before other release work. If hooks cannot package correctly on that toolchain, including iOS release, the SDK instead ships as a standard Flutter FFI plugin whose podspec and Gradle build ship or download the same prebuilt, hash-checked libraries; never silently raise the floor.

The hook's hash manifest is generated from verified build artifacts before packing the Dart SDK. Publish the same staged source tree that was inspected and tested. Host tests must discover the installed asset without an Oasis checkout path. Keep explicit library injection available for AXTON's source-level tests if still needed.

### JavaScript and generated contracts

Compile `.mts` to executable `.mjs` and emit `.d.mts`, with matching package exports. Rewrite internal emitted import suffixes through a tested build configuration. Do not merely rename files. Server/client import the shared native package, whose platform dependencies pin V exactly. Test ESM import, Node CommonJS `require(esm)`, and Oasis's real Jest/Nest loading path; do not add a second CJS build unless these checks demonstrate it is required.

The compiler generates bare package imports (`@axtonjs/server`, `@axtonjs/client`) by default, while explicit source-runtime overrides remain available to AXTON's internal fixtures. The compiler's version output, all SDK manifests, binaries and release manifest agree. Keep Prisma peer metadata honest: current AXTON metadata says <7 while Oasis uses Prisma 7. Verify the existing adapter against Oasis's version before declaring support; distribution must not silently introduce an incompatible peer range.

## One version and automatic publication

Use Google's release-please to maintain one root release PR, version and changelog. Configure a single release unit with its standard manifest/config files and `extra-files` updaters for Node, Dart and Cargo manifests. Internal dependency pins and lockfiles must match; use a small deterministic synchronization/check step only where built-in updaters do not cover a format. Do not build a custom version allocator, release queue, or historical-commit reconciliation service.

Initial intended public version is 0.1.0, subject to availability. Conventional squash-merge titles (`fix:`, `feat:`, breaking-change markers) drive version proposals; reviewers inspect the resulting version and changelog in the release PR. Configure pre-1.0 bump behavior explicitly. Feature PRs do not manually bump versions. Documentation-only changes can accumulate until the next code release.

`release-please.yml` runs on main using a narrowly scoped GitHub App token. Merging the release PR creates immutable `vV`; this tag push starts `release-publish.yml`. The App needs Contents and Pull requests write on AXTON only. The built-in GITHUB_TOKEN does not start the follow-on tag workflow. Never substitute a main-branch or manual-dispatch event for pub.dev's required tag-push event.

Release-please handles version/tag/release notes, not package publication. Keep build and publish in ordinary Actions jobs using registry CLIs. Its GitHub Release is evidence that a tag exists, not that every registry upload succeeded. Configure it as a prerelease until package verification completes.

`release-publish.yml` runs on that tag push and:

1. Confirms tag/version equality and that the tagged commit belongs to main; runs correctness gates for that commit, not a mutable branch head.
2. Builds the full target matrix and packs all packages in staging.
3. Produces `release-manifest.json` containing V, commit SHA, artifact names/targets, sizes, hashes and npm archive integrity values. Tests packages installed outside the source checkout.
4. Uploads verified Dart native archives to a public GitHub prerelease with immutable versioned URLs. These assets must be anonymously downloadable before pub.dev consumers can install the package; a private draft is insufficient.
5. Publishes npm packages in dependency order under the `alpha` dist-tag and publishes the inspected Dart package from the tag-triggered job.
6. Installs V from the real registries on clean runners; validates compiler, native loading and basic persistent client/server operation.
7. Marks the GitHub release complete only after all checks pass. Nothing moves npm `latest`.

Registry publication is not atomic. A pub.dev version can become visible before final completion; the design does not pretend a GitHub completion marker hides it. Pre-publication installed-package tests are the principal protection. Oasis only adopts a completed release, by exact version.

Use concurrency per version with `cancel-in-progress: false`; retries for the same tag must not race. Do not merge the next release PR while the preceding release is incomplete. Existing tags at a different SHA fail closed. These are workflow checks and an operator rule, not a custom scheduler.

### Failure and repair

Recovery is minimal. Persist verified archives and the manifest as release assets so retries do not depend on expiring Actions artifacts. On retry, skip an artifact already published only if its integrity matches the manifest; otherwise stop. A matching name/version alone is not evidence, and a retry never rebuilds.

Retry a failed tag-triggered run to preserve pub.dev's required event context. A bad published artifact requires a new forward version; package rollback does not imply a database downgrade. Do not automate destructive registry actions. Never replace native binaries at an existing version URL.

## First publication and human-owned setup

The first release has an explicit bootstrap phase because package administration and trusted-publisher configuration may require an existing package. Do not upload dummy packages merely to reserve names.

| Owner action | When | Agent work around it |
| --- | --- | --- |
| Sign in to npm; verify email, publishing/2FA access for the `axtonjs` organization | Before first upload | Inventory every SDK and platform package; prepare inspected archives and exact commands |
| Sign in to pub.dev with a Google account; confirm package name | Before first upload | Prepare SDK, LICENSE, README, changelog and passing publish dry run |
| Complete browser login/2FA for initial real uploads | After artifact review | Publish the verified V artifacts; do not request passwords or OTPs in chat |
| Configure npm trusted publisher for every npm package, bound to `zanminwang/axton`, `release-publish.yml`, environment `release` | After package creation where necessary | Produce exact package checklist; subsequent uploads use OIDC |
| Mark the `axton` package unlisted on pub.dev's admin page | After first Dart upload | Link the admin page; confirm the listing state |
| Enable pub.dev GitHub automation: repository `zanminwang/axton`, pattern `v{{version}}`, environment `release` | After first Dart upload | Validate tag workflow against these exact values |
| Create/install GitHub release App scoped to AXTON with Contents and Pull requests write; store its Client ID and private key in GitHub configuration | Before automatic tagging | Reference `AXTON_RELEASE_APP_CLIENT_ID` variable and `AXTON_RELEASE_APP_PRIVATE_KEY` secret; no key in source or chat |
| Grant required repository settings access; protect main and version tags | Before enabling automatic releases | Configure workflow permissions and release environment without per-release manual approval |

A domain-verified pub.dev publisher is optional. It needs domain verification and can be set up after the initial Google-account publication; it is not a first-release dependency. No account is needed on nodejs.org or dart.dev for package hosting: the registries are npmjs.com and pub.dev.

Bootstrap uses the exact build/test/archive pipeline, then authenticated initial publishing only for names that do not yet exist. Native assets go public first. After all real V artifacts are verified, configure OIDC and release the next genuine patch through merge → App-created tag → OIDC publishing. If no patch exists, wait for the next real change; never overwrite V or claim automation is proven from a dry run alone. First publication and end-to-end automation verification are separate acceptance records.

## Oasis adoption and removal boundary

Use a separate Oasis change after the completed public release exists. Introduce `ops/axton/version` plus an update/check command as Oasis's one editable version pin; dependency manifests and lockfiles mirror it exactly. This is the consumer's chosen version, distinct from AXTON's release-version source.

Replace both Dart path dependencies, generated TS runtime imports, backend dynamic loader package targets and declaration aliases. Simplify Docker to consume the npm addon. Keep application contract generation/checking, now invoking the pinned CLI. Remove source-build steps from CI, dev scripts and mobile release builds after installed native assets work. Update CI change classification and deployment watches for the new pin and package locks.

Run backend and mobile checks on a clean checkout/container that cannot see `third_party/axton` or a sibling AXTON checkout. Exercise iOS release startup, Android startup and real local sync. Only then delete the vendored tree, vendor script, obsolete native-build action, old native-library path helpers, linker settings and Rust installation steps used exclusively for AXTON.

Do not rewrite historical SQL migration files or dated work logs merely because their provenance refers to the old pin. Update living docs and links to AXTON documentation. Do not run synthetics against production as a substitute for local integration tests. Follow each repository's AGENTS/SOP and issue conventions when implementation starts.

## Acceptance criteria

- Every declared artifact at V traces to one commit and has verified integrity.
- Clean Node installs execute SDKs and compiler without Rust or a source checkout.
- Dart/Flutter installed packages load matching native code on the declared matrix, including iOS release symbol retention.
- First genuine release exists on npm, pub.dev and GitHub; package ownership belongs to the user's accounts.
- A later real merge demonstrates automatic tag creation and OIDC publication, with no manual per-release action.
- The workflow never sets npm `latest`; the pub.dev package is unlisted; every README carries the alpha notice.
- The retry skip/verify rule (skip on matching integrity, stop otherwise) has a small executable test.
- Oasis passes required checks using exact package versions and contains no active dependency on vendored AXTON source.
- Existing device data, unsent calls and backend schema remain compatible; no migration/reset is introduced by packaging.

## Sources checked

- https://github.com/googleapis/release-please — standard release PR/version/changelog management.
- https://github.com/googleapis/release-please-action — GitHub Actions integration.
- https://docs.npmjs.com/trusted-publishers/ — OIDC configuration and supported publishing tools.
- https://dart.dev/tools/pub/automated-publishing — tag-push requirement and package configuration.
- https://dart.dev/tools/pub/publishing — initial Google-account publication and publisher transfer.
- https://dart.dev/tools/hooks — build-time native asset hooks.
- https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow — token-trigger behavior.

## Self-review record

Corrected six hazards during review: source `.mts` packages in node_modules; pub.dev's tag-only OIDC; GITHUB_TOKEN tag pushes not starting publication; native assets hidden in draft releases; non-atomic cross-registry visibility; unnecessary custom version scheduling (replaced by release-please). Added explicit Prisma 7 verification, iOS release linking, integrity-checked retries and preservation of historical migrations. Account ownership, name availability and actual binary/platform success remain implementation evidence to obtain, not claims made by this document.

Maintainer decisions, 2026-09-29: npm scope `@axtonjs` (the maintainer's `axtonjs` organization; `@axton` was unavailable); alpha distribution (`alpha` dist-tag, no `latest` promotion, unlisted pub.dev package, alpha READMEs, 0.x); two host targets for the first release; minimal skip-or-stop recovery without registry doubles; the Dart native-asset spike runs first, with the FFI-plugin fallback.

Bootstrap correction, 2026-09-29 (#213): pub.dev rejected v0.1.0's Dart package because it carried `hook/native_manifest.dart`; pub.dev accepts only `hook/build.dart` and `hook/link.dart` among hook Dart files. The generated manifest moved to `lib/src/native_manifest.dart`, imported by the hook, and packing refuses any other hook Dart file. The same bootstrap's `npm publish release/<file>` was read as a GitHub repository; local archives are published by `./`-prefixed path. v0.1.0 published npm only; the Dart package starts at 0.1.1, which completes the bootstrap.
