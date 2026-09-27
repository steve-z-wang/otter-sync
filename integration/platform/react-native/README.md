# React Native integration harness

A small test application for [React Native support #100](https://github.com/zanminwang/axton/issues/100). It exercises generated Entry/AddEntry/Edit APIs and the real native carrier independently of the To-do product demo (#31).

## Prerequisites and setup

Use macOS with Xcode, CocoaPods, Node, Rust, PostgreSQL command-line tools, and an installed iOS simulator runtime. The app lockfile selects Expo 57, React Native 0.86.3 and React 19.2.3; it does not change the root TypeScript toolchain.

From the repository root:

```sh
npm ci
bash scripts/build.sh
(cd integration/e2e/fixtures/round-trip && npm ci && npm run generate)
bash integration/platform/react-native/generate.sh
rustup target add aarch64-apple-ios-sim
bash packages/client-react-native/native-module/scripts/build-ios.sh simulator
(cd integration/platform/react-native && npm ci)
```

The harness reuses the Prisma package/schema of the [round-trip fixture](../../e2e/fixtures/round-trip/README.md) for its disposable backend. Its additional AddEntry mutation and generated mobile entry point are owned by `models/entry.model`; it does not modify the fixture or the public To-do example.

## Build

Use an embedded JavaScript Release build for the restart test:

```sh
cd integration/platform/react-native
npx expo prebuild --platform ios --no-install
(cd ios && pod install)
npm run ios:build
```

`ios:build` compiles the Release simulator app without installing it on any simulator; the product is `ios/build/Build/Products/Release-iphonesimulator/axtonrnharness.app`, which the runner uses by default. `npm run ios:release` additionally installs and launches the app on the selected simulator. Set `AXTON_RN_APP_BUNDLE` when the app is built elsewhere. `expo prebuild` regenerates the whole `ios` directory, so run `pod install` again after it. The `packages/client-react-native/plugins/expo-path-spaces` config plugin quotes the two Expo-generated script phases (Expo Constants and the React Native bundle phase) that otherwise fail when the repository path contains spaces; the build is arm64-only because the vendored Rust library carries that simulator slice. Native projects and compiled libraries are generated artifacts and are not committed. The native module is a local package and must be built before CocoaPods resolves its vendored library.

## Run

From the repository root:

```sh
AXTON_RN_APP_BUNDLE=/absolute/path/to/axtonrnharness.app \
  bash integration/platform/run_react_native_ios_smoke.sh
```

By default the runner creates two disposable simulators on the newest installed iOS runtime with the first iPhone device type, then removes only those simulators. Override `AXTON_RN_SIM_RUNTIME` and `AXTON_RN_SIM_DEVICE` with installed identifiers, or supply two dedicated simulator UDIDs as arguments. Existing harness installs on caller-provided simulators are refused to avoid resetting their data, and the runner leaves its installation on caller-provided simulators afterwards; it deletes only simulators it created.

The runner creates an isolated PostgreSQL cluster and two per-client proxies. It installs the same bundled application separately, writes test-only configuration into each app's Documents directory, and checks JSON assertion results there. The app reads and writes through the generated client; the files only coordinate the test and record evidence.

Scenarios:

1. Alice and Bob subscribe and exchange live edits through native HTTP/WebSocket. The seeded record is not among those live edits: a subscription starts at the head its first handshake acknowledges ([#150](https://github.com/zanminwang/axton/issues/150)), so each phone asks for the Scope's earlier publications with `subscription.bootstrap()` ([#151](https://github.com/zanminwang/axton/issues/151)). The harness backend republishes nothing; what the SDK delivers here is the channel's history through the bounded historical pages, and live delivery after it.
2. Alice's first successful push response is dropped; retry must reuse its receipt without executing the handler twice.
3. Alice's proxy disconnects real network traffic. She creates a record and queues a dependent edit locally.
4. Her process is terminated and relaunched while disconnected; records, queued work, and client ID must survive.
5. Bob edits another record while Alice is disconnected.
6. Alice reconnects. Both clients and PostgreSQL must agree, with distinct client IDs, zero pending work, and no duplicate handler execution.

The runner prints an evidence directory containing phase assertions, screenshots and backend results. Any assertion failure or timeout fails the run. Local polling in the harness waits for assertions; the SDK's synchronization itself uses live delivery and the explicit historical load rather than periodic polling.

## Evidence

Verified 2026-09-15 on branch `codex/react-native` at commit `c54ff70` (rerun after the review fixes; an earlier run on the pre-review working tree also passed) with Xcode 26.5 (17F42), iOS 26.5 simulator runtime (23F77), two disposable iPhone 17 simulators, Node 26.4.0, cargo 1.98.1, CocoaPods 1.16.2, Expo 57.0.22, React Native 0.86.3, React 19.2.3, arm64 simulator slice of `libaxton_mobile.a`.

Commands, from the repository root after the setup above:

```sh
node --test packages/client-react-native/plugins/expo-path-spaces.test.cjs
(cd integration/platform/react-native && ./node_modules/.bin/tsc --noEmit)
(cd integration/platform/react-native && npx expo prebuild --platform ios --no-install && (cd ios && pod install) && npm run ios:build)
bash integration/platform/run_react_native_ios_smoke.sh
```

Result: `PASS: two real RN clients, lost-response retry, offline writes, process restart and convergence`. The Release app embedded `main.jsbundle` (1.5 MB, no `node:` built-ins, N-API binary or Node `ws` in the bundle) and linked `axton_mobile_call`. Phase assertions recorded by the app:

| Phase | Result |
| --- | --- |
| alice/bob online | malformed native input and path traversal rejected; rollback left no row; live edits exchanged both ways; queues drained |
| alice offline | proxy returned 503/refused upgrades; local create plus dependent edit visible through the watch; 2 pending |
| alice restart | same client ID after `simctl terminate`/`launch` while disconnected; row and 2 pending mutations retained |
| bob remote | edit committed while Alice was disconnected |
| alice settle, bob observe | both clients converged to the PostgreSQL rows with 0 pending |

Backend after the run: handler calls `{add: 1, edit: 4}`, one dropped push response, two rows. Alice's and Bob's client IDs differed. Screenshots and JSON assertions were written to the printed evidence directory.

Limits: the evidence covers an arm64 iOS simulator with the app in the foreground. Physical devices, Android, background execution and the x86_64 simulator slice are not covered.

That run predates the Rust-owned runtime carrier ([#134](https://github.com/zanminwang/axton/issues/134)): the module then exposed the synchronous `clientCall` host over `axton_mobile_call`, which is removed. The app now reaches the runtime through `runtimeOpen`/`runtimeSubmit`/`runtimeDrain`/`runtimeDetach` and the `axtonWake` event, and its first assertion checks that the raw carrier refuses a malformed open request and an envelope for an unopened runtime. The harness has not been re-run on a simulator since that change.
