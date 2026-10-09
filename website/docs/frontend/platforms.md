# Supported platforms

AXTON clients use the native Rust engine and SQLite. Building the application package and validating it on the target platform are separate steps.

| Platform | Available support |
| --- | --- |
| macOS and Linux | TypeScript on Node.js and Dart: native builds, SQLite and HTTP round-trip tests are verified |
| iOS | Flutter native linking/build setup is available; FFI, SQLite and app-restart runtime validation is not complete |
| Android | Runtime and device validation is not complete |
| Browser | Native client bindings do not provide browser/WASM support |
| Windows | Not verified |

## Desktop setup

Build the native libraries with `bash scripts/build.sh`. TypeScript uses the Node addon. From a checkout, Dart takes an explicit `libraryPath`: `target/debug/libaxton_dart.dylib` on macOS or `target/debug/libaxton_dart.so` on Linux. See [client setup](setup.md) for language-specific examples.

## Flutter native integration

A released `axton` package's build hook bundles the prebuilt native library for iOS devices (arm64), iOS simulators (arm64 and x64) and Android (arm64-v8a, armeabi-v7a and x86_64). It downloads the library of the package's version from the AXTON GitHub release, checks its SHA-256 and caches it; the application needs no linker settings and no `libraryPath`. See the [package README](https://github.com/zanminwang/axton/blob/main/packages/frontend/dart/README.md#native-library) to build from local library files instead.

The repository includes a simulator integration harness. It requires Xcode and a usable installed iOS runtime:

```sh
bash integration/platform/run_ios_simulator_smoke.sh
```

The harness builds the simulator libraries, stages the `axton` package as a release does and builds a copy of the Flutter app against it, so the build hook bundles the library from local files. It creates a disposable simulator and checks local writes, queued calls, close/reopen and app restart. It removes only the simulator and files it creates. Passing the build alone does not establish that all runtime checks pass.

## React Native

The repository includes a React Native TypeScript adapter, reusable Expo native module, and a two-simulator integration harness. Use a native Expo build; Expo Go does not contain AXTON's Rust library. This integration currently targets arm64 iOS simulators and is consumed from a repository checkout.

[`databasePath(name = "axton.sqlite"): Promise<string>`](https://github.com/zanminwang/axton/blob/main/packages/frontend/client-react-native/README.md) resolves a basename under persistent Application Support storage, creates the parent directory, and rejects invalid path names. Keep that path stable across launches so local records, queued calls and client identity can be reopened.

Generated read/watch/Mutation/Query/transaction APIs are shared with Node. React Native's runtime transaction does not expose nested savepoints. Native WebSocket failures do not expose a structured HTTP status; HTTP 401 refresh and application-managed socket credentials are documented separately in the package guide. Background execution while iOS suspends the app is not promised.

The runnable demo is the [To-do example](https://github.com/zanminwang/axton/blob/main/examples/todo/README.md): two simulators, local writes, offline work and synchronization through the example backend ([getting started](../getting-started.md)). See the [package guide](https://github.com/zanminwang/axton/blob/main/packages/frontend/client-react-native/README.md) for installation and API limits, and the [SDK integration harness](https://github.com/zanminwang/axton/blob/main/integration/platform/react-native/README.md) for exact build/run steps and runtime evidence. The simulator sequence uses embedded JavaScript and actual network interruption; host tests or native linking alone do not establish completion.

## Verification

`bash scripts/test.sh` runs the macOS/Linux host checks with real SQLite, native bindings and a disposable PostgreSQL backend. Platform-specific simulator checks run separately. See [testing](https://github.com/zanminwang/axton/blob/main/docs/engineering/testing/running.md) for the full workflow.
