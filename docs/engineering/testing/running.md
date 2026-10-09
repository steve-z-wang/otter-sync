# Running tests

Run commands from the repository root. The [testing overview](../testing.md) explains each test category.

## Prerequisites

The host gate needs Rust, Node, Dart, Python and PostgreSQL command-line tools. The current [CI workflow](../../../.github/workflows/verify.yml) pins Rust 1.98.1, Node 26.4.0 and Dart 3.12.1, with PostgreSQL 16. Make the tools available on PATH.

## Rust subset

```sh
cargo test --workspace --locked
```

This includes the simulation and uses temporary local SQLite files; it does not require a running PostgreSQL service. Individual crate commands are listed under [Component tests](components/README.md) and [Simulation](simulation/README.md). Run times have not been measured for this guide.

## Language and integration setup

Before focused JavaScript or generated API tests, install root dependencies and build the native artifacts:

```sh
npm ci
bash scripts/build.sh
```

For Dart tests, also install package dependencies and select the native library:

```sh
(cd packages/frontend/dart && dart pub get)
case "$(uname -s)" in
  Darwin) export AXTON_LIBRARY="$PWD/target/debug/libaxton_dart.dylib" ;;
  Linux) export AXTON_LIBRARY="$PWD/target/debug/libaxton_dart.so" ;;
esac
export AXTON_DART_LIBRARY="$AXTON_LIBRARY"
(cd packages/frontend/dart && dart analyze && dart test)
```

`AXTON_LIBRARY` is the library the SDK tests load and `AXTON_DART_LIBRARY` the one the generated-API, Action and end-to-end Dart clients load. The Dart SDK calls the library's C ABI from the test's own isolate; each client's database work runs on a native runtime thread, and no worker isolate is started ([Bindings](../architecture/sdks/bindings.md#5-building-block-view)).

Focused database and end-to-end runners create temporary PostgreSQL clusters and clean them up on exit. Their commands are linked under [Integration](integration/README.md) and [End-to-end](end-to-end.md). The [generated API runner](../../../integration/generated-api/verify.sh) regenerates its checked-in fixtures; inspect any resulting changes.

## Release packages

```sh
node scripts/release/version.mjs check
node --test integration/release/*.test.mjs
bash integration/release/verify-installed.sh
```

The version check compares every manifest, internal pin and lockfile with the version in `.release-please-manifest.json`; the tests check it, the [target inventory](../../../scripts/release/targets.json), and the [release manifest](../../../scripts/release/manifest.mjs) with its retry rule against scripted registry answers. The [installed-package runner](../../../integration/release/verify-installed.sh) builds this host's release addon, compiler and Dart library, packs the npm/Dart packages with [pack.sh](../../../scripts/release/pack.sh), installs them into a scratch project outside the repository and exercises them there, including a round trip against a temporary PostgreSQL cluster. In the local staged path it also runs installed CLI-generated Dart against that backend with a bundled library, Bootstrap, Mutation settlement and read Store modes. Explicit host-only packing narrows the verification manifest; full release packing still requires every host/mobile library. It installs third-party packages from the npm registry. With `--registry V` it installs version V from npm instead.

[verify-dart.sh](../../../integration/release/verify-dart.sh) does the same for the Dart package: given a directory with a staged `axton-dart-<V>.tar.gz` and the release's libraries, or `--registry V` for pub.dev and the GitHub release, it runs a Dart application outside the repository whose build hook bundles this host's library, and opens, writes and reopens a local database without `libraryPath`. The [release workflow](../../../.github/workflows/release-publish.yml) runs both runners before and after publishing ([Releasing](../releasing.md)).

## Full host gate

```sh
bash scripts/test.sh
```

The script checks the release version, builds artifacts, checks Rust formatting and linting, runs Rust and language tests, then exercises persistence, generated APIs, end-to-end flows, documentation examples and the installed npm packages. [CI](../../../.github/workflows/verify.yml) runs it on macOS and Linux and additionally checks optimized artifacts. [Device smoke tests](../../../integration/platform/README.md) are separate.

Performance diagnostics are also separate from correctness tests:

```sh
cargo run -p axton-sim --example capacity --release
```
