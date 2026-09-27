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
(cd packages/dart && dart pub get)
case "$(uname -s)" in
  Darwin) export AXTON_LIBRARY="$PWD/target/debug/libaxton_dart.dylib" ;;
  Linux) export AXTON_LIBRARY="$PWD/target/debug/libaxton_dart.so" ;;
esac
export AXTON_DART_LIBRARY="$AXTON_LIBRARY"
(cd packages/dart && dart analyze && dart test)
```

`AXTON_LIBRARY` is the library the SDK tests load and `AXTON_DART_LIBRARY` the one the generated-API, Action and end-to-end Dart clients load. The Dart SDK calls the library's C ABI from the test's own isolate; each client's database work runs on a native runtime thread, and no worker isolate is started ([Bindings](../architecture/sdks/bindings.md#5-building-block-view)).

Focused database and end-to-end runners create temporary PostgreSQL clusters and clean them up on exit. Their commands are linked under [Integration](integration/README.md) and [End-to-end](end-to-end.md). The [generated API runner](../../../integration/generated-api/verify.sh) regenerates its checked-in fixtures; inspect any resulting changes.

## Full host gate

```sh
bash scripts/test.sh
```

The script builds artifacts, checks Rust formatting and linting, runs Rust and language tests, then exercises persistence, generated APIs, end-to-end flows and documentation examples. [CI](../../../.github/workflows/verify.yml) runs it on macOS and Linux and additionally checks optimized artifacts. [Device smoke tests](../../../integration/platform/README.md) are separate.

Performance diagnostics are also separate from correctness tests:

```sh
cargo run -p axton-sim --example capacity --release
```
