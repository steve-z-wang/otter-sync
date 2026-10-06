# Persisted Store boundaries

`run.sh` keeps two explicit contracts. The internal Rust `stream_upgrade` regression opens the original released v0.2 DDL and populated fixture twice, retaining the historical authority migration, cursor, opaque `channel` field and exact frozen-request byte assertions. This is internal legacy coverage, not protocol-4 adoption.

Generated JavaScript and Dart public clients refuse the same original files twice with `protocol_mismatch`. Independent read-only SQLite dumps before and after prove that schema, rows, original queue bytes and subscription metadata remain unchanged. SQLite journal-mode/header bookkeeping is not a claimed file-byte invariant. Each language then opens a separate new bound Store, commits local CRUD and a named pending Mutation, closes and reopens it, and verifies local content, optimism and exact persisted protocol-4 intent.

Build native artifacts first (`bash scripts/build.sh`), resolve `integration/generated-api` Dart dependencies, and set `AXTON_DART_LIBRARY`. Run `bash integration/persistence/client/run.sh`. The generated API verifier includes this gate. No client implicitly adopts, clears or upgrades a legacy file into a bound Store.
