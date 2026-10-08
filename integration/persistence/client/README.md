# Persisted Store boundaries

`run.sh` checks fresh-file admission and reopen. The Rust `stream_upgrade` test name is historical: its current assertion refuses the original released v0.2 fixture unchanged, then proves a separate fresh file is independent. It no longer runs an old-format migration or claims upgrade success.

Generated JavaScript and Dart clients refuse the original files twice with `unsupported Store format`. Read-only SQLite dumps before and after verify the original schema, rows, queued bytes and subscription metadata are unchanged. Closed snapshots use immutable reads; a nonempty WAL fails that snapshot check. Each language opens a separate fresh format-5 file, commits local CRUD and a named Mutation, closes and reopens, then checks content, optimism and retained input. These checks prove neither migration nor automatic recovery of old pending work.

Build native artifacts, resolve generated Dart dependencies, set `AXTON_DART_LIBRARY` and run `bash integration/persistence/client/run.sh`. The generated API verifier includes this gate. Protocol 5 supplies no old-format upgrade or compatibility bridge.
