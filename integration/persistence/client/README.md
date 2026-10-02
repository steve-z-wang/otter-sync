# Original-layout native reopen

`run.sh` compiles a small business schema whose field and Load argument are named `channel`. It installs the exact released v0.2 SQLite DDL and populated fixture, then reopens each file twice through generated JS and Dart clients. It checks removal of the client holding table/index, authority marker one, retained cursors, client identity, opaque business data, exact frozen logical requests and current authority negotiation across both reopens.

Build native artifacts first (`bash scripts/build.sh`), resolve `integration/generated-api` Dart dependencies, and select `AXTON_DART_LIBRARY`. Run `bash integration/persistence/client/run.sh`. The generated API verifier also runs it. Rust migration tests own raw persisted-byte, rollback, marker-zero and settlement coverage; PostgreSQL Stream tests own actual migrated-server settlement and saved replay.
