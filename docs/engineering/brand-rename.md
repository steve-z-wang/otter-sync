# AXTON rename

The alpha rename changes both the product identity and developer-facing names. This is a breaking source and storage change; there are no compatibility aliases or automatic migration from pre-rename builds.

## Integration names

- Rust crates use `axton-*`, Rust imports use `axton_*`, and the compiler command is `axton`.
- JavaScript packages use `@axtonjs/*` (the unpublished React Native packages keep `@axton/*`); report types use `AxtonReport`.
- Dart imports use `package:axton/axton.dart`.
- Native libraries and C symbols use `axton`; the Expo module is `AxtonNative`.
- Environment variables use `AXTON_*`. Example app identifiers use `dev.axton`.

Regenerate application bindings, rebuild native artifacts, update imports and environment configuration, and regenerate Expo native projects together. Do not mix artifacts from before and after the rename.

## Persistent state

Internal SQLite and PostgreSQL tables now use the `axton_` prefix, including client identity, queued work, receipts, record stamps and scope positions. The reserved model prefix changes with these tables. The PostgreSQL setup SQL creates the new tables; it does not migrate old metadata.

For disposable development environments, use fresh client database paths and a fresh backend database. Preserve any business data you need separately. Do not point the renamed runtime at existing pre-rename databases: old metadata and pending local work will not be adopted automatically.

For an environment containing data that must survive, stay on the previous build until a separately tested storage migration is available. Back up both client and backend state; do not delete local databases to upgrade when they contain unsynchronized work or local-only data.

See [client storage](architecture/client/storage/README.md) and [server persistence](architecture/server/persistence.md) for the owning contracts.
