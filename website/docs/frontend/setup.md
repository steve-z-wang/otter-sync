# Set up the client

Generate your Model and named Mutation/Query interfaces first ([schema generation](../schema/define.md)). TypeScript, Dart and React Native use the same Rust engine and SQLite Store.

## Open local storage

Supply a writable database path and one Stream. The generated client supplies the schema; the generic runtime also requires `schema`. A connection is optional. Tokens and URLs are connection settings, not Store identity.

```ts
const client = await GeneratedClient.open({
  path: 'local.sqlite', stream: 'User:alice',
  connection: {
    url: 'http://127.0.0.1:4242', token: 'alice',
    projectionGeneration: '1',
    options: { onError: console.error },
  },
});
```

```dart
final client = await GeneratedClient.open(
  path: 'local.sqlite', stream: 'User:alice',
  connection: StoreConnection(
    url: 'http://127.0.0.1:4242', token: () => 'alice',
  ),
);
```

Opening commits local storage before starting network work; connectivity is not required to reopen a Store. Normal reopen retains its Store identity, pending Mutations, frozen Batch and delivery progress. Use a separate file for each Stream binding and one active owner per physical file. Protocol 5 requires a fresh format-5 file; another format is refused without changing the original database, WAL or SHM. Keep the old release available to drain or export unresolved work before retiring its backend. Configure the same `projectionGeneration` on client and backend; it defaults to `'1'`.

## Native libraries

Installed Node packages select their platform addon. Installed Dart packages bundle a native library through their build hook. For a source checkout, run `bash scripts/build.sh` and pass Dart's `libraryPath` pointing to `target/debug/libaxton_dart.dylib` on macOS or `libaxton_dart.so` on Linux. React Native requires the [native module](https://github.com/zanminwang/axton/blob/main/packages/frontend/client-react-native/native-module/README.md) in a native build; Expo Go cannot supply it.

Before any Store opens on Android, initialize its stable application support/files directory once:

```dart
// Obtain this path from the application's platform directory provider.
Client.configureApplicationData(applicationSupportDirectory.path);
```

This is process-wide application configuration. Reusing the same directory is safe; changing it after initialization fails. Do not generate a temporary directory per client or use this setting to bypass physical Store ownership. The explicit `libraryPath` variant supports source-checkout development. A normal macOS/iOS/Linux desktop host can use the engine's stable application-directory default.

## Bootstrap and observe

```ts
await client.bootstrap();
const stop = client.models.entry.watch({}, rows => render(rows));
```

```dart
await client.bootstrap();
final subscription = client.models.entry.watch().listen(render);
```

Bootstrap waits for complete finite authority units at its captured head; it does not promise perpetual freshness. An empty result still completes. Models marked `@@bootstrap` are selected initially; later rematerialization also covers held authority. The Stream supplied at open continues delivering after Bootstrap. Watch local Models for continuous state and use Query/Fetch results as invocation snapshots.

Close the client when its application session ends; cancel individual observers when their view ends. See [the generated API](client-api.md) for named Mutations, request-level storage, local transactions and unsent-work controls.
