# Set up the client

Generate your Model and named Mutation/Query interfaces first ([schema generation](../schema/define.md)). TypeScript, Dart and React Native use the same Rust engine and SQLite Store.

## Open local storage

Supply a writable database path, one Stream and a connection with stable identity. The identity is available offline: backend, viewer and contract must match backend configuration. Tokens and URLs are connection details, not Store identity.

```ts
const client = await GeneratedClient.open({
  path: 'local.sqlite', stream: 'User:alice',
  connection: {
    url: 'http://127.0.0.1:4242', token: 'alice',
    identity: { backend: 'my-api', viewer: 'alice', contract: 'my-app' },
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
    identity: const StoreIdentity(
      backend: 'my-api', viewer: 'alice', contract: 'my-app'),
  ),
);
```

Opening commits local storage before starting network work; connectivity is not required to reopen a Store. Normal reopen retains its incarnation, pending Calls, manifests and delivery progress. Use a separate file for each binding and one active owner per file. Configure the same `projectionGeneration` on client and backend; it defaults to `'1'`.

## Native libraries

Installed Node packages select their platform addon. Installed Dart packages bundle a native library through their build hook. For a source checkout, run `bash scripts/build.sh` and pass Dart's `libraryPath` pointing to `target/debug/libaxton_dart.dylib` on macOS or `libaxton_dart.so` on Linux. React Native requires the [native module](https://github.com/zanminwang/axton/blob/main/packages/client-react-native/native-module/README.md) in a native build; Expo Go cannot supply it.

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

Bootstrap waits for the entire bounded manifest and real delta catch-up. An empty result still completes. Models marked `@@bootstrap` are selected initially; later rematerialization also covers held authority. The Stream supplied at open continues delivering after Bootstrap. Watch local Models for continuous state and use Query/Fetch results as invocation snapshots.

Close the client when its application session ends; cancel individual observers when their view ends. See [the generated API](client-api.md) for named Mutations, request-level storage, once results, local transactions and unsent-work controls.
