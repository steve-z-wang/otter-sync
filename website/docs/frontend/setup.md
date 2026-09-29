# Set up the client

Read, watch and update local data through the generated client. TypeScript and Flutter share the same Model, Mutation and Query contract, backed by the Rust engine and SQLite. Select a language above each example; Flutter examples use Dart.

The local Model examples use `Entry` from the [round-trip fixture](https://github.com/zanminwang/axton/blob/main/integration/e2e/fixtures/round-trip/models/entry.model); the [To-do example](../getting-started.md) uses `Todo` with `AddTodo` and `SetTodoDone` Mutations. [Generate your interfaces](../schema/define.md) before importing them.

## Set up the runtime

Packages are currently used from source. Build the native artifacts from the repository root:

```sh
bash scripts/build.sh
```

=== "TypeScript"

    The TypeScript client currently runs on Node.js 22.18 or newer. Build the native addon before importing generated code. The source package is `packages/client-js`; the generated `client.ts` of the examples already imports it through a relative path.

    Generate with `--client-runtime` pointing to that source package, relative to your generated directory. See the [schema guide](../schema/define.md#generate-from-a-source-checkout) for the working command.

=== "Flutter"

    Flutter uses the Dart client package. Add a local dependency to your application's `pubspec.yaml`, adjusting the path to your checkout:

    ```yaml
    dependencies:
      axton:
        path: /absolute/path/to/axton/packages/dart
    ```

    Run `flutter pub get`, or `dart pub get` in a Dart application. The package currently requires Dart 3.12 or newer. Generated code imports `package:axton/axton.dart`.

    A checkout's package bundles no native library, so `libraryPath` is required: `target/debug/libaxton_dart.dylib` on macOS or `libaxton_dart.so` on Linux. A released package's build hook bundles the library for each supported target, iOS and Android included; see [platform setup](platforms.md). Choose a writable application directory for the SQLite file.

## Open local storage

=== "TypeScript"

    ```ts
    import { GeneratedClient } from './generated/client.ts';

    const client = await GeneratedClient.open({ path: 'local.sqlite' });
    const entry = await client.models.entry.get({ id: 'entry-1' });
    console.log(entry?.text);
    ```

=== "Flutter"

    ```dart
    import 'generated/generated.dart';

    final client = await GeneratedClient.open(
      path: 'local.sqlite',
      libraryPath: '/absolute/path/to/axton/target/debug/libaxton_dart.dylib',
    );
    final entry = await client.models.entry.get(const EntryIdentity(id: 'entry-1'));
    print(entry?.text);
    ```

These examples open local storage without a connection. A fresh database returns null until you write local data or synchronize a channel. Use one active client per SQLite file and a separate file per signed-in user.

## Connect to your backend

Start the fixture backend with `bash integration/e2e/fixtures/round-trip/run.sh` (or the [To-do backend](../getting-started.md) with its own token and channel), then connect the client:

=== "TypeScript"

    ```ts
    import { GeneratedClient } from './generated/client.ts';

    const client = await GeneratedClient.open({
      path: 'local.sqlite',
      server: {
        url: 'http://127.0.0.1:4242',
        token: 'demo-user',
      },
      connection: { onError: console.error },
    });
    await client.scopes.subscribe('book:demo');
    ```

=== "Flutter"

    ```dart
    import 'generated/generated.dart';

    final client = await GeneratedClient.open(
      path: 'local.sqlite',
      libraryPath: '/absolute/path/to/axton/target/debug/libaxton_dart.dylib',
      server: SyncServer(
        url: 'http://127.0.0.1:4242',
        token: () => 'demo-user',
      ),
      onError: (error) => print(error),
    );
    await client.scopes.subscribe('book:demo');
    ```

Configure the server once. AXTON submits durable calls over HTTP, catches up from saved channel cursors over HTTP, and receives ongoing record changes over WebSocket. Direct calls use the separate request/response route. Every received page passes through the Rust engine into local SQLite and updates `watch` subscriptions.

Subscribing wakes the connection; it does not wait for initial records, and it does not download the records the channel already holds: a subscription starts at the position the server acknowledges for it and delivers what is published from then on. Call `bootstrap()` on the handle it returns to load that earlier history in the background ([subscribe and observe](sync.md#subscribe-and-observe)). The registration is durable, so it works offline and survives a restart, and so is the load. A client with no subscribed channels can still send durable calls and receive its own result and authority. Subscribe when your UI needs later changes made elsewhere. [Call results](sync.md#receive-your-own-results) and live synchronization are described in the sync guide. Replace the demo URL and token with your application's endpoint and credentials. On a physical device, localhost refers to that device; use a reachable development-server address.

Omit `server` to open local storage without starting a connection.

For expiring credentials, supply a token function and `refreshAuth`. See [server connection options](runtime.md#server-connection).

## Watch and write

=== "TypeScript"

    ```ts
    const stop = client.models.entry.watch({}, entries => console.log(entries), console.error);

    // This write stays local. Use client.mutations for backend work.
    await client.transaction(async tx => {
      await tx.models.entry.update({ id: 'entry-1' }, { text: 'Draft', note: null });
    });
    ```

=== "Flutter"

    ```dart
    final subscription = client.models.entry.watch().listen(
      (entries) => print(entries),
      onError: (Object error) => print(error),
    );

    // This write stays local. Use client.mutations for backend work.
    await client.transaction((tx) async {
      await tx.models.entry.update(
        const EntryIdentity(id: 'entry-1'),
        const EntryPatch(text: Present('Draft'), note: Present(null)),
      );
    });
    ```

Watch emits an initial local result and distinct committed results. Standalone `client.models` CRUD and `tx.models` CRUD change only local storage; they do not upload. A later backend authority update for the same identity can replace cached local content. Use a generated Mutation or Query for backend work; [Mutations and Queries](client-api.md#mutations-and-queries) explains durable acceptance and direct results.

In Flutter, use the watch stream with `StreamBuilder<List<Entry>>`; retain it for the view's lifetime rather than reopening a client on every build. Dart's `Present(null)` clears a nullable field; omitting the field leaves it unchanged.

## Connection and cleanup

=== "TypeScript"

    ```ts
    await client.connection!.pause();
    // Local reads and writes remain available.
    await client.connection!.resume();

    // When the owning view/application finishes:
    stop();
    await client.close();
    ```

=== "Flutter"

    ```dart
    await client.connection!.pause();
    // Local reads and writes remain available.
    await client.connection!.resume();

    // When the owner of this client finishes:
    await subscription.cancel();
    await client.close();
    ```

Pausing cancels network activity; resuming reconnects and catches up from saved progress. Database/client lifetime belongs to the application; watch subscriptions belong to their views.

See [Client API](client-api.md) for typed calls, [offline work and sync](sync.md) for connection/recovery behavior, and [advanced client APIs](runtime.md) for SQL, savepoints and prerequisites.
