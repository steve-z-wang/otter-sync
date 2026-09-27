# Client runtime

The generated client is the whole client: besides the [typed Model, Mutation and Query APIs](client-api.md) it carries the runtime members described here, for escape-hatch reads, savepoints, connection control, recovery and prerequisites. TypeScript returns promises and Dart returns futures unless stated otherwise. Native validation failures reject the call; Dart reports them as `StateError`.

## Opening and schema changes

=== "TypeScript"

    ```ts
    const client = await GeneratedClient.open({ path: 'local.sqlite' });
    ```

=== "Flutter"

    ```dart
    final client = await GeneratedClient.open(
      path: 'local.sqlite',
      libraryPath: '/absolute/path/to/libaxton_dart.dylib',
    );
    ```

The compiled schema is embedded in the generated client. `client.clientId` is a read-only, persistent identity for that database, used for retry deduplication. Use one active client per database and a separate file per signed-in user. Do not duplicate a database and then let both copies independently send calls under the same client identity.

When the schema compiled into the client differs from the one the database was built for, the runtime decides at open ([local storage](storage.md)): an added Model or nullable field is applied in place; anything else leaves the file untouched and opens a fresh database file beside it, `local.sqlite.1`, which resynchronises from the backend. If the old file still holds unsent calls, it stays open for them instead; `syncState().schema.pending` tells you, and once they are sent you call `rebuild()`:

=== "TypeScript"

    ```ts
    const { schema: state } = await client.syncState();
    if (state.pending) {
      console.log(`sending ${state.pending.pending} changes before upgrading`);
      // … connect, wait for pending to reach 0, then:
      const report = await client.rebuild();
      console.log(report.newFile, report.leftPending);
    }
    ```

=== "Flutter"

    ```dart
    final state = (await client.syncState())['schema'] as Map<String, dynamic>;
    if (state['pending'] != null) {
      // … connect, wait for pending to reach 0, then:
      final report = await client.rebuild();
      print(report['newFile']);
    }
    ```

`rebuild()` switches the same client to the new file and rejects while unsent calls remain. `rebuild({ discardPending: true })`, or `discardPending: true` at open, rebuilds at once; the report names `leftPending` calls and `leftDirect` local-only records that stay in `oldFile`. Nothing is moved between schemas and the old file is never deleted by the runtime. `migration` is still accepted for compatibility and ignored.

## Escape-hatch reads

Typed reads (`models.<name>.get`, `query`, relation accessors, `watch`) are in the [client API](client-api.md). Two untyped reads remain for cases the generated API does not cover. Both read local SQLite through Rust; results are `RecordValue` rows (`Record<string, unknown>` in TypeScript, `Map<String, dynamic>` in Dart).

| Method | Input | Result |
| --- | --- | --- |
| `querySpec(model, query)` | Model name and `filter`, `orderBy`, `limit` | Matching records with requested order/limit |
| `readSql(sql, parameters)` | Read-only SQL and bound parameters | Result rows |

TypeScript's `readSql` takes an optional positional second argument; Dart uses named `parameters:`.

=== "TypeScript"

    ```ts
    const rows = await client.querySpec('Entry', {
      filter: { note: null },
      orderBy: [{ field: 'text', direction: 'ascending' }],
      limit: 20,
    });
    const matches = await client.readSql(
      'SELECT id, text FROM "Entry" WHERE text = ?', ['Draft'],
    );
    ```

=== "Flutter"

    ```dart
    final rows = await client.querySpec('Entry', {
      'filter': {'note': null},
      'orderBy': [{'field': 'text', 'direction': 'ascending'}],
      'limit': 20,
    });
    final matches = await client.readSql(
      'SELECT id, text FROM "Entry" WHERE text = ?',
      parameters: ['Draft'],
    );
    ```

`querySpec` calls its equality filter `filter`; the generated API calls it `where`. SQL rejects writes. Bind values instead of interpolating them into SQL.

## Transactions and savepoints

`client.transaction(callback)` commits the callback's result or rolls back on failure; see [transactions](client-api.md#transactions). Standalone Model CRUD runs in its own local transaction; a durable call privately commits its intent and any inferred optimism together. Inside a transaction, `tx.transaction` is the runtime transaction; on Node and Dart it also offers `savepoint(callback)`, a nested scope that rolls back on failure and returns its callback's result (React Native's does not).

=== "TypeScript"

    ```ts
    await client.transaction(async tx => {
      await tx.models.entry.update({ id: 'entry-1' }, { text: 'Draft' });
      try {
        await (tx.transaction as Transaction).savepoint(async () => {
          await tx.models.entry.update({ id: 'entry-1' }, { note: 'Temporary' });
          throw new Error('Discard this note');
        });
      } catch {
        // The note rolls back; the earlier local update can still commit.
      }
    });
    ```

=== "Flutter"

    ```dart
    await client.transaction((tx) async {
      await tx.models.entry.update(
        const EntryIdentity(id: 'entry-1'),
        const EntryPatch(text: Present('Draft')),
      );
      try {
        await tx.transaction.savepoint(() async {
          await tx.models.entry.update(
            const EntryIdentity(id: 'entry-1'),
            const EntryPatch(note: Present('Temporary')),
          );
          throw StateError('Discard this note');
        });
      } catch (_) {
        // The note rolls back; the earlier local update can still commit.
      }
    });
    ```

Await every call and nested callback. Savepoints must be properly nested, not run concurrently. An escaped transaction, unfinished operation or overlapping savepoint fails. Inside the transaction use `tx` reads; a call on the outer `client` from inside its own callback fails promptly with `transaction_active` on Node and Dart (on React Native, Mutation and Query calls fail and other outer calls wait behind the transaction).

## Server connection

Pass `server` when opening the generated client, or call `client.connect` after opening local storage. TypeScript accepts `ServerOptions`; Dart uses `SyncServer`. Only one connection may be active per client. Network I/O happens outside the local transaction queue.

=== "TypeScript"

    ```ts
    const connection = await client.connect(
      { url: backendUrl, token: () => accessToken },
      {
        onError: error => console.error(error),
        refreshAuth: async () => { accessToken = await renewAccessToken(); },
      },
    );
    ```

=== "Flutter"

    ```dart
    final connection = await client.connect(
      SyncServer(url: backendUrl, token: () => accessToken),
      onError: (error) => print(error),
      refreshAuth: () async { accessToken = await renewAccessToken(); },
    );
    ```

| Option | TypeScript | Dart |
| --- | --- | --- |
| `url` | HTTP or HTTPS backend base URL | HTTP or HTTPS backend base URL |
| `token` | String or function returning a string/promise | Function returning a string/future |
| `onError` | `(error: unknown) => void`, in connection options | Named callback on `connect` / `open` |
| `refreshAuth` | `() => Promise<void>`, in connection options | Named async callback on `connect` / `open` |
| Direct timeout | `connection.directTimeoutMs` on `open`, or `directTimeoutMs` on `connect`: integer milliseconds, 1–2,147,483,647; default 30,000 | `directTimeout` on `open` / `connect`: positive `Duration`; default 30 seconds |

Here `backendUrl`, `accessToken` and `renewAccessToken` belong to your application. Credentials travel in authorization headers. Token functions run for new requests and connections, so they can read refreshed credentials. Authentication failures can invoke `refreshAuth`; background failures reach `onError` and retry with backoff.

### Catch-up and live updates

AXTON manages these phases automatically:

1. Connect to `/sync/live` and subscribe to the current channel set. The server installs listeners, then acknowledges the subscription with each channel's current position. A channel with no saved cursor adopts the acknowledged position as its starting point in one local transaction and fetches nothing older; that happens once per subscription, and a later session never repeats it.
2. If a saved cursor is behind, fetch missing records through one `POST /sync/pull` for all channels, repeated while a channel has more. Queue WebSocket pages arriving while catch-up runs. If every cursor is current, skip this step. Only channels with a saved cursor are requested; one still waiting for its starting point is subscribed on the socket and asked for nothing.
3. Continue receiving WebSocket updates. HTTP and WebSocket pages enter the same serialized Rust processing path, using each channel's saved cursor.

For either source, a page applies as one transaction and names a range for each channel it covers. A channel already covered by its cursor is left alone. A range spanning the current cursor applies: for example, at cursor `100`, a range `90 → 120` advances the channel to `120`, and each record's stamp decides whether its content is newer. A range starting beyond the current cursor is a gap. Then nothing from the page applies, and HTTP recovery fetches the missing range. Pages update SQLite and watches through the same engine logic.

Durable submission of Mutations and queued Queries runs independently through `POST /sync/mutations`; direct calls use `POST /sync/actions` with the configured finite timeout. A connection with no subscribed channels can still submit calls without opening a socket.

A `bootstrap()` load is a third, independent work class on the same connection: one bounded `POST /sync/pull` at a time across all channels, asked for while the connection is running and not paused, retried with the same backoff after a transport failure, and taking turns between channels that have one registered. It does not hold up the socket, the catch-up request or Action submission, and pausing the connection defers its next page instead of failing it. Progress is committed page by page, so closing the client or losing the network resumes where it stopped.

Reconnection and subscription changes repeat catch-up from saved progress; a new session is not a new starting point, and an acknowledged position below saved progress is reported through `onError` rather than rewinding the channel. The client checks that every HTTP response and queued WebSocket page belongs to the current session before applying it. Pause and close cancel requests and sockets; resume creates a new session. The runtime does not poll for remote changes.

## Connection controls

TypeScript calls the returned object `Connection`; Dart calls it `RuntimeConnection`.

| Method | Behavior |
| --- | --- |
| `pause()` | Stop background network work; local reads/writes remain available |
| `resume()` | Resume a paused connection and schedule work |
| `wake()` | Ask the driver to re-evaluate pending work |
| `close()` | Permanently stop this connection; the client database stays open |
| `closed` (Dart) | Future that completes when the connection closes |

All controls return promise/future void. Pause/close cancel network activity that is still in flight and discard what the canceled session later delivers; a direct call whose response had already arrived is still applied and answers its result. Persisted frozen requests remain available for retry. After close, call `client.connect` again to resume sync. `await client.close()` closes its connection and native database resources and is idempotent; subsequent client operations fail.

## Pending work and recovery

`client.syncState()` returns `{ clientId, pending, beforeImages, cursors, channels, rejections, schema }`. `pending` counts queued work; `schema` is `{ rebuilt, pending, lastRebuild }` from the open-time schema check ([opening and schema changes](#opening-and-schema-changes)); `beforeImages` is a diagnostic count; `cursors` maps channels to received positions; `channels` lists desired subscriptions; `rejections` contains `{ ordinal, code }` entries. `client.models.<name>.syncState(identity)` returns one record's `{ pending, rejections }`: pending entries carry an ordinal, Mutation or Query name, phase, prerequisite states and `diverged` when replay failed over newer authority. Both are local snapshots, not network probes.

=== "TypeScript"

    ```ts
    const state = await client.models.entry.syncState({ id: 'entry-1' });
    for (const item of state.pending) console.log(item.ordinal, item.name, item.phase);
    for (const rejection of (await client.syncState()).rejections) {
      console.log(rejection.code);
      // After your UI has handled it:
      await client.dismissRejection(rejection.ordinal);
    }
    ```

=== "Flutter"

    ```dart
    final state = await client.models.entry.syncState(
      const EntryIdentity(id: 'entry-1'),
    );
    for (final item in state.pending) {
      print('${item.ordinal} ${item.name}: ${item.phase}');
    }
    for (final rejection in (await client.syncState())['rejections'] as List) {
      print(rejection['code']);
      // After your UI has handled it:
      await client.dismissRejection(rejection['ordinal'] as int);
    }
    ```

| Method | Result / effect |
| --- | --- |
| `syncState()` | The client's snapshot above |
| `models.<name>.syncState(identity)` | `{ pending, rejections }` for that record; pending entries carry `diverged` |
| `dismissRejection(ordinal)` | Remove a handled rejection from the durable local inbox; does not retry it |
| `drop(ordinal)` | Remove eligible unsent work and recompute local state; frozen/sent work cannot be cancelled this way |

Phases are `queued` (not frozen) and `frozen` (request retained for sending or retry); a receipt completes a frozen call and removes it, so there is no phase after `frozen`. An ordinal is local bookkeeping. To retry a rejected business operation, make a new call after resolving the cause. See [sync and recovery](sync.md).

## Prerequisites

A schema can require host I/O, such as an upload, before a durable call can be sent. The local change remains visible while this work is pending.

=== "TypeScript"

    ```ts
    await client.runPrerequisites({
      Uploaded: async args => { await uploadFile(args.key); },
    });
    ```

=== "Flutter"

    ```dart
    await client.runPrerequisites({
      'Uploaded': (args) async { await uploadFile(args['key']); },
    });
    ```

`uploadFile` is application code. Dart accepts the equivalent map of async callbacks. Callbacks run one at a time and must tolerate retry after a crash or restart; starting a connection does not automatically supply or run your host callbacks.

| Method | Behavior |
| --- | --- |
| `pendingTasks()` | Return unresolved tasks, including `key`, `state`, schema-derived `name`/`arguments` and, for a failed task, `error` |
| `runPrerequisites(handlers)` | Run pending tasks; success marks ready, a callback failure marks failed with the error's text, a task with no handler is marked failed with `missing prerequisite handler` |
| `setReadiness(key, state)` | Set `ready`, `pending` or `failed`; use the task's opaque key, not a reconstructed key |

Callback failures are recorded as failed tasks with their reason rather than rethrown by the runner; a task no handler covers is recorded the same way and the run goes on. Inspect `pendingTasks` or a record's `syncState` to display them. To retry, set the failed key to `pending`, then run callbacks again. Mark ready only when the prerequisite actually completed.

## Protocol primitives

The engine's protocol methods (`freeze`, `acknowledge`, `applyPull`, the last two returning reports for records they could not apply) are not part of the application surface; they exist on the runtime handle the framework's own tests use. Application synchronization is managed by `connect`. Wire fields are defined in the [protocol source](https://github.com/zanminwang/axton/blob/main/crates/core/src/protocol.rs) and exercised by [shared wire fixtures](https://github.com/zanminwang/axton/blob/main/fixtures). Do not manufacture receipts, advance cursors yourself or rewrite frozen requests to recover from a network failure.
