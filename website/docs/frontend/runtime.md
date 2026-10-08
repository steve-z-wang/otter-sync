# Client runtime

The generated client is the whole client: besides the [typed Model, Mutation and Query APIs](client-api.md) it carries the runtime members described here, for escape-hatch reads, savepoints, connection control, recovery and prerequisites. TypeScript returns promises and Dart returns futures unless stated otherwise. Native validation failures reject the call; Dart reports them as `StateError`.

## Opening and schema changes

Open with `path`, the single `stream`, and an optional `StoreConnection`. Protocol 5 requires a fresh format-5 file; an unsupported existing file is refused intact. The generated facade supplies its compiled schema. Binding happens before network startup, so an offline reopen can validate the same database without a handshake. The backend binds the Store to its authenticated principal; credentials do not create a new file lifecycle.

Normal reopen retains the database's incarnation and durable calls. A supported schema or projection-generation change creates a new materialization context; retained Mutation contracts remain available for frozen retries. Bootstrap rematerializes held authority under the active context without inventing delivery progress. Keep the configured projection generation the same on backend and client, and change it when projection behavior changes.

A database has one physical owner. Opening it again while its current owner lives fails with `store_in_use`; close the existing Store first. `resetStore({ discardPending: true })` explicitly abandons pending work and changes incarnation; omitting the option refuses a reset with pending work. Old detached handles cannot authorize requests in the new lifecycle.

On Android, Dart hosts must call `Client.configureApplicationData(stableApplicationDirectory)` once before opening a Store. The directory is process-wide application configuration, including for the bundled ABI; it is not a per-client lock option. See [setup](setup.md).

## Escape-hatch reads

Typed reads (`models.<name>.get`, `query`, relation accessors, `watch`) are in the [client API](client-api.md). Untyped reads remain for cases the generated API does not cover. They read local SQLite through Rust; results are `RecordValue` rows (`Record<string, unknown>` in TypeScript, `Map<String, dynamic>` in Dart). SQL reads the Model tables described under [local table layout](#local-table-layout).

| Method | Input | Result |
| --- | --- | --- |
| `querySpec(model, query)` | Model name and `filter`, `orderBy`, `limit` | Matching records with requested order/limit |
| `readSql(sql, parameters)` | Read-only SQL and bound parameters | Result rows |
| `watchSql(sql, parameters, …)` | Read-only SQL and bound parameters | The current rows, then each different result ([watch SQL](#watch-sql-over-several-models)) |

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

### Watch SQL over several Models

`watchSql` keeps a read-only SQL answer current, such as a page that joins several Models. It delivers the current rows first, then each result that differs from the last one. The runtime asks SQLite which tables the statement reads; you never list them. It runs the statement again only after a commit that writes one of those tables: your own writes and transactions, optimistic Mutations and their settlement or rejection, Stream delivery, Bootstrap and cached Query/Fetch reads. A commit to any other Model does not run it.

=== "TypeScript"

    ```ts
    const stop = client.watchSql(
      'SELECT e.id, e.text, count(m.id) AS media FROM "Entry" e ' +
        'LEFT JOIN "Media" m ON m.entryId = e.id WHERE e.note IS ? GROUP BY e.id',
      [null],
      rows => console.log(rows),
      error => console.error(error),
    );
    // When the view is disposed:
    stop();
    ```

=== "Flutter"

    ```dart
    final rows = client
        .watchSql(
          'SELECT e.id, e.text, count(m.id) AS media FROM "Entry" e '
          'LEFT JOIN "Media" m ON m.entryId = e.id WHERE e.note IS ? GROUP BY e.id',
          parameters: [null],
        )
        .listen(print, onError: (Object error) => print(error));
    // When the view is disposed:
    await rows.cancel();
    ```

TypeScript takes the parameters as the second argument and returns a function that stops the watch, like `watch`; Dart returns a `Stream` and takes named `parameters:`, like `readSql`. The statement must be a single read-only `SELECT` or `WITH … SELECT` over Model tables. A write, a `PRAGMA`, an engine table (`axton_*`) or a statement SQLite cannot run fails the watch at once: TypeScript calls `onError`, Dart ends the stream with the error. A later run that fails goes to the connection's `onError` and the watch stays. Calling it from inside a transaction callback fails with `transaction_active` (React Native waits for the commit instead). Closing the client ends the watch.

A commit to a table the statement reads runs it again even when none of its rows changed; the result is then compared and nothing is delivered. Keep watched statements as narrow as the view needs.

### Local table layout

The layout SQL sees is a stable contract, so product SQL keeps working across AXTON upgrades:

- A Model's table is named exactly the Model name (`"Entry"`, `"MomentPlacement"`), and each column exactly its field name. SQLite compares names without case; quote a name only when it is a reserved word or contains unusual characters.
- Every table AXTON owns is named `axton_*`. Do not read those tables; their layout can change in any release, and `watchSql` refuses them.
- Changing either rule is a breaking change.

A column holds the field's local value: an `Int` or `Boolean` is an integer (`1` or `0` for a boolean), a `Float` a real, a list JSON text, and a `String`, `Uuid`, enum or `DateTime` text, a `DateTime` as a UTC ISO 8601 instant in milliseconds. The table shows the local view, so rows include optimistic local changes and local-only records.

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

Await every call and nested callback. Savepoints must be properly nested, not run concurrently. An escaped transaction, unfinished operation or overlapping savepoint fails. A Mutation queued with `tx.mutations` inside a savepoint belongs to it: rolling the savepoint back discards that Mutation and its companion changes, and its Call fails with `transaction_rolled_back`, while Mutations queued outside the savepoint are kept. A savepoint cannot start while a Mutation's input callback runs ([named Mutations](client-api.md#mutations-and-queries)). Inside the transaction use `tx` reads; a call on the outer `client` from inside its own callback fails promptly with `transaction_active` on Node and Dart (on React Native, Mutation and Query calls fail and other outer calls wait behind the transaction).

## Server connection

Pass `connection` when opening the bound generated client. `client.connect` can reconfigure its transport later: TypeScript accepts `ServerOptions`; Dart uses `SyncServer`. Only one connection may be active per client. Network I/O happens outside the local transaction queue.

=== "TypeScript"

    ```ts
    const connection = await client.connect(
      { url: backendUrl, token: () => accessToken, headers: { 'x-app-build': '42' } },
      {
        onError: error => {
          if (error instanceof AdmissionRefused) console.warn('update required', error.body);
          else console.error(error);
        },
        refreshAuth: async () => { accessToken = await renewAccessToken(); },
      },
    );
    ```

=== "Flutter"

    ```dart
    final connection = await client.connect(
      SyncServer(
        url: backendUrl,
        token: () => accessToken,
        headers: {'x-app-build': '42'},
      ),
      onError: (error) {
        if (error is AdmissionRefused) {
          print('update required: ${error.body}');
        } else {
          print(error);
        }
      },
      refreshAuth: () async { accessToken = await renewAccessToken(); },
    );
    ```

| Option | TypeScript | Dart |
| --- | --- | --- |
| `url` | HTTP or HTTPS backend base URL | HTTP or HTTPS backend base URL |
| `token` | String or function returning a string/promise | Function returning a string/future |
| `headers` | Optional `Record<string, string>` | Optional `Map<String, String>` |
| `onError` | `(error: unknown) => void`, in connection options | Named callback on `connect` / `open` |
| `refreshAuth` | `() => Promise<void>`, in connection options | Named async callback on `connect` / `open` |
| Direct timeout | `connection.directTimeoutMs` on `open`, or `directTimeoutMs` on `connect`: integer milliseconds, 1–2,147,483,647; default 30,000 | `directTimeout` on `open` / `connect`: positive `Duration`; default 30 seconds |

Here `backendUrl`, `accessToken` and `renewAccessToken` belong to your application. Credentials travel in authorization headers. A platform host build (such as the Node and React Native packages) supplies the HTTP carrier; `open` and `connect` take no carrier. Such a TypeScript carrier implements `Transport`: current protocol-5 effects route `handshake`, `push`, `pull`, `action`, `fetch` and `materialize` to `/sync/handshake`, `/sync/mutations`, `/sync/pull`, `/sync/actions`, `/sync/fetch` and `/sync/materialize`. These carry Store admission, immutable Batches, finite delivery, fresh named Query/Model reads and owned materialization. Token functions run for new requests and connections, so they can read refreshed credentials. Authentication failures can invoke `refreshAuth`; background failures reach `onError` and retry with backoff.

`headers` travel with every request and the WebSocket upgrade, for example your app's platform and build so the backend's [`admit`](../backend/api.md#admission) can turn away an outdated release. Headers AXTON sets itself (`Authorization`, `Content-Type`, the WebSocket handshake) are refused when you connect. If the backend refuses the client, `onError` receives one `AdmissionRefused` carrying the `status` and `body` the backend chose, and the connection stops: nothing is retried, `refreshAuth` is not called, and local reads and writes go on. Queued work waits; connect again (for example after an update, with new `headers`) to resume. On React Native a refused WebSocket upgrade is not visible, so the refusal arrives with the first HTTP request instead.

### Catch-up and live updates

The native runtime owns one Stream subscription and its durable cursor. A fresh Store establishes its Bootstrap boundary before subscribing; reconnect uses existing committed progress. The server's acknowledgement can trigger HTTP catch-up even if no subsequent live frame arrives.

HTTP Delta and live delivery apply through the same native commit-unit path. A unit commits its records, authority evidence and delivery prefix atomically. An independent successful prefix may commit before a later unit fails. A unit that violates a required constraint cannot be split merely by lowering a transport limit. Adaptive smaller requests permit independent earlier units to progress, without skipping the failed group.

Named Mutations use immutable Batches with independently committed member outcomes. Query and Fetch use fresh finite read requests. Bootstrap, Sync and settlement-target recovery use immutable plans whose required units commit whole. Head capture advances no progress; handshake commits S and C=S, while B appears only after the final Bootstrap unit. Reopen retains committed coverage and frozen work.

Pause and close cancel requests and sockets. A replaced session's late response cannot write into the active Store. Normal reconnect retains context/incarnation; explicit reset creates a new lifecycle.

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

`client.syncState()` returns `{ clientId, pending, beforeImages, cursors, streams, rejections }`. `pending` counts queued work; `beforeImages` is a diagnostic count; `cursors` maps streams to received positions; `streams` describes the single bound Stream; account-wide `rejections` contains `RefusedAct` entries `{ id, name, version, code, act }`, keyed by `id`. `client.models.<name>.syncState(identity)` returns one record's `{ pending, rejections }`, whose rejection entries use `{ ordinal, code }`: pending entries carry an ordinal, Mutation name, phase, prerequisite states and `diverged` when replay failed over newer authority. Both are local snapshots, not network probes.

=== "TypeScript"

    ```ts
    const state = await client.models.entry.syncState({ id: 'entry-1' });
    for (const item of state.pending) console.log(item.ordinal, item.name, item.phase);
    for (const rejection of (await client.syncState()).rejections) {
      console.log(rejection.code);
      // After your UI has handled it:
      await client.dismissRejection(rejection.id);
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
      await client.dismissRejection(rejection['id'] as int);
    }
    ```

| Method | Result / effect |
| --- | --- |
| `syncState()` | The client's snapshot above |
| `models.<name>.syncState(identity)` | `{ pending, rejections }` for that record; pending entries carry `diverged` |
| `dismissRejection(ordinal)` | Remove a handled rejection from the durable local inbox; does not retry it |
| `drop(ordinal)` | Remove eligible unsent work and recompute local state; frozen/sent work cannot be cancelled this way |

Diagnostic phases are `queued` (not frozen) and `frozen` (request retained for sending or retry). An accepted receipt can leave the Call pending while required local authority is installed; the durable queue remains until native settlement completes. Receipt acceptance alone does not remove it. An ordinal is local bookkeeping. To retry a rejected business operation, make a new call after resolving the cause. See [sync and recovery](sync.md).

### Unsent work

An unsent-work screen reads three streams that cover the whole client, not one record. Each delivers the current value when it starts, then a new value after each local commit that changes it, and never a repeat. The runtime computes them from committed state; nothing polls.

| Member | Delivers |
| --- | --- |
| `rejections.watch` | The refusals kept until dismissed, oldest first. Each is a `RefusedAct`: `{ id, name, version, code, act }` |
| `failures.watch` | The queued calls that wait on a prerequisite task that failed, oldest first. Each is a `FailedAct`: `{ ordinal, name, version, act, tasks }`, where each task is `{ key, name, arguments, error }` |
| `outbound.watchPending` | The number of queued calls that have not settled: it changes when a call is queued, settled, refused or dropped |

`act` is the call as it was submitted: `{ args, operations }`, the call's arguments and its Model operations with their values (`{ model, op, identity, values? }`). Recover what the author wrote from `act.args`. A legacy mutation has `args: null`. A task's `name` and `arguments` come from the schema and are `null` for an opaque key.

=== "TypeScript"

    ```ts
    const stopRefused = client.rejections.watch((refused) => {
      for (const item of refused) console.log(item.id, item.name, item.code, item.act.args);
    });
    const stopFailed = client.failures.watch(
      (failed) => {
        for (const item of failed) console.log(item.ordinal, item.name, item.tasks.map((task) => task.error));
      },
      (error) => console.error(error),
    );
    const stopCount = client.outbound.watchPending((count) => console.log(`${count} unsent`));
    // When the screen closes:
    stopRefused();
    stopFailed();
    stopCount();
    ```

=== "Flutter"

    ```dart
    final refused = client.rejections.watch().listen((items) {
      for (final item in items) print('${item.id} ${item.name} ${item.code} ${item.act.args}');
    });
    final failed = client.failures.watch().listen((items) {
      for (final item in items) print('${item.ordinal} ${item.name} ${item.tasks.map((task) => task.error)}');
    });
    final count = client.outbound.watchPending().listen((pending) => print('$pending unsent'));
    // When the screen closes:
    await refused.cancel();
    await failed.cancel();
    await count.cancel();
    ```

In TypeScript `watch` takes a listener and an optional `onError`, and returns a function that stops delivery. In Dart it returns a `Stream` that starts when listened to and stops when cancelled. A stream that cannot start fails (`onError`, or a stream error) and delivers nothing; closing the client ends it. Starting one inside a transaction callback fails with `transaction_active`.

| Resolution | Effect |
| --- | --- |
| `rejections.get(id)` | One kept refusal, or `null` |
| `rejections.dismiss(id)` | Remove a refusal; nothing is retried |
| `failures.retry(taskKeys)` | Make each task pending again for every call waiting on it; the handler you registered at open runs it |
| `failures.drop(ordinal)` | Remove an unsent call and its local changes. No refusal is kept for it, because you decided; its `Call` completes with `dropped`. A call that depended on its records (one that edits a record it creates, for example) is refused with `dependency.rejected` and appears in `rejections`. A call already sent cannot be dropped |

The earlier `dismissRejection(ordinal)` and `drop(ordinal)` stay; `drop` keeps a `dropped` refusal you then dismiss.

#### Repair inside a transaction

`rejections.dismiss`, `failures.retry` and `failures.drop` are also on the transaction a `client.transaction` callback receives. There, a resolution applies at once for the rest of the callback and commits or rolls back with it. So a repair can drop a failed call and queue its replacement in one step: the replacement is planned without the dropped call's local changes and does not wait for it, and if the callback throws, both are undone and the original call stays as it was. A dropped call's `Call` completes, and a retried task's handler runs, only after the commit. A Mutation's input callback cannot resolve unsent work.

=== "TypeScript"

    ```typescript title="action-contract"
    const call = await client.transaction(async (tx) => {
      await tx.failures.drop(1);
      return tx.mutations.edit({ todo: { id: 'todo-1', title: 'Fixed' } });
    });
    console.log(call.status);
    ```

=== "Flutter"

    ```dart title="action-contract"
    final call = await client.transaction((tx) async {
      await tx.failures.drop(1);
      return tx.mutations.edit(const EditInput(todo: EditTodoUpdate(id: 'todo-1', title: Present('Fixed'))));
    });
    print(call.status);
    ```

## Prerequisites

A schema can require host I/O, such as an upload, before a durable call can be sent. The local change remains visible while this work is pending. Register one handler per prerequisite name when you open the client; the runtime runs it whenever a task becomes pending - when a Mutation that needs it is queued, when the client opens and finds one left from an earlier run, and when you reset one to `pending`. You never start the handlers yourself.

=== "TypeScript"

    ```ts
    const client = await GeneratedClient.open({
      path: "app.sqlite", stream: `User:${viewer}`, connection,
      prerequisites: {
        Uploaded: async (args, signal) => {
          const response = await fetch(`${backendUrl}/uploads/${String(args.key)}`, { method: "PUT", signal });
          if (response.status >= 500) throw new PrerequisiteRetry(`upload: HTTP ${response.status}`);
          if (!response.ok) throw Error(`upload refused: HTTP ${response.status}`);
        },
      },
    });
    ```

=== "Flutter"

    ```dart
    final client = await GeneratedClient.open(
      path: 'app.sqlite', stream: 'User:$viewer', connection: connection,
      prerequisites: {
        'Uploaded': (args, cancelled) async {
          final request = await http.putUrl(Uri.parse('$backendUrl/uploads/${args['key']}'));
          unawaited(cancelled.then((_) => request.abort()));
          final response = await request.close();
          if (response.statusCode >= 500) throw PrerequisiteRetry('upload: HTTP ${response.statusCode}');
          if (response.statusCode >= 300) throw StateError('upload refused: HTTP ${response.statusCode}');
        },
      },
    );
    ```

The request body is elided. A handler receives the task's schema-declared arguments and a cancellation - an `AbortSignal` in TypeScript, a `cancelled` future in Dart - that fires when the client closes. Handlers run one at a time and must tolerate running again after a crash or restart. Each name must be a prerequisite the schema declares, or `open` fails. A prerequisite you register no handler for is left for you to settle with `setReadiness`.

| Handler | Result |
| --- | --- |
| Returns | The task is ready; its calls can be sent. |
| Throws `PrerequisiteRetry` | Transient: the task stays pending and runs again after 1 s, then 2 s, 4 s and so on up to 30 s, with some jitter. The count starts again when the client reopens. |
| Throws anything else | Terminal: the task is failed with the error's text and is not retried until you reset it. |

| Method | Behavior |
| --- | --- |
| `pendingTasks()` | Return unresolved tasks, including `key`, `state`, schema-derived `name`/`arguments` and, for a failed task, `error` |
| `setReadiness(key, state)` | Set `ready`, `pending` or `failed`; use the task's opaque key, not a reconstructed key. `pending` runs its handler again at once, even one waiting out a retry delay |

Show failed tasks from [`failures.watch`](#unsent-work), `pendingTasks` or a record's `syncState`. To retry one, call `failures.retry([key])` or set its key to `pending`; to give up, drop the call that needs it with `failures.drop`. Mark ready only when the prerequisite actually completed. Closing the client cancels a running handler and waits for nothing: whatever the handler does after that is ignored.

A task that failed stays failed while any call waits on it. A call queued later that needs the same task inherits the failure: it is listed in `failures` at once, with that task, and does not wait silently. The task is not reset for it; retrying stays your decision, and one retry covers every call that waits on the task. Once no call waits on a task, a later call that needs it starts a fresh, pending task. So order matters in a repair: drop the failed call first, then queue the replacement, and the replacement starts the task afresh, which its handler runs once the transaction commits; queue the replacement first and it joins the failed task and keeps its failure.

## Protocol primitives

The application SDK exposes no `freeze`, `acknowledge` or `applyPull` methods. Application synchronization is managed by the bound connection. Rust owns frozen Batches, acknowledgment and authority installation; wire fields are defined in the [protocol 5 source](https://github.com/zanminwang/axton/blob/main/crates/core/src/protocol_v05.rs) and exercised by [current carrier tests](https://github.com/zanminwang/axton/blob/main/crates/core/tests/protocol_v05.rs). Do not manufacture receipts, advance cursors yourself or rewrite frozen requests to recover from a network failure.
