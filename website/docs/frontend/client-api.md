# Generated client

This page covers the TypeScript and Dart APIs emitted by the schema compiler. Follow [getting started](../getting-started.md) for a running backend, or [generate your interfaces](../schema/define.md) first.

Choose TypeScript or Flutter above a code example to switch languages throughout the page. Flutter examples use Dart.

Local Model examples below use an `Entry` model with `id`, `text` and nullable `note`. Mutation and Query examples use the [generated operation fixture](https://github.com/zanminwang/axton/blob/main/integration/action-contract/schema.model), whose `Todo` model has `id`, `title`, `state` and nullable `note`. TypeScript imports come from `generated/client.ts`; Dart imports come from `generated/generated.dart`.

## Open a client

=== "TypeScript"

    ```ts
    import { GeneratedClient } from './generated/client.ts';

    const client = await GeneratedClient.open({
      path: 'local.sqlite',
    });
    ```

=== "Flutter"

    ```dart
    import 'generated/generated.dart';

    final client = await GeneratedClient.open(
      path: 'local.sqlite',
      libraryPath: '/absolute/path/to/libaxton_dart.dylib',
    );
    ```

Both examples open local storage. To start background sync, supply `server` as shown in [client setup](setup.md#connect-to-your-backend).

| Option | Required | Behavior |
| --- | --- | --- |
| `path` | Yes | SQLite file to create or reopen. The application selects a writable directory. Use a separate file per signed-in user. |
| `server` | No | Backend URL and credentials: `ServerOptions` in TypeScript, `SyncServer` in Dart. AXTON manages durable and direct Mutation and Query requests, HTTP catch-up and WebSocket updates. |
| `connection` (TypeScript) | No | `onError`, `refreshAuth` and `directTimeoutMs` for the connection. `onError` also receives an `AxtonReport` for each record AXTON could not apply ([Sync](sync.md#recover-from-connection-failures)). |
| `onError`, `refreshAuth`, `directTimeout` (Dart) | No | Callbacks and a `Duration` for direct requests, passed to `open`. |
| `libraryPath` (Dart) | Outside iOS | Absolute native library path; iOS can use symbols linked into the process. |
| `migration` | No | Defaults and optional cursor rewind for an explicitly changed schema. See [runtime migration](runtime.md#opening-and-schema-changes). |

Returns `Promise<GeneratedClient>` / `Future<GeneratedClient>`. Opening can fail on native library loading, an unwritable or incompatible database, or an invalid schema. Completion means local storage is open, not that initial server data has arrived. Omitting `server` keeps the client local-only.

## Model APIs

### Get a record

=== "TypeScript"

    ```ts
    const entry = await client.models.entry.get({ id: 'entry-1' });
    console.log(entry?.text);
    ```

=== "Flutter"

    ```dart
    final entry = await client.models.entry.get(
      const EntryIdentity(id: 'entry-1'),
    );
    print(entry?.text);
    ```

`get(identity)` returns the complete typed record or `null` when it is absent from local storage. It does not call the backend loader. A newly opened cache can return `null` until channel synchronization supplies the record.

### Query records

=== "TypeScript"

    ```ts
    const entries = await client.models.entry.query({
      where: { note: null },
      orderBy: [{ field: 'text', direction: 'ascending' }],
      limit: 20,
    });
    ```

=== "Flutter"

    ```dart
    final entries = await client.models.entry.query(
      where: const EntryFilter(note: Present(null)),
      orderBy: const [EntryOrder(EntryOrderField.byText)],
      limit: 20,
    );
    ```

Returns a typed list. `where` is an equality filter; supplied fields must all match. An omitted filter selects all local records of that model. `orderBy` is a list of fields and ascending/descending directions; Dart expresses descending order with `descending: true`. `limit` caps the result. Do not rely on an unspecified row order. These generated filters are not a general SQL expression language.

### Watch records

=== "TypeScript"

    ```ts
    const stop = client.models.entry.watch(
      { where: { note: null } },
      entries => console.log(entries),
      error => console.error(error),
    );
    // When the view is disposed:
    stop();
    ```

=== "Flutter"

    ```dart
    final subscription = client.models.entry
        .watch(where: const EntryFilter(note: Present(null)))
        .listen(print, onError: (Object error) => print(error));
    // When the view is disposed:
    await subscription.cancel();
    ```

TypeScript returns an unsubscribe function; Dart returns `Stream<List<Entry>>`. A listener receives an initial query result and distinct results after committed local changes, including sync changes. Identical query results are suppressed. `watch` accepts equality filters, not `query`'s ordering or limit options. It reports current query results, not a log of every intermediate write.

### Follow a relation

The compiler emits relation methods only for relationships declared in the schema. A forward relation returns the related record or `null`; an inverse collection returns a list. Methods take the source model's identity, and read the local database.

For a `Comment.book` relationship, `client.models.comment.book(commentIdentity)` follows the forward reference. The [relations fixture](https://github.com/zanminwang/axton/blob/main/fixtures/compiler/relations.model) defines `Book.comments` and `Comment.book`; the [generated API checks](https://github.com/zanminwang/axton/blob/main/integration/generated-api/verify.sh) exercise those accessors. A singular inverse needs a unique foreign key; an ambiguous inverse is rejected during compilation.

## Transactions

=== "TypeScript"

    ```ts
    await client.transaction(async tx => {
      await tx.models.entry.update({ id: 'entry-1' }, { text: 'Draft' });
      const draft = await tx.models.entry.get({ id: 'entry-1' });
      if (draft?.text !== 'Draft') throw Error('local update missing');
    });
    ```

=== "Flutter"

    ```dart
    await client.transaction((tx) async {
      const id = EntryIdentity(id: 'entry-1');
      await tx.models.entry.update(id, const EntryPatch(text: Present('Draft')));
      final draft = await tx.models.entry.get(id);
      if (draft?.text != 'Draft') throw StateError('local update missing');
    });
    ```

`transaction<T>(callback)` returns the callback's result after local commit. Throwing or a failed operation rolls it back. Await each operation, including nested callbacks; unfinished work is rejected. Inside the callback, use `tx.models` for reads that must see earlier writes in the same transaction. Calling the outer `client` from inside its own transaction callback - a read, a Mutation or a Query - fails with `transaction_active` on Node and Dart (React Native rejects Mutations and Queries and lets other outer calls wait behind the transaction) instead of waiting on itself.

`GeneratedTransaction` exposes `models` and the underlying `transaction`; it has no `mutations`, `queries` or watch method. For nested savepoints, see [transactions and savepoints](runtime.md#transactions-and-savepoints).

## Mutations and Queries

A Mutation or Query is a typed backend operation declared in the [schema](../schema/define.md#declare-mutations-and-queries): a Mutation may change business state or perform external effects, while a Query reads without business side effects. Each has a default delivery route and an explicit override. The method you call selects the route and fixes its return type; no option switches it.

| Method | Delivery | Returns | `await` resolves when |
| --- | --- | --- | --- |
| `client.mutations.<name>(args, options?)` | Durable | `Call<Output>` | The intent, its queue entry and the declared Model optimism commit locally |
| `client.mutations.call.<name>(args, options?)` | Direct | `Output` | The backend outcome is received and its authority applied locally |
| `client.queries.<name>(args, options?)` | Direct | `Output` | The backend outcome is received and its authority applied locally |
| `client.queries.enqueue.<name>(args, options?)` | Durable | `Call<Output>` | The intent and its queue entry commit locally; no optimism is inferred |

Dart returns the same types as `Future`s and takes named arguments. Choose durable delivery for work that must be accepted offline and sent later; choose direct delivery when the caller needs the backend's answer now. All four routes run the registered backend handler for that name and version and resolve Model results through the versioned Loader. None runs inside a local `client.transaction` callback.

These examples use the [operation fixture](https://github.com/zanminwang/axton/blob/main/integration/action-contract/schema.model). Its `AddTodo` Mutation takes a create operand, optional update, delete list, nullable enum and string list; `FindTodos(text String, cursor String?)` is a Query returning `todos Todo[]` and `nextCursor String?`.

=== "TypeScript"

    ```typescript title="action-contract"
    const todo: TodoCreate = { id: 'todo-1', title: 'Done', state: 'open', note: null };
    const call = await client.mutations.addTodo({ todo, gone: [], status: null, tags: [] });
    console.log(call.status);
    const outcome = await call.wait();
    // The result holds only AddTodo's declared outputs; the created Todo is local authority by now.
    if (outcome.error === null) console.log(outcome.result.count, await client.models.todo.get({ id: 'todo-1' }));
    else console.error(outcome.error.code, outcome.error.execution);
    const page = await client.queries.findTodos({ text: 'design', cursor: null });
    console.log(page.todos, page.nextCursor);
    ```

=== "Flutter"

    ```dart title="action-contract"
    final call = await client.mutations.addTodo(
      todo: const TodoCreate(id: 'todo-1', title: 'Done', state: Status.open, note: null),
      gone: const [],
      status: null,
      tags: const [],
    );
    final outcome = await call.wait();
    if (outcome is CallSuccess<AddTodoOutput>) {
      print([outcome.result.count, await client.models.todo.get(const TodoIdentity(id: 'todo-1'))]);
    } else if (outcome is CallFailure<AddTodoOutput>) {
      print(outcome.error.code);
    }
    final page = await client.queries.findTodos(text: 'design', cursor: null);
    print([page.todos, page.nextCursor]);
    ```

The overrides use the other route. `mutations.call` waits for the final Mutation outcome, and `queries.enqueue` queues a Query to run after reconnecting. The fixture's `SendEmail` Mutation has no outputs, so its durable call is a `Call<void>`:

=== "TypeScript"

    ```typescript title="action-contract"
    const confirmed = await client.mutations.call.addTodo({
      todo: { id: 'todo-2', title: 'Now', state: 'open', note: null },
      gone: [], status: null, tags: [],
    });
    const queued = await client.queries.enqueue.findTodos({ text: 'design', cursor: null });
    const later = await queued.wait();
    const email = await client.mutations.sendEmail({ to: 'team@example.test', subject: 'Todo', body: 'Created' });
    console.log(confirmed.count, later.error, email.status);
    ```

=== "Flutter"

    ```dart title="action-contract"
    final confirmed = await client.mutations.call.addTodo(
      todo: const TodoCreate(id: 'todo-2', title: 'Now', state: Status.open, note: null),
      gone: const [],
      status: null,
      tags: const [],
    );
    final queued = await client.queries.enqueue.findTodos(text: 'design', cursor: null);
    final later = await queued.wait();
    final email = await client.mutations.sendEmail(
      to: 'team@example.test',
      subject: 'Todo',
      body: 'Created',
    );
    print([confirmed.count, later, email.status]);
    ```

A durable call returns a `Call<Output>` with exactly two members: `status` (`pending`, `succeeded` or `failed`) and `wait()`. Its return confirms local acceptance, not backend success. Initial validation or local commit failure rejects before a handle exists. `wait()` resolves to a `CallOutcome`: `{ result, error }` in TypeScript, with `error` null on success, and `CallSuccess` or `CallFailure` in Dart. A pending network retry keeps waiting. `CallError.code` is a stable machine code, and `execution` is `rejected` for a known rejection or `unknown` when the outcome cannot be observed. A timeout with unknown execution does not prove the backend did nothing. TypeScript exports `Call`, `CallStatus`, `CallOutcome`, `CallError` and `CallOptions`; Dart exports `Call`, `CallStatus`, `CallOutcome`, `CallSuccess`, `CallFailure`, `CallError` and `CallStore`.

A direct call does not enter the durable queue and infers no local optimism. It rejects with `CallError` when execution fails, and there is no automatic offline fallback: without a connection it rejects with `action.unavailable` instead of queueing. Each direct invocation gets a fresh call ID. Inferred optimism applies only to a durable Mutation with Model operands; a durable plain-value Mutation such as `SendEmail` changes nothing locally until its outcome arrives. A queued Query captures its arguments at enqueue and reads when the backend executes it, not at enqueue time; it derives no speculative result from local data.

TypeScript sets `connection.directTimeoutMs` in milliseconds (an integer from 1 to 2,147,483,647); Dart sets `directTimeout` on `open` or `connect` to a positive `Duration`. Both default to 30 seconds. See [server connection](runtime.md#server-connection) for option placement. Queued calls use background retry instead of this direct timeout.

A result holds only the outputs the operation declares. A Model input is not a result: once a call completes, the backend's authority for each Model it created, updated or deleted is already applied locally, whatever the outputs or `store` say, so read it from `client.models`. An output may share an input's name and still name a different record; the fixture's `EditAndRead(todo Todo.update) { todo Todo }` can edit one Todo and return another. A handler selects an explicit Model output by returning an identity object with every `@@id` field. A Model output is the Loader snapshot for that invocation: a later call in the same batch can change the batch-final authority, and a pending local edit can change what `client.models.todo.get(...)` shows, while the result retains its own snapshot. Records the backend handler changed beyond the Model inputs are not returned to the caller; they arrive through the Channels they belong to. Optional outputs can be null, lists preserve order and duplicates, and no outputs means void. Results held by live calls are kept in client memory; reopening preserves pending work and completion state, but does not restore a past business result to a new handle. The backend retains committed outcomes of both kinds for replay without a TTL or automatic pruning.

### Storing Model results

By default, Model records returned by explicit Model outputs also update the matching local Models, for Queries as well as Mutations. Pass `store` when calling to return results without storing them, for example search suggestions. `false` stores none of those outputs; a map names outputs, and unnamed ones stay stored. All four routes accept it and the result type is unchanged.

=== "TypeScript"

    ```typescript title="action-contract"
    const suggestions = await client.queries.getTodos({}, { store: false });
    const page = await client.mutations.call.openTodo({ store: null }, { store: { suggestions: false } });
    const queued = await client.queries.enqueue.findTodos({ text: 'x', cursor: null }, { store: false });
    const outcome = await queued.wait();
    console.log(suggestions.todos, page.mainTodo, outcome.error);
    ```

=== "Flutter"

    ```dart title="action-contract"
    final suggestions = await client.queries.getTodos(store: const GetTodosStore.none());
    final page = await client.mutations.call.openTodo(
      store: null,
      outputStore: const OpenTodoStore.outputs(suggestions: false),
    );
    print([suggestions.todos, page.mainTodo]);
    ```

`store` only controls these output records. Records a Mutation's Model inputs target are always reconciled, a record also returned by a stored output is stored, and an already stored row is left unchanged by an unstored read. A Query with `store: false` creates, updates or deletes no local row from its outputs. The option does not change backend behavior: the outcome is still saved for replay, and a retry keeps the original choice. A call ID replayed with a different `store` is rejected with `call.identity_conflict`. Output names `toWire`, `toString`, `hashCode`, `runtimeType` and `noSuchMethod` are reserved for Model outputs a handler selects, because they would collide with the Dart selector. In Dart, each operation's `{Name}Store` selector extends `CallStore`; its parameter is `store` unless the operation has a business input named `store`, as `OpenTodo` does; it is then `outputStore`.

The TypeScript/React Native call observer requires a working `WeakRef`; a runtime without it rejects durable invocation with `CallError` code `action.unsupported_runtime`. Dart uses `WeakReference`. SDKs keep active waits strongly until they settle, while otherwise allowing unobserved handles to be collected. Exceptions from a diagnostic `onError` callback after authority commits are reported through the runtime's uncaught-error channel (`reportError` or an asynchronous throw in JavaScript; the current Zone in Dart). They do not replace the call result, retry the handler or become transport errors.

### Reuse a Query result with `once`

A direct Query can opt in, at the call site, to reusing the complete result of an earlier successful call. Pass `once: true`: the first call runs as an ordinary direct call and, when it succeeds, saves its complete result in the local database; a later `once` call with equal arguments and store policy returns that saved result without a request. `refresh: true` (only together with `once`) always requests and replaces the saved result when the request succeeds. `client.queries.invalidate.<name>(args)` discards the saved results of one argument set. A call without `once` is unchanged: a fresh request that neither reads nor writes saved results.

=== "TypeScript"

    ```typescript title="action-contract"
    const first = await client.queries.findTodos({ text: 'design', cursor: null }, { once: true });
    const reused = await client.queries.findTodos({ text: 'design', cursor: null }, { once: true });
    const refreshed = await client.queries.findTodos(
      { text: 'design', cursor: null },
      { once: true, refresh: true },
    );
    const everything = await client.queries.getTodos({}, { once: true, store: false });
    await client.queries.invalidate.findTodos({ text: 'design', cursor: null });
    console.log(first.nextCursor, reused.todos, refreshed.todos, everything.todos);
    ```

=== "Flutter"

    ```dart title="action-contract"
    final first = await client.queries.findTodos(text: 'design', cursor: null, once: true);
    final reused = await client.queries.findTodos(text: 'design', cursor: null, once: true);
    final refreshed = await client.queries.findTodos(
      text: 'design',
      cursor: null,
      once: true,
      refresh: true,
    );
    final everything = await client.queries.getTodos(
      once: true,
      store: const GetTodosStore.none(),
    );
    await client.queries.invalidate.findTodos(text: 'design', cursor: null);
    print([first.nextCursor, reused.todos, refreshed.todos, everything.todos]);
    ```

| Situation | What a `once` call does |
| --- | --- |
| A saved result exists (and no `refresh`) | Returns it with no request, no connection needed and no local write: it does not update Models, wake `watch` listeners or change subscriptions |
| No saved result, or `refresh` | Runs a direct call with a fresh call ID. On success, the returned Model authority (per `store`) and the saved result commit in one local transaction before the call resolves |
| The same Query and arguments are already in flight | Waits for that request instead of sending another; every caller gets its own decoded result |
| The request fails | Rejects with the `CallError` and saves nothing; a failed `refresh` keeps the previous result |
| No connection and nothing saved (or `refresh`) | Rejects with `action.unavailable`; it is never queued |

The saved value is the complete typed result: scalars, Model results, list order and membership, and pagination values such as `nextCursor`. A successful empty result is saved too. Each call decodes a new result object, so changing a returned list, `Date` or object affects neither the saved result nor another caller. A saved paginated result is only the page that was requested.

A saved result is the answer of an earlier request, not the current local view. Channel deliveries and local writes change Models, never saved results, and a hit never reapplies an old Model result. Read `client.models` for current local data, and use `refresh` or `invalidate` when your application decides a saved result is outdated. Saved results never expire on their own: no time limit or automatic freshness applies. They stay until invalidated, replaced by a successful refresh, or discarded by a schema change or local database rebuild.

The saved result belongs to the Query name and version, the normalized arguments (key order, UUID case and date offsets do not matter; list order and explicit `null` do) and the `store` policy. `store` still controls only which Model results update local Models, so `once` with `store: false` returns a saved result that did not store Models, and a later call that asks for Models (the default) does not reuse it. `store: false` does not make the call ephemeral: its result is still saved in the local database. `invalidate` takes only the Query's business arguments, needs no connection, resolves after its local commit and discards the saved results for every `store` policy. A request that was already in flight still resolves its callers, but cannot save its result after an invalidation.

Saved results belong to the local database file, not to the signed-in user. The runtime does not derive identity from the access token, and refreshing a token for the same identity keeps them. Open a separate database per backend, account or tenant, or delete the database when the identity changes; changing credentials on a shared file isolates neither Models nor saved results. See [local storage](storage.md#manage-cached-data).

`once` and `refresh` exist only on direct Query methods. Mutations, `mutations.call` and `queries.enqueue` do not accept them: the generated TypeScript types and Dart signatures reject them, and in TypeScript the runtime also rejects them from untyped callers with `CallError` code `action.invalid_options` before any request, as it does for `refresh` without `once` in both languages. Like every Query, a `once` call or `invalidate` from an application transaction callback fails with `transaction_active`. In Dart, the parameters are `once` and `refresh` unless the Query has business inputs with those names; they are then `callOnce` and `callRefresh`, following the `outputStore` rule. A Query may not be named `invalidate`.

## Local-only writes

=== "TypeScript"

    ```ts
    await client.transaction(async tx => {
      await tx.models.entry.create({ id: 'draft', text: 'Only here', note: null });
      await tx.models.entry.update({ id: 'draft' }, { note: 'Remember this' });
      await tx.models.entry.delete({ id: 'draft' });
    });
    ```

=== "Flutter"

    ```dart
    await client.transaction((tx) async {
      const id = EntryIdentity(id: 'draft');
      await tx.models.entry.create(
        const Entry(id: 'draft', text: 'Only here', note: null),
      );
      await tx.models.entry.update(id, const EntryPatch(note: Present('Remember this')));
      await tx.models.entry.delete(id);
    });
    ```

### Creation defaults

Fields with a [creation default](../schema/reference.md#creation-defaults) may be omitted from a create, locally or in a Mutation operand. The native client fills them once before writing or sending; an explicit value, including an explicit null, wins. These examples use the operation fixture's `Note` Model, whose fields except `memo` have defaults.

=== "TypeScript"

    ```typescript title="action-contract"
    await client.models.note.create({ memo: null });
    const saved = await client.mutations.call.addNotes({ note: { memo: 'draft', tag: null }, many: [] });
    console.log(saved.saved.id, saved.saved.at);
    ```

=== "Flutter"

    ```dart title="action-contract"
    await client.models.note.create(const NoteCreate(memo: null));
    final saved = await client.mutations.call.addNotes(
      note: const NoteCreate(memo: 'draft', tag: Present(null)),
      many: const [],
    );
    print([saved.saved.id, saved.saved.at]);
    ```

`create` does not return the generated values; read or watch the Model, or use a Mutation output, to see them.

`create(record)`, `update(identity, patch)` and `delete(identity)` return `Promise<void>` / `Future<void>`. They change local storage without calling the backend. Use `client.mutations.<name>` for backend work. A later server update for the same identity may replace the cached local record; the application owns any conflict policy. Invalid identities, field values, references or uniqueness constraints can reject a local write and roll back the transaction.

## Channels

=== "TypeScript"

    ```ts
    const followed = await client.scopes.subscribe('book:demo');
    console.log(followed.status.initialization, followed.status.connection);
    await followed.unsubscribe();
    ```

=== "Flutter"

    ```dart
    final followed = await client.scopes.subscribe('book:demo');
    print('${followed.status.initialization} ${followed.status.connection}');
    await followed.unsubscribe();
    ```

`client.scopes.subscribe(channel)` persists the desired subscription, wakes a running connection and answers with a handle for that registration: the same channel answers with the same handle while it is subscribed. It resolves on the local commit, so it works with no network. `client.channels.subscribe / unsubscribe` are the retained spelling of the same registrations, by channel name.

**Subscribing delivers later changes, not the channel's existing records.** The position the server acknowledges the first time a session is negotiated becomes that subscription's starting point; nothing published earlier is downloaded, and reconnecting keeps that starting point rather than jumping ahead. Use `watch` to observe what arrives. To load what the channel already held, call `bootstrap()` on the handle:

=== "TypeScript"

    ```ts
    const followed = await client.scopes.subscribe('book:demo');
    followed.bootstrap().catch(console.error);
    console.log(followed.status.bootstrap.phase);
    ```

=== "Flutter"

    ```dart
    final followed = await client.scopes.subscribe('book:demo');
    followed.bootstrap().catchError((Object error) => print(error));
    print(followed.status.bootstrap.phase);
    ```

`bootstrap()` asks for everything published to the channel before this subscription's starting point. It registers that work when you call it, whether or not you await the returned `Promise`/`Future`, so it runs in the background while your screen shows what is already local. It resolves once that work is committed; calls made while it is running share one task, a call after it has completed resolves from local state even offline, and a call after a failure retries from the progress that was committed. Handle the rejection - a background call that nobody awaits is still a rejected `Promise`/`Future`.

Completing it does **not** mean you have a point-in-time snapshot of the channel, that the data is currently fresh, or that every record loaded successfully: a record whose loader failed is reported through the connection's error channel and is corrected the next time it is delivered. What it does mean is that the records published before your starting point, and the changes up to the position the load finished at, have been processed. Later changes keep arriving the ordinary way.

`status` is a snapshot with `active`, `initialization` (`pending` until the starting point is committed, then `ready`), `connection` (`offline`, `connecting`, `catching-up`, `live`, `stopped`) and `bootstrap`; `live` means the stream is healthy, not that all records have arrived. `bootstrap` is `{phase, error}`, where `phase` is `not-requested`, `waiting-for-initialization` (asked for, but the starting point it is bounded by is not committed yet), `loading`, `catching-up` (the history is loaded and the load is waiting for ordinary delivery to reach the position it finished at), `complete` or `failed` with the `{code, message}` that failed it. Waiting for connectivity is not a failure and has no timeout. `watch(listener)` delivers the current snapshot and every change, and returns a function that stops observing (Dart returns a `Stream`). `unsubscribe()` removes this registration; work through a handle that was unsubscribed, or whose client was closed, fails with `subscription.closed`. Closing the client stops the handles and removes no subscription. Unsubscribing also removes that registration's load: waiters get `subscription.closed`, a call that raced the removal is rejected with `subscription.closed` too, and subscribing again starts the history over. An explicit retry starts a new run, so a call still waiting on the run it replaced is rejected with `bootstrap.superseded` rather than resolved by it. Closing the client keeps the load: this process's waiters are rejected with `client_closed`, and the next client resumes it without another `bootstrap()` call.

A channel name must match what your backend publishes to. A subscription is a request for data; loaders must still enforce read permissions. Unsubscribing stops that channel's synchronization and removes nothing: records, their stamps and pending edits stay. See [sync and recovery](sync.md) for cache and account-change behavior.

## Status and lifecycle

- `client.syncState()` returns the client's pending count, cursors, channels and rejections; `client.models.<name>.syncState(identity)` returns one record's pending calls and rejections, typed by the Model. Neither sends network requests. See [pending work and recovery](runtime.md#pending-work-and-recovery).
- `client.clientId` is this database's durable client identity.
- `client.connection` is the connection created by `open` or `client.connect`. It is `undefined` / `null` when there is none. See [connection controls](runtime.md#connection-controls).
- Recovery, prerequisite and escape-hatch members (`dismissRejection`, `drop`, `pendingTasks`, `setReadiness`, `runPrerequisites`, `querySpec`, `readSql`) are on the same object; see the [client runtime reference](runtime.md).
- `await client.close()` stops the connection and releases the local database handle. Close the client when its owning application scope ends; cancel individual watchers when their views end. Calls after close fail.

## Generated data types

| Type or helper | Meaning |
| --- | --- |
| `Entry` | Complete state, including its identity fields. A nullable field is still present in a complete record. |
| `EntryIdentity` | Only the fields declared in `@@id`; composite identities contain every key field. |
| `EntryPatch` | Only editable non-identity fields. Omission leaves a field unchanged; explicit `null` clears a nullable field. |
| `AddTodoInput` (TypeScript) | Typed arguments for `AddTodo`; its update operand uses `values`. |
| `AddTodoPatchUpdate` (Dart) | Typed update operand, with identity and `Present`-wrapped changed fields. |
| `EntryFilter` (Dart) | Typed equality filter. `Present(null)` explicitly filters for null. |
| `EntryOrderField`, `EntryOrder` (Dart) | Typed ordering field and direction. |
| `Present<T>` (Dart) | Distinguishes omission from an explicitly supplied value, including null. |

UUID fields are strings; DateTime fields use language date/time values and encode to UTC strings. Avoid integers outside the JSON/JavaScript safe range. See the [schema compiler reference](../schema/reference.md) for the supported field types.

## Extension points

`ReadPort` declares `read`, `querySpec`, `related`, and `referencing`. `WritePort` adds local direct writes; TypeScript `LivePort` adds `watch`. These are forwarding contracts for generated Model facades, not alternate storage engines supplied automatically by the generator.

TypeScript exports Model classes (`EntryModel`, `EntryLiveModel`, `EntryTxModel`), `LiveModels`, `TxModels`, `liveModels(port)`, `txModels(port)` and `GeneratedTransaction`. Dart exposes corresponding facades. Construct these only when adapting a compatible port; normal applications obtain them through `GeneratedClient`.

TypeScript's `encodeEntry`, `decodeEntry`, `encodeEntryIdentity`, `encodeEntryPatch` and `encodeEntryWhere`, and Dart's `toRecord`/`fromRecord`, perform wire conversions. They assume schema-compatible data; casts in generated decoders are not a substitute for validating arbitrary untrusted input. Generated Mutation and Query methods encode arguments, invoke the shared runtime and decode typed results.
