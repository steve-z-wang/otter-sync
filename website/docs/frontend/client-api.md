# Generated client

This page covers the TypeScript and Dart APIs emitted by the schema compiler. Follow [getting started](../getting-started.md) for a running backend, or [generate your interfaces](../schema/define.md) first.

Choose TypeScript or Flutter above a code example to switch languages throughout the page. Flutter examples use Dart.

Local Model examples below use an `Entry` model with `id`, `text` and nullable `note`. Mutation and Query examples use the [generated operation fixture](https://github.com/zanminwang/axton/blob/main/integration/action-contract/schema.model), whose `Todo` model has `id`, `title`, `state` and nullable `note`. TypeScript imports come from `generated/client.ts`; Dart imports come from `generated/generated.dart`.

## Open a client

One physical SQLite file belongs to one Client, Store identity and Stream. The backend binds that Store to the authenticated principal. Reopening preserves delivery progress, queued Mutations and materialization evidence; credentials do not create a new Store.

=== "TypeScript"

    ```ts
    const client = await GeneratedClient.open({
      path: 'local.sqlite',
      stream: 'User:alice',
      connection: {
        url: 'http://127.0.0.1:4242', token: 'alice',
        projectionGeneration: '1',
        options: { onError: console.error },
      },
    });
    ```

=== "Flutter"

    ```dart
    final client = await GeneratedClient.open(
      path: 'local.sqlite', stream: 'User:alice',
      connection: StoreConnection(
        url: 'http://127.0.0.1:4242', token: () => 'alice',
      ),
    );
    ```

`path` and `stream` are required; `connection` is optional. Opening commits local storage before starting its connection and works offline. `projectionGeneration` defaults to `'1'`; configure the same generation on backend and client. Changes to it or the normalized Model contracts require rematerialization. Retain supported old contracts so durable queued Mutations remain retryable.

`prerequisites` registers named prerequisite handlers. Dart `libraryPath` selects an explicit development library; installed packages use their bundled library. Android applications initialize the process's stable support/files directory once with `Client.configureApplicationData(path)` before opening any Store. This directory is application configuration, not a per-client lock option.

Use one active owner per physical SQLite file. Protocol 5 requires a fresh format-5 file; an unsupported file is refused intact. Existing pending work needs a coordinated drain/export/migration using the old release before its server is retired. `resetStore({discardPending: true})` (Dart: `resetStore(discardPending: true)`) creates a new incarnation and abandons pending calls; a normal close/reopen does neither. Failed native loading, incompatible binding, invalid schema, physical ownership conflicts and unwritable storage fail opening.

Watch `client.models` for ongoing state. There are no custom incoming-store callbacks or per-client multiple Stream registrations.

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

`get(identity)` returns the complete typed record or `null` when it is absent from local storage. It does not call the backend loader. A newly opened cache can return `null` until stream synchronization supplies the record. To read the record from the backend instead, use [`client.fetch`](#fetch-a-record-from-the-backend).

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

TypeScript returns an unsubscribe function; Dart returns `Stream<List<Entry>>`. A listener receives an initial query result and distinct results after committed local changes, including sync changes. Identical query results are suppressed. `watch` accepts equality filters, not `query`'s ordering or limit options. It reports current query results, not a log of every intermediate write. To keep an answer that joins several Models current, use [`watchSql`](runtime.md#watch-sql-over-several-models).

### Follow a relation

The compiler emits relation methods only for relationships declared in the schema. A forward relation returns the related record or `null`; an inverse collection returns a list. Methods take the source model's identity, and read the local database.

For a `Comment.book` relationship, `client.models.comment.book(commentIdentity)` follows the forward reference. The [relations fixture](https://github.com/zanminwang/axton/blob/main/fixtures/compiler/relations.model) defines `Book.comments` and `Comment.book`; the [generated API checks](https://github.com/zanminwang/axton/blob/main/integration/generated-api/verify.sh) exercise those accessors. A singular inverse needs a unique foreign key; an ambiguous inverse is rejected during compilation.

## Fetch a record from the backend

`client.fetch.<model>(identity, options?)` reads one record from the backend through that Model's [Loader](../backend/api.md#loaders), the same Loader that serves synchronization. You declare no Query and write no handler for it. Three generated APIs read a record in different ways:

| API | Reads | Network |
| --- | --- | --- |
| `client.models.entry.get(identity)` | The local database | Never |
| `client.fetch.entry(identity, options?)` | One record, through the backend's Loader for `Entry` | One request per call (overlapping identical calls share one) |
| `client.queries.<name>(args, options?)` | A named business read that your Query handler implements ([Mutations and Queries](#mutations-and-queries)) | One fresh direct request |

=== "TypeScript"

    ```ts
    const entry = await client.fetch.entry({ id: 'entry-1' });
    console.log(entry?.text);
    const preview = await client.fetch.entry({ id: 'entry-2' }, { store: false });
    console.log(preview === null ? 'not readable' : preview.text);
    ```

=== "Flutter"

    ```dart
    final entry = await client.fetch.entry(const EntryIdentity(id: 'entry-1'));
    print(entry?.text);
    final preview = await client.fetch.entry(
      const EntryIdentity(id: 'entry-2'),
      store: false,
    );
    print(preview == null ? 'not readable' : preview.text);
    ```

The call returns `Promise<Entry | null>` / `Future<Entry?>`: the complete record, with its identity, as the Loader returned it for this call. It is `null` when the Loader answered `null` for that identity: no readable record exists. The result is a snapshot, not a live record. It never contains pending local edits, which stay pending and are replayed as usual.

- **Storage.** By default (`store: true`) the call resolves only after the record is stored locally and permitted cache writes have committed. An explicit null applies only when ordinary cache admission allows it. Current Stream authority is kept, and the call still returns its own snapshot. With `store: false` the record is returned without changing local data . `store` is the only option and must be a boolean.
- **Every call reads the backend.** No result is cached on the client. The backend records each call's response, a `store: false` preview included, so a retry of the same call ID replays it, as for a direct Query; those records are not pruned automatically ([Database](../backend/database.md#protocol-5-persistence)). A call for the same identity with the same `store` choice as a call still in flight shares that call: one request, one Loader call and one local store, and each caller receives its own object. A later call makes a new request.
- **No offline fallback.** A local row does not satisfy the call. Without a connection it rejects with `fetch.unavailable`; it is never queued. It uses the connection's direct timeout and credential refresh ([Server connection](runtime.md#server-connection)).
- **No subscription.** A Fetch joins no Stream and changes no subscription, cursor or `bootstrap()` progress.
- **Permissions stay in the Loader.** Return `null` for a record this user may not see if no readable snapshot exists; an ordinary null does not delete a stored copy; throw `CallRejected` if it should be an error, which deletes nothing.
- **Not inside transactions.** `tx` has no `fetch`. Calling `client.fetch` inside a `client.transaction` fails with `transaction_active`.

Failures reject with `CallError`:

| `code` | Meaning |
| --- | --- |
| The Loader's `CallRejected` code, `loader.failed`, `loader.invalid`, `loader.unregistered`, `model_version_unsupported`, `call.identity_conflict` | The backend answered with this failure. `execution` is `rejected` |
| `fetch.invalid_options` | An invalid identity or option, refused before any request |
| `fetch.unavailable` | No connection, the connection was stopped before the response, or the client was closed while waiting |
| `fetch.timeout` | The direct timeout passed |
| `fetch.transport_failed` | The request or credential refresh failed; `cause` carries the message and the HTTP status, if any |
| `fetch.invalid_response` | The response did not answer this request |
| `fetch.store_failed` | The permitted local cache transaction failed; no content or progress from that transaction commits. |
| `fetch.schema_pending` | The local database is waiting to be rebuilt for an incompatible schema change |
| `fetch.schema_changed` | The local database was rebuilt while the call waited |

A failure never deletes the local row. `execution` is `rejected` for the backend's codes, `fetch.invalid_options` and `fetch.schema_pending`, and `unknown` otherwise; a read has no side effects either way. A call on a client that is already closed rejects with the runtime's plain `client_closed` error (`Error` in TypeScript, `StateError` in Dart), not a `CallError`. A result the generated decoder cannot read rejects with `action.observation_failed`, as for a direct call.

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

`transaction<T>(callback)` returns the callback's result after local commit. Throwing or a failed operation rolls it back. Await each operation, including nested callbacks; unfinished work is rejected. Inside the callback, use `tx.models` for reads that must see earlier writes in the same transaction. Calling the outer `client` from inside its own transaction callback - a read, a Mutation, a Query or a Fetch - fails with `transaction_active` on Node and Dart (React Native rejects Mutations, Queries and Fetches and lets other outer calls wait behind the transaction) instead of waiting on itself. To send backend work with the transaction, queue it through `tx.mutations` ([queue Mutations in a transaction](#queue-mutations-in-a-transaction)).

The callback receives a generated transaction with `models` and the underlying `transaction`; when the schema declares Mutations it is an `ApplicationTransaction`, which adds `mutations` for queued Mutations. It has no `mutations.call`, `queries`, `fetch`, `loads` or watch method. For nested savepoints, see [transactions and savepoints](runtime.md#transactions-and-savepoints).

## Mutations and Queries

Only named Mutations are durable remote writes. The first await commits the owned local scope and returns a `Call`; `Call.wait()` waits for the backend outcome and required local settlement. Acceptance that is still awaiting Stream authority remains pending.

| Method | Returns | Completion |
| --- | --- | --- |
| `client.mutations.publish(inputOrCallback)` / `tx.mutations.publish(...)` | `Call<PublishOutput>` | Owned local writes, optimism and queue entry committed locally |
| `call.wait()` | `CallOutcome<PublishOutput>` | Backend outcome and local settlement committed |
| `client.queries.find(args, options?)` | `FindOutput` | Validated invocation snapshot and permitted cache writes committed |
| `client.fetch.entry(identity, options?)` | `Entry` or null | Validated Loader snapshot and permitted cache writes committed |

The examples use [the typed SDK fixture](https://github.com/zanminwang/axton/tree/main/integration/v05-sdk).

=== "TypeScript"

    ```ts title="v05-sdk"
    const call = await client.mutations.publish(async tx => {
      const draft = await tx.models.draft.get({ id: 'draft-1' });
      if (!draft) throw Error('draft missing');
      await tx.models.draft.delete({ id: draft.id });
      return { entry: { id: 'entry-1', text: draft.text }, call: 'publish' };
    });
    const outcome = await call.wait();
    if (outcome.error) console.error(outcome.error.code);
    const snapshot = await client.queries.find({ id: 'entry-1' });
    ```

=== "Flutter"

    ```dart title="v05-sdk"
    final call = await client.mutations.publish.withTransaction((tx) async {
      final draft = await tx.models.draft.get(const DraftIdentity(id: 'draft-1'));
      if (draft == null) throw StateError('draft missing');
      await tx.models.draft.delete(DraftIdentity(id: draft.id));
      return PublishInput(
        entry: EntryCreate(id: 'entry-1', text: draft.text), call: 'publish');
    });
    final outcome = await call.wait();
    if (outcome is CallFailure<PublishOutput>) print(outcome.error.code);
    final snapshot = await client.queries.find(id: 'entry-1');
    ```

A typed input can be supplied directly instead. Dart's strongly typed callable invoker uses `.publish(input)` or `.publish.withTransaction(callback)`; both submit the same named Mutation through the same engine scope. A business input named `call` retains its name.

The callback executes before its returned input is applied optimistically. Its `tx.models` reads and device-only writes belong to that Mutation. Acceptance keeps those companions; refusal undoes only owned effects and preserves later independent work. The callback cannot submit another Mutation, use remote reads, or capture another scope. It may be async; await every operation before returning its typed input.

### Storing Model results

Query and Fetch accept one request-level boolean `store`, default `true`. Explicit `false` returns the invocation snapshot without writing Model content, current cursor, or materialization evidence. It does not disable backend tracking or future Stream delivery. Default and explicit true have the same storage policy; each invocation is a fresh request.

A returned snapshot can differ from current Store state: Stream authority, tombstones or pending work may prevent the ordinary cache write. Read or watch `client.models` for continuous state. An omitted list item is not a deletion; an explicit Loader null is an ordinary cache absence and cannot override current Stream authority. Cache reads do not establish Stream progress.

=== "TypeScript"

    ```ts title="v05-sdk"
    const snapshot = await client.queries.find({ id: 'entry-1' }, { store: false });
    const entry = await client.fetch.entry({ id: 'entry-1' }, { store: false });
    ```

=== "Flutter"

    ```dart title="v05-sdk"
    final snapshot = await client.queries.find(id: 'entry-1', store: false);
    final entry = await client.fetch.entry(const EntryIdentity(id: 'entry-1'), store: false);
    ```

### Fresh Query results

Each Query invocation has a fresh request and snapshot. There are no `once`, `refresh` or Query-result invalidation controls. Use local Model watches for continuous state. Query and Fetch snapshots carry null cursors: they cannot replace currently protected Stream content or deletion evidence, and missing results are not authoritative deletion. `store: false` returns the invocation result without installing Models.

### Queue Mutations in a transaction

`client.transaction` commits local CRUD and several named Mutation scopes atomically. Each queued Call keeps its own backend fate. Invoke `tx.mutations` with the same typed input or callback form as the client. Its callback receives only that Mutation's `tx.models` capability.

Calling `Call.wait()` before the outer transaction commits fails promptly with `transaction_uncommitted` without settling the Call. A rolled-back scope's Call fails with `transaction_rolled_back` and is never sent. A failed operation poisons its scope even if caught; leaked or unawaited work prevents commit. Captured cross-client or expired transaction handles fail promptly.

Node uses async context and Dart uses Zones to enforce ownership. React Native lacks async context in Hermes and conservatively refuses overlapping async transaction callbacks across clients. Independent ordinary operations on another Store remain available. Do not use an outer client inside its own callback; use the supplied transaction handle.

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

Generated Model create inputs permit omission only for schema-declared defaults. Dart's `{Model}Create` implements that input contract; a complete Model can also supply it. Omission chooses the declared default once when the named Mutation is accepted locally. Explicit null clears a nullable field; it is not omission. The durable frozen input retains generated values across retries.

### Local writes to synced Models

A local write may target a Model that Streams also deliver. For example, an application can store a profile it looked up over REST before any Stream has delivered that person. The write is never sent, and newer server data for that identity replaces it:

- **It is never sent.** A `tx.models` or `client.models` write never becomes a queued call or a backend request, whatever Model it targets.
- **Newer server data replaces it.** A later Stream authority replaces direct local state. Ordinary Query/Fetch cache writes follow current-protection admission; private receipt settlement can finalize only its owned effects. Nothing restores the local write afterwards, not a later rejection and not a restart. A record that exists only because of a local write has no server version yet, so the first server data for it replaces it.
- **A rejection does not undo it.** A local write can land on a record that a pending Mutation also changes. If the backend rejects that Mutation, AXTON removes only that Mutation's changes, including its `local` changes, and the local write stays until newer server data replaces it. The exception is a record whose only creation is a pending Mutation: if that create is rejected, the record goes, local writes included.

Stream authority retires preceding direct changes and preserves later pending operation overlays. Ordinary Query and Fetch snapshots never replace current Stream content or tombstones. Equal-position schema rematerialization adapts the authoritative base while preserving later local work.

## Bootstrap and Stream delivery

The Stream supplied at open is the client's only Stream. `await client.bootstrap()` waits for its entire Bootstrap: finite immutable authority units applied atomically through the captured head. An empty plan still completes. Capturing a head never advances delivery progress. Normal reopen resumes persisted work.

Initial Bootstrap selects Models marked `@@bootstrap`. Rematerialization additionally covers held authority and absence, including unmarked Models. Mutation receipt recovery can materialize exact missing targets through an internal settlement-owned plan without running the application's Bootstrap handler again, implicitly tracking, or allocating a new cursor.

Bootstrap and delta failures commit neither the failing unit nor its progress. Earlier independent committed units remain valid. Required constraint closure is indivisible; reducing transport page size cannot split it. Membership Remove releases current live-content protection without deleting the Model or clearing historical evidence; true Stream deletion remains protected.

## Status and lifecycle

- `client.syncState()` returns the client's pending count, cursors, streams and rejections; `client.models.<name>.syncState(identity)` returns one record's pending calls and rejections, typed by the Model. Neither sends network requests. See [pending work and recovery](runtime.md#pending-work-and-recovery).
- `client.clientId` is this database's durable client identity.
- `client.connection` is the connection created by `open` by the open configuration. It is `undefined` / `null` when there is none. See [connection controls](runtime.md#connection-controls).
- Recovery, prerequisite and escape-hatch members (`dismissRejection`, `drop`, `pendingTasks`, `setReadiness`, `querySpec`, `readSql`, `watchSql`) are on the same object; see the [client runtime reference](runtime.md).
- `client.rejections`, `client.failures` and `client.outbound` watch the refused calls, the calls stuck on a failed prerequisite and the pending count account-wide, and resolve them; `rejections` and `failures` are also on the transaction of a schema with Mutations. See [unsent work](runtime.md#unsent-work).
- `await client.close()` stops the connection and releases the local database handle. Close the client when its owning application stream ends; cancel individual watchers when their views end. Calls after close fail.

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

UUID fields are strings. Avoid integers outside the JSON/JavaScript safe range. See the [schema compiler reference](../schema/reference.md) for the supported field types.

### Dates and times

A `DateTime` field, argument or result holds a UTC instant at millisecond precision, the precision of a JavaScript `Date`, on every client and on the server. A TypeScript `Date` already has that precision, so a stored `Date` has the same `getTime()` as the one you wrote.

A Dart `DateTime` can carry microseconds and a local time zone. Generated Dart drops the sub-millisecond part before it writes or sends a value, so a record you read back holds exactly what was stored, and a later copy from the backend compares equal to it. Every `DateTime` Dart reads is in UTC (`isUtc` is true), including one you wrote as a local time. Dart's `==` compares `isUtc` and microseconds as well as the instant, and `isAtSameMomentAs` still sees microseconds, so compare a value you created with one read back through `toAxtonPrecision()`, which returns what AXTON stores for it:

=== "TypeScript"

    ```ts
    const picked = new Date(2026, 8, 28, 14, 30);
    console.log(picked.toISOString()); // the UTC text AXTON stores and returns
    ```

=== "Flutter"

    ```dart
    final picked = DateTime(2026, 9, 28, 14, 30, 0, 0, 250); // local, with microseconds
    final stored = picked.toAxtonPrecision(); // what a DateTime field reads back as
    print(stored.isUtc); // true
    print(stored == picked); // false: == also compares isUtc and microseconds
    // A value read back compares equal to picked.toAxtonPrecision().
    ```

## Extension points

`ReadPort` declares `read`, `querySpec`, `related`, and `referencing`. `WritePort` adds local direct writes; TypeScript `LivePort` adds `watch`. These are forwarding contracts for generated Model facades, not alternate storage engines supplied automatically by the generator.

TypeScript exports Model classes (`EntryModel`, `EntryLiveModel`, `EntryTxModel`), `LiveModels`, `TxModels`, `liveModels(port)`, `txModels(port)` and `GeneratedTransaction`. For Fetch it exports `FetchModels`, `fetchModels(port)` and the `FetchPort` contract, whose `fetchModel(model, version, identity, decode, options)` the runtime `Client` implements. Dart exposes corresponding facades, including `FetchModels`. A schema without Models has no Fetch facade, and the names `FetchModels` and `FetchPort` are reserved only when a schema has Models. Construct these only when adapting a compatible port; normal applications obtain them through `GeneratedClient`.

TypeScript's `encodeEntry`, `decodeEntry`, `encodeEntryIdentity`, `encodeEntryPatch` and `encodeEntryWhere`, and Dart's `toRecord`/`fromRecord`, perform wire conversions. They assume schema-compatible data; casts in generated decoders are not a substitute for validating arbitrary untrusted input. Generated Mutation and Query methods encode arguments, invoke the shared runtime and decode typed results.
