# API reference

Use this index to find the interface you call or implement. Local Model examples use `Entry`; the [To-do example](getting-started.md) uses the `Todo` model and `AddTodo` / `SetTodoDone` Mutations. Generated names follow your schema.

## Application interfaces

| Interface | Use it to | Reference |
| --- | --- | --- |
| `GeneratedClient.open` | Open a local database and optionally start background sync | [Generated client](frontend/client-api.md#open-a-client) |
| `onStore`, `StoreHooks`, `StoreChange` | React to typed incoming Model changes inside their local storage transaction | [React to incoming records](frontend/client-api.md#react-to-incoming-records) |
| `client.models.<model>` | Read, query, watch and follow relations in local data | [Model APIs](frontend/client-api.md#model-apis) |
| `client.fetch.<model>` | Read one record from the backend through its Loader, stored locally by default | [Fetch a record](frontend/client-api.md#fetch-a-record-from-the-backend) |
| `client.transaction` | Commit local reads, direct writes and queued Mutations together | [Transactions](frontend/client-api.md#transactions) |
| `tx.models.<model>` | Create, update or delete local-only records | [Local-only writes](frontend/client-api.md#local-only-writes) |
| `tx.channels.subscribe / unsubscribe` | Change local Channel intent inside a transaction, including an `onStore` callback | [React to incoming records](frontend/client-api.md#react-to-incoming-records), [Transactions](frontend/client-api.md#transactions) |
| `client.mutations.<name>`, `Call<Output>` | Accept a Mutation durably with its optimism; inspect `status` or await `wait()` for the final outcome | [Mutations and Queries](frontend/client-api.md#mutations-and-queries) |
| `tx.mutations.<name>(args, { local })` | Queue a Mutation in a local transaction, with local-only changes that follow its backend outcome | [Queue Mutations in a transaction](frontend/client-api.md#queue-mutations-in-a-transaction) |
| `client.mutations.call.<name>` | Run a Mutation directly and await its final result | [Mutations and Queries](frontend/client-api.md#mutations-and-queries) |
| `client.queries.<name>`, `client.queries.enqueue.<name>` | Run a Query directly, or queue it durably and receive a `Call<Output>` | [Mutations and Queries](frontend/client-api.md#mutations-and-queries) |
| `client.queries.<name>(args, { once, refresh })`, `client.queries.invalidate.<name>` | Reuse, refresh or discard the saved complete result of a direct Query | [Reuse a Query result](frontend/client-api.md#reuse-a-query-result-with-once) |
| `client.loads.<name>(args, { once, refresh })`, `Load`, `LoadStatus`, `LoadPhase`, `LoadOptions` | Start a durable paged Load, or reuse or refresh a recorded one; observe, wait, cancel, retry or forget it | [Load data in pages](frontend/loads.md) |
| `client.loads.invalidate.<name>`, `client.loads.get`, `client.loads.list` | Forget a recorded `once` job, reattach to a job by ID, list recent jobs | [Fresh start, once and reattach](frontend/loads.md#fresh-start-once-and-reattach) |
| `LoadError`, Dart `LoadException` | Read the `code` and `message` of a failed, cancelled or refused Load | [Errors](frontend/loads.md#errors) |
| `CallOutcome`, `CallError`, `CallOptions`, Dart `CallSuccess` / `CallFailure` / `CallStore` | Read a durable outcome, handle failures and choose which Model outputs are stored | [Storing Model results](frontend/client-api.md#storing-model-results) |
| `client.scopes`, `Subscription` | Subscribe to a named channel and follow that registration's status | [Channels](frontend/client-api.md#channels) |
| `subscription.bootstrap()`, `status.bootstrap` | Load what the channel held before this subscription started, and follow that load | [Channels](frontend/client-api.md#channels) |
| `client.channels` | The retained spelling: subscribe or unsubscribe by channel name | [Channels](frontend/client-api.md#channels) |
| `client.connection` | Pause, resume or wake background sync | [Connections](frontend/runtime.md#connection-controls) |
| `client.syncState`, `client.close` | Inspect pending work and release resources | [Status and lifecycle](frontend/client-api.md#status-and-lifecycle) |
| Model, Identity, Patch, Filter and Order types | Pass typed data to generated methods | [Generated data types](frontend/client-api.md#generated-data-types) |
| `Mutations<Tx>`, `Queries<Tx>`, `MutationContext<Tx>`, `QueryContext<Tx>` | Implement each operation's authoritative business logic | [Handlers](backend/api.md#handlers) |
| `Loads<Tx>`, `LoadContext<Tx>`, `LoadHandlerCall`, `LoadNext`, `JsonValue` | Implement each Load's paged enumeration | [Implement the backend handler](frontend/loads.md#implement-the-backend-handler), [Load handlers](backend/api.md#load-handlers) |
| `Loaders<Tx>`, `LoaderCall` | Return current records for synchronization | [Loaders](backend/api.md#loaders) |
| `touch`, `Touch` | Declare a record a handler changed beyond its Model inputs, so it is stamped and delivered to its Channels (not returned to the caller) | [Channels](backend/api.md#channels) |
| `channel(name)`, `Channel`, `ModelMembership`, `RecordRef`, Model reference functions | Add records to a Channel once, or remove them, so every later change reaches its subscribers | [Channels](backend/api.md#channels) |
| `createBackend`, `Options<Tx>` | Connect your implementations to the backend runtime | [Backend setup](backend/api.md#createbackend), [What your backend owns](backend/api.md#what-your-backend-owns) |
| `backend.listen` | Serve sync requests and close the listener | [Listener](backend/api.md#listener), [Deploy the backend](backend/deployment.md) |
| `Authenticate`, `devAuth` | Identify the caller | [Authentication](backend/api.md#authentication) |
| `admit`, `Admit`, `AdmissionRefusal` | Refuse an outdated or unwanted client with your own status and body | [Admission](backend/api.md#admission) |
| `CallRejected`, `translateRejection`, `onError`, `EngineError` | Reject business operations and diagnose failures | [Errors](backend/api.md#errors) |
| `backend.transaction`, `TransactionCall` | Write outside a handler with the same `touch` and `channel`; subscribers wake after commit | [Background writes](backend/api.md#background-writes) |
| `backend.publish` | Settle the same `touch` and `channel` inside a transaction your code already owns; call the returned wake after it commits | [In a transaction you own](backend/api.md#in-a-transaction-you-own) |
| `pg`, `prisma`, `drizzle`, `PostgresDriver`, `persistence` | Run business and sync storage in one PostgreSQL transaction through your own access tool | [Database](backend/database.md) |

## Advanced interfaces

| Interface | Use it to | Reference |
| --- | --- | --- |
| React Native `databasePath` | Resolve a persistent local database path | [React Native setup](frontend/platforms.md#react-native) |
| `Client`, `Transaction`, `QuerySpec`, `RecordValue` | Access the generic runtime beneath generated APIs | [Client runtime](frontend/runtime.md) |
| `ServerOptions`, `SyncServer`, `ConnectionOptions` | Configure the backend connection, its headers, and refresh credentials | [Server connection](frontend/runtime.md#server-connection) |
| `AdmissionRefused` | Recognize a backend's admission refusal in `onError`; the connection has stopped | [Server connection](frontend/runtime.md#server-connection) |
| `Transport`, `HttpRoute` (TypeScript) | Type the HTTP carrier a platform host build supplies (not passed to `open`/`connect`): it receives the route `push`, `pull`, `action`, `fetch` or `load` | [Server connection](frontend/runtime.md#server-connection) |
| `RuntimeConnection`, `AuthenticationExpired` | Control Dart sync and identify authentication failures | [Connections](frontend/runtime.md#connection-controls) |
| `syncState`, `models.<name>.syncState`, `dismissRejection`, `drop` | Inspect a record's pending work and handle rejected or unsent calls | [Recovery APIs](frontend/runtime.md#pending-work-and-recovery) |
| `open({ prerequisites })`, `PrerequisiteRetry`, `pendingTasks`, `setReadiness` | Complete prerequisite I/O before a durable call can be sent | [Prerequisites](frontend/runtime.md#prerequisites) |
| `freeze`, `acknowledge`, `applyPull` | Exercise the engine protocol in tests and tooling | [Protocol primitives](frontend/runtime.md#protocol-primitives) |
| `ReadPort`, `WritePort`, `LivePort`, `FetchPort`, `FetchOptions`, model factories and codecs | Bind generated facades to a compatible runtime | [Generated extension points](frontend/client-api.md#extension-points) |
| `loaderHooks`, `Native` | Prepare a loader call or supply the native backend binding | [Backend extension points](backend/api.md#extension-points) |
| Compiler command and `.model` declarations | Generate and evolve the interface contract | [Schema compiler](schema/reference.md) |

The generated application API is the normal entry point. Raw backend protocol methods marked `@internal` in the implementation are not a supported application integration surface; use `listen`, handlers, loaders and transaction-bound notifications.
