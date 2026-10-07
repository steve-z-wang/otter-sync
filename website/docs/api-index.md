# API reference

Use this index to find the interface you call or implement. Local Model examples use `Entry`; the [To-do example](getting-started.md) uses the `Todo` model and `AddTodo` / `SetTodoDone` Mutations. Generated names follow your schema.

## Application interfaces

| Interface | Use it to | Reference |
| --- | --- | --- |
| `GeneratedClient.open` | Bind a local database to one Stream and a durable Store identity | [Generated client](frontend/client-api.md#open-a-client) |
| `client.models.<model>` | Read, query, watch and follow relations in local data | [Model APIs](frontend/client-api.md#model-apis) |
| `client.readSql`, `client.watchSql` | Read local data with SQL once, or keep a SQL answer over several Models current | [Escape-hatch reads](frontend/runtime.md#escape-hatch-reads), [Watch SQL](frontend/runtime.md#watch-sql-over-several-models), [Local table layout](frontend/runtime.md#local-table-layout) |
| `client.fetch.<model>` | Read one record from the backend through its Loader, stored locally by default | [Fetch a record](frontend/client-api.md#fetch-a-record-from-the-backend) |
| `client.transaction` | Commit local reads, direct writes and queued Mutations together | [Transactions](frontend/client-api.md#transactions) |
| `tx.models.<model>` | Create, update or delete local-only records | [Local-only writes](frontend/client-api.md#local-only-writes) |
| `client.mutations.<name>`, `Call<Output>` | Accept a Mutation durably with its optimism; inspect `status` or await `wait()` for the final outcome | [Mutations and Queries](frontend/client-api.md#mutations-and-queries) |
| `tx.mutations.<name>(inputOrCallback)` | Queue a Mutation in a local transaction, with local-only changes that follow its backend outcome | [Queue Mutations in a transaction](frontend/client-api.md#queue-mutations-in-a-transaction) |
| `client.queries.<name>` | Read an invocation snapshot with request-level boolean storage policy | [Mutations and Queries](frontend/client-api.md#mutations-and-queries) |
| `CallOutcome`, `CallError`, `CallOptions`, Dart `CallSuccess` / `CallFailure` | Read a durable outcome, handle failures and handle failures; Query/Fetch choose a boolean storage policy | [Storing Model results](frontend/client-api.md#storing-model-results) |
| `client.bootstrap()` | Await finite marked-Model Bootstrap coverage | [Bootstrap](frontend/loads.md) |
| `client.connection` | Pause, resume or wake background sync | [Connections](frontend/runtime.md#connection-controls) |
| `client.syncState`, `client.close` | Inspect pending work and release resources | [Status and lifecycle](frontend/client-api.md#status-and-lifecycle) |
| Model, Identity, Patch, Filter and Order types | Pass typed data to generated methods | [Generated data types](frontend/client-api.md#generated-data-types) |
| Dart `toAxtonPrecision()` (`AxtonDateTime`) | Get the UTC, millisecond `DateTime` AXTON stores for a value, to compare it with one read back | [Dates and times](frontend/client-api.md#dates-and-times) |
| `Mutations<Tx>`, `Queries<Tx>`, `MutationContext<Tx>`, `QueryContext<Tx>` | Implement each operation's authoritative business logic | [Handlers](backend/api.md#handlers) |
| `Loaders<Tx>`, `LoaderCall` | Return current records for synchronization | [Loaders](backend/api.md#loaders) |
| `invalidate`, `RecordDeclaration` | Declare a record a handler changed beyond its Model inputs, so holding Streams receive current authority (not a business result) | [Streams](backend/api.md#streams) |
| `ctx.stream`, `ctx.streams(names)`, `Stream`, `BootstrapStream`, `RecordDeclaration`, `RecordRef`, Model reference functions | Track records durably and request selected authority invalidation | [Streams](backend/api.md#streams) |
| `createBackend`, `Options<Tx>` | Connect your implementations to the backend runtime | [Backend setup](backend/api.md#createbackend), [What your backend owns](backend/api.md#what-your-backend-owns) |
| `backend.listen` | Serve sync requests and close the listener | [Listener](backend/api.md#listener), [Deploy the backend](backend/deployment.md) |
| `Authenticate`, `devAuth` | Identify the caller | [Authentication](backend/api.md#authentication) |
| `admit`, `Admit`, `AdmissionRefusal` | Refuse an outdated or unwanted client with your own status and body | [Admission](backend/api.md#admission) |
| `CallRejected`, `translateRejection`, `onError`, `EngineError` | Reject business operations and diagnose failures | [Errors](backend/api.md#errors) |
| `backend.transaction`, `TransactionCall` | Write outside a handler with the same `invalidate` and `stream`; subscribers wake after commit | [Background writes](backend/api.md#background-writes) |
| `backend.publish` | Settle the same `invalidate` and `stream` inside a transaction your code already owns; call the returned wake after it commits | [In a transaction you own](backend/api.md#in-a-transaction-you-own) |
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
| `rejections` (`watch`, `get`, `dismiss`), `failures` (`watch`, `retry`, `drop`), `outbound.watchPending`, `RefusedAct`, `FailedAct`, `FailedTask`, `SubmittedAct`, `ActOperation` | Watch and resolve refused calls, calls stuck on a failed prerequisite and the pending count, account-wide | [Unsent work](frontend/runtime.md#unsent-work) |
| `tx.rejections.dismiss`, `tx.failures.retry`, `tx.failures.drop` | Resolve unsent work inside a transaction, atomically with a replacement call | [Repair inside a transaction](frontend/runtime.md#repair-inside-a-transaction) |
| `open({ prerequisites })`, `PrerequisiteRetry`, `pendingTasks`, `setReadiness` | Complete prerequisite I/O before a durable call can be sent | [Prerequisites](frontend/runtime.md#prerequisites) |
| `freeze`, `acknowledge`, `applyPull` | Exercise the engine protocol in tests and tooling | [Protocol primitives](frontend/runtime.md#protocol-primitives) |
| `ReadPort`, `WritePort`, `LivePort`, `FetchPort`, `FetchOptions`, model factories and codecs | Bind generated facades to a compatible runtime | [Generated extension points](frontend/client-api.md#extension-points) |
| `loaderHooks`, `Native` | Prepare a loader call or supply the native backend binding | [Backend extension points](backend/api.md#extension-points) |
| Compiler command and `.model` declarations | Generate and evolve the interface contract | [Schema compiler](schema/reference.md) |

The generated application API is the normal entry point. Raw backend protocol methods marked `@internal` in the implementation are not a supported application integration surface; use `listen`, handlers, loaders and transaction-bound notifications.
