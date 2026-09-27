# Local storage

AXTON stores cached records, queued Mutation and Query intent, durable Load jobs, channel progress and rejection details in a local SQLite file. It does not persist completed business result objects on the client. This page explains how to manage the file and recover from storage or synchronization failures.

## Choose a database path

Choose a writable directory owned by your application. Use one active client per file and a separate file per signed-in user. Closing the client releases its connection and native resources; reopening the same file preserves local records and pending work.

The database contains a persistent client identity used for request deduplication. Do not let independently writable copies of the same file send calls to the same backend. Switch accounts by closing the current client and opening the appropriate user's file, rather than changing only its authentication token.

## Change the schema

Each database records the schema it was built for. Opening it with a newer generated schema takes one of three ways:

| Change | What happens |
| --- | --- |
| None | The database opens. |
| A new Model, or a new nullable field | Applied in place; cached records, queued calls and frozen request bytes are preserved. |
| Anything else: a required field, a removed or retyped field, a changed identity, unique constraint, relation, enum or model version, a removed model, or a file from an earlier AXTON runtime | The file is left untouched and a fresh database is opened beside it (`<path>.1`, `<path>.2`, …). A small `<path>.current` file names the one in use. The new database keeps the old subscriptions and synchronises from the beginning. |

Durable [Loads](loads.md) are not carried into a rebuilt database: the rebuild report lists the abandoned Load IDs (`abandonedLoads`) and the application starts new Loads ([Schema changes](loads.md#schema-changes)).

Before switching, the runtime looks at the old database's unsent calls. If there are any, it keeps that database open so they can still be sent; `syncState().schema.pending` reports how many remain and why the schema is incompatible. When they are sent, call `rebuild()`. If they cannot be sent, call `rebuild({ discardPending: true })`: the report tells you how many queued calls and local-only records stay in the old file. Nothing is copied between schemas, and the runtime never deletes an old file; delete numbered files you no longer need. See [opening and schema changes](runtime.md#opening-and-schema-changes). Update backend tables separately through your database's migration process.

<!-- txm-draft: verify against implementation -->
## Transactions that queue Mutations

A [transaction that queues Mutations](client-api.md#queue-mutations-in-a-transaction) saves its local writes, each queued Mutation and each Mutation's `local` changes in one local commit. If the app stops before that commit, none of them are saved; after it, all of them survive a restart. AXTON stores the `local` changes, not the callback, so reopening resumes the queued Mutation and later applies its outcome to those changes without running your code again: acceptance keeps them and rejection undoes them. Later local writes keep their place either way, and newer server data for the same record still replaces them.

## Recover pending work

| Situation | What to do |
| --- | --- |
| A request times out | Let sync retry the persisted frozen request. The backend may already have committed it. |
| Frozen work remains pending | Check connectivity and authentication; a receipt AXTON cannot apply is refused and the batch resent, so check `onError` on both sides. |
| A Mutation or Query is rejected | Inspect its `wait()` outcome and record `syncState`, then dismiss the handled rejection. |
| A prerequisite fails | Resolve its cause, reset its readiness to `pending`, then run its callback again. |
| Another client wrote to the same file | Close the stale instance and reopen it; keep one active client per file. |

Do not manually delete pending batches, channel cursors or backend receipts to clear an error. These records work together to prevent duplicate execution and complete local changes from their receipts. Preserve the database for diagnosis when an error cannot be resolved through the public APIs.

[Sync and recovery](sync.md) shows the application calls for these cases.

## Manage cached data

Unsubscribing stops that channel's synchronization and removes nothing: cached records, their stamps, before images and pending edits stay, and another subscribed channel can still update them. It does remove the subscription itself, including its receive position and any `bootstrap()` progress, so subscribing to that name again is a new subscription that starts at the position the server acknowledges next and downloads the channel's history only if you ask for it again. Retained records are readable but not kept fresh without a channel that delivers them. Permissions are enforced by your backend. When a record is no longer visible, publish it to the affected channels so their loaders can return null. There is no automatic eviction of cached records.

A subscription, its receive position and the progress of a `bootstrap()` load are stored in the local database, so they survive a restart: reopening resumes from the saved position instead of starting over, and an unfinished load continues without being called again. A rebuilt local database ([change the schema](#change-the-schema)) keeps the channel names you subscribed to but not their positions or their load progress, so each one starts again at the position the server acknowledges next.

Results saved by [`once` Query calls](client-api.md#reuse-a-query-result-with-once) are stored in the same local database and are reused offline and after a restart. They are keyed by the compiled client schema: a schema change starts a new set and removes the previous one when the database opens, and a rebuilt database starts with none. They are scoped to the database file, not to a user: open a separate database for each backend, account or tenant, or delete it when the signed-in identity changes. Remove saved results with `client.queries.invalidate.<name>(args)`; nothing expires them automatically.

Receipts and pull changes carry a per-record stamp. A newer stamp replaces the record's authoritative state; a delayed lower stamp cannot overwrite it, whichever path delivers it. Deletions apply across channels, and the deleted record's stamp is kept so older content cannot resurrect it. See [how state moves](../concepts.md) for the relationship between records, channels and pending writes.

## Storage size

Cached records, queued calls, rejection details, saved `once` Query results and backend receipts persist. Client business results held by live `Call` handles are memory-only. Saved `once` results have no size limit; your application bounds them through the argument sets it uses and `invalidate`. The runtime does not impose a cache-size limit or automatically expire these entries. Backend call outcomes are retained without TTL or automatic pruning; backend invalidations compact by channel/Model/identity, but distinct identities still consume space.

Measure database size, pending work and synchronization lag with your application's working set. Local reads, including read-only SQL, use on-disk SQLite tables. They do not copy the full record set into a separate query projection.
