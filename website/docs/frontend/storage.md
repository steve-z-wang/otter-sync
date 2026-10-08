# Local storage


## Choose a database path

Protocol 5 requires a fresh format-5 file. Another format is refused intact, including its WAL and SHM. Keep the old release and backend available while unresolved work is drained or exported using the old release under a separately owned recovery process. Opening a fresh file neither transfers nor abandons that work.


Use a writable application directory and one active client per physical file. A Store binds backend, viewer, Stream and contract; changing credentials cannot change that binding. Normal close/reopen retains its incarnation and frozen request identities. Independently writable copies must not send the same queued calls.


## Change the schema

Compatible changes, including a new Model or nullable field, adapt local tables without dropping pending operations. The Store retains full prior descriptors for durable frozen Mutation contracts. Active rematerialization installs current-compatible authority at the same Stream position while preserving later local operation overlays.

Unsupported changes fail without silently discarding pending work. Explicit `resetStore` changes incarnation, rebuilds the Store and ends old handles. Pending work prevents reset unless `discardPending` is explicitly chosen. Update backend business tables through your own forward migrations. See [opening and schema changes](runtime.md#opening-and-schema-changes).

## Transactions and pending work

A local transaction commits direct local writes and each named Mutation's input, optimism and companions together. A crash before commit saves none; a crash after commit preserves all. The callback itself is never replayed. Acceptance keeps companions; refusal removes only the original Call's owned work. Later independent local changes retain their order.

Retry uses the exact persisted Mutation intent and retained context. A request timeout does not imply server refusal: the handler may already have committed its receipt. An accepted Call remains pending until its required authority or receipt-proved materialization settles locally. Check connection diagnostics, prerequisites and [unsent work](runtime.md#unsent-work); do not edit internal queue, guard, cursor or receipt tables.

## Cached data and progress

The Store follows its single bound Stream. A membership Remove retains cached Models and record guards while releasing live-content protection; a true Stream tombstone remains protected. Cache presence grants no permission. Viewer Loaders and current application access rules decide visibility.

Bootstrap manifest coverage and ordinary Stream progress persist separately. A page commits its actual atomic units and coverage together; a failure cannot skip an identity or advance progress. Tail capture records a head, and completion waits for actual Delta installation through that head. Reopen resumes the saved manifest and cursor.

Ordinary Query and Fetch records carry null cursors. They may populate unprotected cache, but never advance Stream progress, replace current Stream authority or clear a tombstone. An ordinary null returns absence without deleting a cached row. Returned invocation snapshots and the current Model projection are distinct.


## Storage size and SQL


Each Model has a table named exactly the Model, with one column per field. Read these tables using `readSql` or `watchSql`. Tables beginning `axton_` belong to the engine; do not read or modify them. Local SQL reads use the on-disk SQLite projection rather than a separate full-record copy.
