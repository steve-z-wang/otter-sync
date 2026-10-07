Every Query invocation has a fresh request and snapshot; `store: false` returns it without installing Models.

AXTON stores local Models, queues named Mutations and follows one Stream. Generated TypeScript and Dart clients share the native engine's durable state.

## Models and identities

A Model is the complete client-facing shape returned by its viewer Loader. An identity names one record; generated Create and Patch inputs have distinct field requirements. Nullable record fields remain present in complete snapshots. A Model without a Loader is device-only and cannot be published.

`@@bootstrap` selects initial historical materialization. It does not imply tracking, parent presence or universal cache completeness. Existing reference metadata supports navigation and declared device cascades; it is not a domain foreign-key existence policy.

## Mutations and local transactions

Only named Mutations produce durable remote writes. A first await commits the input and its optimism locally and returns a Call; `Call.wait()` awaits acceptance or refusal and the required local settlement. The callback form receives `tx.models`, performs device-only companion writes, then returns the typed input. Input and companion work commit atomically. Acceptance retains companions; refusal undoes only that Call's owned work, preserving later independent operations.

`client.transaction` is the local atomicity boundary. Direct `tx.models` writes remain local. Several named Mutations in one local transaction retain independent remote fates. Their backend handler, publication and durable receipt commit in one server transaction.

## Reads and authority

Queries and Fetch return invocation snapshots. Their request-level `store` boolean defaults to true. Permitted cache writes finish before the response resolves, but returned snapshots may differ from the current Store. Ordinary reads carry `cursor: null`; they cannot establish Stream progress or replace current Stream content or tombstones. An ordinary null returns absence without deleting a cached row.

Every Query invocation has a fresh request and snapshot; `store: false` returns it without installing Models.

## Streams and Bootstrap

One physical file has one Client, Store identity and Stream. The server binds the Store to its authenticated principal. Its persisted incarnation survives normal reopen and changes on explicit reset. Credentials can refresh independently. The backend explicitly tracks identities; global invalidation publishes changed content to existing holders, and selected invalidation refreshes selected existing holders without enrollment.

A Stream cursor orders authoritative content and membership changes within that Stream. The local record guard retains installed positions for each materialization contract. Membership Remove stops live-content protection but preserves the record, its guard and true deletion protection. Canonical Stream absence establishes a tombstone.

`client.bootstrap()` awaits a finite frozen authority plan. Handshake commits initial S and C=S; B remains absent until the final complete Bootstrap unit commits. Sync advances only proven complete coverage. Rematerialization also covers held authority; reconnect retains committed progress.

Delivery units contain identities that must commit together for actual constraints. Independent units may commit separately; a failed group cannot be skipped. Publication evidence and bounded targeted materialization let accepted Calls recover historical authority without automatic tracking or cursor restamping.

See [client APIs](frontend/client-api.md), [Bootstrap](frontend/loads.md), [backend handlers](backend/api.md) and [sync recovery](frontend/sync.md).
