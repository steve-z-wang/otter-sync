# Client

The client runtime keeps local state in SQLite and synchronizes it with a server.

- [Runtime](runtime.md) — Own every task from submission to outcome: scheduling, the application transaction, the connection lanes, direct calls and observers.
- [Frontend interface](frontend-interface.md) — Expose reads, writes, subscriptions and status to the runtime.
- [Engine](engine/README.md) — Local reads and writes, mutations, cursors, rollback and completion from receipts.
- [Storage](storage/README.md) — Execute Engine-requested SQL and transactions; no sync policy.
- [Connection](connection/README.md) — HTTP/WebSocket, catch-up and reconnect.

## How the parts work together

An SDK talks only to the [runtime](runtime.md): it submits complete tasks, runs the effects the runtime asks for - HTTP, the socket, timers, credential refresh, application callbacks - and delivers the outcomes the runtime publishes. The runtime runs one command at a time against the [frontend interface](frontend-interface.md). Each command runs in a [storage](storage/README.md) transaction and is carried out by the [engine](engine/README.md), which owns every rule about optimism, queueing, cursors and settlement. The runtime also drives the [connection](connection/README.md) state machines, which decide when to push, stream, catch up and retry, and turns their network actions into effects; it answers a task only after the commit it depends on, and publishes statuses and watch results the same way. The connection never decides anything about the data. Because all durable state lives in tables, a client can be closed at any commit and reopened without losing queued work, receipts or cursors.
