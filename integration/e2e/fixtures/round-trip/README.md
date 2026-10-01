# Round-trip fixture

The generated backend stores Entry rows in PostgreSQL and serves Node and Dart clients over HTTP and WebSocket. `initialize` tracks `entry-1` on `book:demo`; `notify`, `publishMany` and `publishOne` invalidate and track rows for live delivery. `invalidateRecords` targets existing tracking at a newer authority stamp without changing the business row. `tombstone` deletes and globally invalidates a row, so its viewer Loader answers null while tracking remains durable.

`failLoads`/`allowLoads` exercise Loader errors. `head`, `positionOf` and `members` inspect retained Stream state; `reset` starts scenarios with exact empty cursors. Bootstrap, unsubscribe, offline reopen and JS/Dart parity retain their existing evidence. Historical removal decoding is covered by storage migration tests; this fixture declares no withdrawal.
