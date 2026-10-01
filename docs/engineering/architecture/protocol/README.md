# Protocol

- [Common](common.md) — Shared fields, counters and encoding conventions.
- [Push](push.md) — Durable Mutation and queued Query batches, per-call outcomes, record authority and legacy mutation wire compatibility.
- [Direct calls](actions.md) — Direct request/response envelope for Queries, direct Mutations and Model Fetch, and shared call result semantics.
- [Pull](pull.md) — One request for every scope, pages of record changes with per-scope cursors, and records the server could not read.
- [Subscriptions](subscriptions.md) — WebSocket subscription requests and acknowledgments.
- [Loads](loads.md) — Batched native Load pages: request and response envelopes, per-item outcomes, correlation and bounds.
