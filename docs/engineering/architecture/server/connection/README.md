# Connection

- [Transport](transport.md) — Send and receive HTTP/WebSocket messages.
- [Controller](controller.md) — Manage WebSocket subscriptions and stream pages as commits arrive.

## Code map

| Part | Code location |
|---|---|
| Transport | HTTP and WebSocket in [server/index.mts](../../../../../packages/backend/server/index.mts) |
| Controller | [server/live.rs](../../../../../crates/server/src/live.rs) (`Subscriptions`); executor `serveLive` in [server/index.mts](../../../../../packages/backend/server/index.mts) |
