# Controller

The controller decides when the client talks to the server. Rust owns the decisions, the session and the cursor rules, and the client [runtime](../../runtime.md) drives them; the host language owns timers, sockets, HTTP and credential storage, and executes the effects the runtime asks for.

- [Scheduling](scheduling.md) — Per-lane state machine: when to run a cycle, when to retry, how pause, resume, wake and close behave.
- [Push lane](push-lane.md) — Freeze a batch, send it, hand the receipt to the engine, repeat.
- [Downlink worker](downlink-worker.md) — Own inbound delivery: subscribe over WebSocket, catch up over HTTP from the durable cursor, queue and commit pages, recover from gaps and subscription changes.
  - [Live session](live-session.md) — One socket attempt of the worker: wire subscription, epoch and handshake order.

## How the parts work together

A connection runs two independent lanes, each with its own [scheduling](scheduling.md) state. The **push lane** (uplink) sends queued mutations over HTTP. The **downlink lane** is the Rust [Downlink worker](downlink-worker.md), which holds a WebSocket session at a time and delivers server pages; each session begins with an HTTP catch-up from the durable cursor, because the stream starts at the server's current head. The [runtime](../../runtime.md) drives both lanes as its own work, interleaved with foreground tasks in arrival order: it turns each lane's decisions into effects and each effect result back into lane events, and every lane keeps its scheduling in Rust.

The lanes meet in the engine, not in the controller. A receipt completes its batch at once ([Settlement](../../engine/settlement.md)), which may unblock a dependent mutation, so the push lane's cycle goes on until there is nothing to send. Pages from the downlink lane never complete a batch; they carry the same authority the receipt already did, or newer. Every task or continuation that commits - a write, a subscription change, a readiness change, a dropped mutation - wakes both lanes, and a page the Downlink worker applied wakes the push lane. Subscription changes additionally invalidate the live session so the worker renegotiates with the new channel set.

Pause, resume, wake and stop are `connection` tasks that reach both lanes; `Client.close` closes the connection first. Errors from either lane reach the application through `onError` as runtime `report`s, and when the application supplied `refreshAuth`, a 401 on either lane - or on a direct call - asks for one `refreshAuth` effect, shared by everything that hit it together. The refresh itself runs in the host because it needs the application's callback and the platform's credential store; the runtime only decides when to ask and what waits for it.

## Decision: no HTTP polling fallback

The downlink lane is the only path that pulls pages; the push lane never pulls. This was decided and is not open. The consequence when a WebSocket cannot be established (a proxy that blocks upgrades, for example): pushes still succeed and complete from their receipts, with the server's content for the records they targeted; the downlink lane retries with backoff indefinitely, and other clients' changes arrive only once the socket connects.
