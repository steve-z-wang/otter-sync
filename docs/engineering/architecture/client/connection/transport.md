# Transport

## 1. Introduction and Goals

The transport moves bytes. It knows the four HTTP routes and the WebSocket route, adds the bearer token, honors cancellation, and buffers streamed frames within a bound. It never looks inside a request body or a frame, not even to tell the acknowledgement from a page; those come from and go to Rust.

## 3. Context and Scope

Configuration is `{url, token}`, where `token` is a string or a function returning one. Push, catch-up, direct Action calls and Model Fetch use `POST <url>/sync/mutations`, `POST <url>/sync/pull`, `POST <url>/sync/actions` and `POST <url>/sync/fetch`; the stream is a WebSocket on `<url>/sync/live`. All five carry `Authorization: Bearer <token>`. The runtime names the route of each `http` effect (`push`, `pull`, `action`, `fetch`). In TypeScript a carrier is a `Transport`, `(kind: HttpRoute, body, signal) => Promise<string>`, exported with the `HttpRoute` union; the shared `httpTransport` maps each route to its path for Node and React Native, and a custom carrier must accept `fetch` too. In Dart `RuntimeConnection` routes `fetch` to `ServerSession.fetch`. A non-2xx response becomes an error carrying `status` (TypeScript) or `AuthenticationExpired` for 401 and `HttpFailure` otherwise (Dart); the effect executor passes that status back in the effect result, which is what the [runtime](../runtime.md) uses to ask for an auth refresh ([Scheduling](controller/scheduling.md#6-runtime-view)). Direct calls use the connection's configured carrier independently of the durable push and live lanes. `directTimeoutMs` in TypeScript accepts an integer from 1 to 2,147,483,647 milliseconds; `directTimeout` in Dart accepts a positive `Duration`. Both bound the entire direct attempt, including token acquisition and refresh, and default to 30 seconds. The runtime enforces that deadline with a `timer` effect. For an Action call a timeout or lost response leaves execution unknown; a Fetch is a read and claims nothing. Authentication retry reuses the prepared request body and call ID.

Native Load pages use `POST <url>/sync/loads` with the same bearer token, as the `load` route of an `http` effect; the runtime bounds each attempt with its own `timer` using the direct-call deadline ([Load worker](controller/load-worker.md#6-runtime-view)). The TypeScript `httpTransport` maps exactly the `push`, `pull`, `action` and `load` routes and throws `unknown route` for anything else, posting nothing.

The Dart connection maps `load` to `ServerSession.load`, which posts to `/sync/loads`, and still refuses any other unknown route. React Native reuses the TypeScript `httpTransport`, and a custom TypeScript `Transport` receives `load` as its `kind`. The Dart `refreshAuth` executor fails with the status its error carries (`HttpFailure` or `AuthenticationExpired`), so a 401 or 403 refusal reaches the runtime as in TypeScript.

## 5. Building Block View

Both transports do the same things with language-native tools:

| Concern | TypeScript | Dart |
| --- | --- | --- |
| HTTP | `fetch` with an `AbortSignal` | one `HttpClient` per request, force-closed on cancel |
| WebSocket | `ws` with an 8 MiB frame limit | `dart:io` with an 8 MiB text check |
| Frame buffer | 64 frames; the socket is paused while a frame is delivered | 128 frames or 8 MiB in total |
| Overflow | buffer cleared, the frame that arrived is kept, `overflow` reported to the worker | same |
| Oversized frame | `ws` closes the socket (code 1009) | the frame is refused and the socket ends |
| Cancellation | abort signal terminates the socket, even mid-upgrade | a future completes and closes the socket |

The buffer only bounds delivery: frames are handed to the [Downlink worker](controller/downlink-worker.md) one at a time, in order, and handing one over only queues it. On `overflow` the worker recovers every channel from the durable cursor; overflowing never restarts an in-flight HTTP request, so a burst of pages cannot starve the catch-up that advances the durable cursor. The worker's own bound on pages it holds is the same in both languages. An oversized frame is not a protocol violation the worker judges: either transport ends the socket and reports `closed`, and the worker reconnects with backoff like any other dropped socket.

Code: [client-js/transport.mts](../../../../../packages/client-js/transport.mts), [client-js/live.mts](../../../../../packages/client-js/live.mts), [dart/live.dart](../../../../../packages/dart/lib/src/live.dart).

## 10. Quality Requirements

- **The socket sends the subscribe frame and delivers frames in order without interpreting them; cancellation ends a stalled token, an in-flight request and an opening handshake without reporting a close, and a token that resolves late cannot start a request.** Evidence: [live.test.mjs](../../../../../integration/bindings/client-js/live.test.mjs) `internal socket sends the subscribe frame, delivers frames in order, and cancellation ends the socket`, `live transport cancellation does not wait for a stalled token`, `close cancels opening handshake…`, `client close abandons a stalled live token…`; [live_test.dart](../../../../../packages/dart/test/live_test.dart) `the socket sends the subscribe frame and delivers frames in order`, `cancel push before token resolution prevents any later HTTP request`, `HTTP catch-up cancellation ends stalled token and in-flight response`.
- **Overflow preserves in-flight HTTP progress and converges on the latest head.** Evidence: `bounded receive overflow preserves in-flight HTTP progress and recovers the latest head`; [live_test.dart](../../../../../packages/dart/test/live_test.dart) `bounded receive buffer (128 pages) overflows into recovery without restarting the in-flight HTTP catch-up` for the page bound and `bounded receive buffer (8 MiB) overflows on ten large pages without restarting the in-flight HTTP catch-up` for the byte bound.

Tests read, not executed, except the live suites cited for overflow, run 2026-09-15.

## 11. Risks and Technical Debt

**Accepted limitation.** The TypeScript transport is Node-only: it depends on the `ws` package and sets an upgrade header browsers cannot set. Browser and WebAssembly support is planned ([#59](https://github.com/zanminwang/axton/issues/59)).
