# Transport

## 1. Introduction and Goals

The server transport terminates HTTP and WebSocket traffic, authenticates each request once, enforces size limits and turns engine errors into statuses. It contains no sync logic.

## 3. Context and Scope

`backend.listen({port, host = "127.0.0.1"})` starts one Node HTTP server with four POST routes, `/sync/mutations`, `/sync/actions`, `/sync/fetch` ([Model Fetch](../../protocol/actions.md#model-fetch)) and `/sync/pull`, and a WebSocket upgrade on `/sync/live`. Other paths are `404`; other methods `405`. Bodies and frames are limited to 1 MiB.

`POST /sync/loads` serves batched native Load pages ([Protocol / Loads](../../protocol/loads.md)). It authenticates once per request (`401` without it) and answers a malformed envelope `400 request.invalid`; every item outcome, a rejection included, answers `200` inside the body.

## 5. Building Block View

Request handling is a pipeline: `authenticate` (null or blank → `401 unauthenticated`), read the body under the size cap (`413 request_too_large`), parse strict UTF-8 JSON (`400 request.invalid`), call the engine inside a transaction, answer `200` with the engine's JSON. Engine errors map by their code, never by message text ([Bindings](../../sdks/bindings.md)):

| Engine code | Status | Body |
| --- | --- | --- |
| `request.invalid` | 400 | `{code}` |
| `client.owner_mismatch` | 403 | `{code}` |
| `gap`, `overlap` | 409 | `{code}` |
| `model_version_unsupported` (pull and live subscribe only) | 409 | `{code, model, version}` |
| any other code, or a non-engine error | 500 | `{code: "server"}`, and the error goes to `onError` |

A push answers `200` even when one or more mutations are rejected: `mutation_version_unsupported`, `model_version_unsupported`, `handler.failed` and `loader.failed` are per-mutation entries in the receipt's `rejections`, never a status of their own ([Server / Push §9](../engine/push.md#9-architecture-decisions), [#95](https://github.com/zanminwang/axton/issues/95)). A Model Fetch likewise answers `200` with a `failed` completion when its read was rejected, `model_version_unsupported`, `loader.unregistered` and Loader failures included; only a malformed envelope or identity is `400 request.invalid` ([fetch.test.mjs](../../../../../integration/persistence/server/fetch.test.mjs)).

The live path closes with `1002` when negotiation fails with `request.invalid`, and with `1011` for any other failure (a failed pull, or a controller error such as `live.invalid_page`), which also goes to `onError`. What to pull and send is decided by the Rust controller; `serveLive` only executes its actions ([Controller](controller.md)).

The upgrade path authenticates before accepting the socket and refuses with a raw `401`, `500` (authenticate threw) or `503` (server closing). `close` stops upgrades, closes sockets with `1001`, then closes the server.

Code: `createHttpHandler`, `attachLive`, `serveLive`, `listen` in [server/index.mts](../../../../../packages/server/index.mts).

## 7. Deployment View

One Node process runs the listener, the handlers, the loaders and the native engine. The listener binds to `127.0.0.1` unless `host` says otherwise, speaks plain HTTP and `ws://`, reads no forwarded-for headers, and enforces the 1 MiB limits itself. The supported placement is behind a reverse proxy that terminates TLS, forwards the POST routes and relays the `/sync/live` upgrade with the `Authorization` header preserved. Live wakeups are process-local, so one process serves each set of live subscribers. The author-facing description, including a proxy example and what has and has not been validated, is [Deploy the backend](../../../../../website/docs/backend/deployment.md).

## 10. Quality Requirements

- **Unauthenticated requests are refused, valid ones reach the native engine, malformed bodies are `400`, and server failures are `500 {code: "server"}` reported to `onError`.** Evidence: [runtime.test.mjs](../../../../../integration/persistence/server/runtime.test.mjs) `HTTP adapter authenticates and serves the real native persistence path`, `onError captures server-side failures and HTTP responds with {code:"server"}`, `listen answers pull over HTTP with authentication and closes cleanly`.
- **Every mapped status is produced from the real engine, and the mapping keys on the code alone.** Evidence: `HTTP maps engine codes to statuses: 403, 409 gap/overlap/version fields, 404, 405, 413, 400` (real backend over PostgreSQL) and `HTTP classifies native failures by code, not message wording; unknown codes fall back to 500` (a fake native whose messages are reworded; also asserts the live `1002`/`1011` close codes and that unclassified codes reach `onError` as `EngineError`).

- **Behind a reverse proxy that forwards HTTP and relays the WebSocket upgrade with headers preserved, push, pull and live work; a proxy that strips `Authorization` is refused.** Evidence: `a reverse proxy forwarding HTTP and the WebSocket upgrade with headers serves push, pull and live; a stripped Authorization header is refused` (an in-process proxy with TCP-level upgrade pass-through). Verified 2026-09-14 by `bash integration/persistence/server/run.sh`.

Verified 2026-09-14: `bash integration/persistence/server/run.sh` passed with the proxy and structured-error tests.

## 11. Risks and Technical Debt

**Accepted limitation.** No TLS, CORS, compression or proxy-header handling is built in; the listener is meant to sit behind a reverse proxy on loopback ([Deployment View](#7-deployment-view)). TLS termination and specific proxy products are not exercised by any test; browser clients ([#59](https://github.com/zanminwang/axton/issues/59)) and multi-process live delivery ([#62](https://github.com/zanminwang/axton/issues/62)) are separate decisions.

**Accepted limitation.** The 1 MiB limits are fixed; the internal options exist but `listen` does not expose them ([#11](https://github.com/zanminwang/axton/issues/11)).
