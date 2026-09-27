# Downlink lifecycle across an in-process replica rebuild

Status: design for issue [#162](https://github.com/zanminwang/axton/issues/162); implementation and verification remain pending.

## Goal and current failure

After `Client.rebuild` replaces an incompatible local replica, an already connected client's Downlink lane must keep making progress without another application `connect` call. The rebuilt replica carries requested Channel names with fresh local subscription identities and no origin; the next handshake must establish those new origins. Explicitly paused and stopped lanes retain those intents.

Today `bindings/common/src/lib.rs` replaces `Entry.downlink` with `DownlinkWorker::default()` after a successful `Client::rebuild`. That clears `ConnectionDriver.running`; the TypeScript and Dart host loops still exist, but neither sends another `start`. It also resets `LiveSession.epoch` and `DownlinkWorker.requests`, so a late socket frame or HTTP response from the old replica can match a new socket or request with the same numeric ID. The old socket and Bootstrap HTTP call can remain held by the host. The binding's push `SyncCycle` reset is outside this issue.

## Contract and scope

1. A successful rebuild is one atomic logical transition at the binding boundary: the new replica is installed, then the existing Downlink worker is reset for that replica before the command returns. A refused or failed rebuild changes neither worker state nor its identifiers.
2. Running intent survives. The next pump first abandons old host I/O, then starts a session against the carried Channel names and the new replica. It does not wait for an application `start`, a subscription change, or unrelated delivery. A rebuild with no Channels remains idle until a later registration wakes it.
3. Paused intent survives: old I/O is abandoned, no socket or Bootstrap request starts while paused, and `resume` starts normally. Stopped intent survives: a rebuild cannot restart a closed lane; only a subsequent explicit `start` may do so.
4. Every old socket `message`, `closed`, or `overflow`, and every old ordinary or Bootstrap HTTP `response` or `failed`, is ignored after the reset. It cannot initialize a Channel, apply a page, complete or fail a Bootstrap run, change retry state, or close the new session. This includes callbacks already queued when rebuild begins and callbacks received after new I/O starts.
5. Rebuild discards the worker's old queued pages, controls, pending requests, session, retry timers, and Bootstrap scheduling. Durable Bootstrap state is not carried into the fresh replica; the existing storage contract remains authoritative. Starting a new lane re-evaluates any persisted barriers before issuing I/O, as it does on `start`.
6. Rebuild does not change wire messages, public connection controls, Bootstrap semantics, or Channel terminology. The wider Scope-to-Channel rename belongs to #152.

The existing [`Downlink worker`](../../engineering/architecture/client/connection/controller/downlink-worker.md), [`scheduling`](../../engineering/architecture/client/connection/controller/scheduling.md), and [`reconciliation`](../../engineering/architecture/client/storage/reconciliation.md) documents own the lasting component rules. This design describes the issue-specific transition.

## Design

Add an explicit `DownlinkWorker::reset_for_rebuild()` operation, called by `RuntimeHost` only after `Client::rebuild` succeeds. Reset in place instead of assigning `DownlinkWorker::default()`. Preserve the socket epoch allocator and the shared HTTP request ID allocator so values issued before rebuild are never issued again within that worker's lifetime. Do not use the new replica's subscription generation as the fence: it can equal the old replica's value. Keep the existing per-session epoch and per-request ID checks in `enqueue`.

The reset records whether the driver was running and paused, clears all transient session and queue state, and initializes a fresh driver/schedule in the same intent. For a running, unpaused lane, mark it dirty and due immediately. For a paused lane, retain `running=true, paused=true`, with work due upon `resume`. For a stopped lane, leave the driver stopped. Reset Bootstrap's in-memory request slot and backoff, not its durable ledger contract. `ConnectionDriver` should expose only the lifecycle snapshot/reset operation the worker needs; avoid letting the binding interpret sync policy.

The worker's first post-rebuild `next` returns a one-shot host reset action before any new `open` or `request` action. The TypeScript and Dart executors abort their old socket and both classes of HTTP work, clear their old session/outstanding status, and give subsequent Bootstrap requests a fresh cancellation token. The worker itself has already discarded their events. An old callback that races the abort remains harmless because the numeric identifiers are never reused. This action is required even if no Channel is currently registered or the lane is paused: otherwise a long-running old Bootstrap call can remain open indefinitely. The worker's reset action replaces a normal `close` for that old session, so it cannot accidentally target a new socket. Repeated pumps emit it once.

Both SDK rebuild methods must wake the downlink host after the native rebuild command succeeds. TypeScript can use its existing `channels` event, which the connected lane already listens to; Dart can publish to `_channels` after `_subscriptions.rebuilt()`. Do not wake on a failed rebuild. The wake must be serialized after the native rebuild response and must work when the host loop is sleeping without a timer. The host's existing wake-generation check prevents the wake from being lost during the idle decision. No second `start` is sent, and no public API is added.

The ordering on a connected rebuild is:

```text
native rebuild succeeds -> worker reset preserves intent and ID allocators
SDK invalidates old subscription handles -> SDK wakes Downlink host
first pump -> reset host I/O -> optionally open new session / schedule fresh work
old callbacks (in any order) -> ignored by epoch/request fence
new handshake -> initialize carried Channels at the new acknowledged heads
```

## Tests and evidence boundary

The primary regression belongs in `bindings/common/tests/session.rs`, because it can drive the actual `rebuild` command and the worker behind the same handle. Build an incompatible schema with a carried Channel and pending work, start the lane, issue an old socket epoch and both HTTP request kinds, rebuild, then prove a new `open` arrives without `start`, with greater epoch and request IDs. Enqueue old message/close/overflow/response/failure before and after the new handshake; assert no new-replica cursor, Model, Bootstrap status, or session action changes. Check refused rebuild leaves the old lane intact. Test running, paused/resumed, and stopped/restarted cases, including an idle lane with no Channels.

Use focused `crates/sqlite/tests/downlink_worker.rs` cases for reset queue clearing, monotonic identifiers, Bootstrap scheduling, and ordering of the one-shot host reset action. Use TypeScript `integration/bindings/client-js/connection.test.mjs` and Dart `packages/dart/test/connection_test.dart` to assert the new host action aborts old socket/ordinary/Bootstrap requests and a sleeping lane is woken after rebuild. Existing SDK rebuild tests can check subscription handles are invalidated and the new handshake uses fresh identities. A real network test is valuable if the mock transport cannot expose a callback that resolves after abort; it must prove that stale data cannot land in the new SQLite file.

This planning change inspected source and existing test assertions. It did not execute tests or claim the behavior is fixed.

## Alternatives and risk

Replacing the worker and sending `start` from each SDK would restore activity, but the new worker would recycle epoch and request IDs unless a separate generation were threaded through every event. Keeping the worker and its allocators makes the fence local to the component that already owns correlation. Preserving the old worker's pending queues is unsafe because they were created against the old replica.

The concrete implementation must check numeric exhaustion instead of wrapping identifiers; existing code increments `u64` without checked arithmetic, while JSON bindings require safe integer values. This issue should use the established safe-counter contract for emitted IDs. It must also verify that a reset action preceding a fresh `open` is executed in order by both hosts. Those are implementation review points, not unresolved product decisions.
