# Push lane

## 1. Introduction and Goals

The push lane moves frozen batches to the server and their receipts back into the engine. It never pulls: authoritative content arrives through the [Downlink worker](downlink-worker.md).

## 3. Context and Scope

The client [runtime](../../runtime.md) drives the lane: when [scheduling](scheduling.md) says `sync` it restarts the push-only `SyncCycle`, asks it for the next batch, sends the batch as an `http {route: "push", body}` effect, and hands the receipt back to the cycle. Network: `POST /sync/mutations`, which the SDK's effect executor sends through the [transport](../transport.md). The protocol seams `freeze`, `ack` and `pull` remain tasks for tests and tools; the lane never uses them.

## 5. Building Block View

The Rust `SyncCycle` remembers one active action so a request that failed is retried with the same bytes. In push-only mode `next` asks the engine to freeze; a batch already in flight comes back unchanged ([Batching](../../engine/push/batching.md)). `complete` decodes the receipt and acknowledges it, which completes the batch with the server's content ([Settlement](../../engine/settlement.md)). The cycle's full mode, which also issues HTTP pulls per subscribed channel, is retained for tests and the internal protocol fixture and is not reachable from `connect`.

Code: [client/transport.rs](../../../../../../crates/client/src/transport.rs) (`SyncCycle`); the lane in `push_turn`, `push_result` and `push_receipt` in [client/runtime/lanes.rs](../../../../../../crates/client/src/runtime/lanes.rs).

## 6. Runtime View

One cycle: restart the push-only cycle, then repeat freeze → send → settle until the cycle has nothing to send, then report success to the driver. The freeze and the settlement are separate runtime units, each one local transaction, and nothing is held while the batch is out; the settlement commits before the `callCompleted` events of the calls it decided. A transport error ends the cycle with a failure and a `report`; the frozen batch stays in flight and the next cycle resends it unchanged. A receipt completes its batch at once; nothing waits for the live lane. A receipt the engine refuses (another client or batch, or missing authority) is a cycle failure: the batch stays frozen and is resent. A receipt record the engine cannot apply is not: it is skipped, the batch completes, and the runtime reports what could not apply (skipped, conflict, diverged) as a `records` diagnostic, which the SDK hands to `onError` as one `AxtonReport` each.

## 10. Quality Requirements

- **A retried push reuses the frozen request; the call's outcome follows the settlement commit; a receipt completes the push without a pull and the visible row is the server's; a page carrying the same authority later is a no-op that advances the cursor; the lane never issues a pull.** Evidence: [sqlite/tests/runtime_lanes.rs](../../../../../../crates/sqlite/tests/runtime_lanes.rs) `the_push_lane_freezes_sends_settles_and_backs_off_with_one_shared_refresh`, `a_receipt_applies_its_authority_and_leaves_reads_to_the_stream`; [live.test.mjs](../../../../../../integration/bindings/client-js/live.test.mjs) `a receipt record the client cannot apply reaches onError and the batch still completes`; [live_test.dart](../../../../../../packages/dart/test/live_test.dart) `what a page cannot apply reaches onError as an AxtonReport…` (its last step covers a receipt).
- **A client with no subscribed channels still pushes.** Evidence: [live.test.mjs](../../../../../../integration/bindings/client-js/live.test.mjs) `a reusable server config isolates cancellation and no-channel clients only push`.

Executed 2026-09-26 (owner, [#165](https://github.com/zanminwang/axton/pull/165)): `cargo test --workspace --locked`, 687 passed, including the runtime tests above; `live.test.mjs` and `live_test.dart` read, not executed, in this pass.
