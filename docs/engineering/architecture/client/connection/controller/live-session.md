# Live session

## 1. Introduction and Goals

A live session is one socket attempt of the [Downlink worker](downlink-worker.md): the wire subscription the worker asked for, the epoch that fences the frames of that socket, and the handshake order. It decides nothing about delivery, so replacing the socket cannot lose queued work or a durable position: the session holds no queue, no cursor and no client.

## 3. Context and Scope

`LiveSession` is a value inside the worker and is driven only by it; neither the [runtime](../../runtime.md) nor an SDK reaches it. Its state is the epoch, the `SubscribeRequest` the socket sent, whether the acknowledgement arrived, and the subscription generation the channels were snapshotted under ([Frontend interface](../../frontend-interface.md), `subscription_generation`).

| The worker asks | The session answers |
| --- | --- |
| `begin(channels, models, generation)` | The next epoch and the subscribe frame the host sends once the socket is open: the `subscribe` of the runtime's `socket` effect ([Protocol / Subscriptions](../../../protocol/subscriptions.md)). |
| `current(epoch)`, `open()` | Whether that epoch is the session now open, and whether one is open at all. |
| `generation()` | What the open session subscribed under, so a committed subscription change invalidates it. |
| `acknowledge(ack)` | Handshake order: the first acknowledgement of the session, for exactly the channels it subscribed (`ack.confirms`). Anything else is `invalid live subscription acknowledgement`. |
| `streamed()`, `acknowledged()` | Whether a streamed page is in order; a page before the acknowledgement is `live page before acknowledgement`. |
| `close()` | The epoch of the socket to close, if one is open; the runtime cancels that socket's effect. After a replica rebuild the worker discards it: the runtime abandons that socket on `reset` instead ([Downlink worker](downlink-worker.md)). |

## 5. Building Block View

- **Epoch.** Every session has one, allocated by the session and never reused: the counter lives as long as the worker, survives a replica rebuild, and fails rather than wrap past the safe integer range. An I/O event names the epoch it belongs to, so whatever an abandoned socket still delivers is ignored and the host does not need to know why a session ended. HTTP answers are fenced by the worker's request id instead ([Downlink worker](downlink-worker.md)).
- **Handshake order.** Exactly one acknowledgement, first, for exactly the subscribed channels; pages only after it. Both refusals are protocol violations the worker turns into `close {reason}` plus a retry with backoff.
- **What it does not own.** Page application, the frame queue, cursors, pull building, the retry schedule and Bootstrap progress all belong to the worker ([#150](https://github.com/zanminwang/axton/issues/150)).

Code: [client/live.rs](../../../../../../crates/client/src/live.rs).

## 10. Quality Requirements

- **Only the open epoch's frames count, and the handshake order is enforced.** Evidence: [sqlite/tests/downlink_worker.rs](../../../../../../crates/sqlite/tests/downlink_worker.rs) `every_event_of_a_replaced_socket_is_fenced_by_its_epoch`, `protocol_violations_close_with_a_reason_and_retry`, `a_session_subscribes_pulls_only_when_behind_and_then_streams` (its first step is a page before the acknowledgement).
- **A new session subscribes again with the current channel set, without an application event.** Evidence: `a_dropped_socket_reconnects_with_backoff_and_resubscribes`, `a_subscription_change_ends_the_session_and_the_next_one_uses_the_new_set`.
