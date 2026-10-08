# Subscriptions

Historical carrier reference. Current behavior is owned by [protocol 5](0.5.md). This page does not promise support for old local files or an alternate current API.

Current bound clients have one Stream fixed at open; [protocol 4](0.4.md) defines their subscribe/ACK and repair evidence. Multiple registered Streams and the legacy frame format below belong to protocol 3.

Engine behavior: [Client / Connection / Controller](../client/connection/controller/README.md), [Server / Connection / Controller](../server/connection/controller.md).

## 3. Context and Scope

- Endpoint: WebSocket upgrade on `/sync/live` with `Authorization: Bearer <token>`; refused with a raw `401`, `500` or `503` before the upgrade.
- Client frame, exactly one: `{"type":"subscribe","streams":[…],"models":{…},"capabilities":["stream-authority-v1"]}`. `models` is the same read-contract declaration as on a [pull](pull.md) and required. It carries no cursors and no client id; unsupported capability is refused before acknowledgement; malformed metadata is a protocol error.
- Server acknowledgement: `{"type":"subscribed","cursors":{stream: head}}`: every requested stream with its current head. For a stream the client has not initialized yet, that head is the subscription's origin ([#150](https://github.com/zanminwang/axton/issues/150)).
- Server frames: [Pull](pull.md) pages without a `type` key, naming only the streams that moved; each stream's `from` is where the previous frame (or the acknowledgement's head) left it.
- Close codes: `1002` protocol violation (a second client frame, a malformed subscribe, or a declaration the server refuses, with reason `model_version_unsupported`), `1011` server failure, `1001` server shutting down.

```json
{"type":"subscribe","capabilities":["stream-authority-v1"],"models":{"Entry":2,"Comment":1},"streams":["book:demo","inbox:alice"]}
{"type":"subscribed","cursors":{"book:demo":47,"inbox:alice":9}}
{"cursors":{"book:demo":{"from":47,"to":48,"head":48}},
 "changes":[{"stream":"book:demo","cursor":48,"kind":"upsert","model":"Entry","identity":{"id":"e"},"stamp":13,"state":{"text":"Hello!","note":null}}]}
```

## 5. Building Block View

- Stream names must be non-blank strings (`check_stream`: neither empty nor only whitespace) and at least one is required; the same rule refuses a durable registration and a publication intent. `SubscribeRequest` normalizes them (deduplicated, UTF-16 order); `SubscriptionAck::confirms` checks that the acknowledgement names exactly that set. `LiveMessage` tells an acknowledgement (it has a `type`) from a page (it has none).
- The declaration is checked at the handshake and kept for the session; every frame it streams is loaded at those versions. The acknowledgement is produced inside the negotiating transaction, which also reads each stream's head; streaming starts there.

Code: [core/protocol.rs](../../../../crates/core/src/protocol.rs) (`SubscribeRequest`, `SubscriptionAck`, `LiveMessage`); the server side in [server/live.rs](../../../../crates/server/src/live.rs); the client side in [client/live.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/live.rs).

## 6. Runtime View

HTTP catches up; the WebSocket carries only what is new. The frame carries every desired stream, initialized or not, and carries no cursors; what each head means to the client depends on what it has committed ([Downlink worker](../client/connection/controller/downlink-worker.md)):

| The client's stored boundary for a stream | What the acknowledged head is |
| --- | --- |
| None yet (a registration with NULL cursors) | **the subscription's origin**: one local transaction commits it as both the starting boundary and the cursor, so delivery begins there and nothing published earlier is fetched. Head zero initializes at zero. Loading a Stream's existing records is the explicit operation of [#151](https://github.com/zanminwang/axton/issues/151) |
| A committed cursor below the head | a catch-up target: the client pulls its own range and keeps its origin; saved 100 against head 120 fetches 100 to 120 and does not jump to 120 |
| A committed cursor equal to the head | nothing to do: the client consumes the stream at once |
| A committed cursor above the head | a reported protocol or server-state fault: the session ends and nothing rewinds |

Catch-up requests name the initialized streams only, so a stream waiting for its origin is delivered to but never pulled from, and a NULL boundary is never read as zero. The server keeps its initial drain after listener registration - it scans from the acknowledged heads so publications racing head capture are not lost - and the client keeps its automatic delta recovery on reconnect, stream gaps and overflow. A frame whose `from` does not meet the local cursor of a stream is a gap: the frame is held and a pull fills the gap. Changing the stream set means closing the socket and negotiating again; there is no resubscribe frame, and a recreated subscription at the same name is a new identity that starts over at the next acknowledged head.

## 10. Quality Requirements

- Subscribe, acknowledgement and frame decode, normalize and refuse as the shared fixture says. Evidence: [core/tests/contracts.rs](../../../../crates/core/tests/contracts.rs) `live_frames_decode_as_acknowledgement_or_page_and_scopes_normalize` over [fixtures/protocol/live-messages.json](../../../../fixtures/protocol/live-messages.json).
- Only one subscribe frame is accepted and streams are normalized; the acknowledgement carries heads; commits on several streams share one pull and a frame names only what moved. Evidence: [server/tests/runtime.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/server/tests/runtime.rs) `live_subscribe_requires_one_subscribe_frame_and_normalizes_scopes`, `live_page_progression_checks_every_scope_it_asked_for`; [server/tests/live.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/server/tests/live.rs) `open_registers_every_scope_before_the_acknowledgement_then_pulls_all_once_from_their_heads`, `a_scope_below_its_head_continues_and_one_at_its_head_ends_the_drain`, `commits_on_several_scopes_share_one_pull_and_the_frame_names_only_what_moved`.
- A client whose cursors equal the heads does not catch up; one that is behind pulls once and then streams. Evidence: [sqlite/tests/downlink_worker.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/downlink_worker.rs) `heads_equal_to_the_cursors_mean_no_catch_up_at_all`, `a_session_subscribes_pulls_only_when_behind_and_then_streams`; both SDKs' live suites ([live.test.mjs](../../../../integration/bindings/client-js/live.test.mjs), [dart/test/live_test.dart](../../../../packages/dart/test/live_test.dart)).
- An uninitialized stream takes the acknowledged head as its origin and loads nothing older; an initialized one keeps its cursor and catches up; a head below it faults. Evidence: `a_fresh_subscription_initializes_at_the_acknowledged_head_and_loads_no_history`, `an_acknowledgement_at_head_zero_initializes_at_zero`, `a_reconnect_catches_up_from_the_saved_cursor_instead_of_the_new_head`, `a_head_below_the_committed_cursor_is_a_fault_that_rewinds_nothing`, `a_mixed_set_catches_up_one_scope_initializes_another_and_never_asks_for_a_third`; over a real socket, [integration/e2e/subscriptions.test.mjs](../../../../integration/e2e/subscriptions.test.mjs).

Executed 2026-09-16 (2026-09-25 for the initialization bullet): `cargo test -p axton-core -p axton-server -p axton-sqlite --locked`, the JS and Dart live suites, `bash integration/e2e/run.sh`.

## 11. Risks and Technical Debt

- **Resolved ([#63](https://github.com/zanminwang/axton/issues/63)).** The acknowledgement's vestigial `rejections` array is gone with the new frame.

Unsubscribe removes the registration and its explicit bootstrap task, retaining Models and stamps. Resubscribe initializes a new ordinary origin; it starts no hidden reconciliation or historical walk. Only an explicit `bootstrap()` requests history.
