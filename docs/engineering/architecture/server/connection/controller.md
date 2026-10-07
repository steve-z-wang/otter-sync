# Controller

Protocol-5 policy and failure boundaries are owned by [protocol 5](../../protocol/0.5.md). Rust owns synchronization; language bindings execute networking, timers and interface callbacks. Descriptions of protocol-4, epoch/Load jobs, once caches or legacy queue tables below are historical component context, not current public APIs.

## 1. Introduction and Goals

The server controller holds one WebSocket per client, learns its streams once, and sends a frame as soon as a transaction that touched any of them commits. It reuses the pull engine for every frame, so streamed and fetched pages have one shape.

## 3. Context and Scope

Input: an authenticated socket from the [transport](transport.md), commit wakes from [Publish](../engine/publish.md). Output: the acknowledgement and pages defined in [Protocol / Subscriptions](../../protocol/subscriptions.md). Negotiation and every page run inside a database transaction through the [backend interface](../backend-interface.md).

## 5. Building Block View

The state is the Rust `Subscriptions`, one per socket: per accepted stream a `StreamState` with the cursor streamed so far (starting at the head read during negotiation) and a *pending* flag set by commits; one outstanding pull at most for the whole session; a *closed* flag; and the client's declared read contracts (`models`), which every `pull` action carries. It is driven by events and answers with an ordered list of actions. The Rust side also decodes the subscribe frame, normalizes streams, reads heads for the acknowledgement, and validates that every frame starts each stream where the stream left it.

The TypeScript executor (`serveLive`) keeps no sync decision: it carries events in and performs actions out. The Node binding keeps the open sessions in a process-wide registry under a numeric handle, so the controller's state survives between native calls ([Bindings §3](../../sdks/bindings.md#3-context-and-scope)).

Code: `Subscriptions`, `LiveEvent`, `LiveAction`, `decode_subscribe`, `negotiate`, `pull`, `page_progress` in [server/live.rs](../../../../../crates/server/src/live.rs); `negotiateLive`, `liveEvent`, `liveClose` in [bindings/node/src/server.rs](../../../../../bindings/node/src/server.rs); the executor `serveLive` in [server/index.mts](../../../../../packages/server/index.mts).

## 6. Runtime View

1. The executor waits for the first frame. A second client frame at any time closes the socket with `1002`.
2. `negotiateLive` decodes the frame, checks the declared model versions against the config, reads heads and builds the acknowledgement inside one transaction, then opens the session: `Subscriptions::open` answers `listen` for every stream, `send` the acknowledgement (which carries each stream's head), and one `pull` for every stream from its head, in that order. Listeners are therefore registered before anything is sent, and a commit that lands between the negotiation transaction and the registration is caught by that first pull.
3. The executor performs each action and reports what it learns as an event; the controller answers with the next actions.

| Event | Controller rule | Actions |
| --- | --- | --- |
| `committed {stream}` (a transaction touching the stream committed) | Mark the stream *pending*. If no pull is outstanding, start one. | `pull {cursors, models}` for every *pending* stream from its streamed cursor, or none while a pull is outstanding. |
| `pulled {page}` (the page `pullLive` returned) | Check that every stream in the page starts at its streamed cursor; advance each to its `to`. Streams below their head keep pulling; commits that arrived meanwhile are pulled next; otherwise the drain ends. | `send {frame}` naming only the streams that advanced, then `pull` when the drain continues. |
| `closed` (the socket closed or failed) | Set *closed*, clear every *pending*. Later events produce nothing. | none |

| Action | Executor does |
| --- | --- |
| `listen {stream}` | Register a commit listener with the wake hub that dispatches `committed`; remove it on close. |
| `send {frame}` | Send the frame if the socket is open. |
| `pull {cursors, models}` | Run `pullLive` in a transaction, at the declared versions, and dispatch `pulled` with its page. |

4. A refused declaration (`model_version_unsupported`) closes the handshake with `1002` like a malformed subscribe, without reporting a server failure. An invalid page (`live.invalid_page`) or an event the session cannot accept (`live.invalid_event`: an unknown stream, a `pulled` no pull was asked for, or a handle that is not open) is a defect: the executor reports it and closes with `1011`, as it does when a pull fails; the client reconnects with backoff. On close the executor dispatches `closed`, removes every listener and releases the handle with `liveClose`.

## 10. Quality Requirements

- **Nothing is sent before commit; a rolled-back publication sends nothing; a duplicate receipt wakes nobody; pages split at 50 and the next page starts where the previous ended; reconnecting works.** Evidence: [runtime.test.mjs](../../../../../integration/persistence/server/runtime.test.mjs) `live transport negotiates, wakes only after commit, reconnects, and cleans up`.
- **Only one subscribe frame is accepted and streams are normalized; every frame's cursor progression is checked before sending.** Evidence: [server/tests/runtime.rs](../../../../../crates/server/tests/runtime.rs) `live_subscribe_requires_one_subscribe_frame_and_normalizes_scopes`, `live_page_progression_checks_every_scope_it_asked_for`.
- **Listeners are registered before the acknowledgement and every stream is pulled once from its head; commits on several streams share one pull and the frame names only what moved; a stream below its head continues and one at its head ends the drain; after close no event produces an action; an invalid page or an unknown stream is an error.** Evidence: [server/tests/live.rs](../../../../../crates/server/tests/live.rs) `open_registers_every_scope_before_the_acknowledgement_then_pulls_all_once_from_their_heads`, `commits_on_several_scopes_share_one_pull_and_the_frame_names_only_what_moved`, `a_scope_below_its_head_continues_and_one_at_its_head_ends_the_drain` (pure transitions, no host).
- **A publication committed between the negotiation transaction and the acknowledgement arrives as a page on the same socket.** Evidence: [runtime.test.mjs](../../../../../integration/persistence/server/runtime.test.mjs) `a publication committed between negotiation and the acknowledgement is delivered by the first drain` (the negotiation result is held on a gate while a push commits; no listener exists yet, and the first drain delivers it after the acknowledgement).

Executed 2026-09-16: `cargo test -p axton-server --locked` and `bash integration/persistence/server/run.sh` with the multi-stream frames.

## 11. Risks and Technical Debt

**Potential risk.** Every commit that touches a subscribed stream triggers one pull transaction per socket (several streams share it); there is no shared page cache. Not measured ([#12](https://github.com/zanminwang/axton/issues/12)).

**Accepted limitations.** Wakes are process-local ([Publish](../engine/publish.md)). Changing streams requires a new socket, and a subscribe may name any number of streams.
