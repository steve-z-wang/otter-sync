# Common

## 1. Introduction and Goals

Three runtimes exchange the same messages: Rust, TypeScript and Dart. The protocol therefore fixes one byte-exact JSON encoding, one numeric range and one identity encoding, so that a request frozen on a client can be re-sent byte for byte, a receipt can be cached against it, and a record has the same key everywhere.

## 2. Architecture Constraints

Wire names are short and stable: `streams` and `cursors` name streams and their positions, `stamp` is a record's version. Every counter (cursor, stamp, batch sequence, ordinal, version) is an integer in `0..=2^53−1` so JavaScript reads it exactly.

## 5. Building Block View

**Canonical JSON.** Object keys are sorted by UTF-16 code units, which is JavaScript's sort order, arrays stay in place, and scalars follow RFC 8785, so `-0` and `1.0` encode as `0` and `1`. Request bytes, receipt caching and identity keys all use it.

**Counters.** Any finite integral number spelling (`1e0`, `0.0`, `-0`) within range is accepted; some positions additionally forbid zero.

**Identity.** A record key is the model name plus the normalized identity object; its canonical JSON is the `identityKey` the server stores and the key the client ledgers use ([Models](../schema/models.md)).

**State shapes.** A received state must contain every non-identity field (nullable ones default to `null`), may not contain identity fields, and drops unknown fields, which is what lets an older client accept states from a newer server. Loader output on the server is looser: it may include identity fields and omit nullable ones. A patch names only known non-identity fields and keeps explicit `null`.

**Admission.** A client may send application headers, such as its platform and build, with every request and the live upgrade ([client transport](../client/connection/transport.md)). The backend's `admit` may refuse any of them before the route runs, with a status of 400-599, a JSON body the application chose, and the response header `axton-admission: refused` ([server transport](../server/connection/transport.md)). The marker, not the status, makes an answer a refusal: it refuses the client, not one request, so the client stops its connection and reports the refusal once ([Scheduling](../client/connection/controller/scheduling.md#6-runtime-view)). The same status without the marker is an ordinary failure.

**Errors.** Core has one error kind carrying a message. The server runtime has a structured error `{code, message, details?}` whose codes the HTTP layer maps to statuses ([SDKs / Bindings](../sdks/bindings.md), [Server / Connection / Transport](../server/connection/transport.md)).

**Shared fixtures.** [fixtures/protocol/counter-boundaries.json](../../../../fixtures/protocol/counter-boundaries.json) lists the pull counter boundary cases both sides must agree on; [fixtures/protocol/pull-page.json](../../../../fixtures/protocol/pull-page.json) the page cases ([Pull](pull.md)); [fixtures/protocol/receipt-authority.json](../../../../fixtures/protocol/receipt-authority.json) the receipt cases ([Push](push.md)).

Code: [core/lib.rs](../../../../crates/core/src/lib.rs) (`canonical_json`), [core/protocol.rs](../../../../crates/core/src/protocol.rs) (`counter`, `read_counter`, `limits`), [core/schema.rs](../../../../crates/core/src/schema.rs) (`RecordKey`, state and patch validation).

## 8. Crosscutting Concepts

Three limits are shared by both sides but not negotiated on the wire: 20 mutations and 256 KiB per push, 50 changes per stream in a pull page. They are defined once, in `limits` of [core/protocol.rs](../../../../crates/core/src/protocol.rs), and every consumer reads them from there: the push request decoder and the client's [batching](../client/engine/push/batching.md), the per-stream continuation rule (`CursorRange::continues`) and the [server pull](../server/engine/pull.md) scan. A page with more than 50 changes per named stream is refused by `PullPage::validate`. Making the limits configurable is [#11](https://github.com/zanminwang/axton/issues/11).

Native Load pages have their own item, identity, state and byte bounds, owned by [Protocol / Loads](loads.md#6-runtime-view).

Host resource limits are not protocol rules and stay with each transport: 1 MiB HTTP bodies and WebSocket frames on the server, 8 MiB WebSocket frames and the page buffers on the clients ([Client transport](../client/connection/transport.md), [Server transport](../server/connection/transport.md)).

[fixtures/protocol/live-messages.json](../../../../fixtures/protocol/live-messages.json) records the limit values and the subscription message cases ([Subscriptions](subscriptions.md)).

## 10. Quality Requirements

- **Encoding is byte-identical to JavaScript's: key order, number spelling and unknown-field preservation**. Evidence: [core/tests/contracts.rs](../../../../crates/core/tests/contracts.rs) `canonical_numbers_match_javascript_and_utf16_key_order`, `a_page_names_its_scopes_and_keeps_unknown_fields_out_of_the_records`, `server_pull_request_accepts_js_integer_number_spellings`, `shared_wire_fixtures_preserve_counter_boundaries`.
- **A received state tolerates extra fields and refuses missing required ones**. Evidence: `received_state_supports_additive_schema_evolution`, `state_is_complete_but_patch_preserves_absent_and_null`.

Executed 2026-09-16: `cargo test -p axton-core --locked` passed with the tests above.

## 11. Risks and Technical Debt

**Accepted limitation.** Client-direction errors cross the bindings as message text, with a machine-readable `details.code` only where the client runtime provides one; nothing branches on the wording. Owned by [SDKs / Bindings](../sdks/bindings.md).

## Authority capability and historical metadata

New requests advertise `capabilities: ["stream-authority-v1"]`. Negotiation is excluded from saved-call logical equality, so decoration never changes a frozen call's identity. Valid unsupported requests are refused before handlers or progress; malformed metadata is `request.invalid`. Live subscribe is refused before acknowledgement. The coordinated [cutover](../../../../website/docs/backend/deployment.md#stream-membership-cutover) upgrades all runtimes together.

Fresh Load pages, receipts and direct responses emit no `memberships` claims. Historical top-level claims remain decodable and replayable but supply no cache ownership or permission. Saved nested business fields named `memberships`, `scope` or `channel`, including result and continuation data, retain their exact meaning. Replay runs no Handler, Loader or tracking declarations.

Every canonical path stages authority by Model, normalized identity and stamp, without consulting client Stream holdings. Newer null is canonical absence; Loader diagnostics retain prior content. Historical identity-only Remove supplies cursor coverage and changes no Model, stamp, hook, cascade or queue. Retained legacy global recovery epochs and tokens still guard their old evicted bases; ordinary Stream delivery and application cleanup create no such fence.

The gate runs on native/server envelopes before principal validation, claims, handlers, Loaders or progress. HTTP uses 426 for unsupported capability and 400 for malformed metadata; live closes with 1002 and `protocol.unsupported` before acknowledgement. Authentication and application `admit` remain separate. Historical persisted push bytes remain unchanged: capable outgoing transport copies add negotiation only after the last typed encoding. Saved logical equality excludes negotiation alone. Production SDK/native transports and custom runtime effect executors receive negotiated bytes. `freeze`, `downlink_request`, `LoadWorker.dispatch`, `BootstrapTask.encode_request` and `LiveSession.begin` export capable carrier bytes directly. Lower-level `prepare_action`/`prepare_fetch` return typed logical requests: applications sending their bare core encoding must wrap final bytes with `with_capabilities` themselves.

Request size checks retain the original logical payload bound and allow only 39 bytes of known capability metadata above it. This permits a retained full-limit request to advertise negotiation without changing its saved identity or original frozen bytes; arbitrary capability names grant no extra allowance. New batch selection measures the final capable envelope. A single frozen Load page may use this exact metadata allowance; multi-page Load batches must still fit within the 1 MiB final wire limit.
