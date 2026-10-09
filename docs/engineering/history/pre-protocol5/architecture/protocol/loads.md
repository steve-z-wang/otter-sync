# Loads

Historical carrier reference. Current behavior is owned by [protocol 5](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/architecture/protocol/0.5.md). This page does not promise support for old local files or an alternate current API.

This is the retained protocol-3 Load carrier. It is not a public 0.4 request path; see [protocol 4](0.4.md) for finite Bootstrap manifests and ordinary reads.

## 1. Introduction and Goals

`POST /sync/loads` carries pages of native [Loads](../schema/loads.md) ([#173](https://github.com/zanminwang/axton/issues/173)). One request batches ready pages of independent Load jobs, and one response answers each page with its own outcome. The batch is transport grouping only: each item has its own identity, its own backend transaction and its own local application ([guarantees N2, N3, N5](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/guarantees.md#n-native-loads)).

## 3. Context and Scope

The request envelope is exactly `{loads: [...]}` with 1 to 8 items:

```json
{
  "loads": [{
    "loadId": "<uuid>", "callId": "<uuid>",
    "name": "ProjectTodos", "version": 1,
    "args": {"projectId": "p1"},
    "continuation": null, "models": {"Todo": 1}
  }]
}
```

| Field | Meaning |
| --- | --- |
| `loadId` | The job's UUID, fixed for its lifetime |
| `callId` | The page's UUID, persisted with the frozen request before the first send; replaced only by an explicit retry |
| `name`, `version` | The retained Load contract; the request carries no kind |
| `args` | Canonical normalized business arguments, fixed for the job |
| `continuation` | `null` for the first page, otherwise the previous page's non-null `next` wrapper `{state}` |
| `models` | A nonempty map of the Model read contracts the client stores the page's authority at |

No other member is accepted, so call-site options such as `once` can never reach the wire. The response envelope is `{loads: [...]}` with exactly one item per request item, each carrying `loadId`, `callId`, an `outcome` and `records`:

| `outcome.status` | Shape | Meaning |
| --- | --- | --- |
| `succeeded` | `{status, data, next}` with `records` of authority | `data` holds exactly the declared identity lists; `next` is required: `null` (the job completes when this page commits) or `{state}` |
| `failed` | `{status, error: {code, message}}` with `records: []` | A terminal rejection of this page. Saved and replayed for the same call ID, except for deterministic defects and identity conflicts, which are unsaved ([Server / Engine / Loads](../server/engine/loads.md#6-runtime-view)) |
| `retryable` | `{status, error: {code, message}}` with `records: []` | The item's transaction rolled back or its commit is uncertain; nothing terminal was saved, and the client resends the same call ID |

An error message is at most 1,024 bytes. A `succeeded` page's `data`, `next` and `records` are saved together and replayed unchanged. Each distinct `(model, identity)` of `data` has exactly one record with state, and there is no other record: no error record, no deletion and nothing unrequested. Identity lists keep order and duplicates.

### Continuation

`Continuation` is exactly `{state}` and rejects unknown members; `{state: null}` is a continuation, distinct from `null`. `state` is portable JSON: no undefined, functions, cycles, non-finite numbers, BigInt or class instances with custom serialization. Every integral number must be within ±(2^53−1), including integral doubles such as `1e300`, and normalizes to an integer (`1.0` is `1`). Dates are application-encoded strings. The canonical state is at most 64 KiB and nests at most 64 levels, counting arrays and objects. Both sides validate it in Rust; a violation is the item's `load.invalid_continuation`.

## 5. Building Block View

The wire types (`Continuation`, `LoadNext`, `LoadIntent`, `LoadBatchRequest`, `LoadOutcome`, `LoadError`, `LoadPageResponse`, `LoadBatchResponse`, `LoadPageReply`, `LoadItemError`) and their validators are in [core/loads.rs](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/core/src/loads.rs), re-exported from `axton_core`; the bounds are the `LOAD_*` constants of `limits` in [core/protocol.rs](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/core/src/protocol.rs). The server's envelope validator and response encoder are in [server/loads.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/server/src/loads.rs); the HTTP route is in [server/index.mts](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/packages/server/index.mts). The client asks for the route as the `load` route of an `http` effect ([Runtime](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/architecture/client/runtime.md)).

## 6. Runtime View

**Correlation.** Outcomes correlate by `loadId` and `callId`, never by array position. Load IDs and call IDs are canonical lowercase UUIDs, each unique within the envelope.

**What the envelope refuses.** A request is refused whole (`request.invalid`, HTTP 400) for its byte bound, a shape other than `{loads: [object…]}`, an item count outside 1 to 8, an invalid or duplicate ID, a blank name, a non-positive version, an empty `models` map or an unknown member. An unknown name or version, invalid business arguments, an invalid continuation and undeclared read contracts are item rejections, so siblings still run. A response is refused whole only for its byte bound, its shape, its item count, an item without UUID IDs, an object `outcome` and an array `records`, or a duplicate, unrequested, missing or extra correlation. The client then applies nothing, and every page keeps its frozen identity for a resend. Everything else about an item - an unknown status, a missing `next`, an invalid error, a malformed record, records on an unsuccessful page, content that does not match the request - fails only that item, which the client records as `load.protocol_invalid`, `load.page_too_large` or `load.invalid_continuation` by kind.

**Bounds.** These are implementation defaults in `limits`, not negotiated wire fields:

| Bound | Value |
| --- | --- |
| Items per batch | 8 |
| Request body | 1 MiB |
| Identity entries per page, across declared lists, duplicates included | 1,000 |
| Canonical page (IDs, outcome and records) | 1 MiB |
| Batch response | 8 MiB of pages plus a 64 KiB envelope allowance (8,454,144 bytes) |
| Continuation state | 64 KiB, depth 64 |
| Error message | 1,024 bytes |

A bound is never met by silent truncation. A page with more than 1,000 identities or over 1 MiB is the item's `load.page_too_large`. A frozen request that exceeds 1 MiB even as a batch of one is never sent: the client fails that job with `load.request_too_large`. Because every page is bounded, eight full pages always fit one response; the envelope bound is a guard.

**Whole-request failures.** Authentication and envelope failures fail the whole request with the transport's ordinary statuses ([Server transport](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/architecture/server/connection/transport.md)); the client keeps the frozen pages. Every item outcome, a rejection included, answers `200`.

## 10. Quality Requirements

- **`null`, `{state: null}` and nested states round-trip unchanged; state bounds are exact.** Evidence: [core/tests/loads.rs](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/core/tests/loads.rs) `first_and_end_null_stays_distinct_from_a_null_state`, `nested_states_round_trip_as_normalized_portable_json`, `continuation_state_is_bounded_portable_json`.
- **Envelopes are structural and bounded, correlation is by ID, and a malformed page fails only its own item.** Evidence: `request_envelopes_are_structural_and_bounded`, `only_correlation_structure_rejects_a_response_envelope`, `a_malformed_page_shape_fails_only_its_own_item`, `pages_carry_declared_identity_lists_and_matching_authority`, `eight_maximal_pages_fit_one_response`, `server_encoding_bounds_item_errors_instead_of_refusing_the_batch`, `loads_never_route_as_actions`.

Verified 2026-09-27 by the host gate (`bash scripts/test.sh`, which runs `cargo test --workspace --locked`); the server side of the envelope is in [Server / Engine / Loads](../server/engine/loads.md#10-quality-requirements).

Fresh responses carry canonical Model authority without enrollment claims or client holdings. Saved historical top-level metadata remains replayable; nested business result and continuation JSON is unchanged. All delivery paths share Model/identity/stamp ordering ([Common](common.md#authority-capability-and-historical-metadata)).
