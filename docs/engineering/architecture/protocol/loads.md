<!-- load-draft: verify against implementation -->
# Loads

## 1. Introduction and Goals

`POST /sync/loads` carries pages of native [Loads](../schema/loads.md). One request batches ready pages of independent Load jobs, and one response answers each page with its own outcome. The batch is transport grouping only: each item has its own identity, its own backend transaction and its own local application ([guarantees N2, N3, N5](../../guarantees.md#n-native-loads)).

## 3. Context and Scope

The request envelope is `{loads: [...]}`:

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
| `models` | The Model read contracts the page's outputs resolve through |

The response envelope is `{loads: [...]}` with exactly one item per request item, each carrying `loadId`, `callId`, an `outcome` and `records`:

| `outcome.status` | Shape | Meaning |
| --- | --- | --- |
| `succeeded` | `{status, data, next}` with `records` of authority | `data` holds the declared identity lists; `next` is `null` (the job completes when this page commits) or `{state}` |
| `failed` | `{status, error: {code, message}}` with `records: []` | A terminal rejection of this page; saved and replayed for the same call ID, except deterministic defects, which are unsaved ([Server / Engine / Loads](../server/engine/loads.md#6-runtime-view)) |
| `retryable` | `{status, error}` with `records: []` | The item's transaction rolled back or its commit is uncertain; nothing terminal was saved, and the client resends the same call ID |

A `succeeded` outcome's `data`, `next` and `records` are saved together and replayed unchanged, so the client can enforce the identity bound, attribute failures and check that identities and records correspond.

### Continuation

`Continuation` is `{state}` and rejects unknown fields. `state` is portable JSON: no undefined, functions, cycles, non-finite numbers, BigInt or class instances with custom serialization. Integers outside the JavaScript safe range and dates are encoded by the application as strings. The canonical state is at most 64 KiB and nests at most 64 levels. Both sides validate it in Rust; a violation is the item's `load.invalid_continuation`.

## 5. Building Block View

<!-- load-draft: TODO confirm name -->
Wire types (`LoadIntent`, `LoadNext`, `LoadBatchRequest`, `LoadBatchResponse`, `LoadPageResponse`) and the batch validator live in the core Load module (planned `crates/core/src/loads.rs`) and are re-exported from `axton_core`. The HTTP route is served beside the others in [server/index.mts](../../../../packages/server/index.mts); the client asks for it as the `load` route of an `http` effect ([Runtime](../client/runtime.md#3-context-and-scope)).

## 6. Runtime View

**Correlation.** Outcomes correlate by `loadId` and `callId`, never by array position. Before any item runs, the server validates the complete envelope in Rust: unique load and call IDs, item count and byte bounds. Envelope validation is structural; an unknown name or version, or invalid business arguments, is an item rejection that does not stop its siblings. Before any page is applied, the client validates the whole response the same way: a malformed envelope, or a duplicate, extra or missing correlation, rejects the envelope and applies nothing, and every page keeps its frozen identity for a later resend. Once correlation is valid, a malformed item fails only its own job.

**Bounds.** These are initial implementation defaults, not negotiated wire fields:

| Bound | Value |
| --- | --- |
| Items per batch | 8 |
| Request body | 1 MiB |
| Identity entries per page, across declared lists | 1,000 |
| Encoded page outcome | 1 MiB |
| Batch response | 8 MiB |
| Continuation state | 64 KiB, depth 64 |

<!-- load-draft: TODO confirm name -->
A bound is never met by silent truncation. An oversized successful page is saved as the item's `load.page_too_large`; other size violations are typed terminal item failures (codes to be confirmed).

**Whole-request failures.** Authentication and envelope failures fail the whole request with the transport's ordinary statuses ([Server transport](../server/connection/transport.md)); the client retries the frozen pages. Every item-level outcome, including a rejection, answers `200`.

## 10. Quality Requirements

- **Correlation is by ID, every request item has exactly one response item, and a bad envelope applies nothing.** Required behavior: [guarantees N3–N5](../../guarantees.md#n-native-loads).
- **`null`, `{state: null}` and nested states round-trip unchanged.**

Evidence: to be recorded from the core wire tests of [#173](https://github.com/zanminwang/axton/issues/173).
