# Types

## 1. Introduction and Goals

A field type fixes how a value is written in the schema, normalized on every runtime, stored on the client and typed in generated code. One set of rules keeps the client, the server and the wire in agreement, and bounds numbers to what JavaScript represents exactly.

## 3. Context and Scope

- Source: a type name in a field declaration, optionally followed by `[]` for a list and `?` for nullable.
- Descriptor: `{"kind":"scalar","name":…}`, `{"kind":"enum","name":…}` or `{"kind":"list","element":…}` plus a `nullable` flag, produced by [Compiler / Generate](../compiler/generate.md).
- Consumers: the core `Schema` normalizes every value that enters a record; [Client / Storage](../client/storage/README.md) maps descriptors to column types; generated code maps them to language types.

## 5. Building Block View

| Source | Descriptor | Normalized value | TypeScript | Dart | SQLite |
| --- | --- | --- | --- | --- | --- |
| `String` | `string` | any string | `string` | `String` | `TEXT` |
| `Bool`, `Boolean` | `boolean` | boolean | `boolean` | `bool` | `INTEGER` |
| `Int` | `int` | finite, integral, within ±2^53−1 | `number` | `int` | `INTEGER` |
| `Float` | `float` | finite; `-0` becomes `0` | `number` | `double` | `REAL` |
| `UUID` | `uuid` | 36-character RFC 4122, version 1–8, lowercased | `string` | `String` | `TEXT` |
| `DateTime` | `dateTime` | RFC 3339, re-encoded as UTC with millisecond precision | `Date` | `DateTime` | `TEXT` |
| declared enum | `enum` | one of the declared names | string union | `enum` | `TEXT` |
| `T[]` | `list` | array of normalized `T` | `T[]` | `List<T>` | `TEXT` (JSON) |

Rules:

- An enum declares non-empty, unique identifier values; a value may carry `@deprecated(reason: "…")`, which reaches generated code only ([Mutations](mutations.md#9-architecture-decisions)).
- A list element must be a scalar; a list of enums or of lists is refused. A list cannot be nullable.
- `null` is accepted only for a nullable field.
- A `DateTime` keeps millisecond precision because a JavaScript `Date` cannot carry more. Normalization drops the sub-millisecond part, moving a pre-1970 instant to the earlier millisecond; generated Dart applies the same truncation before encoding ([Typed API / Client](../frontend-sdk/api.md)), and every decoded Dart `DateTime` is UTC.

Code: source names and list/nullable parsing in [compiler/parse.rs](../../../../crates/compiler/src/parse.rs); type resolution in [compiler/validate.rs](../../../../crates/compiler/src/validate.rs); descriptor validation and value normalization in [core/schema.rs](../../../../crates/core/src/schema.rs); language mapping in [compiler/emit.rs](../../../../crates/compiler/src/emit.rs); column types in [client/ddl.rs](../../../../crates/client/src/ddl.rs).

## 8. Crosscutting Concepts

The same normalization runs wherever a value enters a record: before an operation is queued ([Local operations](../client/engine/local-operations/README.md)), when the server decodes arguments and loader rows ([Server Push](../../history/pre-protocol5/architecture/server/engine/push.md), [Server Pull](../../history/pre-protocol5/architecture/server/engine/pull.md)), and when a received state is applied ([Client Pull](../../history/pre-protocol5/architecture/client/engine/pull.md)). Query ordering compares normalized values: strings by UTF-16 code units, numbers as floating point, nulls first ([Queries](../client/engine/local-operations/queries.md)).

## 10. Quality Requirements

- An integer outside the safe range is refused on every path, so no runtime can widen a value another cannot read. Evidence: [core/tests/contracts.rs](../../../../crates/core/tests/contracts.rs) `state_is_complete_but_patch_preserves_absent_and_null`; PostgreSQL `bigint` narrowing in [runtime.test.mjs](https://github.com/zanminwang/axton/blob/v0.4.2/integration/persistence/server/runtime.test.mjs) `loader safely converts PostgreSQL BigInt scalar and list values`.
- Identity values normalize identically everywhere, so one record has one key. Evidence: `identities_are_exact_normalized_and_independent_of_scopes` in the same core test file.
- Booleans and lists survive the SQLite round trip. Historical v0.4.2 evidence: [sqlite/tests/engine.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/engine.rs) `model_rows_round_trip_booleans_lists_and_copy_aside`.

## 11. Risks and Technical Debt

- **Accepted limitation:** enum columns are `TEXT` without a check constraint; only normalization rejects unknown names, so rows stored before an enum value was removed remain in the table as strings the schema cannot decode. A changed value set of an enum a stored field uses is an incompatible schema change, so compatible local open refuses it intact rather than converting such rows ([Client / Storage / Reconciliation](../client/storage/reconciliation.md)).
- Creation defaults (`@default`) are specified in [Models](models.md#5-building-block-view); they use the normalization rules above.
