<!-- load-draft: verify against implementation -->
# Loads

## 1. Introduction and Goals

A **Load** is a versioned, read-only backend operation that fills local Models in successive pages until the application backend reports completion ([#173](https://github.com/zanminwang/axton/issues/173)). It is the third native operation kind beside [Mutations and Queries](actions.md). A Query answers one request with a typed result; a Load is a durable job: the client runtime persists it, requests its pages, stores each page through Loader authority and `onStore`, and resumes it after reopen. It needs no Channel enrollment and does not replace [Bootstrap](../client/engine/pull.md#5-building-block-view).

This document owns the declaration, its output rules, versioning and names. The wire format is [Protocol / Loads](../protocol/loads.md), backend execution [Server / Engine / Loads](../server/engine/loads.md), the client job ledger and page application [Client / Engine / Loads](../client/engine/loads.md), scheduling the [Load worker](../client/connection/controller/load-worker.md), and the required behavior [guarantees N1–N7](../../guarantees.md#n-native-loads).

## 3. Context and Scope

```text
load ProjectTodos(projectId String) {
  todos Todo[]
}
```

- `load Name(inputs) { outputs }` requires braces and at least one output.
- Inputs follow the ordinary scalar, enum, nullable and list input rules of Queries. Model mutation operands and `@sequence` are refused at the member.
- Every output is a non-null list of Model identities, resolved through the retained Loader read contract of that Model. Several named Model lists are allowed, and an empty list is valid. Scalar, single-Model and nullable-list outputs are refused in this version.
- A Load is read-only in business terms, like a Query: its Handler context has no `touch` or `channel` ([enforcement](actions.md#8-crosscutting-concepts)). Framework claim, stamp and replay metadata are the permitted writes.

The Handler receives `{ctx, args, continuation}` and returns `{data, next}`. `continuation` is `null` on the first request and afterwards the previous non-null `next`; `next: null` ends the job, while `{state: null}` is a legitimate continuation. State is bounded portable JSON owned by the application ([Protocol / Loads](../protocol/loads.md#continuation)). The typed signatures are in [Typed API / Server](../sdks/typed-api/server.md).

Once reuse, refresh and invalidation are call-site options of the generated client ([Typed API / Client](../sdks/typed-api/client.md)). They never appear in the declaration, the descriptor, the operation history, the wire or the Handler arguments, and changing them needs no version bump.

## 5. Building Block View

A Load compiles to a `LoadDescriptor` in its own `Schema.loads` collection (empty by default), separate from the `actions` collection, so a Load never reaches Mutation or Query routing. Each descriptor carries `name`, `version`, value `inputs`, the `input` snapshot with retained enum snapshots, `outputs` (each with kind `model`, cardinality `list`, source `handlerIdentity` and its `modelReadVersion`) and `outputEnums`. Retained versions are recorded in `history/loads.json`, which mirrors the `history/actions.json` envelope under the key `loads`; the CLI flags are `--load-history` and `--initialize-load-history` ([Generate](../compiler/generate.md)). Normalization and history comparison reuse the operation helpers.

<!-- load-draft: TODO confirm name -->
Code: the parser and validator in [compiler/parse.rs](../../../../crates/compiler/src/parse.rs) and [compiler/validate.rs](../../../../crates/compiler/src/validate.rs); history in [compiler/history.rs](../../../../crates/compiler/src/history.rs); descriptors and portable state in the core Load module (planned `crates/core/src/loads.rs`).

## 8. Crosscutting Concepts

**Names.** Mutations, Queries and Loads share one lower-camel name namespace, so a name is declared once across the three kinds. `get`, `list` and `invalidate` are reserved Load names, because they are management members of `client.loads`. The generated names `Loads`, `Load`, `LoadStatus`, `LoadPhase`, `LoadOptions`, `LoadError`, `LoadNext`, `JsonValue` and `LoadContext` exist, and are reserved, only in schemas that declare a Load; a schema without Loads generates exactly what it did before.

**Versions.** `@version(n)` defaults to 1, and a breaking input or output change requires a new version. The backend must keep a Handler, and with it the continuation interpreter, for every retained version. An incompatible change to the opaque state format also requires a new version, although the compiler cannot detect a change made only in the Handler. The client schema keeps every retained Load version, so a version bump does not fail jobs already running at the older version; a job fails with `load.contract_unavailable` only when its frozen version is no longer retained.

**No cross-kind reuse.** History reconciliation refuses to drop a retained name, so a name retained as a Mutation or Query can never become a Load, and a retained Load name can never become a Mutation or Query. Migrating between kinds is outside this feature. The Mutation–Query kind change of [Mutations and Queries](actions.md#3-context-and-scope) is unchanged.

**Glossary.** Existing Bootstrap identifiers and documents that say "load" describe a Scope's historical interval and are not renamed here ([#152](https://github.com/zanminwang/axton/issues/152)); new code and documents use `loads` and `LoadJob` for native Loads. A **Loader** remains the per-Model read function a Load resolves its identities through.

## 9. Architecture Decisions

**A native operation, not a Query loop.** Paging a Query from application code would lose progress on exit, need a JavaScript or Dart loop per SDK, and couple completion to one process. A Load is persisted and scheduled by Rust, applies each page atomically with its progress, and replays pages by call ID ([guarantees N1–N3](../../guarantees.md#n-native-loads)).

**Identity-only outputs.** Outputs are identities because a Load's purpose is to store Models through their Loaders and stamps; it exposes no aggregate business result, public cursor, delivery override or `store: false`.

**Continuation, not a cursor.** The continuation is arbitrary application state, not a Channel cursor. The framework assumes no monotonicity, ordering or inequality between consecutive states; the Handler owns traversal, consistency and termination.

## 10. Quality Requirements

- **A Load declaration, its outputs and its versions are validated before any output is written; Loads never become Mutation or Query descriptors and share their name namespace.** Required behavior: [guarantees N1–N7](../../guarantees.md#n-native-loads).

Evidence: to be recorded from the compiler, core and history tests of [#173](https://github.com/zanminwang/axton/issues/173).

## 11. Risks and Technical Debt

**Accepted limitation.** A continuation format change made only in the Handler is not detected by the compiler; the application must bump the version.

**Not included.** Scalar or single-Model outputs, demand-driven paging, snapshot creation, automatic query membership maintenance and deletion or eviction of loaded Models.
