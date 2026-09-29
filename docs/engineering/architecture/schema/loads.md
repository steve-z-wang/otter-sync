# Loads

## 1. Introduction and Goals

A **Load** is a versioned, read-only backend operation that fills local Models in successive pages until the application backend reports completion ([#173](https://github.com/zanminwang/axton/issues/173)). It is the third native operation kind beside [Mutations and Queries](actions.md). A Query answers one request with a typed result; a Load is a durable job: the client runtime persists it, requests its pages, stores each page through Loader authority and `onStore`, and resumes it after reopen. It needs no Channel enrollment, though its Handler may add the records a page returns to Channels, and it does not replace [Bootstrap](../client/engine/pull.md#5-building-block-view).

This document owns the declaration, its output rules, versioning and names. The wire format is [Protocol / Loads](../protocol/loads.md), backend execution [Server / Engine / Loads](../server/engine/loads.md), the client job ledger and page application [Client / Engine / Loads](../client/engine/loads.md), scheduling the [Load worker](../client/connection/controller/load-worker.md), and the required behavior [guarantees N1–N8](../../guarantees.md#n-native-loads).

## 3. Context and Scope

```text
load ProjectTodos(projectId String) {
  todos Todo[]
}
```

- `load Name(inputs) { outputs }` requires braces and at least one output.
- Inputs follow the ordinary scalar, enum, nullable and list input rules of Queries. Model operands, Model-typed inputs and `@sequence` are refused at the member, as are output directives.
- Every output is a non-null list of Model identities, resolved through the retained Loader read contract of that Model. Several named Model lists are allowed, and an empty list is valid. Scalar, single-Model and nullable-list outputs are refused in this version.
- A Load is read-only in business terms, like a Query: its Handler context has no `touch`, and its `channel(name)` only adds records the page returns ([Server / Engine / Loads](../server/engine/loads.md#6-runtime-view)). Framework claim, stamp, replay and Channel membership metadata are the permitted writes. No declaration syntax is involved: enrollment is Handler code, so it needs no Load or Model version.

The Handler receives `{ctx, args, continuation}` and returns `{data, next}`. `continuation` is `null` on the first request and afterwards the previous non-null `next`; `next: null` ends the job, while `{state: null}` is a legitimate continuation. State is bounded portable JSON owned by the application ([Protocol / Loads](../protocol/loads.md#continuation)). The typed signatures are in [Typed API / Server](../sdks/typed-api/server.md).

Once reuse, refresh and invalidation are call-site options of the generated client ([Typed API / Client](../sdks/typed-api/client.md)). They never appear in the declaration, the descriptor, the operation history, the wire or the Handler arguments, and changing them needs no version bump.

## 5. Building Block View

A Load compiles to a `LoadDescriptor` in its own `Schema.loads` collection (empty by default), separate from the `actions` collection, so a Load never reaches Mutation or Query routing. Each descriptor carries `name`, `version`, value `inputs`, the `input` snapshot (retained enum snapshots, never Models), `outputs` (each with kind `model`, cardinality `list`, source `handlerIdentity`, its `modelReadVersion` and an identity `handlerType`) and `outputEnums`, and nothing else: core refuses any other member, such as `kind` or `sequence`, so an Action descriptor can never be read as a Load, and `Schema::action` never finds a Load. Retained versions are recorded in `history/loads.json`, which mirrors the `history/actions.json` envelope under the key `loads`; the CLI flags are `--load-history` and `--initialize-load-history` ([Generate](../compiler/generate.md)). Normalization and history comparison reuse the operation helpers.

Code: the grammar in [compiler/parse.rs](../../../../crates/compiler/src/parse.rs), `validate_load` in [compiler/validate.rs](../../../../crates/compiler/src/validate.rs), reserved names in [compiler/action_names.rs](../../../../crates/compiler/src/action_names.rs), `reconcile_load_history` in [compiler/history.rs](../../../../crates/compiler/src/history.rs), the backend handler types in [compiler/emit.rs](../../../../crates/compiler/src/emit.rs); `LoadDescriptor`, `validate_loads` and portable state in [core/loads.rs](../../../../crates/core/src/loads.rs).

## 8. Crosscutting Concepts

**Names.** Mutations, Queries and Loads share one lower-camel name namespace, so a name is declared once across the three kinds. `get`, `list` and `invalidate`, in any letter case, are reserved Load names, because they are management members of `client.loads`, and the route members `call` and `enqueue` stay reserved too. The generated names `Loads`, `Load`, `LoadStatus`, `LoadPhase`, `LoadOptions`, `LoadError`, `LoadException`, `LoadNext`, `JsonValue`, `LoadContext`, `LoadChannel` and `LoadHandlerCall`, and each Load's `{Name}Input` and `{Name}HandlerOutput` (`{Name}V<n>…` for a retained version), are reserved only in schemas that declare a Load; a schema without Loads generates exactly the bytes it did before.

**Versions.** `@version(n)` defaults to 1, and a breaking input or output change requires a new version. The backend must keep a Handler, and with it the continuation interpreter, for every retained version. An incompatible change to the opaque state format also requires a new version, although the compiler cannot detect a change made only in the Handler. The client schema keeps every retained Load version, so a version bump does not fail jobs already running at the older version; a job fails with `load.contract_unavailable` only when its frozen version is no longer retained or its frozen output Model read contracts no longer match. One Load may not read a Model at two read versions across its outputs.

**No cross-kind reuse.** History reconciliation refuses to drop a retained name, so a name retained as a Mutation or Query can never become a Load, and a retained Load name can never become a Mutation or Query. Migrating between kinds is outside this feature. The Mutation–Query kind change of [Mutations and Queries](actions.md#3-context-and-scope) is unchanged.

**Glossary.** Existing Bootstrap identifiers and documents that say "load" describe a Scope's historical interval and are not renamed here ([#152](https://github.com/zanminwang/axton/issues/152)); new code and documents use `loads` and `LoadJob` for native Loads. A **Loader** remains the per-Model read function a Load resolves its identities through.

## 9. Architecture Decisions

**A native operation, not a Query loop.** Paging a Query from application code would lose progress on exit, need a JavaScript or Dart loop per SDK, and couple completion to one process. A Load is persisted and scheduled by Rust, applies each page atomically with its progress, and replays pages by call ID ([guarantees N1–N3](../../guarantees.md#n-native-loads)).

**Identity-only outputs.** Outputs are identities because a Load's purpose is to store Models through their Loaders and stamps; it exposes no aggregate business result, public cursor, delivery override or `store: false`.

**Continuation, not a cursor.** The continuation is arbitrary application state, not a Channel cursor. The framework assumes no monotonicity, ordering or inequality between consecutive states; the Handler owns traversal, consistency and termination.

## 10. Quality Requirements

- **The declaration, its outputs and its names are validated at the member, and Loads share the operation namespace.** Evidence: [compiler/tests/loads.rs](../../../../crates/compiler/tests/loads.rs); hand-written descriptors in [core/tests/loads.rs](../../../../crates/core/tests/loads.rs) `malformed_load_descriptors_are_refused`, `loads_never_route_as_actions`, `schemas_without_loads_serialize_unchanged`.
- **Versions are retained, a retained Load cannot be removed or change kind, and other histories stay byte-identical.** Evidence: [compiler/tests/history.rs](../../../../crates/compiler/tests/history.rs) `same_version_breaking_load_changes_need_a_new_version`, `load_version_two_retains_version_one_and_its_model_reader`, `retained_loads_cannot_be_removed_or_change_kind`; [compiler/tests/cli.rs](../../../../crates/compiler/tests/cli.rs) `cli_retains_load_history_without_rewriting_other_histories`.

Verified 2026-09-27 by the host gate (`bash scripts/test.sh`, which runs `cargo test --workspace --locked`); generated outputs for schemas without Loads were compared byte for byte with the pre-Load compiler the same day.

## 11. Risks and Technical Debt

**Accepted limitation.** A continuation format change made only in the Handler is not detected by the compiler; the application must bump the version.

**Not included.** Scalar or single-Model outputs, demand-driven paging, snapshot creation, automatic query membership maintenance and deletion or eviction of loaded Models.
