# Mutation inputs and local policies

## 1. Introduction and Goals

Applications declare named backend operations with `mutation Name(...)` or `query Name(...)`; [Mutations and Queries](actions.md) owns that syntax and generated interface. Each retained Mutation version fixes the accepted input and the local Model operations derived from it. An offline Call preserves its version and normalized input across reopen and retry.

The parser also accepts the low-level `mutation Name { slots }` declaration used by policy descriptors and compiler fixtures. It is not the protocol-5 wire envelope or an anonymous write API.

## 3. Context and Scope

A Model input names a Model and `create`, `update` or `delete`, with single, optional or list cardinality. An update may restrict its patch fields. A create binding ties a child's relation to another input's identity; mismatches are refused before business execution. Prerequisites gate local dispatch and are not backend arguments.

The compiler retains input Models/enums and versioned policy snapshots. Sequence paths derive dependencies against earlier queued Calls that reach matching records. See [dependencies](../client/engine/push/dependencies.md) and [prerequisites](prerequisites.md).

## 5. Building Block View

- [parse.rs](../../../../crates/compiler/src/parse.rs) and [validate.rs](../../../../crates/compiler/src/validate.rs) own source syntax, input restrictions, binding and sequence checks.
- [history.rs](../../../../crates/compiler/src/history.rs) retains published operation contracts and checks compatible changes.
- [policies.rs](../../../../crates/client/src/policies.rs) derives local lifecycle and sequence dependencies.
- [protocol_v05.rs](../../../../crates/core/src/protocol_v05.rs) owns frozen named intent with explicit `inputPath`, operation/value entries and the immutable Batch digest.
- [server protocol_v05.rs](../../../../crates/server/src/protocol_v05.rs) selects the trusted name/version, reconstructs input and normalizes it against the retained schema before execution.

The caller's descriptor fingerprint belongs to immutable intent; it does not replace the backend's retained contract. Compatible widening or field reordering need not reject a frozen Call. Business handlers and viewer Loaders remain the permission authority.

## 6. Runtime View

The local transaction saves named input, optimism and companions together. Default values for fresh creates are evaluated once before that input freezes; retries do not regenerate them. Direct Model writes and companions are device-only. The backend executes each Batch member independently and persists its immutable outcome. An accepted Call settles locally only after its required authority/evidence commits. [Protocol 5](../protocol/0.5.md) owns execution, retry and settlement boundaries.

An empty allowed update patch remains a valid no-op input. Its handler still runs; declared publication and Loader readback follow the normal rules. No identity stamp is allocated. A returned Model identity alone never tracks a Stream.

## 9. Architecture Decisions

Incompatible input changes require a new operation version. Compatible widening may keep a version; existing fields retain their meaning and required create inputs cannot silently grow. Retain old contracts and their handlers while they are supported. Version decrease or disappearance of a retained declaration is refused by the compiler.

Member `@deprecated` annotations affect generated notices only. They preserve schema, descriptors, history and execution. Retaining operation/Model versions within format 5 is separate from adopting an older framework format: protocol 5 provides no old-file conversion or compatibility bridge.

## 10. Quality Requirements

[Compiler cases](../../../../crates/compiler/tests/compiler.rs) cover input shapes, bindings and sequences; [history tests](../../../../crates/compiler/tests/history.rs) cover compatible changes and retained contracts. [Current server carrier tests](../../../../crates/server/tests/protocol_v05.rs) cover trusted input reconstruction; [SQLite queue tests](../../../../crates/sqlite/tests/protocol05_queue.rs) cover durable named work. These are source coverage references, not newly executed gates.
