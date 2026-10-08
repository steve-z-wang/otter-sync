# Relations

## 1. Introduction and Goals

A relation lets a model point at another by identity, exposes the reverse navigation, and declares whether deleting the target also deletes the referencing rows on the client.

## 3. Context and Scope

- Source: a field whose type is a model. With `@reference(via: [localFields], onTargetDelete: delete|none)` it is a reference; without it, it is the inverse of a reference and may name that reference with `@inverse(RelationName)`.
- Descriptor: `relations: [{name, target, fields, targetFields, onDelete}]` on the referencing model. Inverses are emitted only for [code generation](../compiler/generate.md) and are not part of the runtime schema.
- Consumers: cascades and dependency derivation on the client; generated navigation accessors; mutation slot bindings ([Mutations](mutations.md)).

## 5. Building Block View

Rules the compiler enforces:

- A reference is singular. `via` lists as many local fields as the target has identity fields, each of the matching type and not a list. `onTargetDelete` defaults to `none`.
- An inverse must resolve to exactly one reference from the target model back to this one; when several exist, `@inverse(name)` picks the reference declared with that positional name. A singular inverse (`Child?`) requires the reference fields to be the referencing model's identity or one of its `@@unique` sets.

Behavior the runtimes give a relation:

- **Cascade (client).** Deleting a record deletes every record reachable through relations with `onTargetDelete: delete`, in both the visible and before-image tables, once per record even with cycles. Cascaded deletes are recorded as local effects of the mutation; they are never sent ([Writes](../client/engine/local-operations/writes.md), [Client Pull](../client/engine/pull.md)).
- **Dependencies.** A queued create of a record another operation references becomes a lifecycle dependency ([Dependencies](../client/engine/push/dependencies.md)).
- **Navigation.** `related` follows a reference (null when a reference field is null); `referencing` filters the referencing model by the reference fields ([Queries](../client/engine/local-operations/queries.md)).

Code: resolution in [compiler/validate.rs](../../../../crates/compiler/src/validate.rs); descriptor checks in [core/schema.rs](../../../../crates/core/src/schema.rs); cascade in [client/mutate.rs](../../../../crates/client/src/mutate.rs) (`descendants`).

## 10. Quality Requirements

- Relation metadata compiles and a singular inverse without a unique key is refused. Evidence: [compiler/tests/compiler.rs](../../../../crates/compiler/tests/compiler.rs) `relationships_bindings_and_dependency_metadata`, `singular_inverse_requires_a_unique_foreign_key`.
- Deleting a record deletes its declared local children, in direct writes, queued mutations and authoritative deletes, and cycles terminate (guarantee L5). Evidence: [sqlite/tests/client.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/client.rs) `schema_cascade_is_optimistic_same_fate_and_not_extra_wire_operations`, `direct_cascade_handles_cyclic_relationships_once`; [sqlite/tests/downlink.rs](../../../../crates/sqlite/tests/downlink.rs) `delete_cascades_to_descendants_and_their_claims`.

## 11. Risks and Technical Debt

- **Accepted limitation:** `onTargetDelete` is a client-side rule. The server runtime has no relation handling; a handler must delete children itself, and the client's cascaded deletes never reach it. Evidence: [server/lib.rs](../../../../crates/server/src/lib.rs) never reads `relations`. Stated for authors in the [schema reference](../../../../website/docs/schema/reference.md#relations) and [What your backend owns](../../../../website/docs/backend/api.md#what-your-backend-owns).
