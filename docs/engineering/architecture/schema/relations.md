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

- **Cascade (client).** Deleting a record deletes every record reachable through relations with `onTargetDelete: delete`, in both the visible and before-image tables, once per record even with cycles. Cascaded deletes are recorded as local effects of the mutation; they are never sent ([Writes](../client/engine/local-operations/writes.md), [Client Pull](../../history/pre-protocol5/architecture/client/engine/pull.md)).
- **Dependencies.** A queued create of a record another operation references becomes a lifecycle dependency ([Dependencies](../client/engine/push/dependencies.md)).
- **Navigation.** `related` follows a reference (null when a reference field is null); `referencing` filters the referencing model by the reference fields ([Queries](../client/engine/local-operations/queries.md)).

Code: resolution in [compiler/validate.rs](../../../../crates/compiler/src/validate.rs); descriptor checks in [core/schema.rs](../../../../crates/core/src/schema.rs); cascade in [client/mutate.rs](../../../../crates/client/src/mutate.rs) (`descendants`).

## 10. Quality Requirements

The [compiler tests](../../../../crates/compiler/tests/compiler.rs) cover resolved relation metadata and unique inverse keys. Current [cascade tests](../../../../crates/sqlite/tests/protocol05_cascade.rs) and [companion cascade tests](../../../../crates/sqlite/tests/protocol05_companion_cascade.rs) cover local child effects and replay. [Delivery-plan tests](../../../../crates/server/tests/delivery_plan.rs) cover finite authority dependency grouping. These are maintained source references, not new execution evidence.

## 11. Risks and Technical Debt

`onTargetDelete` supplies device-side effects; it does not execute server business deletes. A handler must delete and invalidate each server-side child itself, and local cascaded deletes are never sent. Core/server still use retained relation metadata to group cascade dependencies into complete finite authority units ([Protocol 5](../protocols/sync.md)). This grouping preserves apply atomicity; it is not an application cascade implementation.
