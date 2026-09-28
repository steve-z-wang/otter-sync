# Slot mutations

## 1. Introduction and Goals

This page covers the low-level `mutation Name { slots }` block, which carries the retained queue and replay machinery and remains for fixtures and descriptors. Applications declare backend operations as `mutation Name(...)` or `query Name(...)` ([Mutations and Queries](actions.md)); the parser tells the two apart by the token after the name.

A slot mutation is a named, server-visible write. The schema fixes the shape of its operations so both runtimes decode it identically, and gives it a version so that mutations queued under an older schema keep executing after the schema moves on. Without the version, a client that was offline during a deploy would push operations the server can no longer interpret.

## 3. Context and Scope

A mutation is declared as a set of *slots*, each binding a name to one operation on one model:

```
mutation AddComment {
  book    Book.create
  comment Comment.create(book: book)[]     // list slot, bound to the parent slot
  @@version(2)
  @@sequence(after: [Rename(book: comment.book)])
}
```

The compiled descriptor `{name, version, slots, sequence}` goes to the client as `schema.clientPolicies` and to the server as every retained version with an input snapshot ([Compiler / Generate](../compiler/generate.md)). On the wire an instance is `{name, version, operations:[{model, op, identity, values?}]}` ([Protocol / Push](../protocol/push.md)). Generated builders produce that shape; the server's decoder consumes it; the client's dependency derivation reads the policies.

## 5. Building Block View

**Slots.** `op` is `create`, `update` or `delete`. An update may list the fields it is allowed to patch, `Entry.update<title, note>`; without a list it may patch every non-identity field. A slot is single by default, optional with `?`, or a list with `[]`.

**Bindings.** `(relation: parentSlot)` ties a create slot to a single parent slot of the relation's target model. The server checks that each child's foreign key equals the parent's identity, so a client cannot attach a child to a parent it did not create in the same mutation.

**Version and sequence.** `@@version(n)` defaults to 1. `@@sequence(after: [Target(targetSlot.path: sourceSlot.path)])` declares that an instance waits for earlier queued instances of `Target` whose side reaches the same record as its own. Each side is a slot followed by zero or more relation names, and a list slot on either side contributes each of its elements; `Target()` with no arguments waits for every earlier instance. For example, `AddCoAuthors(members.space: request.space)` waits only for earlier `AddCoAuthors` that add a member to the request's Space. Every path segment must be a reference, not a stored field or an inverse, and both sides must reach the same Model; otherwise the compiler refuses the directive. The `@sequence` of a [Mutation](actions.md) uses the same arguments. How a sequence becomes a dependency is described in [Dependencies](../client/engine/push/dependencies.md).

**Decoding on the server.** Operations are matched to slots in order by `(model, op)`; a list slot consumes every consecutive match. A later slot with the same `(model, op)` as an earlier non-single slot, with no single slot fixing a position between them, is refused by the compiler as `ambiguous slot`, so the walk is deterministic for every emitted schema. Failures have stable codes: an unknown mutation, a wrong shape or a missing required create field is `mutation.invalid`; an empty patch decodes to `patch: {}` and the update is a no-op that still stamps, reads back and may publish its record; a known field outside the allowed patch fields is `<name>.not_allowed`; a binding mismatch is `<name>.invalid` ([Server Push](../server/engine/push.md)).

**History.** Each version's input (the models and enums its slots touch, its requirements and its sequence) is snapshotted. A version may not decrease and a retained mutation may not disappear. Changing the input at the same version is refused unless the change is compatible: patch fields and enum values may grow, existing fields must be identical, and a model a `create` slot targets may not gain a non-nullable field, because old clients would send creates without it ([Compiler / Validate](../compiler/validate.md)).

Code: parsing in [compiler/parse.rs](../../../../crates/compiler/src/parse.rs) and checks in [compiler/validate.rs](../../../../crates/compiler/src/validate.rs); history in [compiler/history.rs](../../../../crates/compiler/src/history.rs); server decoding in [server/lib.rs](../../../../crates/server/src/lib.rs) (`decode`); client policies in [client/policies.rs](../../../../crates/client/src/policies.rs).

## 6. Runtime View

An incompatible change to a mutation's input requires `@@version(n+1)`; compatible changes keep the same version. The compiler keeps the previous snapshot, the server keeps a `nameVn` handler for it, and the client keeps its policy, so instances queued before the upgrade still decode. A batch that names a known mutation with an unregistered version rejects only that mutation with `mutation_version_unsupported`, without calling its handler; unrelated mutations in the same batch still commit ([P6/P7 / Server Push](../server/engine/push.md#9-architecture-decisions), [#95](https://github.com/zanminwang/axton/issues/95)).

## 9. Architecture Decisions

**Version lifecycle — agreed target ([#91](https://github.com/zanminwang/axton/issues/91)).** The compiler checks compatibility; an added input is not automatically compatible (for example, a new required field breaks old requests). Keep old contracts and handlers while their versions are supported. Versioned registration is defined in [Typed API / Server](../sdks/typed-api/server.md#9-architecture-decisions); history storage is owned by [Generate](../compiler/generate.md#9-architecture-decisions).

**Deprecation is member-level, GraphQL style (decided 2026-09-15, superseding the version-level wording in [#91](https://github.com/zanminwang/axton/issues/91)).** `@deprecated` or `@deprecated(reason: "…")` marks a model field, an enum value or a mutation slot. It is a generated-code notice only: TypeScript members get `/** @deprecated reason */`, Dart members get `@Deprecated('reason')`. The member stays in the schema, the descriptors, the history and every runtime path; nothing is removed, re-routed or retired, and the compatibility rules of this section and of [Models](models.md#5-building-block-view) are unchanged. Ending support for a version remains a separate, explicit decision.

**An empty update patch is a no-op, not a refusal (decided 2026-09-15, [#49](https://github.com/zanminwang/axton/issues/49)).** `Model.update<>` is a valid declaration: its allowed list is empty, the generated input has no settable field, and the server decodes the operation to `{identity, patch: {}}`. The record remains a target of the mutation: the handler runs, a stamp is allocated, the loader reads it back into the receipt, and a publication distributes it. No layer special-cases the empty patch. Evidence: [server/tests/runtime.rs](../../../../crates/server/tests/runtime.rs) `empty_patch_decodes_as_a_no_op_update`, `a_patch_of_only_unknown_fields_decodes_as_a_no_op_update`; [compiler/tests/compiler.rs](../../../../crates/compiler/tests/compiler.rs) `relationships_bindings_and_dependency_metadata`.

**Ambiguous adjacent slots are refused at compile time (decided 2026-09-15, [#54](https://github.com/zanminwang/axton/issues/54)).** Operations carry no slot name, so two slots of one `(model, op)` are only distinguishable when a single slot fixes a position between them or the earlier one is itself single. The compiler reports the later slot; the wire format and the decoders in [server/lib.rs](../../../../crates/server/src/lib.rs) and [client/policies.rs](../../../../crates/client/src/policies.rs) are unchanged, and retained history versions keep decoding as they always did. Evidence: [compiler/tests/compiler.rs](../../../../crates/compiler/tests/compiler.rs) `structural_refusals_a_schema_author_is_likely_to_hit`, `semantic_errors_report_the_offending_declaration`.

## 10. Quality Requirements

- **Slot shapes, bindings and sequences that do not resolve are refused at compile time.** Evidence: [compiler/tests/compiler.rs](../../../../crates/compiler/tests/compiler.rs) `schema_and_mutations`, `relationships_bindings_and_dependency_metadata`, `rejects_dependency_typos`, `sequence_prior_paths_that_do_not_resolve_are_refused_at_the_directive`.
- **A sequence path on either side is emitted as written, and one without relations keeps its existing descriptor.** Evidence: [compiler/tests/compiler.rs](../../../../crates/compiler/tests/compiler.rs) `sequence_prior_side_may_be_a_relation_path_into_a_prior_slot`; [compiler/tests/cli.rs](../../../../crates/compiler/tests/cli.rs) `cli_sequence_paths_fixture_is_current`.
- **An incompatible input change at the same version is refused, a compatible one accepted, and old inputs are retained.** Evidence: [compiler/tests/history.rs](../../../../crates/compiler/tests/history.rs); [compiler/tests/cli.rs](../../../../crates/compiler/tests/cli.rs) `cli_retains_history_and_does_not_overwrite_on_break`.
- **The server decodes known fields, ignores unknown ones, and refuses disallowed patches and binding mismatches with stable codes.** Evidence: [server/tests/runtime.rs](../../../../crates/server/tests/runtime.rs).

Tests read, not executed, except the two sequence-path tests: `cargo test -p axton-compiler --locked` (2026-09-28).

## 11. Risks and Technical Debt

None open. The greedy-decoding risk and the empty-update defect recorded here were resolved by the decisions in section 9 ([#54](https://github.com/zanminwang/axton/issues/54), [#49](https://github.com/zanminwang/axton/issues/49)).
