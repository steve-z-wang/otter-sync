# Schema contract tests

Verify identities, state/patch semantics and operation descriptors owned by [Schema](../../architecture/schema/README.md).

[contracts.rs](../../../../crates/core/tests/contracts.rs) asserts exact normalized identities independent of Streams, complete state versus absent/null patch values, scalar and enum normalization, Model operand identity shapes, and validation of retained operation metadata. For example, `state_is_complete_but_patch_preserves_absent_and_null` accepts an empty patch but refuses a missing required state field, null for a non-null field and an identity change.

These descriptor assertions do not establish generated-language typechecking or database constraints. [Compiler tests](compiler.md) own source-to-descriptor behavior; [storage integration](../integration/persistence.md) owns the database boundary.

Run `cargo test -p axton-core --test contracts --locked`.

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/components/schema.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
