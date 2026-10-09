# Compiler tests

Verify the [compiler](../../architecture/compiler/README.md) stages and generated contracts.

[parse.rs](../../../../crates/compiler/tests/parse.rs) asserts declaration positions, syntax diagnostics, plain typed parse data, compile equivalence with parse/validate/generate and deterministic parsing. Its `parse_reports_syntax_errors_with_the_found_token_and_nothing_semantic` checks that an unknown field type survives Parse and fails Validate at the declaration.

[history.rs](../../../../crates/compiler/tests/history.rs) owns retained Model/Mutation history; [other compiler suites](../../../../crates/compiler/tests) cover CLI and emitter contracts. Text emission assertions establish generated text only; [SDK integration](../integration/bindings.md) owns actual language compilation. Creation defaults reach real SQLite through [protocol05_defaults.rs](../../../../crates/sqlite/tests/protocol05_defaults.rs), a separate runtime boundary.

Run `cargo test -p axton-compiler --locked`.

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/components/compiler.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
