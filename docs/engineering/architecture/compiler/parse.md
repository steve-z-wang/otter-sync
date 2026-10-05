# Parse

## 1. Introduction and Goals

Parse turns one or more `.model` files into structured declarations with token positions, so a schema author can be pointed at the offending place.

## 3. Context and Scope

- Input: the CLI concatenates every `*.model` file in the input directory in path order; the library takes one string.
- Output: `Declarations`, a typed tree of enums, models, legacy slot mutations, operations (each with its `CallKind`, ordinary inputs, Model operands and named outputs), native Loads, and prerequisites, each with source positions. Directive arguments, bindings and sequence arguments stay as JSON expressions. [Validate](validate.md) consumes it; `compile` is `generate::descriptors(validate(parse(source)))`.
- Errors: `line:col: message (found 'token')` for syntax errors; the CLI rewrites the line into `path:line` for the file that contains it. Every declaration, field, slot, directive and prerequisite field also keeps its token position for [Validate](validate.md).

## 5. Building Block View

`abstract model Name` and `model Name extends Parent` retain abstraction and the single parent name on `ModelDecl`; fields remain complete declarations. `@@bootstrap` is a duplicate-checked, argument-free concrete-model directive. Abstract model directives are refused at their source position; validation resolves inheritance and concrete field rules.

- **Lexer.** Identifiers and digit runs, double-quoted strings with backslash escapes, the punctuation `{ } ( ) [ ] ? , . @ < > :`, and `//` line comments. Anything else is an error. Every token carries its line and column.
- **Grammar.** `enum`, `model`, `mutation`, `query` and `prerequisite` declarations. An operation uses `mutation Name(input Type, operand Model.create?) { output Type }` or `query Name(input Type) { output Type }`; braces are optional for void output. After `mutation Name`, the next token selects the form: `(` begins an operation, `{` the legacy slot block ([slot mutations](../schema/mutations.md)); `query` always takes the parenthesized form. The retired `action` keyword is a syntax error ("action declarations were replaced: declare mutation Name(...) or query Name(...)"). `@version` and `@sequence` appear above an operation; `@@id` and `@@unique` stay inside Models. Ordinary values support scalar or enum types, `?` for nullable values and `[]` for lists; Model operands support create/update/delete and single, optional or list cardinality. Parse applies no kind rule: a Query with a Model operand or `@sequence` parses and is refused by [Validate](validate.md). The existing directive argument grammar still applies; an argument name may be a dotted path, `members.space:`, which only a sequence argument accepts ([Validate](validate.md)). Unsupported member annotations are rejected. `load Name(input Type) { output Model[] }` declares a native Load ([#173](https://github.com/zanminwang/axton/issues/173)), kept apart from operations and never given a `CallKind`: braces with at least one output are required and `@sequence` above it is a syntax error, while `@version` applies as for operations. Model operands and non-list outputs parse and are refused by [Validate](validate.md).
- **CLI.** `axton compile INPUT_DIR OUTPUT_DIR [--mutation-history FILE] [--initialize-mutation-history] [--model-history FILE] [--initialize-model-history] [--action-history FILE] [--initialize-action-history] [--load-history FILE] [--initialize-load-history] [--schema-fence FILE] [--backend-runtime SPEC] [--client-runtime SPEC]`.

Code: `lex`, `Parser`, `parse` and the `Declarations` types in [compiler/parse.rs](../../../../crates/compiler/src/parse.rs); file handling in [compiler/main.rs](../../../../crates/compiler/src/main.rs).

## 10. Quality Requirements

- `mutation Name(` and `query Name(` parse to operations with their kind, `mutation Name {` stays a slot mutation, and `action` or a `query` block is a syntax error. Evidence: [compiler/tests/parse.rs](../../../../crates/compiler/tests/parse.rs) `operation_keywords_select_kind_and_keep_the_legacy_block_separate`, `retired_action_keyword_and_blockless_queries_are_syntax_errors`.
- A syntax error names its line. Evidence: [compiler/tests/compiler.rs](../../../../crates/compiler/tests/compiler.rs) `rejects_unknown_with_location`.
- With several input files, the CLI names the file and the line within that file, for syntax and semantic errors alike. Evidence: [compiler/tests/cli.rs](../../../../crates/compiler/tests/cli.rs) `cli_relocates_errors_into_the_file_that_declares_them`.
- Parsing records every declaration with its position and applies no semantic rule; an unknown type parses and is refused only by Validate. Evidence: [compiler/tests/parse.rs](../../../../crates/compiler/tests/parse.rs) `parse_keeps_every_declaration_with_its_position`, `parse_reports_syntax_errors_with_the_found_token_and_nothing_semantic`.
- A model `@@version` follows the mutation rules: positive, within the safe range, at most one per declaration, 1 when omitted. Evidence: `model_version_follows_the_mutation_rules`.
- `@default(...)` on a field keeps its expression kind (string, number text, identifier, or a zero-or-more-argument call whose arguments are counted, never evaluated) and its position; a repeat is refused. Evidence: `default_keeps_its_expression_kind_and_position`.
- `@deprecated` parses on fields, enum values and slots with an optional string reason; another argument, a non-string reason or a repeat is refused. Evidence: `deprecated_is_a_field_level_directive_with_an_optional_reason`.
- The split changes no output: the checked-in fixtures and the example compile to byte-identical files before and after it. Evidence: `compile_is_parse_then_validate_then_generate_and_declarations_are_plain_data`; verified 2026-09-14 by compiling `fixtures/compiler/*.model` and `integration/e2e/fixtures/round-trip/models` with both binaries and diffing the output files of each (seven then; six since mutation history moved out of the output directory).

## 11. Risks and Technical Debt

- **Accepted limitation:** numeric literals (JSON grammar, assembled from adjacent tokens such as `-`, `1`, `.`, `5e3`) exist only inside `@default(...)` and `@@version`; directive arguments elsewhere stay identifiers, strings and lists.
