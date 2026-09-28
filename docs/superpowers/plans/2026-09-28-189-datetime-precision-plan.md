# DateTime Precision and Omitted Optional Slots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close [#189](https://github.com/zanminwang/axton/issues/189): the Dart SDK sends every `DateTime` at the millisecond precision AXTON stores, and an omitted optional Model operand is documented and tested as identical to an explicit `null`.

**Architecture:** The core already normalizes every stored value and argument to UTC at millisecond precision (`crates/core/src/schema.rs`). The Dart SDK gains one public helper, `AxtonDateTime.toAxtonPrecision()`, and every generated Dart encoder routes a `DateTime` through it. Omitted optional operands are already recorded as `null` by core normalization (`normalize_inputs` in `crates/core/src/actions.rs`); that behavior is kept, documented and tested.

**Tech Stack:** Rust compiler (Dart emitter), Dart SDK, generated fixtures, action-e2e harness (Node backend, PostgreSQL, Dart and TypeScript clients), website docs.

## Constraints

- Design: the `[design]` comment on #189. Milliseconds everywhere; reads decode as UTC (`isUtc == true`); no change to TypeScript code or to what is recorded for an omitted slot.
- Truncation drops the microsecond component of the UTC value, which matches chrono's `SecondsFormat::Millis` for pre-1970 instants too.
- Regenerate every checked-in generated fixture the emitter change touches; inspect the diff.
- Do not run the full `scripts/test.sh` locally; run focused suites and let CI run the host gate.

## Task 1: Shared Dart helper

**Files:** create `packages/dart/lib/src/date_time.dart`; modify `packages/dart/lib/axton.dart`; test `packages/dart/test/date_time_test.dart`.

- [ ] Write failing tests: a microsecond UTC value truncates to its millisecond; a local value becomes the same instant in UTC with `isUtc`; a whole-millisecond UTC value is returned equal; a pre-1970 value with microseconds moves to the earlier millisecond; `toIso8601String()` of the result has exactly three fractional digits and `Z`.
- [ ] Implement `extension AxtonDateTime on DateTime { DateTime toAxtonPrecision() }` and export it.
- [ ] Run `dart analyze` and `dart test test/date_time_test.dart` in `packages/dart`.

## Task 2: Generated Dart encoders use the helper

**Files:** modify `crates/compiler/src/emit.rs` (`encoded`, `_dartActionEncode`, the generated `export ... show` list); test in the compiler's Dart emit tests; regenerate fixtures.

- [ ] Add a failing compiler test that generated Dart encodes a `DateTime` field, patch, filter and action argument with `toAxtonPrecision().toIso8601String()` and re-exports `AxtonDateTime`.
- [ ] Change the emitter; run `cargo test -p axton-compiler`.
- [ ] Regenerate checked-in fixtures (`integration/generated-api`, `integration/action-e2e`, `integration/action-runtime-dart`, `integration/action-contract`, `integration/load-e2e`, example apps and any other `generated.dart`) with the commands their runners use; confirm only the DateTime encoding and the export list changed.

## Task 3: Local round trip (generated-API Dart test)

**Files:** `integration/generated-api/generated_test.dart`.

- [ ] Test: a Model created locally with a microsecond `DateTime` (UTC and local) reads back equal to its millisecond truncation in UTC; a patch, an identity lookup and an equality filter with the microsecond value find the stored row.
- [ ] Run `bash integration/generated-api/verify.sh`.

## Task 4: Server round trip and omitted slots (action-e2e)

**Files:** `integration/action-e2e/source/app.model` (new `Restamp(note Note.update<createdAt>?, at DateTime) { at DateTime }`), regenerated action-e2e outputs and history, `integration/action-e2e/backend-fixture.ts`, new `integration/action-runtime-dart/action_e2e_datetime.dart`, `integration/action-e2e/action.test.mts`, `scripts/test.sh` (analyze the new script).

- [ ] Dart script against the real backend: `AddNote` with a microsecond `createdAt` reads back truncated before and after the backend's authority; `Restamp` with a microsecond patch and argument returns and stores the truncated values; offline, `restamp(at:)` and `restamp(note: null, at:)` record identical `axton_mutation.args` with `note: null`.
- [ ] TypeScript test: the handler received the truncated instants, PostgreSQL holds them, and the two Dart Restamp calls reached the handler with identical input; the same omitted/`null` pair from the TypeScript client records identical args and identical handler input.
- [ ] Run `bash integration/action-e2e/run.sh`.

## Task 5: Documentation

- [ ] Website: schema reference (DateTime row, optional operand omitted ≡ `null`), client API guide (precision, UTC reads, comparing values, the helper), backend API guide (handler receives `null` for an omitted operand), interface index (the helper).
- [ ] Engineering: `docs/engineering/architecture/schema/types.md` quality evidence, `docs/engineering/architecture/sdks/typed-api/client.md` (Dart encoding), testing docs where the new coverage is listed.
- [ ] Run `python3 website/scripts/check_examples.py`.

## Verification

- [ ] `packages/dart`: `dart analyze` and `dart test` with `AXTON_LIBRARY`/`AXTON_DART_LIBRARY` set.
- [ ] `cargo test -p axton-compiler`; `bash integration/generated-api/verify.sh`; the action-runtime-dart analyze and test line from `scripts/test.sh`; `bash integration/action-e2e/run.sh`; `python3 website/scripts/check_examples.py`.
- [ ] `cargo fmt --check` and `cargo clippy -p axton-compiler` for the emitter change.
