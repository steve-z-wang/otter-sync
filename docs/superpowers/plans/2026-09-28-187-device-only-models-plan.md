# Device-only Models Implementation Plan

> **For agentic workers:** Use `superpowers:executing-plans` to implement this plan checkpoint by checkpoint, with `superpowers:test-driven-development` for each behavior change.

**Goal:** A Model's Loader becomes optional. A Model without one is device-only: it is never published, a backend refuses to start when a Mutation would send it on the wire, and the client keeps writing it locally without sending anything.

**Architecture:** The engine already carries the set of Models with a Loader (`Config.loaders`) and refuses every read and settlement of any other Model with `loader.unregistered`. The TypeScript backend passes only the registered Models, the engine refuses a descriptor that needs a missing Loader at `Config::decode`, and the declaration collector refuses a device-only Model at the call. No schema syntax, history, wire protocol or host operation changes.

**Design:** the `[design]` comment on [#187](https://github.com/zanminwang/axton/issues/187). It deliberately replaces the issue's `@@local` proposal: whether a Model syncs is decided by where it is written.

## Global constraints

- Registering a Loader for every Model, an always-null one included, keeps working unchanged.
- A Model that registers a Loader registers every retained version; only whole-Model omission is new.
- Error codes stay as they are: `config.invalid` at startup, `loader.unregistered` at runtime. Messages name the Model.
- Baseline: `main` at `0853c30`.

## Checkpoint 1: Engine startup check and named refusals

**Files:** `crates/server/src/lib.rs` (`Config::decode`), `crates/server/src/settlement.rs` (`unregistered`), call sites in `readback.rs`, `loading.rs`, `fetch.rs`, `loads.rs`; tests in `crates/server/tests/runtime.rs`.

- [ ] Failing test: a config whose slot mutation, typed Mutation Model operand, Mutation or Query Model output, or Load Model output names a Model outside `loaders` is `config.invalid`, and the message names the operation and version, the slot or output, and the Model. A config that omits the Loader of a Model nothing names decodes.
- [ ] Implement the check after the existing `loaders` check in `Config::decode`, over `mutations[].slots`, `schema.actions[]` and `schema.loads[]`.
- [ ] `unregistered(model)` names the Model; every `loader.unregistered` site uses it. Update assertions that pinned the old text.
- [ ] Fix test configs that the new check refuses (they must register what they write). `cargo test -p axton-server -p axton-sim --locked`.

## Checkpoint 2: TypeScript runtime

**Files:** `packages/server/index.mts`, `packages/server/effects.mts`; tests in `integration/persistence/server/runtime.test.mjs`, `effects.test.mjs`, `membership.test.mjs`.

- [ ] Failing tests: a backend that omits the Loader of a Model no Mutation writes starts; a Loader registered under a key that names no Model is refused; a slot writing a Loader-less Model fails startup with the named error; `touch`, `channel(name).<model>.add/remove` and `channel(name).add/remove([Model(…)])` naming it throw at the call in a handler (the mutation fails with `handler.failed`, nothing is published) and in `backend.transaction` (the body's error reaches the caller, nothing commits); an always-null Loader keeps the old behavior.
- [ ] `createBackend`: an `undefined` registration omits the Model; `null` and malformed values stay errors. Pass only registered Models as the engine's `loaders`. Refuse unknown loader keys (a typo would otherwise silently make a Model device-only).
- [ ] `effectsFor(models, enums, loaded?)`: declarations naming a Model outside `loaded` throw `<caller>: Model <Name> has no Loader, so it is device-only and cannot be published`.
- [ ] `bash integration/persistence/server/run.sh`; prettier.

## Checkpoint 3: Generated types and fixtures

**Files:** `crates/compiler/src/emit.rs`, `crates/compiler/tests/compiler.rs`; regenerated `backend.ts` fixtures; `integration/generated-api/test.ts`, `integration/action-contract/positive.ts`.

- [ ] Failing compiler test: every `Loaders<Tx>` member is optional.
- [ ] Emit `name?:`. Regenerate every checked-in generated backend.
- [ ] Type checks: a `Loaders<Tx>` omitting Models type-checks; a partial versioned registration still does not.
- [ ] `bash integration/generated-api/verify.sh`; action-contract `tsc` and `check-negative.sh`.

## Checkpoint 4: End to end

**Files:** `integration/action-e2e/backend-fixture.ts`, `integration/action-e2e/action.test.mts`.

- [ ] Omit the Composition Loader (Composition is written only locally and as a local companion). Replace the Composition Loader counter with the omission itself.
- [ ] New test: the client creates, updates, reads and deletes Compositions with plain local writes; no request of any kind carries them and nothing is pending; a Fetch of one is the saved `loader.unregistered` rejection.
- [ ] `bash integration/action-e2e/run.sh`.

## Checkpoint 5: Documentation

- [ ] Backend interface (startup rule, runtime refusals, quality evidence), schema models/actions where Loaders are required, typed server API, website backend API guide (`Loaders`), schema guide if it says every Model needs a Loader. Check relative links and anchors.
- [ ] `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `npm run typecheck`.
