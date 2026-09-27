# Model Fetch Implementation Plan

> **For agentic workers:** Use `superpowers:executing-plans` to implement this plan checkpoint by checkpoint. If the user delegates implementation to subagents, use `superpowers:subagent-driven-development`. Review each checkpoint before continuing.

**Goal:** Generate `client.fetch.<model>(identity)` for one remote Loader read, with default local storage and completion after commit.

**Architecture:** Rust owns request validation, concurrent-flight sharing, network effects, response admission, storage and completion. SDKs encode/decode generated types and execute effects. The server resolves existing versioned Model Loaders through a generic Fetch endpoint; there is no synthetic Query, business Handler or durable client Fetch job.

**Tech stack:** Rust core/client/server/compiler, SQLite, application-owned PostgreSQL transactions, Node native binding, TypeScript/React Native and Dart.

**Design:** [Fetch specification](../specs/2026-09-27-153-model-fetch-design.md). **Issue:** [#153](https://github.com/zanminwang/axton/issues/153).

## Global constraints

- Default `store: true`; `store: false` skips local application and onStore. Only boolean storage options are accepted.
- One await returns the invocation's complete Model snapshot or null. It never returns the current optimistic local view.
- Every sequential call reads remotely. Only overlapping identical requests join; no completed cache or offline fallback.
- Fetch never touches Channel membership/cursors, Mutation sequencing or Load progress.
- Use existing Model read versions and Loader contracts. No Fetch-specific schema declaration or action history.
- No network I/O under a local writer transaction; no storage success before commit.
- Scope is implementation of this specification, with documentation and tests. Do not redesign Query, Load or the shared schema-upgrade policy.
- Baseline inspected: `ed566eb77f9b0b47f2c128bd212261d08687e324`. Rebase on current main before implementation and inspect concurrent #173 changes.

## Preparation

- [ ] Read AGENTS.md, the issue and its comments, the spec, architecture/guarantees and testing instructions. Record the implementation branch/worktree in a `[start]` issue comment. Preserve this planning branch or branch from it in an isolated worktree.
- [ ] Inspect `git status`, then rebase onto current main. If #173 has added route or effect variants, preserve them and add Fetch beside them. Record material contract changes on the issue.
- [ ] Run `npm ci`, `bash scripts/build.sh` and `cargo test --workspace --locked`. Record baseline failures separately from Fetch failures. Set up Dart and PostgreSQL per [running tests](../../engineering/testing/running.md).

## Checkpoint 1: Shared Fetch wire contract

**Files:** create `crates/core/src/fetch.rs`; modify `crates/core/src/lib.rs`; extend `crates/core/tests/contracts.rs`. Extract shared result normalization from `crates/core/src/actions.rs` only where needed.

**Interfaces produced:** `FetchRequest` with `call_id`, `model`, `version`, `identity`, `store`; `FetchResponse` with the existing `CallCompletion` and `Vec<AuthorityRecord>`. Methods: request `decode(bytes, schema)` / `encode()` and response `decode(bytes, request, schema)` / `encode()`, all returning the crate's `Result`. The wire has camelCase callId and omits default storage. Keep Fetch validation independent of ActionDescriptor.

- [ ] Add failing request tests to `contracts.rs`, using its existing `schema()` and `ID` helpers. Start with this normalization contract:

```rust
#[test]
fn fetch_defaults_to_storage_without_an_action() {
    let raw = serde_json::json!({
        "callId": "123e4567-e89b-42d3-a456-426614174000",
        "model": "Entry", "version": 1, "identity": {"id": ID}
    });
    let request = FetchRequest::decode(raw.to_string().as_bytes(), &schema()).unwrap();
    assert!(request.store);
    assert_eq!(request.identity, serde_json::json!({"id": ID.to_lowercase()}));
    let encoded: serde_json::Value = serde_json::from_slice(&request.encode().unwrap()).unwrap();
    assert!(encoded.get("store").is_none());
}
```

- [ ] Add table-driven negatives: missing/extra/wrong identity fields, invalid UUID/version, unknown Model, object-valued store, oversized bytes; equivalent composite identities normalize to the same key. Run `cargo test -p axton-core --test contracts fetch` and confirm the new contract fails before implementation.
- [ ] Implement structural decoding, retained Model validation and normalization. Reuse existing canonical JSON, scalar, identity and Model result codecs. No automatic defaults for missing identity input.
- [ ] Add response tests for a complete row, null, failed outcome, mismatched call ID, wrong/duplicate/additional authority, result/authority disagreement and the exact record cardinality under each storage policy. For null plus store true, assert one stamped null record. A refusal has no records.
- [ ] Implement response validation before exposing result or authority to callers. Add same-version compatible-field projection/default cases and unsupported read-version cases using the existing result contract fixtures.
- [ ] Run `cargo test -p axton-core --test contracts` and `cargo fmt --all -- --check`. Commit `feat(core): define single-model fetch protocol`.

## Checkpoint 2: Authorized server resolver and HTTP endpoint

**Files:** create `crates/server/src/fetch.rs` and `crates/server/tests/fetch.rs`; modify `crates/server/src/lib.rs`, `crates/server/src/action_results.rs`, `bindings/node/src/lib.rs`, `packages/server/index.mts`. Add `integration/persistence/server/fetch.test.mjs` and its explicit invocation in `integration/persistence/server/run.sh` (the runner does not discover new files automatically).

**Interfaces consumed:** checkpoint 1's request/response and existing Config, Host, ClaimCall, SaveCall, EnsureStamp and Load. **Produced:** `process_fetch(config: &Config, owner: &str, bytes: &[u8], host: &impl Host) -> Result<String>` as an async Rust function; native `processFetch`; authenticated `POST /sync/fetch` wrapped in the existing application transaction.

- [ ] Build a recording Host fixture in `fetch.rs` following `server/tests/actions.rs`. Configure a Model/Loader but no Actions. Assert one Load request for the correct owner, identity and version; no HandleAction, touch, membership or publication operations. Run `cargo test -p axton-server --test fetch` to observe the missing implementation.
- [ ] Extract the reusable authorized single-record read from `action_results.rs`. Preserve existing Action behavior and tests. For stored Fetch, acquire stamp evidence before the fresh read and use that one normalized state for both result and authority. False storage performs the read without authority allocation.
- [ ] Implement per-owner claim/replay with a Fetch-tagged canonical fingerprint. Tests must prove repeated call ID returns the saved result after the business row changes, distinct owner does not share it, changed intent conflicts, and Action/Fetch cross-kind reuse conflicts without decoding another kind's saved response. Keep existing Action fingerprint bytes unchanged.
- [ ] Add Loader null/refusal/failure/invalid-row/version tests. Distinguish terminal rejection from transaction failure: save terminal outcomes according to existing server classification; propagate storage/internal transaction failures and roll back. Validate the returned completion with checkpoint 1's decoder.
- [ ] Expose the native method and `/sync/fetch` through the same authenticated transaction wrapper used by `/sync/actions`. Commit before returning bytes. No new application Handler map or Loader callback signature.
- [ ] In the PostgreSQL integration test, exercise the HTTP/native path with real call-ledger persistence: commit replay, rollback retry, null authority, store false, owner isolation and lack of Channel changes. Explicitly add the new test after the other sequential database suites:

```sh
node --test "$root/integration/persistence/server/fetch.test.mjs"
```

- [ ] Run `cargo test -p axton-server`, `bash scripts/build.sh` and `bash integration/persistence/server/run.sh`. Commit `feat(server): resolve model fetches through loaders`.

## Checkpoint 3: Rust direct lifecycle and atomic store completion

**Files:** create `crates/client/src/fetch.rs`; modify `crates/client/src/lib.rs`, `crates/client/src/runtime/{protocol,commands,tasks,direct,transactions,effects}.rs`, `crates/client/src/store_delivery.rs`. Add cases in `crates/sqlite/tests/runtime_lanes.rs` and `crates/sqlite/tests/store_hooks.rs` using their existing runtime/SQLite fixtures. Change `authority.rs` only if a reusable acceptance result is missing.

**Interfaces:** runtime `Command::Fetch { model, version, identity, store }`; JSON command `{kind:"fetch",model,version,identity,store?}`. Its task outcome contains the Fetch completion outcome; the generated API unwraps `result`. HTTP effects use the new `fetch` operation route. StoreDelivery gets a Fetch variant or a validated common single-record continuation, with explicit apply success/failure available to the originating task.

- [ ] Add a runtime effect test that submits two identical Fetch tasks before answering HTTP. Assert exactly one HTTP effect, separate request IDs, one call ID and no new Mutation queue entry. Assert a third invocation after both complete produces another HTTP effect/call ID. Run `cargo test -p axton-sqlite --test runtime_lanes fetch` and observe failure.
- [ ] Generalize only the direct lifecycle pieces needed for Fetch: frozen bytes, HTTP/deadline effects, one credential refresh, response admission and terminal waiter cleanup. Keep Query once logic and Action decoding unchanged. Store Fetch flight keys in Rust memory, including replica generation and normalized store policy. Do not create a SQLite Fetch table.
- [ ] Add distinct-identity/version/policy tests, canonical composite-key joining, concurrent independent requests and join-during-store-wait. Joined callers receive one outcome after one application; remove every flight on success and failure.
- [ ] Add hook-gated store tests: pause an async onStore callback, assert no Fetch outcome yet, finish it, then assert the outcome and committed row. Throw in the hook and assert both hook writes and incoming authority roll back and every joined task rejects. Store false must produce no hook and no row change.
- [ ] Add older-stamp no-op, identical-stamp no-op, equal-stamp conflict, schema rejection, stamped deletion and pending optimistic-write tests. The returned snapshot remains the server value while the local visible row may include the pending edit. Reject a single-record apply failure instead of completing from a successful server envelope alone.
- [ ] Add deterministic effect tests for deadline, refresh success/failure, late reply, stop-before-reply, stop-after-admission, close and rebuild. During incompatible Mutation drain, reject new Fetch and discard affected in-flight replies with `fetch.schema_pending`; after rebuild fence the old generation. Verify no Fetch write can bypass onStore and Mutation drain can still progress.
- [ ] Run `cargo test -p axton-sqlite --test runtime_lanes`, `cargo test -p axton-sqlite --test store_hooks` and `cargo test -p axton-client`. Commit `feat(client): manage fetch requests and storage in Rust`.

## Checkpoint 4: Generated APIs and thin SDK transports

**Files:** modify `crates/compiler/src/{emit,generate,main}.rs` and compiler tests; `packages/client-js/{index,connection,bridge}.mts`; `packages/dart/lib/src/{client,connection,bridge}.dart`. Inspect `packages/client-react-native/index.ts` and `packages/client-react-native/live.mts`; update any mirrored HTTP operation types and extend `integration/bindings/react-native/actions.test.mjs` for Fetch routing. Extend `integration/generated-api/{test.ts,generated_test.dart}` and negative fixtures; regenerate emitted TS/Dart fixtures. Add `integration/bindings/client-js/fetch.test.mjs` and `packages/dart/test/fetch_test.dart`.

**Interfaces produced:** generated `client.fetch.<model>(identity, options?) -> Promise<Model | null>` and Dart `client.fetch.<model>(ModelIdentity identity, {bool store = true}) -> Future<Model?>`. A shared raw SDK method submits the Fetch command; Rust still owns all lifecycle logic. Each caller decodes its own result with the existing Model codec.

- [ ] Add positive generated-type cases for single/composite identities, null, dates/enums and store false. Add negative cases for an incomplete identity, wrong scalar, output-map store, once/refresh options and Fetch access through a transaction/onStore context. For TS the intended compile contract includes:

```ts
async function checkFetch(client: GeneratedClient, id: string) {
  const result: Entry | null = await client.fetch.entry({ id });
  const preview: Entry | null = await client.fetch.entry({ id }, { store: false });
  // @ts-expect-error Fetch storage accepts a boolean, not an output map
  await client.fetch.entry({ id }, { store: { entry: false } });
  // @ts-expect-error no persistent once option
  await client.fetch.entry({ id }, { once: true });
  return [result, preview];
}
```

- [ ] Generate the facade and identity/result codecs without synthetic Actions. Add model-only, empty-schema and helper-name-collision fixtures; reserve only emitted helper names. No mutation of retained action history. Reuse Model versions already emitted in descriptors.
- [ ] Add the Fetch HTTP operation to both default connections and custom-transport typings. Route it to `/sync/fetch`, preserving credentials, deadline and error details from the Rust effects. SDK code must not add its own request joining, retry loops, writer queue or cache.
- [ ] Test raw and generated SDK behavior against the native Rust runtime: default storage, false storage, absent result, hook rejection, independent objects for joined callers and typed backend failure. Cover Fetch routing in React Native's host carrier tests; do not claim a host test proves a mobile device run.
- [ ] Run `bash scripts/build.sh`, `bash integration/generated-api/verify.sh`, `node --test integration/bindings/client-js/fetch.test.mjs` and the Dart tests using the library environment described in running-tests documentation. Run the existing Action runtime tests to guard the shared direct lifecycle: `bash integration/action-runtime-ts/verify.sh`.
- [ ] Commit `feat(sdk): generate typed model fetch APIs`.

## Checkpoint 5: Documentation, complete verification and review

**Files:** update `docs/engineering/architecture/client/{frontend-interface,runtime}.md`, `docs/engineering/architecture/server/backend-interface.md`, `docs/engineering/architecture/protocol/actions.md` and SDK typed-API documentation. Update `website/docs/frontend/client-api.md`, `website/docs/api-index.md`, and relevant testing pages with actual implemented tests. Keep dated plans separate from lasting documentation.

- [ ] Document the three distinct APIs with TS and Dart examples: local models.get, one-shot remote fetch and named Query. Describe default store, null versus failure, Loader snapshots, concurrent sharing and offline rejection. Document `/sync/fetch` and custom transport routing, with no new business Handler requirement.
- [ ] Review every acceptance item in the spec against tests and implementation. Verify no completed cache, client Fetch table, synthetic Query, new SDK scheduler or accidental Channel enrollment was added.
- [ ] Run `cargo test --workspace --locked`, then `bash scripts/test.sh`. The full gate covers formatting/lints, languages, PostgreSQL, generated APIs and website examples. Record executed commands and any unavailable platform checks. Do not count the pre-feature baseline as Fetch evidence.
- [ ] Check docs links/anchors and `git diff --check`. Review the whole change for auth/null distinction, per-owner replay, callback rollback, task leaks, stale-generation replies and #173 route coexistence. Fix findings and rerun affected checks.
- [ ] Post implementation evidence and remaining limits to #153; open a PR with `Closes #153`, attach it to the task and update issue labels per the workflow. Self-review the final PR before requesting acceptance. Merge only under the user's authorization for this implementation task; this planning handoff itself does not execute a merge.

## Plan review coverage

| Requirement | Checkpoint |
| --- | --- |
| Identity, Model versions, wire bounds, exact snapshot/authority agreement | 1 |
| Loader authorization, null/error distinction, stamps, replay and HTTP | 2 |
| Rust ownership, concurrent sharing, atomic store/hooks, lifecycle fences | 3 |
| Default/false storage and generated TS/RN/Dart surfaces | 3–4 |
| No hidden cache/queue or schema operation, Model-only schemas | 1–4 |
| Guides, real-boundary evidence, final review | 5 |

The specification and plan have been checked against the inspected baseline. These are future implementation steps; no Fetch implementation or feature test result is claimed by this planning change.
