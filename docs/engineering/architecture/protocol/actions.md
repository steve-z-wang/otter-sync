# Direct calls

## 1. Introduction and Goals

A direct call uses request/response delivery for a typed final result: the default route of a Query and the `mutations.call` override of a Mutation ([Mutations and Queries](../schema/actions.md)). It shares the operation executor, per-call identity and Model result rules with [durable pushes](push.md), but has no durable client queue or inferred local optimism. Internal names keep the earlier Action spelling: the `/sync/actions` endpoint, `ActionStore` and the `action.*` codes.

## 3. Context and Scope

`POST /sync/actions` carries `{call: {callId, name, version, args, store?}, models}`. `callId` is a UUID generated once by the client; `models` declares the Model read contracts needed for results and authority. An operation with only ordinary outputs and inputs may use an empty `models` map. The request carries no kind: the server takes it from the retained descriptor for `name` and `version`. The response carries one correlated `completion` and `records` of authority. The client validates correlation and applies authority after the server commits.

`POST /sync/fetch` is the [Model Fetch](#model-fetch) route: one remote read of one Model by identity through its existing Loader, with the same completion envelope and authority encoding but no operation descriptor.

## 5. Building Block View

Wire envelopes, normalization and `ActionStore`: [core/actions.rs](../../../../crates/core/src/actions.rs). Shared execution: [server/actions.rs](../../../../crates/server/src/actions.rs); result and output authority assembly: [server/action_results.rs](../../../../crates/server/src/action_results.rs). HTTP route: [server/index.mts](../../../../packages/server/index.mts). Persistent call claim/response: [Persistence](../server/persistence.md). Model Fetch: wire in [core/fetch.rs](../../../../crates/core/src/fetch.rs), server read in [server/fetch.rs](../../../../crates/server/src/fetch.rs), and the claim/replay/savepoint/save protocol it shares with Actions in [server/calls.rs](../../../../crates/server/src/calls.rs).

## 6. Runtime View

The server authenticates the request, claims its call ID in the application transaction, then either returns the saved response or invokes the retained handler version. Both kinds are claimed and saved, so a Query also stores call metadata. A Query settlement that carries changes or membership declarations is the call's `query.effects_forbidden` rejection before any stamp, readback or publication ([enforcement](../schema/actions.md#8-crosscutting-concepts)). The same transaction contains business writes, Model Loader snapshots, stamps, Channel memberships, publications and the saved response. A repeated call ID replays the stored outcome without re-running the handler or Loader, and allocates no stamp, position or membership change. Direct request/response has a finite transport timeout; a client timeout may leave execution status unknown because the transaction could have committed. A direct call that cannot reach the server fails; it is never moved to the durable queue.

### Store policy

`store` is an optional per-invocation field beside `args`, never passed to the Handler. Omitted or `true` stores every eligible output, `false` stores none, and an object maps explicit Model output names (handler-selected, single, nullable or list) to booleans; unnamed outputs default to `true`. Scalar outputs, input-bound Model outputs and Delete confirmations are not keys. A non-boolean value or non-object is a structural envelope error; an unknown or ineligible key is a per-call `action.invalid` rejection before the Handler runs. The same field appears on each durable push entry.

The policy selects only the additional authority that explicit outputs contribute. The response's authority is the positive union of required authority (the Mutation's input targets) and the identities chosen by enabled outputs, so an identity also selected by an enabled output or targeted by an input is always included. Records the handler only touches are distributed to their Channels but are never caller authority, whatever the policy ([guarantee Q7](../../guarantees.md#q-call-outcomes), [server execution](../server/engine/README.md)). A disabled output-only read gets no stamp allocation and no authority-version read; its result is still the Loader snapshot at the output's read version. Loader reads are deduplicated per record and read version within an invocation. The policy does not change result types, backend persistence of the outcome, delivery or errors.

Keys are validated against the explicit map, so an unknown key is refused even with value `true`. The canonical policy then drops `true` entries (an empty map is the default); clients persist and send it, and the call identity includes it only when it is not the default. Omitted, `true` and `{a: true}` share one identity, `{a: false, b: true}` equals `{a: false}`, and key order is immaterial. Reusing a call ID with a different policy is `call.identity_conflict`; a replay returns the saved result and authority without running the Handler or Loader.

A successful Model output is the Loader snapshot at that invocation. Its record authority may differ from the client's optimistic view after later local work is replayed. The direct route applies committed authority without draining an independent durable queue. The backend keeps saved call outcomes without TTL or automatic pruning; client SDKs hold result objects only in memory.

### Model Fetch

Model Fetch ([#153](https://github.com/zanminwang/axton/issues/153)) reads one complete Model from the application's existing versioned Loader. It introduces no operation kind, Handler, action version or history entry, so a schema with Models and no operations can use it.

```json
{"callId": "123e4567-e89b-42d3-a456-426614174000", "model": "Todo", "version": 1, "identity": {"id": "todo-1"}, "store": false}
```

`store` is a boolean, omitted for the default `true`, so omitted and `true` are one request. `version` names a retained Model read version; the generated client always sends its local Model version. The response is the direct envelope: `completion.outcome.result` is the normalized Model object, identity included, or `null`, and `records` holds exactly one AuthorityRecord for that identity when `store` is `true` and the call succeeded (null state for absence), and none otherwise. The client refuses extra or duplicate records, record errors, another call ID, invalid stamps, incompatible content or a result that disagrees with its authority before any local write. Request and response use the direct endpoint's byte bounds and canonical scalar encoding.

The server runs in the application transaction, like a direct call:

1. Authenticate, decode the envelope and normalize the identity with the requested read contract. A malformed envelope or identity is `400 request.invalid` and claims nothing.
2. Claim `(owner, callId)` under a canonical intent `{kind: "fetch", callId, model, version, identity, store}`. Action intents have no `kind`, so a call ID reused across kinds, or for another Fetch intent, is `call.identity_conflict` and nothing is read; a matching claim replays the saved response without calling the Loader.
3. For a stored read, take stamp evidence first (`ensureStamp`, never an advance), which leaves a stamp row even when the Loader then answers `null`; `store: false` allocates no stamp. Then call the Loader once and build both the result and the authority from that one normalized row.
4. Save the outcome and commit before replying. The saved response holds the full snapshot, `store: false` included, and is kept for replay without automatic pruning, like every call outcome ([#61](https://github.com/zanminwang/axton/issues/61)). An unserved Model or version (`model_version_unsupported`), a missing Loader (`loader.unregistered`), a Loader refusal (its code), a thrown Loader error (`loader.failed`) and an invalid row (`loader.invalid`) are the call's saved `rejected` outcome, replayed like any other. A host or storage fault rolls back the transaction, claim included, so a retry reads again.

A Fetch never touches, publishes or changes membership, and advances no Channel cursor. The Loader's `null` is absence; its refusal is an error that deletes nothing, so authorization stays the Loader's choice between the two ([Backend interface](../server/backend-interface.md#3-context-and-scope)). The client lifecycle is the [runtime](../client/runtime.md#6-runtime-view)'s.

## 10. Quality Requirements

- **One call ID has one committed outcome, and replay does not re-execute application code.** Evidence: [server Action tests](../../../../crates/server/tests/actions.rs) and [PostgreSQL conformance](../../../../integration/persistence/server/driver-conformance.test.mjs).
- **`store` removes only output-only authority, joins the call identity and replays unchanged.** Evidence: [server store tests](../../../../crates/server/tests/action_store.rs), [PostgreSQL persistence](../../../../integration/persistence/server/actions.test.mjs) and [end-to-end](../../../../integration/action-e2e/action.test.mts).
- **A Query settlement with changes or memberships is rejected before framework handling on this path too.** Evidence: [server Action tests](../../../../crates/server/tests/actions.rs) `forged_query_effects_are_rejected_on_the_direct_path_too`.
- **The direct response is validated against the requested operation and Model contracts.** Evidence: [core Action contracts](../../../../crates/core/tests/contracts.rs).
- **A Fetch request carries only a boolean store policy and a canonical identity at a retained read version; its response stores exactly the requested complete row, stamped null authority for absence only when storing, and nothing for a refusal; foreign or disagreeing authority is refused before exposure.** Evidence: [core/tests/contracts.rs](../../../../crates/core/tests/contracts.rs) `fetch_defaults_to_storage_without_an_action`, `fetch_request_carries_only_a_boolean_storage_policy`, `fetch_request_refuses_invalid_envelopes_identities_and_options`, `fetch_composite_identities_normalize_to_one_key`, `fetch_response_stores_exactly_the_requested_complete_row`, `fetch_response_absence_is_null_with_stamped_null_authority_only_when_storing`, `fetch_refusal_has_no_records_and_keeps_its_code`, `fetch_response_refuses_foreign_or_disagreeing_authority_before_exposure`, `fetch_uses_the_requested_retained_read_version`, `fetch_snapshot_follows_same_version_compatible_field_rules`.
- **A Fetch takes stamp evidence before one Loader read, replays its saved snapshot per owner without reading, conflicts across call kinds and intents, saves Loader refusals as terminal outcomes, rolls back on storage faults and never publishes or changes membership.** Evidence: [server/tests/fetch.rs](../../../../crates/server/tests/fetch.rs) (14 tests, including `stored_fetch_takes_stamp_evidence_then_reads_once_and_saves_one_snapshot`, `a_call_id_is_owned_per_principal`, `actions_and_fetches_never_replay_each_others_saved_responses`, `storage_faults_propagate_without_saving_an_outcome`); over HTTP and PostgreSQL, [fetch.test.mjs](../../../../integration/persistence/server/fetch.test.mjs).
