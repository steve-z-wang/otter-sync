# Server

## 1. Introduction and Goals

Generated backend types connect application handlers and versioned Model Loaders to the Rust server. The application decides business state and viewer visibility. The engine validates inputs, manages durable outcomes, materializes authority and publishes explicitly declared identities.

## 3. Context and Scope

`createBackend` binds the generated descriptor to a database adapter, authentication, `protocol5` projection context/transaction-bound authorization, retained Mutation/Query handlers, Loaders and optional Bootstrap preparation. [Backend setup](../../../../../website/docs/backend/setup.md) and [API guide](../../../../../website/docs/backend/api.md) own complete examples.

| Handler/context | Capability |
| --- | --- |
| Mutation `{args, ctx}` | Typed input; authenticated `userId`; application `tx`; call identity; initiating `ctx.stream`; explicit `ctx.streams(names)`; invalidation/publication within the business transaction. |
| Query `{args, ctx}` | Typed input and returned identities/scalars; authenticated viewer and transaction; explicit tracking on the initiating or named Streams. No framework business-write/invalidation lane. |
| Bootstrap `{ctx}` | Optional fenced application enrollment before freezing finite authority; Stream handles track only. The application returns no pages or continuations. |
| Loader `{ids, tx, userId}` | Current viewer projection for aligned typed identities under the request's application transaction. Exactly one Model row or null per requested identity. |

For each retained operation version, registration uses its own kind under `mutations` or `queries`. A historical name can retain a Mutation version and expose a newer Query version. Version-only v1 registrations may use the bare function; otherwise generated maps have explicit `v<n>` members. Loader omission makes a Model device-only; remote operands/outputs/publication cannot name it.

The initiating Stream comes from the authenticated bound request; it is not caller authority to access arbitrary Streams. `authorizeStream` admits the request. Explicit multi-Stream publication uses the application's concrete recipient choices, sharing the backend transaction but not a cross-device local commit.

## 5. Building Block View

[Generated backend emission](../../../../../crates/compiler/src/emit.rs) supplies contracts. [server/index.mts](../../../../../packages/server/index.mts) validates registration, decodes typed inputs, executes application callbacks and collects declarations. [Protocol 5](../../protocol/0.5.md) owns fenced materialization/receipt rules; [host interface](../../server/backend-interface.md) owns the adapter boundary.

## 9. Architecture Decisions

Tracking is an explicit capability separate from returning data. It enrolls missing identity/Stream pairs; repeating a live pair moves no cursor. Invalidation updates existing holders without enrollment. All affected Loader dependencies, including visibility and canonical children, must be declared. Local cascades do not publish backend changes.

Ordinary Query/Fetch responses contain cursor-null Model snapshots. Only Stream materialization gives authoritative positions. Mutation acknowledgments persist execution independently of settlement. Accepted settlement reconciles required input targets through real Stream evidence or retained call-owned private settlement. Declared Model/scalar outputs remain the immutable invocation result; returning them does not install separate Store authority.

Application-owned transactions acquire the persisted publication fence before relevant business work. A Query that declares track may need a complete Serializable retry; earlier output is discarded. Returning/storing a Model alone never upgrades membership. Save external effects transactionally and execute them after commit; handler retries are observable application behavior.

## 10. Quality Requirements

Registration rejects missing/extra/wrong-kind versions; a Loader returns aligned exact contract shapes and malformed values are never absence. Evidence: [backend contract suite](../../../../../integration/action-runtime-ts/backend.test.mts), [real persistence tests](../../../../../integration/persistence/server) and [Action E2E](../../../../../integration/action-e2e).

Frozen retry preserves the outcome without rerunning business work, preparation or publication. Versioned materialization uses a fenced view, and Query tracking retries preserve that correspondence. Historical v0.4.2 evidence: [protocol-4 server tests](https://github.com/zanminwang/axton/blob/v0.4.2/integration/persistence/server/protocol-v04.test.mjs) and [production transport cases](https://github.com/zanminwang/axton/blob/v0.4.2/integration/0.4/production.test.mjs). Consult [verification](../../../testing/0.4.md) for exact scope; a contract fixture is not PostgreSQL isolation evidence.

Current source coverage for replay and finite read/tracking boundaries is [protocol-5 server tests](../../../../../crates/server/tests/protocol_v05.rs), [delivery plans](../../../../../crates/server/tests/delivery_plan.rs), and [real protocol-5 persistence tests](../../../../../integration/persistence/server/protocol-v05-delivery.test.mjs). Their execution belongs to the joined gate; archived 0.4 results are not current isolation evidence.
