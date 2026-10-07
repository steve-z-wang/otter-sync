# Backend interface

Protocol-5 policy and failure boundaries are owned by [protocol 5](../protocol/0.5.md). Rust owns synchronization; language bindings execute networking, timers and interface callbacks. Descriptions of protocol-4, epoch/Load jobs, once caches or legacy queue tables below are historical component context, not current public APIs.

## 1. Introduction and Goals

The server engine asks an application host to execute handlers, materialize viewer state and persist sync metadata in the same application transaction. The host does not independently decide authority, progress, retry outcomes or settlement.

## 3. Context and Scope

The [typed server API](../sdks/typed-api/server.md) describes application callbacks. The [database guide](../../../../website/docs/backend/database.md) describes pg/Prisma/Drizzle setup. Bound request contexts include backend/viewer/Stream/contract, incarnation and materialization; authentication and `authorizeStream` must admit them before request replay or business execution.

Mutation handlers operate on typed input in the application's transaction. Query handlers return business selections and can explicitly track; they have no framework invalidation/business-write capability. Bootstrap preparation only enrolls the application's initial scope. Every Model read uses its registered versioned Loader and current authenticated viewer, including Query/Fetch results, Stream authority and receipt recovery.

Loader output is one aligned exact row or null per input identity. Null is canonical absence only when materialized as Stream authority; ordinary Query/Fetch absence deletes no cached row. Throwing, malformed or incomplete output is a failure, never implicit null. Required failed delivery units cannot advance the committed prefix; already committed earlier units remain, and smaller fresh requests may recover independent prefixes.

## 5. Building Block View

[Host requests](../../../../crates/server/src/host.rs) and [host-contract types](../../../../packages/server/host-contract.mts) define native/application messages. [index.mts](../../../../packages/server/index.mts) executes application callbacks and validates registration. [PostgreSQL v04 functions](../../../../packages/postgres/src/persistence.mts) implement persisted publication groups, namespace fences, immutable response/manifest ownership and replay. [Protocol-4 server](protocol4.md) owns their rules and bounds.

Startup checks every retained handler kind/version and every registered Model read version. Loader omission denotes a device-only Model and forbids its remote use/publication; ordinary local CRUD still works. Returning wider database rows or misordered arrays violates the Loader contract.

## 6. Runtime View

Framework transactions use Serializable isolation. A persisted namespace UPDATE fence precedes relevant business/publication/materialization work. Retryable conflicts rerun the entire transaction and discard earlier callbacks' output; exhausted retries remain infrastructure failure. Saved outcomes, publications and business state commit together.

Preparation publications settle before the final canonical read. Materialization replay returns exact saved carriers without rerunning preparation. Finite manifests freeze identities/ordinals, not all historical payloads. Pages expose complete commit groups and proven safe progress; required companions may be ahead of that prefix but remain bounded by the observed head. The client cannot derive coverage or commit progress from a head/ACK alone.

## 9. Architecture Decisions

Custom hosts must implement the bound protocol-4 variants in the host contract, including fence acquisition, grouped members/positions, saved request ownership and immutable manifest/page storage. The [v04 database adapter](../../../../packages/postgres/src/persistence.mts) is the reference implementation. Legacy `guardRecords` stamps and Load-page helpers remain internal compatibility paths; they do not substitute for protocol-4 publication evidence. The separate [0.3 guarantee reference](../../guarantees-0.3.md) describes those obligations.

A receipt owns mandatory Mutation target recovery. Internal recovery materialization authenticates the retained accepted receipt, invokes no public Bootstrap handler, does not track/restamp and does not advance the client's prefix. Explicit multi-Stream publication commits server effects together but gives each local client an independent completion boundary.

## 10. Quality Requirements

The actual [persistence runner](../../../../integration/persistence/server/run.sh) includes fresh protocol-4 PostgreSQL fixtures and driver checks. [Assembled transport tests](../../../../integration/0.4/production.test.mjs) exercise publication, manifests, late reads, accepted receipt loss/SIGKILL and real SQLite COMMIT refusal. [Verification](../../testing/0.4.md) states their specific assertions and limitations. Typed fake-host registration checks establish shape/admission, not real isolation or publication order.

## 11. Risks and Technical Debt

Business writes outside the fenced transaction can break position/content correspondence; later invalidation does not repair an already inconsistent snapshot. Loader projections depending on unpublished external/time inputs require explicit projection-generation changes or materialized application data. Persistent required-group failure and capacity limits fail visibly instead of returning partial completion. No age-based request/tombstone pruning is introduced in 0.4.
