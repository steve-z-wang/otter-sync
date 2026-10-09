# Protocol 4 client

Historical carrier reference. Current behavior is owned by [protocol 5](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/architecture/protocol/0.5.md). This page does not promise support for old local files or an alternate current API.

## 1. Introduction and goals

The bound native runtime uses the [protocol 4 contract](../protocol/0.4.md) through the existing task, effect, connection and SQLite paths. This page describes the implemented engine; generated SDK facades have their own integration checks.

## 3. Context and scope

A Store belongs to one stable `{backend, viewer, stream, contract}` binding. Native open checks that binding before schema coordination writes and registers its sole Stream. Credentials refresh independently; credential bytes are never binding identity. Normal reopen preserves the delivery incarnation. A materialization ID hashes normalized Model read contracts, bootstrap selection and a stable projection generation. Complete old descriptors remain available for frozen queued Mutations; applying old version numbers to today's Model set cannot reconstruct an old context.

A physical SQLite file has one runtime owner, including alternate path spellings, symlinks, hardlinks and another process. The runtime holds an OS lock in a durable application-data registry keyed by the file's physical identity until close. Lock files are never removed on close or reset. The registry permits SQLite's committed-reader connection while excluding a second runtime. macOS/iOS use Application Support, Linux uses application data, and Android's host initializes a stable app-container directory before open. Unsupported physical identity or missing stable location fails explicitly. External deletion of ownership metadata while clients run is unsupported.

## 4. Solution strategy

Visible content, current Stream protection, retained per-identity authority (`G`) and covered Stream prefix (`C`) are different facts. A direct write changes local content and releases current live protection; pending optimism changes the projection without releasing its base evidence. Neither advances `G` or `C`. A true Stream absence retains deletion evidence. A membership Remove changes holding evidence, not Model content or a fabricated content version.

Ordinary Query/Fetch records carry a null cursor. With `store=true` they may fill unprotected cache, but never delete, establish Stream authority or advance `C`. A historical `G` alone does not block refill after a local delete. Current Stream protection and true Stream deletion do block ordinary stale cache. `store=false` validates and returns the invocation snapshot without writing Models or authority. Invocation output and Store projection can therefore differ. Query once preparation and dispatch failures release their transient flight and request token, so an identical invocation can fetch again; durable request records retain their existing lifetime. Concurrent invocations still join an actively dispatched flight.

Each Delta unit commits its entire final projection, authority and proven prefix atomically. Ahead-of-prefix records may be necessary for a constrained group; their content position does not imply that `C` reached them. A durable immutable page plan resumes after its last committed unit. Constraint or commit failure rolls back the whole failing unit while preserving earlier progress. Declared cascades delete device content without inventing child authority; newer independent child authority and retained local work during equal-position rematerialization remain protected.

The downlink carrier owns one active frozen plan across live and HTTP arrivals. It finishes that plan before considering a different page; an overlapping or ahead range retains its validated head as catch-up demand and is fetched afresh from the committed cursor, without trimming its proof or waiting for another publication. Same-page replay still validates the frozen digest. Transport close, pause and retry retain the active plan; local unit failure releases carrier ownership so a smaller independent prefix can be requested without crossing the failed group.

Real SQLite carrier regressions cover overlaps, failure recovery and lifecycle in [protocol04_downlink.rs](https://github.com/zanminwang/axton/blob/v0.4.2/crates/sqlite/tests/protocol04_downlink.rs).

## 5. Building block view

[Bound Store and evidence](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/protocol04.rs), [unit/manifest progress](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/progress04.rs), [receipt settlement](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/settlement04.rs) and [existing downlink worker](https://github.com/zanminwang/axton/blob/v0.4.2/crates/client/src/downlink04.rs) own the native rules. [SQLite](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/sqlite/src/lib.rs) owns SQL execution and physical-file locking.

## 6. Runtime view: materialization and settlement

Bootstrap keeps immutable finite manifest ordinal coverage separate from its captured tail. Initial public Bootstrap establishes its start boundary once; completion requires both full coverage and actual committed `C` reaching the fixed tail. Socket acknowledgement admits transport and cannot establish coverage. Manifest companions install atomically with items but cover no ordinal. Request limits may shrink after failed independent spans; required atomic groups remain indivisible.

Receipt-owned Materialize uses a saved accepted receipt to request otherwise missing target authority, including a historical unmarked target behind `C`. Its manifest has separate durable ownership and never initializes or advances `C`. Ordinary Fetch cannot substitute for this proof.

An accepted receipt is persisted as accepted-awaiting before locally fallible settlement. Settlement owns one transaction-local receipt assessment for drain readiness and Materialize missing proofs. Finalization reloads accepted ownership and reassesses evidence inside its write transaction; an earlier selection is only a scheduling hint. Frozen intent and retained descriptor are checked before active-materialization authority satisfies an old target; the client does not install an old shape. Private snapshots and permitted later-Remove fallbacks finalize only the original owned operation order, without setting `G` or `C`. Captured operation generations prevent acceptance from reviving work superseded by newer authority, even if a later direct patch released current protection. Later independent writes survive. Queue retirement and terminal Call completion commit together and can be read after reopen.

## 8. Crosscutting concepts: runtime lifecycle

Mutation-local callbacks collect device operations under an owned savepoint before returning input; input normalization, optimism and queueing commit together. Root, nested-scope and companion capabilities are opaque and unique to a runtime. Captured or foreign handles cannot join the current restricted callback. Observer notifications follow committed state.

Explicit `resetStore` preserves binding and file ownership, atomically clears replica state and creates a new incarnation. It refuses pending work unless explicitly discarded, reports abandoned Calls and fences old effects and observers. Close preserves durable work for reopen.

Bound native APIs reject custom onStore/storeHooks, Load commands, store-policy maps, anonymous mutation batches, legacy ACK/pull seams and additional Stream subscription. Device transactions, named Mutations, observers, schema constraints and declared relations remain. Unbound internal compatibility paths are not a protocol-4 migration API. See [runtime](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/architecture/client/runtime.md) for task/effect ownership and [storage](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/architecture/client/storage/README.md) for the SQLite boundary.
