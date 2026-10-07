# AXTON internal refactor specification

## 1. Goals and status

Simplify AXTON around a durable Mutation Batch and one range-based Downlink pipeline, retaining typed APIs, offline local work, explicit tracking and the Rust-owned runtime.

This is a **proposed implementation specification**, not a report of shipped behavior. The [original design](2026-10-07-uplink-downlink-design.md) remains frozen. The [review](2026-10-07-internal-refactor-review.md) records why the sketch needs refinements. The [plan](../plans/2026-10-07-internal-refactor.md) owns execution order and verification.

The confirmed structure is preserved. The following are recommendations introduced by this specification: transient DeliveryPlan storage, conservative constraint units, retained receipt-owned target evidence, normalized scalar arguments, and a fresh-file-first adoption boundary. They must not be described as earlier user decisions.

## 2. Architecture constraints

- One Client exclusively owns one physical SQLite file and one Stream.
- SDKs expose language-facing APIs and perform network I/O. Rust owns orchestration, persistence, retries, apply, settlement and notifications.
- Preserve the open/submit/drain/detach binding carrier and request/effect/event correlation.
- One unacknowledged Mutation Batch per Store; batch IDs increase strictly by one.
- One cloud database transaction per Mutation, not per Batch.
- One publication cursor per Mutation per affected Stream; equal cursors are legal.
- Track is explicit. Query/Fetch output never automatically enrolls a record.
- Query/Fetch records have cursor:null. Both store:true and store:false remain available; the default is true.
- No onStore hooks, per-record stamps or new generic one-time-load API.
- The frozen design document is not edited by implementation tasks.
- Fresh-file implementation comes first. Opening an unsupported existing Store must fail without altering it.

These are source changes based on v0.4.2 or newer main. Capso changes, SYN-18's broad input-syntax redesign and automatic 0.3/0.4 file conversion are outside this implementation.

## 3. Public behavior

### Opening a Store

```dart
final client = await GeneratedClient.open(
  path: 'account.sqlite',
  stream: 'User:$backendUserId',
  connection: StoreConnection(
    url: axtonUrl,
    token: () async => await getToken() ?? '',
  ),
);
```

Remove public StoreIdentity and mandatory backend/viewer/contract fields. Retain offline opening, exclusive physical-file locking and an internal random Store ID. Persist the Stream on first open; refuse reopening the file against another Stream. The caller owns account/backend path separation. Credentials authenticate requests; the cloud validates principal and Stream independently of the client-provided Store ID.

Generated schema descriptors and retained Mutation versions remain framework metadata. They are not replacement identity parameters the application must provide.

### Mutation, local transaction and completion

Preserve typed Mutation calls, transaction scoping, nested rollback, device-only companions, prerequisites and the two completion boundaries:

1. The submission await returns after the local transaction commits its queue and optimistic changes.
2. Call.wait completes after a definitive outcome has been reconciled locally and persisted.

A Call inside an uncommitted transaction remains provisional. Network acknowledgement alone does not resolve its wait. A transport failure is unknown/retryable, never a business rejection. A rejected Mutation rolls back only its own work; later direct writes and other Mutations keep their order.

The bridge may continue using an opaque callId string for SDK handles. It represents `(storeId, mutationId)` locally and is not a second cloud deduplication key or a second persisted intent.

### Reads and initial loading

Keep generated Fetch by Model/ID and named application Queries. Remove Query once, refresh, invalidate and the durable Query result cache. Each successful invocation returns its own result snapshot after allowed cache writes commit; it does not return a subsequently changed live object.

For store:true, decide per record inside the local transaction:

- Current Stream content/deletion protection blocks an ordinary snapshot write.
- With no current protection, a non-null snapshot may fill or replace cursor-null content.
- Ordinary absence does not delete local content or manufacture a tombstone.
- Historical delivery evidence alone does not block a snapshot.
- Pending local operations are replayed over any changed base.
- Preserve the existing declared-cascade read guard: a currently authoritative parent deletion can suppress a returned dependent cache row. An unloaded parent or a device-only deletion does not invent such authority.

store:false writes no Model content. Neither mode manufactures a Stream position, waits for an unrelated cursor, or enrolls returned IDs. Explicit track remains available to backend handlers through their authenticated Stream context.

Bootstrap is the durable finite initial load. The existing Bootstrap handler may explicitly track the application's initial scope; its committed preparation must be idempotent per Store. The initial handshake captures startCursor only after that preparation commits. No handler reruns after a committed preparation merely because a response was lost. Subsequent Bootstrap calls join/resume that Store's progress and return after its final data commit.

Preserve schema reconciliation as a separate internal materialization purpose. Native open compares generated descriptors; a compatible change schedules authority materialization for held identities and any newly selected Bootstrap Models. Keep S/B/C unchanged and persist that transfer's descriptor context/progress in DeliveryProgress. Its completion enables the new descriptor context; it must not reinterpret the original B=S as evidence that new fields/Models were loaded. Retained Mutation descriptors continue to reconstruct already queued work. This is not a new public load API.

## 4. Durable state

Names here are logical; SQL framework tables use the `axton_` prefix. User Model tables remain schema-named. Nullable counters distinguish not-started from the valid empty boundary zero.

### Local tables

| Table | Required facts and lifetime |
| --- | --- |
| Store | Singleton: random id, bound stream, storage format, schema/materialization metadata; nextMutationId, nextLocalSequence; lastAcknowledgedBatchId initially 0; startCursor initially null; bootstrapCursor initially null; cursor initially null. Remains for the file's lifetime. |
| MutationQueue | id PK, name, descriptorVersion, frozen descriptor context, batchId nullable, syncCursor nullable, result nullable, targets nullable, rejectionCode/message nullable, reconciled boolean initially false. One master row from enqueue through durable completion; accepted completion retains the row, with operation payload removed after settlement. Rejected work retains operations until explicit dismissal. |
| MutationQueueOperation | `(mutationId, step)` PK; localSequence; inputPath nullable; model/identity nullable only for arguments; operation; value. This is the only source for request input and owned local operations. |
| LocalWrite | Ordered independent writes and accepted companions: sequence, owning/original operation position, Model, identity, operation, value. Retain while needed to rebuild visible state; retire only when superseded by later authority under the existing replay rules. |
| RecordState | `(model, identity)` PK; historical content position, current optional content/deletion protection, membership position, materialization generation. Retain absence/removal evidence. No independent row version counter. |
| DeliveryProgress | At most one active transfer per coverage lane, plus owned settlement/schema materializations: purpose/owner, planId/digest, header, nextUnit, received parts of an incomplete unit. Advancing nextUnit and the relevant Store cursor is atomic with the unit. Materialization progress does not advance B/C. Clear completed/expired staging after commit. |
| X / before X | Visible Model rows and base/before images used by the existing optimistic rebase algorithm. Same schema-defined columns. RecordState stores authority metadata instead of inserting it into application fields. |

Schema history, dependency/prerequisite metadata and existing SQLite coordination metadata remain where they have independent responsibilities. This list is the sync design, not permission to delete every unlisted engine table.

The reconciled bit is required even for a successful Mutation with zero Model operations. It distinguishes saved server outcome from committed local completion; it does not duplicate a general pending/sending/failed state enum. Queue selection excludes acknowledged rows by batchId, regardless of when reconciliation completes.

Completed accepted rows retain their result for durable Call lookup. No time-based result eviction is introduced in this refactor. This preserves current durability and leaves result retention as an explicit future policy rather than silently losing completion after cleanup.

### Reconstructing typed input

Each server-bound operation has an inputPath identifying a generated descriptor slot and optional list index. Model operations retain create/update/delete payloads. A scalar/object argument is an `argument` operation with no Model or identity. Explicit null and empty lists have argument entries; omitted arguments have no entry. Model lists use their ordered slot indices. No entry stores a second copy of the entire input.

Device-only companions have a null inputPath and never enter the wire request. Declarative derived operations keep their original ownership but are regenerated/validated by the schema rules, not misclassified as additional server input. Retained descriptors reconstruct and validate the exact canonical input before freezing a Batch.

### Cloud tables

| Table | Required facts and lifetime |
| --- | --- |
| Store | id PK; authenticated principal binding; stream; lastProcessedBatchId; progress; current batch digest/count; last completed batch digest/count; Bootstrap preparation state. Lock before validating/resuming a Mutation. |
| MutationResult | `(storeId, batchId, mutationId)` PK; syncCursor, typed result, target manifest, rejectionCode/message. Retain last completed and current Batch only. |
| Stream | stream PK, head. Lock affected Streams in deterministic order. |
| Record | id PK, model, canonical identity_key unique with model, generated identity JSON. No stamp. |
| StreamRecord | `(stream, recordId)` PK; cursor; kind; durable tracking/membership. Retain absence and explicit Remove evidence. Distinguish membership removal from a Loader returning null. |
| PublicationFence | One row id=1. A real row update/lock before authority-producing business work establishes the Serializable publication fence. No redundant held flag. |
| DeliveryPlan | planId PK; authenticated scope, descriptor context, immutable header/digest and expiry. Temporary frozen authority transfer, shared by Bootstrap and compacted repair. |
| DeliveryUnit | `(planId, unitIndex, partIndex)` PK; immutable payload/part digest; unit coverage and completion metadata. Cascade cleanup with the plan. |

StreamRecord replaces StreamMember/StreamLog. A newly tracked pair receives a published position before its transaction commits; a repeated live track does not advance head. The sketch's nullable pre-publication state can exist within a transaction, but cannot be a committed live member that Bootstrap cannot discover. Invalidation updates existing holders only; it does not enroll. Explicit removal retains its cursor and membership meaning.

DeliveryPlan is not publication history and does not redefine membership. Expiry is permitted because committed client progress plus current state can construct a replacement. It is still real storage, intentionally replacing rather than pretending to eliminate the need for a stable paged read.

## 5. Uplink protocol and lifecycle

Use a new wire discriminator, proposed `protocol:5`, and a new local format. Preserve the language bridge carrier; its command payloads evolve with generated SDKs. Do not release the new wire as a compatible protocol-4 patch.

### Messages

```typescript
type MutationRequest = {
  protocol: 5;
  storeId: string;
  stream: string;
  batchId: number;
  digest: string;
  mutations: Array<{
    id: number;
    name: string;
    version: number;
    descriptor: string;
    operations: Array<{
      step: number;
      inputPath: string;
      operation: 'argument' | 'create' | 'update' | 'delete';
      model: string | null;
      identity: unknown;
      value: unknown;
    }>;
  }>;
};

type BatchAcknowledgement = {
  protocol: 5;
  storeId: string;
  batchId: number;
  digest: string;
  results: Array<{
    mutationId: number;
    outcome:
      | { kind: 'accepted'; syncCursor: number;
          result: unknown; targets: SettlementTarget[] }
      | { kind: 'rejected'; code: string; message: string | null };
  }>;
};

type RecordKey = { model: string; identity: unknown };
type ReadRecord = { key: RecordKey; cursor: null; state: unknown | null };
type SettlementTarget =
  | { kind: 'stream'; key: RecordKey; cursor: number; fallback: ReadRecord }
  | { kind: 'private'; record: ReadRecord };
```

Counters use the existing checked JSON-safe integer bound. Reject overflow before enqueue/publication. Digest binds the canonical, ordered, versioned request body with a Batch-specific hash domain; exclude the digest field itself. The server validates canonical input, duplicate IDs/steps, descriptor versions, principal and Stream before executing handlers. Reusing a Batch ID with a different body is a protocol error.

### Local

1. Enqueue all operations and visible optimism in the user's SQLite transaction. Allocate IDs without reusing committed IDs.
2. Restore a Batch newer than lastAcknowledgedBatchId, or atomically assign lastAcknowledgedBatchId+1 to eligible unassigned Mutations. Select by ID after prerequisite readiness. Do not modify assigned payloads, remove members or append newly queued work.
3. Requester emits an HTTP effect through the SDK. Unknown outcome, timeout and reconnect retry the exact Batch. Closing the client leaves it durable.
4. Validate the complete acknowledgement: matching Store/Batch/digest and exactly one outcome per member, with no unknown members. In one transaction, save accepted outcomes, rebuild rejected ownership and save its completion, then advance lastAcknowledgedBatchId. Invalid/partial acknowledgement or failed rebuild advances nothing. Retain/retry the response or request the same Batch again; cloud replay returns its saved outcome without another business execution.
5. Accepted outcomes remain durable while awaiting the target/coverage rules below. Their later reconciliation failure retries locally; it never becomes a fabricated business rejection. The acknowledgement transaction already completed rejection rollback.

The acknowledgement commit may unblock the next independent Batch. Prerequisites keep dependent work unsendable until its dependencies meet the existing success/readiness requirements. Waiting is event/timer driven in Rust, not a polling loop in Dart/JS.

### Cloud

1. Authenticate and validate Store binding, Batch ID and digest. For a fresh Store accept Batch 1. For lastProcessedBatchId replay saved results; for lastProcessedBatchId+1 resume; reject skipped or older IDs without execution.
2. In each Mutation transaction, lock Store and recheck sequence, digest and progress. Duplicate concurrent requests skip already committed work.
3. Establish the publication fence before relevant application reads/writes. Run the handler under a savepoint. Lock affected Streams in sorted order and reserve one position for all this Mutation's changes in each Stream. Roll back the savepoint on a business rejection.
4. Save outcome/targets and increment progress in the same transaction as business changes/publication. Transient database or host execution failure aborts the transaction; it is not a business rejection.
5. The final Mutation updates lastProcessedBatchId and resets progress. Return all saved results. A lost response is replayable. Prune the previous completed Batch only after a valid next Batch proves acknowledgement of it.

For an accepted no-publication Mutation, syncCursor is the initiating Stream head observed under the same fence; targets may be empty. A Mutation publishing only to other Streams still settles for its initiating Store through private target evidence. No implicit tracking is added.

### Acceptance and optimistic settlement

An accepted Mutation is eligible only after its syncCursor is covered by normal Sync and each target is satisfied. If syncCursor predates startCursor, that numeric test alone is insufficient: installed per-key evidence or explicit materialization is still required.

- A Stream target requires compatible installed authority at its cursor or newer. Missing authority triggers a targeted versioned materialization through the Downlink queue. Materialization installs evidence but proves no additional range coverage.
- A later installed membership Remove may make the original authority unreachable. Use the receipt's same-transaction fallback only to finalize this Mutation's owned null state, preserving newer authority and later local writes.
- A private target finalizes only its owned state. It creates neither membership nor positive cursor evidence; current authority wins.
- Empty targets require only outcome, syncCursor coverage and local ownership cleanup.

Reconciliation, companion finalization, surviving operation replay and reconciled=true commit together. Emit Call completion and watch changes afterward. Retain result and rejection visibility across reopen. This is one durable Mutation master record, not a restored Call-intent table.

## 6. Downlink protocol and finite coverage

### Initial connection

Handshake body identifies only stream. Authentication and protocol negotiation remain transport metadata. Cloud checks access, completes any initial Bootstrap preparation, and returns head. No client cursor is needed to establish the socket.

Rust atomically sets startCursor=S and normal cursor=S on first successful initialization; Bootstrap progress remains null. C=S means normal Sync starts after S, not that all pre-S Model content was installed. Reconnect retains S/B/C and uses the new head to repair missed coverage. The connection observes publications after its handshake head; HTTP repairs any disconnect/race.

Store ID is carried as authenticated request metadata when needed for durable preparation/deduplication. It is not another user-supplied identity setting.

### Delivery shape

```typescript
type DeltaRequest = {
  after: number;
  through: number;
  bootstrap?: boolean; // default false
  continuation?: { planId: string; unit: number; part: number };
};

type AuthorityChange =
  | { kind: 'record'; key: RecordKey; cursor: number; state: unknown | null }
  | { kind: 'remove'; key: RecordKey; cursor: number };

type DeliveryHeader = {
  planId: string;
  digest: string;
  stream: string;
  materialization: string;
  bootstrap: boolean;
  after: number;
  through: number;
  observedHead: number;
};

type DeliveryUnit = {
  index: number;
  through: number | null; // null installs data without advancing coverage
  changes: AuthorityChange[];
};
```

Each network fragment names the immutable header, unit index, part index/count and payload digest. Only a complete verified unit can be applied. Empty units prove scanned coverage. A unit with through=null leaves the lane's cursor unchanged. In particular, with S=0 it must not turn a null B into 0 until the final required unit commits.

A materialization request names purpose (`settlement` or `schema`), descriptor context and explicit keys; a schema request can also select newly Bootstrap-marked Model names within the bound Stream. Its immutable response names plan/owner/context and contains authority units without range coverage. It neither enrolls unknown keys nor advances B/C. Schema rematerialization admits equal positions only through the compatible descriptor-generation rules, preserving later local layers. Settlement materialization cannot bypass ordinary duplicate-position checks.

### Why Bootstrap may contain records newer than S

The first request freezes a finite current projection under the publication fence at head H>=S. Include all selected current StreamRecord pairs with cursor greater than committed B, including pairs moved beyond S. Resolve their Loaders in that same transaction, retaining explicit null/Remove results. Freeze payloads before returning the plan; later pages read staged payload, not newer business state.

The logical coverage remains `(B,S]`. Record positions may be newer, bounded by H. Apply those records using per-key Stream evidence; they do not independently advance normal C. B reaches S only after every required unit in this plan commits, including its newer records. A normal Sync delivery that already installed a newer version wins.

This preserves the three Store numbers. It refines “0 to S” to mean initial coverage, not a historical snapshot of content at S. The design does not promise historical Loader versions.

If the plan expires, discard only its incomplete staging and request a new plan from persisted B. A record that moved after an earlier plan is included at its current position. No tail repeatedly chases a moving head. Empty Bootstrap commits B=0 when S=0, so absence of a boolean is unambiguous.

### Normal Sync and constraints

Live WebSocket delivery carries explicit range coverage and complete publication units. A record cursor jump by itself is never proof of a gap or proof of coverage. HTTP repair freezes all current changes in `(C,H]` at one observed H; H is finite for that request, independent of later writes.

The first safe replacement for PublicationGroup is deliberately conservative:

1. Build the full candidate set for the finite compacted range.
2. Within the selected delivery set, union records sharing a publication cursor. Union changed rows of each Model with a unique constraint. Union affected records across declared cascade/dependency edges that require atomic reconstruction. Repeat transitively. Bootstrap filtering does not enroll or fetch excluded Models merely to reconstruct a historical publication group.
3. Freeze each resulting component as an atomic final-state unit. Order units by their earliest covered cursor.
4. After each unit, advance coverage only to the boundary before the earliest remaining candidate; never split a shared cursor. A unit may install later records before the complete prefix reaches them. For Bootstrap, newer-than-S candidates also block final B=S.
5. In SQLite, stage final values, clear conflicting old values, install the final base, then replay local operations and validate constraints before commit.

Relations that are navigation metadata do not become new SQL existence constraints. Missing untracked parents do not enroll/block a record. Preserve existing declarative cascade ownership and publish canonical child deletions explicitly.

Network pagination is independent of atomic units. A large component may span pages and requires disk staging. Exceeding the configured staging bound returns a capacity error with unchanged progress; it must not silently split the unit. Smaller units are an optimization only after equivalence tests. This does not promise a local commit for every network page.

For `through` below a newly observed head, retain the same rule as Bootstrap: include ahead-of-boundary records required to prove the requested coverage. A simple `cursor <= through` query cannot safely implement a compacted range.

## 7. Rust runtime ownership

| Component | Responsibility |
| --- | --- |
| MutationDispatcher | Restore/fix the next Batch, drive retries and submit receipt handling. |
| MutationBatcher | Atomically select eligible rows and freeze their assignment. |
| MutationRequester | Build the immutable request and emit HTTP effects; return response or transport outcome. |
| MutationAcknowledgementHandler | Atomically save accepted outcomes, roll back rejected ownership and advance the acknowledged Batch; schedule accepted settlement. |
| DownlinkEngine | Connect/handshake/reconnect; coalesce Bootstrap/repair/materialization requests; accept network completions into the queue. |
| DownlinkQueue | Bounded range buffer with separate Bootstrap/Sync coverage indexes and targeted materializations. |
| DeltaWorker | Independently consume complete applicable units; request missing coverage from Engine; report commits. |
| DeltaApplier | Execute one atomic Store unit and post-commit notification through the existing runtime path. |
| StreamConnection / DeltaRequester | Rust effect adapters controlled by Engine. SDKs perform their socket/HTTP I/O and route results back. |

Use a Rust control task for the Engine and a Rust Store worker thread for SQLite/apply. The Store worker owns the single writer and serializes local transactions, Query cache writes, receipts and delta apply. Engine handles network events while the Store worker is busy. SDK threads never schedule retries or decide gaps.

Retain requestId -> task waiter, effectId -> outstanding effect, observerId -> subscription and opaque callId -> completion routing. These are correlation registries, not extra sync queues. Register before dispatch; remove/settle exactly once. Connection generation fences reject late results after close/reset without replacing persistent Store identity.

The queue is recoverable memory, not a second authoritative log. Persist only incomplete transfer staging and committed unit progress. A crash discards uncommitted queue entries and refetches from B/C. Reserve capacity for the earliest missing repair; pause HTTP producers and disconnect/recover an overflowing socket rather than lose data while advancing progress. Coalesce one request per missing lane/range; use a Rust timer for backoff.

Bootstrap and Sync cannot share one scalar priority order. Their units connect to different progress counters but enter the same writer. For overlapping deliveries, validate context and immutable plan; skip only units whose exact coverage is already committed, or restart a straddling plan from persisted progress. Do not trim arbitrary record arrays into fabricated coverage proofs.

Post-commit notifications reuse Client.write/notify and runtime observer invalidation. Requery only affected watches; emit changed results. Rollback emits no committed changes. An observer callback failure cannot roll back or replay a committed delivery.

## 8. Removal and retention map

| Existing 0.4 responsibility | Target |
| --- | --- |
| axton_mutation + axton_v04_call + separate completion intent | One MutationQueue lifecycle record; no duplicate frozen args/intent. |
| axton_mutation_operation + separate per-op generation | MutationQueueOperation plus retained ownership/descriptor facts. |
| axton_rejection | Rejection fields and retained operations on MutationQueue. |
| axton_query_cache / once controls | Removed from new format and APIs. No automatic substitute cache. |
| axton_client push/epoch and legacy subscriptions/load ledgers | Replaced by Store Batch/progress fields in the new format; legacy paths not exposed by the new runtime. |
| axton_v04_page / bootstrap manifest, identity and range bookkeeping | DeliveryPlan/DeliveryUnit on cloud and DeliveryProgress locally, only after R1/R2 proof gates. |
| axton_stream_member + axton_stream_log | StreamRecord with nonunique cursor. |
| axton_record.stamp / local stamp tables | Removed after locking/evidence replacement is verified. |
| axton_publication_group | Removed after compacted final-state unit construction passes constraint tests. |
| axton_publication_fence.held | Remove column; retain real fence operation. |
| axton_call / legacy client sequence receipt | New Store + MutationResult on the new wire path. |
| LocalWrite, Model before images, constraints, prerequisite/dependency state, descriptor history | Retained where semantically required; storage consolidation does not remove their guarantees. |
| v04 direct-read request/result persistence | Remove once-result reuse. Keep only bounded in-flight correlation/recovery metadata actually needed by the runtime; read handles are not a new durable offline queue. |

No dropped table in this map authorizes deleting an existing user's database. Add the new server schema forward; preserve legacy tables until the adoption gate explicitly retires the old wire.

## 9. Acceptance and release gates

The implementation plan must prove these behaviors with Rust state-machine tests, real SQLite fault tests, PostgreSQL concurrency tests and generated SDK tests as appropriate:

| ID | Required behavior |
| --- | --- |
| A1 | Exact same Batch retries after timeout/restart; changed digest or membership cannot execute. |
| A2 | Cloud crash after Mutation k resumes at k+1; a rejected k does not roll back prior successes. |
| A3 | Failed acknowledgement commit retries the same Batch without duplicate execution; saved acceptance survives later settlement failure. |
| A4 | Rejection/settlement preserves later direct writes, other Mutations and accepted companions. |
| A5 | Bootstrap catches a selected row moving 20 -> 45 when S=40; expires/restarts safely; S=0 completes. |
| A6 | Compacted unique transfer A@5/B@3 and shared-cursor groups apply atomically across transport pages. |
| A7 | Empty ranges advance; record cursor jumps do not fabricate gaps; partial/overlapping/late delivery cannot skip work. |
| A8 | Missing/untracked/removed receipt targets settle without implicit track or loss of canonical state. |
| A9 | Current authority blocks ordinary reads; explicit local operations clear current protection; historical evidence still rejects old Stream replay. |
| A10 | Queue overflow, offline/reconnect, plan expiration and close during apply lose no committed work. |
| A11 | JS, Dart and React Native share Rust decisions and commit-boundary watch/completion behavior. |
| A12 | Public StoreIdentity and Query once/refresh/invalidate disappear; store:false and auth refresh still work. |
| A13 | Same file through aliases/two processes cannot be opened twice; an old file is refused unchanged. |
| A14 | Scalar/null/omitted/list input and descriptor rollover reconstruct exact requests without a second input blob. |
| A15 | Multi-Stream publication reserves one cursor per Mutation per Stream; retry rolls back duplicate business writes. |

Before release, quantify staged bytes/transaction duration for large Bootstrap and unique-constrained catch-up, and verify cancellation/expiry cleanup. If the conservative unit strategy is unusable, revise the unit builder before deleting PublicationGroup; do not ship a knowingly incomplete fast path.

Publish under an explicit breaking release and wire floor after choosing the application adoption procedure. Old files and unacknowledged protocol-4 calls remain recoverable with the old release. A coordinated in-place upgrade requires a separately reviewed drain/migration design; it is not claimed by this fresh-file implementation.
