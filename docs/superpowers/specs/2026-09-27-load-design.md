# Durable Load Operations

Status: implementation contract handed off under #173. The user reports that another agent has started implementation. The 2026-09-27 once amendment below is user-authorized; this document update contains no implementation code.

Baseline: `ed566eb77f9b0b47f2c128bd212261d08687e324`. Branch: `codex/load-operations`.

## Intent and agreed direction

A native Load operation fills local Models in successive pages until the application backend reports completion. An application starts it once; Rust persists and schedules the work, applies each page through Loader authority and `onStore`, and resumes unfinished work after reopen. Loading does not require historical Channel enrollment.

The user approved a native operation, arbitrary serializable continuation state, one shared Load worker, opportunistic HTTP batching, bounded concurrent batches, independent job progress, and opt-in call-site once reuse with explicit refresh/invalidation. This document makes the remaining mechanical choices explicit. It does not implement query-driven synchronization or replace Channel Bootstrap.

Read the existing [runtime](../../engineering/architecture/client/runtime.md), [operation contract](../../engineering/architecture/schema/actions.md), [store pipeline](../../engineering/architecture/client/engine/pull.md), and [guarantees](../../engineering/guarantees.md) with this specification.

## Public schema

```graphql
load ProjectTodos(projectId String) {
  todos Todo[]
}
```

- `load Name(inputs) { outputs }` is a third native operation declaration. Braces and at least one output are required.
- Inputs use existing ordinary scalar, enum, nullable and list input rules. Model mutation operands and `@sequence` are invalid.
- Every output is a non-null list of Model identities resolved through the retained Loader contract. Multiple named Model lists are allowed. Empty lists are valid. Scalar, single-Model and nullable-list outputs are excluded in version one.
- A Load is read-only in business terms, like a Query. Its context has no `touch` or `channel`; framework claim, stamp and replay metadata are permitted writes. Arbitrary application SQL is trusted, not sandboxed.
- Load names share the normalized operation namespace with Mutations and Queries. `get`, `list` and `invalidate` are reserved Load names because they are management members on `client.loads`. The once/refresh policy is a call-site option, never a schema annotation or a business input.
- `@version(n)` defaults to 1. Breaking input/output changes require a version bump. The backend must retain the continuation interpreter for every supported Load version. Changing an opaque state format incompatibly requires a new version even though the compiler cannot detect that handler-only change.
- Use a separate `LoadDescriptor`, `Schema.loads` (default empty), and `history/loads.json`. Reuse normalization and history-comparison helpers rather than exposing Loads through Mutation/Query routes. A cross-kind name change is rejected while the previous declaration is retained; migration between kinds is outside this feature.

## Backend handler

```ts
type JsonValue = null | boolean | number | string |
  JsonValue[] | { [key: string]: JsonValue };
type LoadNext = null | { state: JsonValue };

async function projectTodos({ ctx, args, continuation }) {
  // null means the first request; {state: null} is a subsequent request.
  const page = await readTodoPage(ctx.tx, args.projectId, continuation);
  return {
    data: { todos: page.ids.map(id => ({ id })) },
    next: page.done ? null : { state: page.nextState },
  };
}
```

Generated `Loads<Tx>` registers versioned handlers beside `Mutations<Tx>`, `Queries<Tx>` and `Loaders<Tx>` in `createBackend`. Handler input is `{ctx, args, continuation}`. `continuation` is `LoadNext`: first call null, later calls the previous non-null `next` wrapper. Context exposes `tx`, authenticated `userId`, stable page `callId` and `loadId`. Handler returns the generated identity-only data shape plus `next`.

The wrapper distinguishes first/end from a legitimate null state. State is portable JSON, not JavaScript `any`: no undefined, functions, cycles, non-finite numbers, BigInt or class instances with custom serialization. Integers outside the JS safe range must be encoded by the application as strings. Dates are application-encoded strings. Validate before the JSON bridge can silently coerce values. Maximum canonical state is 64 KiB and nesting depth 64; an invalid state rejects that item as `load.invalid_continuation`. No monotonicity, ordering, or inequality between consecutive states is assumed. A backend may validly return the same state; it owns termination.

All declared data fields must be present. Resolve identity lists through existing Loader logic and stamps. Deduplicate canonical identities for authority application; repeated identities across lists are not applied twice. A missing/unauthorized identity or failing Loader fails this page; it cannot be silently skipped while advancing its continuation. Handlers should enumerate readable identities under the authenticated principal and transaction snapshot. The final empty page can complete the job.

The handler defines enumeration and consistency. Success means that traversal returned `next: null`, not that the client possesses all database data or an immutable multi-request snapshot. A cutoff, stable keyset order, backend snapshot token, or expiry policy belongs to the handler. No long-lived SQL transaction spans pages. Source changes, filters, and deletion must be considered by the application backend.

## Client API and identity

```ts
const load = await client.loads.projectTodos({ projectId });
// Local durable acceptance; works without a connection. No data yet promised.
await load.wait(); // Promise<void>; resolves after the final local commit.

load.id;
load.status;
const stop = load.watch(status => renderProgress(status));
await load.cancel();
await load.retry();
await load.forget();
load.dispose();

const restored = await client.loads.get(savedLoadId); // handle | null
const recent = await client.loads.list({ limit: 50 }); // status[], newest first
```

Dart generates `client.loads.projectTodos(...)`, `Future<Load?> get(String id)`, `list({int limit = 50})`, `Future<void> wait/cancel/retry/forget`, `Stream<LoadStatus> watch()`, `status`, `id`, and `dispose()`. TypeScript `watch` emits the current snapshot and later distinct snapshots; Dart `watch` does likewise. `list` accepts 1..100, defaults 50, and is a recent management view, not a page of backend business records.

An ordinary start (once omitted or false) creates a fresh UUID job, bypasses once lookup, and never registers its outcome for once reuse. Reattach by ID to resume observation. Opt-in once starts use the durable mapping defined below. Models are read/watched through existing local APIs. Load exposes no aggregate business result, public cursor, delivery override, or `store:false`: storing declared Models is its purpose.

`LoadStatus` contains `{id, name, version, phase, pages, error}`. Phases are `pending`, `loading`, `waiting`, `complete`, `failed`, `cancelled`. `pages` counts committed pages (including empty pages), not unique rows or a percent. `error` is non-null only for failure/cancellation and is a bounded `{code,message}`. `loading` describes an in-flight request or admitted page application; offline, paused, or backoff work is `waiting`. Durable phase and runtime connectivity project into this snapshot in Rust.

`wait()` attaches to the current run and throws its recorded terminal error on failure/cancellation. Client close rejects process-local waiters with `client_closed` but preserves work. Retry is explicit for a failed run: backend-terminal failure restarts the same uncommitted position with a new page call ID; a local store/hook failure keeps the successful backend page call ID so its exact authority and next state replay. Increment the run generation so old waiters cannot resolve from another attempt (`load.superseded`). Retrying active work is idempotent; complete/cancelled work requires a new start. Cancel is idempotent and fences late responses; already committed data stays. Cancel after completion leaves completion intact.

Dispose releases only that handle's observers, never cancels the job. The SDK must not keep disposed handles alive. Forget is allowed only for terminal jobs, removes the ledger row, and makes get return null; live handles fail future management calls with `load.not_found`. Management get/list/forget and handle disposal are deliberately included in version one so restart recovery and cleanup do not depend on retaining language objects. No automatic terminal-job retention in version one. This explicit cleanup keeps ledger lifetime separate from garbage collection.

Load start/refresh/invalidate/retry/cancel/forget and waits are unavailable inside an application transaction or `onStore`; these contexts keep their existing local-only capability. Async query/Model observation remains unchanged.

## Once reuse, refresh and invalidation (2026-09-27 amendment)

```ts
const load = await client.loads.projectTodos({ projectId }, { once: true });
await load.wait();

const refreshed = await client.loads.projectTodos(
  { projectId }, { once: true, refresh: true },
);
await client.loads.invalidate.projectTodos({ projectId });
```

```dart
final load = await client.loads.projectTodos(projectId: projectId, once: true);
final refreshed = await client.loads.projectTodos(
  projectId: projectId, once: true, refresh: true,
);
await client.loads.invalidate.projectTodos(projectId: projectId);
```

`LoadOptions` contains only optional Boolean `once` and `refresh`, both false by default. `refresh:true` without `once:true` is `load.invalid_options` before a job or network effect is created. Invalid option types are rejected at runtime as well as by generated types. Options belong outside TS business args. Dart uses the same collision fallback as Query: option parameters are `callOnce`/`callRefresh` when business inputs occupy `once`/`refresh`; validate final generated parameter collisions. Invalidation takes only business arguments. These controls never enter the backend wire, Handler arguments, descriptor or operation history, and require no backend operation version bump.

The durable reuse key is canonical `(Load name, Load version, normalized business args, retained output Model read-contract versions)` within the active replica file. Use the same input normalization as invocation: object key order, UUID case and equivalent date offsets normalize identically; list order and explicit null remain meaningful. No-argument Loads use `{}`. Never include continuation, page call ID, job/run ID, once/refresh flags, or access tokens in the key. Reuse is per replica, not globally by user ID: applications must use separate databases for different backends/accounts/tenants, as with Query once. Auth token refresh alone does not invalidate it.

| Current mapping | `once:true` | `once:true, refresh:true` |
| --- | --- | --- |
| None | Atomically create and register a fresh job | Same |
| Active (`pending`, `loading`, `waiting`) | Return a handle to that job | Join that job; do not restart it or create a duplicate refresh |
| Complete | Return a completed handle offline; `wait()` resolves locally | Atomically replace the mapping with a fresh job starting at the first page |
| Failed | Return the failed handle; no automatic retry or hidden request | Replace the mapping with a fresh job starting at the first page |
| Cancelled/forgotten | The mapping has been removed; create a new job | Same |

This is reuse of a durable job, not caching an aggregate result. A completed hit does not request pages, apply authority, rerun onStore, increment page counts, emit Model-watch changes, or restart work. Status observation still emits the handle's normal current snapshot. Multiple callers may receive distinct language handle objects but share the same job ID and underlying job. `retry()` on a reused failed handle follows the existing per-page replay rule and resumes its saved position; `refresh` always starts from the beginning unless it joins an already active job. A refresh replaces the old completed mapping on local acceptance, not only on success: if refresh fails, later once callers observe that failure, never silently fall back to an earlier completion. Existing handles to the old completed job keep their historical completion.

`loads.invalidate.name(args)` resolves after local commit, works offline, and removes every once mapping for that operation name and normalized arguments across retained Load/Model read-contract versions. Normalize against each retained version that accepts the provided args (using retained job/key contract metadata when necessary); delete those exact matches without conflating omitted and null inputs. It deletes neither Models nor job rows, cancels no running job and makes no network request. A later once call creates fresh work. An invalidated active job may still complete and update Models under normal stamp rules, but can never reinsert or replace the mapping. Call its handle's `cancel()` too if that work should stop. Completion, retry and network responses never create mappings; only an explicit once start/refresh may do so.

Cancel removes a mapping only if it still points to the cancelled job, atomically with cancellation; cancelling a completed job remains a no-op and retains its mapping. Forget deletes a terminal job and only the mapping still referencing that exact ID. Forgetting an old completed job after refresh must not clear the replacement. Disposing a handle affects neither job nor mapping. Invalidation followed by a once start may intentionally leave two distinct jobs running; their independent generations and record stamps protect normal storage, while the old job cannot restore reuse eligibility.

Once is an explicit application assertion that a previous enumeration is reusable, not a guarantee that Models remain complete/fresh or that permissions are unchanged. Local Model deletion, Channel changes and elapsed time do not automatically invalidate it. The application invalidates or refreshes after such changes when needed. Replica rebuild drops both job rows and once mappings; no old completion may satisfy a once call against an empty replica. A new Load or output Model contract version has a different key.

## Persistence and page replay

Add a dedicated `axton_load` framework table. It stores the UUID job, operation name/version, canonical immutable args, retained Model contracts, committed continuation wrapper, run generation, phase, committed-page count, current page call ID plus canonical frozen request, retry class, attempt count and bounded terminal error. Additive table creation preserves existing queues and subscriptions. Add `axton_load_once` with a unique canonical reuse key and a `load_id` reference plus retained key-contract metadata needed for normalized invalidation. Start lookup and job/mapping insertion share one SQLite transaction, so concurrent same-key starts resolve to one job. Refresh replacement, cancellation and forget use conditional mapping changes keyed by the referenced job ID. Missing referenced job/invalid mapping is a visible ledger error, not an implicit complete hit. Terminal completion lives in `axton_load` and is committed with the final page; there is no separate success flag in the mapping that can get ahead of store. Keep these tables separate from `axton_query_cache`, which stores typed Query results.

Before any page is sent, persist its fresh UUID call ID and exact normalized intent. A page intent includes the operation kind `load`, owner-scoped load ID, name/version, args, continuation and Model contracts. Retry never changes these bytes or uses a different identity until a backend-terminal failure is explicitly retried. HTTP batch identity and ordering are not part of the page identity.

Reuse the backend's atomic call claim/save storage. A Load-specific request fingerprint and response decoder prevent cross-kind replay. Per-page Handler execution, Loader reads, stamp initialization and saved page outcome share one application-owned Repeatable Read-or-stronger transaction. A repeated page ID returns its saved data/authority/next without rerunning Handler or Loader. Infrastructure failure rolls back claim/save and is retryable; a terminal handler/Loader rejection first rolls handler work back to its savepoint, then saves only the rejected outcome before the outer transaction commits. A committed terminal rejection is replayed until explicit retry creates a new call ID. Version one keeps saved outcomes without TTL or automatic pruning, exactly as existing guarantee Q2. This is a prerequisite of the page replay guarantee. Any future #61 retention policy must preserve outstanding page outcomes or retain enough identity metadata to reject an expired replay as `load.replay_expired`; it must never silently execute an expired page ID again. Expiring application continuation tokens remain a typed backend rejection.

Do not store a second durable client response inbox. A successfully received page may be held in bounded runtime memory for apply. If the app exits before commit, its persisted page ID fetches the exact server replay after reopen. This is the same reason a failed `onStore` must not refetch a new page under a fresh call ID.

Within one guarded local transaction: prepare incoming authority, run eligible `onStore`, apply authority, update continuation/page count, clear frozen intent, and set pending/complete. Any per-record validation/store failure or callback failure rolls the *whole page* back, including hook writes and progress, then records failed state in a separate short transaction. Add an explicit native-Load guarantee: the requested page is one typed read operation whose complete declared output must be applicable to count as loaded, analogous to a Query result. It shares fate within that page, but no sibling Load shares its fate. Clarify D7 as the existing Channel delivery/Bootstrap per-record rule; do not change those paths or silently broaden page rollback to them. Preserve affected Model/identity diagnostics on Load page failure. This is an intentional new operation contract, not a claim that current D7 already promises atomic Load pages. An older/equal already-known stamp is a valid no-op and still permits progress; equal-stamp divergent content is a page failure, not success. Failed preparation dispatches no hook. Commit/SQLite infrastructure failures advance nothing and retry with backoff. No success/observer/completion event precedes commit. Other jobs from the same HTTP response commit separately.

Local page applicability is fenced by replica generation, job ID, run generation and page call ID. Cancel/forget/retry/rebuild makes stale replies inert. Close rolls back an open callback, cancels effects and releases waiters without waiting for application callbacks, as today.

## Worker and batching

One Load worker per client runtime discovers pending jobs from SQLite and schedules ready pages. It is a Rust state machine, not a per-job thread and not a JS/Dart loop. Mutation ordering and Channel cursors are untouched.

Initial private limits: at most 8 items per batch, 2 HTTP batches in flight, and 1 outstanding page per job (request, waiting response, or waiting apply). Choose ready jobs in fair oldest-ready order; after a committed page, requeue that job behind ready peers. Coalesce jobs already ready at dispatch; no artificial batching delay. These constants are implementation defaults, not public schema settings.

Ready request bodies are at most 1 MiB. A page can return at most 1,000 identity entries across declared lists and at most 1 MiB of encoded outcome; a batch response is at most 8 MiB. Size failures are typed terminal item failures, not silent truncation. Server handler output/record size is checked before saving an oversized successful outcome. Release a batch slot only after its outcomes have been consumed or moved into per-job retry/failure state, bounding returned pages waiting behind the local writer. Avoid unbounded scans/materialization of all job bodies.

Rust emits existing transport effects for `POST /sync/loads`, shares auth refresh and connection pause/resume controls, and participates in the runtime's admission-order fairness. Network waits hold no SQLite writer. When store is busy, data application waits in the shared scheduler; do not bypass the transaction owner.

Transport failure, 429 and server availability/infrastructure errors use per-job capped exponential backoff with jitter (1 second base, 30 second cap); use the existing transport status and deadline signals; adding Retry-After headers to all bridges is deferred. No overall offline task timeout; each HTTP attempt uses the connection's finite request deadline. Persist attempts, not monotonic deadlines; on reopen recompute a bounded delay. A 401 joins shared refresh; refusal becomes `load.unauthorized`, without an infinite refresh loop. Validation, expired continuation, unsupported version, and declared business rejection fail only that job. A failing job never marks siblings failed merely because they share an HTTP request.

## Wire and server batch behavior

```json
{
  "loads": [{
    "loadId": "<uuid>", "callId": "<uuid>",
    "name": "ProjectTodos", "version": 1,
    "args": {"projectId": "p1"},
    "continuation": null, "models": {"Todo": 1}
  }]
}
```

Successful response items carry `{loadId,callId,outcome:{status:"succeeded",next},records}`. Terminal failure items carry `{loadId,callId,outcome:{status:"failed",error:{code,message}},records:[]}`; rolled-back infrastructure errors use `status:"retryable"` and cannot be persisted as terminal outcomes. The envelope is `{loads:[...]}`. Validate IDs, declared versions, bounds, outcome shape and authority contracts in Rust on both sides. Outcomes correlate by IDs, not array position. Batch responses have exactly one item per request; malformed, duplicate, extra or missing correlations reject the envelope before any item is applied, retaining frozen page identities for an explicit protocol failure/retry path. A malformed correlated page fails only that job once envelope correlation is valid.

Before opening transactions, a Rust batch validator validates the complete envelope, unique load IDs and call IDs, counts and byte bounds. Unknown operation/version or invalid business args remain attributable item rejections rather than preventing siblings from running. The HTTP carrier authenticates once and schedules at most 4 per-item database transactions concurrently. Each invokes the Rust Load executor and commits before its result enters the response. A batch is transport grouping, not a shared database transaction; do not implement it as one transaction with savepoints. Rust owns shape validation, operation dispatch constraints, normalization, claim/save, Loader authority and error classification; TypeScript owns hosting and application transaction execution as it already does. The host must also catch transaction/commit failures after Rust returns: uncertain commit becomes a retryable item and resends the same call ID, allowing server claim replay to determine what committed. No native success can be sent before its outer transaction commits.

Ordinary HTTP returns after all items finish, so one slow item delays siblings within that response. Bounded batches, per-transaction timeouts and independent in-flight batches limit the impact. Streaming responses and adaptive batching are explicitly deferred.

## Channel, rebuild and ownership boundaries

A Load creates no Channel membership, cursor or subscription. It uses record stamps to coexist with live authority and pending local optimism. No record absent from an enumeration page is inferred deleted, and no query-result membership ledger is introduced.

An application that needs continuous updates establishes the relevant subscription and waits for a confirmed server boundary before starting Load. For a resumed subscription it must also ensure its required catch-up boundary is reached. The backend must actually publish relevant changes; this ordering alone cannot fix an unstable enumeration or incomplete publication. Do not add a mandatory Channel field or a new subscription-ready API as part of this feature.

Compatible schema reopen retains jobs and their frozen contracts. Removing a needed Load/Model contract fails the job as `load.contract_unavailable` rather than interpreting old state with a new handler. An incompatible replica rebuild fences and stops old read jobs, rejects existing handles/waiters with `load.schema_changed`, and includes the abandoned Load IDs in the rebuild report. The old database remains under existing retention behavior. No Load ledger row, once mapping, terminal completion or continuation is copied into the fresh empty replica; `loads.get(oldId)` on that replica returns null and the application starts a new Load. Reads must not postpone the existing Mutation drain indefinitely. Reattach/new start operates on the currently active replica only. Test this with target-schema onStore registration and old pending mutations.

The authenticated principal is checked on every page. Use the existing one-replica-per-user expectation and backend owner-scoped claim key; continuation is untrusted client input, never authorization. A Load's request IDs cannot authorize access to another user's saved outcome.

## Verification and non-goals

Required evidence: once hit/join/no-argument cases, canonical key isolation, offline reopen, failed reuse/retry, refresh replacement, invalidation-in-flight fencing, cancel/forget/dispose mapping ownership, and rebuilt-replica misses; parser/descriptor/history negative tests; wire and arbitrary-state round trips; server repeated-ID replay and per-item real-Postgres transaction isolation; SQLite reopen, atomic store/progress/hook rollback, stale response fencing, stamp ordering and schema rebuild; bounded batching/fairness/backoff simulations; generated TS, Dart and React Native host APIs; a real HTTP end-to-end scenario with multiple jobs, reopen and concurrent live/Mutation delivery. Run the full host gate before claiming implementation complete, and distinguish host tests from device runtime validation.

Not included: Oasis migration, package publication, Channel terminology cleanup (#152), single-identity shared fetching (#153), automatic query membership maintenance, snapshot creation, automatic once expiry/freshness, aggregate result retention, scrolling/demand-driven paging, public tuning controls, background execution while a mobile app is suspended, or deletion/eviction of loaded Models.
