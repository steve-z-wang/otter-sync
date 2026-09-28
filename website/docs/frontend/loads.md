# Load data in pages

A **Load** fills local Models from your backend in successive pages until your backend says it is done. You start it once; AXTON stores the job in the local database, requests each page, stores its records through your Loaders and `onStore` callbacks, and resumes unfinished work after the app restarts. Your code reads the loaded records with the ordinary [Model APIs](client-api.md#model-apis).

Use a Load to fill a screen's data set, such as all Todos of a project, when it may span many requests. Use a [Query](client-api.md#mutations-and-queries) when you need one typed answer now, and a [subscription](sync.md#subscribe-and-observe) to receive later changes.

## Declare a Load

```text
model Todo {
  id String
  projectId String
  title String
  @@id(id)
}

load ProjectTodos(projectId String) {
  todos Todo[]
}
```

A `load` takes ordinary inputs, like a Query, and declares one or more outputs, each a list of a Model: `todos Todo[]`. Scalar outputs, single Models, nullable lists, Model operands and `@sequence` are not allowed, and one Load cannot read a Model at two versions. A Load shares its name space with Mutations and Queries, and cannot be named `get`, `list` or `invalidate`. `@version(n)` works as for other operations, and the compiler retains older versions in `history/loads.json` ([schema reference](../schema/reference.md#loads)).

## Implement the backend handler

```ts
import type { Loads } from './generated/backend.ts';

// Tx is your database transaction type. readTodoIds(tx, userId, projectId,
// after, limit) is your query for one keyset page of Todo ids the user may read.
export const loads: Loads<Tx> = {
  async projectTodos({ ctx, args, continuation }) {
    const after = continuation === null
      ? null
      : (continuation.state as { after: string }).after;
    const ids = await readTodoIds(ctx.tx, ctx.userId, args.projectId, after, 200);
    return {
      data: { todos: ids.map(id => ({ id })) },
      next: ids.length < 200 ? null : { state: { after: ids[ids.length - 1]! } },
    };
  },
};
```

Pass `loads` to `createBackend` beside `mutations`, `queries` and `loaders`; it is required when the schema declares a Load. Register a bare function for a Load that has only version 1, or `{ v1, v2 }` for retained versions. The handler receives `{ ctx, args, continuation }`: decoded arguments (a `DateTime` is a `Date`), and a `ctx` with `tx`, `userId`, the page's `callId` and the job's `loadId`, and no `touch` or `channel`. It returns `data`, one list of identities per declared output, and `next`. The generated `{Name}HandlerOutput` type (`ProjectTodosHandlerOutput`) names that return value.

- **Continuation.** `continuation` is `null` for the first page and afterwards the previous page's `next`. Return `next: null` to finish, or `{ state }` to ask for another page; `{ state: null }` is a valid state, distinct from finishing. State is any portable JSON up to 64 KiB and 64 levels deep: no `undefined`, functions, `BigInt`, `NaN`, class instances or strings with a lone UTF-16 surrogate. Encode dates and integers beyond the JavaScript safe range as strings. Invalid state fails the page with `load.invalid_continuation`.
- **Termination is yours.** AXTON never compares states and never ends a Load because a page was empty or repeated a state. A handler that always returns a `next` never finishes. A final empty page is fine.
- **Consistency is yours.** Each page runs in its own database transaction; nothing holds a snapshot across pages. Choose a stable order (a keyset, as above), and decide how rows that change, move or are deleted between pages are treated, for example with a cutoff stored in the state.
- **Read-only.** A Load must not change business data. AXTON cannot inspect your SQL, so this is your responsibility, as for Queries.
- **Authorization.** Enumerate only identities the user may read. Each identity is resolved through that Model's [Loader](../backend/api.md#loaders); a Loader that returns `null`, refuses or throws fails the whole page (`load.record_unavailable`, the rejection code, or `loader.failed`), and so does a handler that throws (`handler.failed`) or throws `CallRejected` (its code). A failed page is saved: retrying it re-reads from the last stored page under a new request. The continuation comes back from the client, so never trust it for authorization.
- **Size.** A page may return at most 1,000 identities across its lists, duplicates included, and 1 MiB of encoded records. A larger page fails with `load.page_too_large`, and retrying reads the same continuation again, so it fails again: reduce your page size in the handler, then retry.
- **Return identities, not records.** TypeScript does not check extra properties on an object returned from an unannotated handler, so returning full `Todo` records compiles; AXTON then fails the page with `handler.invalid`. Map rows to `{ id }` as above, or annotate the return type as `Promise<ProjectTodosHandlerOutput>` to catch it at compile time.
- **Versions.** Keep a handler for every retained version. If you change what your state means in an incompatible way, add `@version(n + 1)`; the compiler cannot detect such a change.

## Start a Load and wait

=== "TypeScript"

    ```ts
    const load = await client.loads.projectTodos({ projectId: 'p1' });
    const stop = load.watch(status => console.log(status.phase, status.pages));
    await load.wait();
    stop();
    const todos = await client.models.todo.query({ where: { projectId: 'p1' } });
    load.dispose();
    ```

=== "Flutter"

    ```dart
    final load = await client.loads.projectTodos(projectId: 'p1');
    final progress = load.watch().listen((status) => print([status.phase, status.pages]));
    await load.wait();
    await progress.cancel();
    final todos = await client.models.todo.query(where: const TodoFilter(projectId: Present('p1')));
    load.dispose();
    ```

Awaiting the start means the job is stored locally; it works offline and promises no data yet. A Load without inputs still takes `{}` in TypeScript (`client.loads.recentTodos({})`) and no argument in Dart. `wait()` resolves once the final page is stored and throws a `LoadError` (TypeScript) or `LoadException` (Dart) with `code` and `message` if the Load fails or is cancelled. Each page commits on its own, so `watch` on your Models shows records as pages arrive. A Load has no result object: read the Models.

## Continuation versus Channel cursor

| | Load continuation | Channel cursor |
| --- | --- | --- |
| Owned by | Your Load handler | AXTON |
| Contains | Any portable JSON you choose | A publication position |
| Meaning | Where your enumeration continues | Which Channel changes a subscription has received |
| Ends | When your handler returns `next: null` | Never; delivery continues |

A Load creates no Channel membership, subscription or cursor, and completing it is not a snapshot: it means your handler finished its traversal and every page was stored. Records missing from a page are never deleted locally.

## Fresh start, once and reattach

=== "TypeScript"

    ```ts
    const fresh = await client.loads.projectTodos({ projectId: 'p1' });
    const reused = await client.loads.projectTodos({ projectId: 'p1' }, { once: true });
    // Keep reused.id in your app state to reattach after a restart.
    const restored = await client.loads.get(reused.id); // Load | null
    const recent = await client.loads.list({ limit: 20 });
    ```

=== "Flutter"

    ```dart
    final fresh = await client.loads.projectTodos(projectId: 'p1');
    final reused = await client.loads.projectTodos(projectId: 'p1', once: true);
    final restored = await client.loads.get(reused.id); // Load?
    final recent = await client.loads.list(limit: 20);
    ```

| Call | What happens |
| --- | --- |
| Without `once` | Always a new, independent job, even with the same arguments |
| `once: true`, nothing recorded | A new job, recorded for these arguments |
| `once: true`, that job is pending, loading or waiting | The same job; no second job starts |
| `once: true`, that job completed | The completed job, offline too: no request, no stored page, no `onStore` call, no Model change |
| `once: true`, that job failed | The failed job; nothing retries it automatically |
| `once: true`, that job was cancelled or forgotten | A new job |
| `get(id)` | The job with that ID, or `null`; use it to follow a job after a restart |
| `list({ limit })` | Status snapshots of recent jobs, newest first; `limit` 1 to 100, default 50 |

`once` is keyed by the Load's name and version, its normalized arguments (key order, UUID case and date offsets do not matter; list order and explicit `null` do) and the Model versions it stores. It belongs to the local database file, not to the signed-in user: use a separate database per account, backend or tenant. In Dart the options are `once` and `refresh`, or `callOnce` and `callRefresh` when your Load has inputs with those names.

`once` says an earlier load can be reused; it does not promise the records are still complete or fresh. Deleting local records, Channel changes and elapsed time do not undo it.

## Refresh and invalidate

=== "TypeScript"

    ```ts
    const refreshed = await client.loads.projectTodos({ projectId: 'p1' }, { once: true, refresh: true });
    await client.loads.invalidate.projectTodos({ projectId: 'p1' });
    ```

=== "Flutter"

    ```dart
    final refreshed = await client.loads.projectTodos(projectId: 'p1', once: true, refresh: true);
    await client.loads.invalidate.projectTodos(projectId: 'p1');
    ```

- **`refresh: true`** (only with `once: true`, otherwise `load.invalid_options`; TypeScript accepts `{ refresh: true }` at compile time and refuses it at runtime) replaces a completed or failed job with a new one that starts from the first page. If the recorded job is still running, it joins that job instead of starting another. The replacement takes effect at once: if the refresh fails, later `once` calls see that failure, not the older success. Handles to the older job keep its history.
- **`invalidate`** takes only the business arguments, works offline and forgets the recorded job for those arguments across retained versions, so the next `once` call starts a new job. It deletes no records and does not cancel a running job; that job can still finish and store its pages, but it will not be reused. Call its `cancel()` as well if it should stop.

## Status, cancel, retry and forget

`load.status` is `{ id, name, version, phase, pages, error }`. `pages` counts stored pages, empty ones included; it is not a percentage. `error` is `{ code, message }` after a failure or cancellation.

| Phase | Meaning |
| --- | --- |
| `pending` | Stored and ready for its next page |
| `loading` | A page request is out, or a received page is being stored |
| `waiting` | Offline, paused or backing off after an error |
| `complete` | The final page is stored |
| `failed` | Stopped by an error; see `error` |
| `cancelled` | Stopped by `cancel()` |

- **`retry()`** restarts a failed job from its last stored page with a new request; pages already stored stay. It may fail again if the cause remains. On a running job it does nothing; on a completed or cancelled job it fails with `load.not_retryable`: start a new Load instead. `wait()` on a failed job rejects at once with its error, so call `wait()` again after `retry()`.
- **`cancel()`** stops any job that is not complete or cancelled, with the error `load.cancelled`, and ignores any late response; stored pages stay. Cancelling a completed job changes nothing.
- **`forget()`** removes a completed, failed or cancelled job (`load.not_terminal` for a running one); `get` then returns `null`, and later calls through an old handle fail with `load.not_found`. Jobs are never removed automatically.
- **`dispose()`** stops this handle's observers only; the job continues. A handle you never dispose stays in memory until the client closes, because it keeps receiving status snapshots, so dispose handles you no longer watch. `get` returns a new handle each time; several handles to one job share its ID and state.

Closing the client rejects pending `wait()` calls with `client_closed` and keeps every job; reopening resumes them. Load calls are not allowed inside `client.transaction`, a Mutation's `local` callback or an `onStore` callback.

## Keep loaded records current

A Load reads records once. To keep them current, subscribe to a Channel your backend publishes the changes to, wait until the subscription's `initialization` is `ready`, then start the Load. Changes published after that point arrive through the subscription, and record stamps make sure an older page never overwrites a newer change, whichever arrives first. This works only if your backend publishes every relevant change and your enumeration is stable.

## Batching and latency

AXTON sends ready pages of different jobs together without waiting to fill a batch. The current limits, which are internal defaults rather than settings, are 8 pages per request, 2 requests in flight and one page per job at a time. A request answers only when all of its pages finish on the backend, so one slow page delays the others in that request. Keep individual pages fast. A failing page affects only its own job.

Network failures, timeouts and server-side transaction failures back the job off, from 1 second up to 30 seconds, and retry the same page request, so the backend never runs a stored page twice. There is no overall timeout: an offline Load waits.

When the access token expires, AXTON asks your `refreshAuth` once, shared with other sync work; if it refuses with status 401 or 403, the Load fails with `load.unauthorized`, and any other refresh failure backs off. In TypeScript the refusal is an error with that `status`; in Dart an `HttpFailure` with that status or an `AuthenticationExpired`.

If the backend refuses a whole request with another 4xx status (not 408 or 429), AXTON sends each page of it in a request of its own, so one bad page cannot fail its neighbours; a page refused again on its own fails its Load with `load.protocol_invalid`. `408`, `429` and `5xx` statuses back off and retry. A frozen page too large to send even on its own fails its Load with `load.request_too_large`.

## Schema changes

A compatible schema change keeps every job. A job whose Load or Model version is no longer in the schema fails with `load.contract_unavailable`. When a schema change [rebuilds the local database](storage.md#change-the-schema), Loads do not move to the new database: existing handles end with a `failed` status whose error is `load.schema_changed`, pending `wait()` calls and every later call through those handles reject with that code, the rebuild report lists the abandoned Load IDs (`abandonedLoads`), `get` returns `null` for them, and a `once` call starts fresh. While the old database stays open for unsent Mutations, Loads pause: starting, retrying or invalidating a Load fails with `load.schema_pending`, while `get`, `list`, `cancel` and `forget` still work.

## Background and device limits

Loads run while the client is open. A mobile app that is suspended or closed makes no progress; its jobs continue when the app opens the client again. AXTON does not schedule background execution.

## Errors

These codes appear as `status.error.code` and on the error `wait()` or a management call throws.

| Code | Meaning |
| --- | --- |
| `load.invalid_options` | `refresh` without `once`, an option that is not a Boolean, or a `list` limit outside 1 to 100; in TypeScript also options that are not an object or name another option; nothing was started |
| `transaction_active` | A Load call from inside `client.transaction`, a Mutation's `local` callback or an `onStore` callback |
| `load.unknown` | No Load of that name and version in this client's schema |
| `load.invalid_args` | The start or invalidation arguments do not match the Load's inputs; nothing was written |
| `load_version_unsupported`, `load.invalid`, `model_version_unsupported` | The backend does not retain this Load version, refused its arguments, or does not retain a Model version the client stores |
| `handler.failed`, `loader.failed`, a `CallRejected` code | The handler or a Loader threw or rejected |
| `handler.invalid`, `loader.invalid`, `loader.unregistered` | The handler returned something other than identity lists (a full record, for example), a Loader returned malformed rows, or no Loader is registered |
| `load.invalid_continuation` | The handler returned a `next` that is missing, malformed, not portable JSON or over its limits |
| `load.record_unavailable` | A Loader returned `null` for an identity the page listed |
| `load.page_too_large` | The page exceeded 1,000 identities or 1 MiB; change the handler's page size |
| `load.store_failed` | A record could not be stored locally; the page was not stored, and your `onError` receives an `AxtonReport` naming each refused record's Model and identity |
| `load.hook_failed` | An `onStore` callback threw; the page and the callback's writes were rolled back |
| `load.protocol_invalid` | The response for this page was malformed, or the backend refused the page's request on its own with a 4xx status |
| `load.request_too_large` | The page's request exceeds 1 MiB even on its own: arguments that large fail the start and nothing is stored; a state that large fails the Load |
| `load.unauthorized` | Credential refresh was refused |
| `load.cancelled` | The job was cancelled |
| `load.contract_unavailable` | The job's Load or Model version is no longer in the schema |
| `load.schema_pending`, `load.schema_changed` | A local schema rebuild is pending, or replaced the database |
| `load.not_retryable`, `load.not_terminal`, `load.not_found` | `retry()` of a completed or cancelled job, `forget()` of a running one, or a forgotten job |
| `load.ledger_invalid` | A local job or once record is damaged; when a once record names a missing job, `invalidate` removes it so the next `once` call starts fresh |
| `client_closed` | The client closed while waiting |

`server.unavailable` and `transaction.conflict` are backend faults the client retries on its own; they never fail a Load.
