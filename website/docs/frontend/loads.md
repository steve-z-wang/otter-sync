<!-- load-draft: verify against implementation -->
# Load data in pages

A **Load** fills local Models from your backend in successive pages until your backend says it is done. You start it once; AXTON stores the job in the local database, requests each page, stores its records through your Loaders and `onStore` callbacks, and resumes unfinished work after the app restarts. Your code reads the loaded records with the ordinary [Model APIs](client-api.md#model-apis).

Use a Load to fill a screen's data set, such as all Todos of a project, when it may span many requests. Use a [Query](client-api.md#mutations-and-queries) when you need one typed answer now, and a [subscription](sync.md#subscribe-and-observe) to receive later changes.

<!-- load-draft: verify against implementation -->
## Declare a Load

```text
model Todo {
  id String
  projectId String
  title String
  @@id([id])
}

load ProjectTodos(projectId String) {
  todos Todo[]
}
```

A `load` takes ordinary inputs, like a Query, and declares one or more outputs, each a list of a Model: `todos Todo[]`. Scalar outputs, single Models, nullable lists, Model operands and `@sequence` are not allowed. A Load shares its name space with Mutations and Queries, and cannot be named `get`, `list` or `invalidate`. `@version(n)` works as for other operations, and the compiler retains older versions in `history/loads.json` ([schema reference](../schema/reference.md#loads)).

<!-- load-draft: verify against implementation -->
## Implement the backend handler

<!-- load-draft: TODO confirm name -->
```ts
import type { Loads } from './generated/backend.ts';

// Tx is your database transaction type.
// Application-provided: one keyset page of Todo ids the user may read.
declare function readTodoIds(
  tx: Tx, userId: string, projectId: string, after: string | null, limit: number,
): Promise<string[]>;

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

Pass `loads` to `createBackend` beside `mutations` and `queries`. The handler receives `{ ctx, args, continuation }`; `ctx` has `tx`, `userId`, the page's `callId` and the job's `loadId`, and no `touch` or `channel`.

- **Continuation.** `continuation` is `null` for the first page and afterwards the previous page's `next`. Return `next: null` to finish, or `{ state }` to ask for another page; `{ state: null }` is a valid state, distinct from finishing. State is any portable JSON up to 64 KiB and 64 levels deep: no `undefined`, functions, `BigInt`, `NaN` or class instances. Encode dates and integers beyond the JavaScript safe range as strings. Invalid state fails the page with `load.invalid_continuation`.
- **Termination is yours.** AXTON never compares states and never ends a Load because a page was empty or repeated a state. A handler that always returns a `next` never finishes. A final empty page is fine.
- **Consistency is yours.** Each page runs in its own database transaction; nothing holds a snapshot across pages. Choose a stable order (a keyset, as above), and decide how rows that change, move or are deleted between pages are treated, for example with a cutoff stored in the state.
- **Read-only.** A Load must not change business data. AXTON cannot inspect your SQL, so this is your responsibility, as for Queries.
- **Authorization.** Enumerate only identities the user may read. Each identity is resolved through that Model's [Loader](../backend/api.md#loaders); a Loader that returns `null`, refuses or throws fails the whole page (`load.record_unavailable`, a rejection code, or `loader.failed`). The continuation comes back from the client, so never trust it for authorization.
- **Size.** A page may return at most 1,000 identities across its lists and 1 MiB of encoded records. A larger page fails with `load.page_too_large`, and retrying reads the same continuation again, so it fails again: reduce your page size in the handler.
- **Versions.** Keep a handler for every retained version. If you change what your state means in an incompatible way, add `@version(n + 1)`; the compiler cannot detect such a change.

<!-- load-draft: verify against implementation -->
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

Awaiting the start means the job is stored locally; it works offline and promises no data yet. `wait()` resolves once the final page is stored and throws a `LoadError` (TypeScript) or `LoadException` (Dart) with `code` and `message` if the Load fails or is cancelled. Each page commits on its own, so `watch` on your Models shows records as pages arrive. A Load has no result object: read the Models.

<!-- load-draft: verify against implementation -->
## Continuation versus Channel cursor

| | Load continuation | Channel cursor |
| --- | --- | --- |
| Owned by | Your Load handler | AXTON |
| Contains | Any portable JSON you choose | A publication position |
| Meaning | Where your enumeration continues | Which Channel changes a subscription has received |
| Ends | When your handler returns `next: null` | Never; delivery continues |

A Load creates no Channel membership, subscription or cursor, and completing it is not a snapshot: it means your handler finished its traversal and every page was stored. Records missing from a page are never deleted locally.

<!-- load-draft: verify against implementation -->
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

<!-- load-draft: verify against implementation -->
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

- **`refresh: true`** (only with `once: true`, otherwise `load.invalid_options`) replaces a completed or failed job with a new one that starts from the first page. If the recorded job is still running, it joins that job instead of starting another. The replacement takes effect at once: if the refresh fails, later `once` calls see that failure, not the older success. Handles to the older job keep its history.
- **`invalidate`** takes only the business arguments, works offline and forgets the recorded job for those arguments across retained versions, so the next `once` call starts a new job. It deletes no records and does not cancel a running job; that job can still finish and store its pages, but it will not be reused. Call its `cancel()` as well if it should stop.

<!-- load-draft: verify against implementation -->
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

- **`retry()`** restarts a failed job from its last stored page with a new request; pages already stored stay. It may fail again if the cause remains. On a running job it does nothing; a completed or cancelled job needs a new start. A `wait()` from before the retry rejects with `load.superseded`.
- **`cancel()`** stops the job and ignores any late response; stored pages stay. Cancelling a completed job changes nothing.
- **`forget()`** removes a completed, failed or cancelled job; `get` then returns `null`. Jobs are never removed automatically.
- **`dispose()`** stops this handle's observers only; the job continues.

Closing the client rejects pending `wait()` calls with `client_closed` and keeps every job; reopening resumes them. Load calls are not allowed inside `client.transaction` or an `onStore` callback.

<!-- load-draft: verify against implementation -->
## Keep loaded records current

A Load reads records once. To keep them current, subscribe to a Channel your backend publishes the changes to, wait until the subscription's `initialization` is `ready`, then start the Load. Changes published after that point arrive through the subscription, and record stamps make sure an older page never overwrites a newer change, whichever arrives first. This works only if your backend publishes every relevant change and your enumeration is stable.

<!-- load-draft: verify against implementation -->
## Batching and latency

AXTON sends ready pages of different jobs together without waiting to fill a batch. The current limits, which are internal defaults rather than settings, are 8 pages per request, 2 requests in flight and one page per job at a time. A request answers only when all of its pages finish on the backend, so one slow page delays the others in that request. Keep individual pages fast. A failing page affects only its own job.

Network failures, `429` and server errors back the job off, from 1 second up to 30 seconds, and retry the same page request, so the backend never runs a stored page twice. There is no overall timeout: an offline Load waits.

<!-- load-draft: verify against implementation -->
## Schema changes

A compatible schema change keeps every job. A job whose Load or Model version is no longer in the schema fails with `load.contract_unavailable`. When a schema change [rebuilds the local database](storage.md#change-the-schema), Loads do not move to the new database: existing handles and `wait()` calls reject with `load.schema_changed`, the rebuild report lists the abandoned Load IDs, `get` returns `null` for them, and a `once` call starts fresh. While the old database stays open for unsent Mutations, Loads pause: starting, retrying or invalidating a Load fails with `load.schema_pending`, while `get`, `list`, `cancel` and `forget` still work.

<!-- load-draft: verify against implementation -->
## Background and device limits

Loads run while the client is open. A mobile app that is suspended or closed makes no progress; its jobs continue when the app opens the client again. AXTON does not schedule background execution.

<!-- load-draft: verify against implementation -->
<!-- load-draft: TODO confirm name -->
## Errors

| Code | Meaning |
| --- | --- |
| `load.invalid_options` | `refresh` without `once`, or an option that is not a Boolean; nothing was started |
| `load.invalid_continuation` | The handler returned state that is not portable JSON or exceeds its limits |
| `load.record_unavailable` | A Loader returned `null` for an identity the page listed |
| `load.page_too_large` | The page exceeded 1,000 identities or 1 MiB; change the handler's page size |
| `handler.failed`, `loader.failed`, a `CallRejected` code | The handler or a Loader threw or rejected |
| `load.store_failed` | A record could not be stored locally; the page was not stored |
| `load.hook_failed` | An `onStore` callback threw; the page and the callback's writes were rolled back |
| `load.unauthorized` | Credential refresh was refused |
| `load.contract_unavailable` | The job's Load or Model version is no longer in the schema |
| `load.schema_pending`, `load.schema_changed` | A local schema rebuild is pending, or replaced the database |
| `load.superseded` | A `retry()` started a newer run than this `wait()` |
| `load.not_found` | The job was forgotten |
| `client_closed` | The client closed while waiting |
