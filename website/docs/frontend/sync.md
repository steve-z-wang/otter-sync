# Sync, offline work and recovery

Local reads and writes go through the Rust engine and SQLite. A connection handles network work in the background. Your app can continue using its local data while the connection is paused or a response is delayed.

## Subscribe and observe

=== "TypeScript"

    ```ts
    const followed = await client.scopes.subscribe('book:demo');
    const stop = client.models.entry.watch({}, entries => render(entries), console.error);
    ```

=== "Flutter"

    ```dart
    final followed = await client.scopes.subscribe('book:demo');
    final subscription = client.models.entry.watch().listen(
      render,
      onError: (Object error) => print(error),
    );
    ```

Here `render` is your UI's update function. Subscribing records the desired channel durably - it works offline and survives a restart - and wakes the connection; it does not wait for the initial data. `watch` emits again when synchronization commits records.

**A subscription delivers changes from the moment it is established, not the channel's existing records.** The first time a connection negotiates a session for it, the position the server acknowledges becomes that subscription's starting point, and records published to the channel before that point are not downloaded. On a new database the initial result is empty and stays empty until something is published. Ask for what the channel already held:

=== "TypeScript"

    ```ts
    const followed = await client.scopes.subscribe('book:demo');
    followed.bootstrap().catch(console.error);
    ```

=== "Flutter"

    ```dart
    final followed = await client.scopes.subscribe('book:demo');
    followed.bootstrap().catchError((Object error) => print(error));
    ```

`bootstrap()` loads what was published to the channel before this subscription's starting point. It starts when you call it, awaited or not, so a first screen can render local data immediately and fill in as the load commits; `await` it instead when the screen has nothing to show without it. The call is durable: it survives a restart and resumes without being called again, it waits for connectivity rather than failing, and a call that finds the work already done resolves offline. Completing it means that history and the changes up to the position the load finished at have been processed - not that you hold a snapshot, that the data is fresh now, or that every record loaded (a failed loader read is reported and corrected on the next delivery). See [`bootstrap()`](client-api.md#channels) for the full contract.

The handle `subscribe` returns tells you where that is: `followed.status` has `initialization` (`pending` until the starting point is committed, then `ready`), `connection` (`offline`, `connecting`, `catching-up`, `live` or `stopped`) and `bootstrap` (`{phase, error}`), and `followed.watch(status => …)` reports the current snapshot and every change. `live` means the stream is healthy, not that everything has arrived. `followed.unsubscribe()` removes this registration, and the load with it; later calls through that handle fail with `subscription.closed`.

Use channel names your backend adds records to, and subscribe when the client needs to receive changes other clients make. A channel is not a database query or an authorization token. Loaders decide which requested records the authenticated user may see.

## Receive your own results

**A subscription is not required to see your own result.** A durable Mutation's inferred local Model changes are optimistic. Its handle's `wait()` returns the final per-invocation result or an error. The receipt also carries batch-final authority for the records its Model inputs target, read through the Loader in the handler transaction, whatever outputs the operation declares. AXTON applies that authority and replays later pending edits over it. Thus the result snapshot and current local Model view can differ. A direct call, such as a default Query, has no automatic local optimism or durable queue; its response carries its result and applies authority through the same local state path.

Subscribe with `client.scopes.subscribe(channel)` as above when the client needs changes made elsewhere: by other users, by background jobs, or by handlers that touch records beyond the Model inputs. Subscription starts synchronization from the point it was established and does not wait for initial data; `bootstrap()` is what fetches what the channel already held. Use `watch` to observe the records, and wait for an existing record to be available locally before updating it.

You can send Mutations and Queries without subscribing to any channel. The receipt still corrects the local row to the server's batch-final state; what you do not receive is later changes from elsewhere. If you subscribe to a channel the record belongs to, the page for your own change carries the same stamp as the receipt and rewrites nothing, whichever arrives first.

## Work offline

=== "TypeScript"

    ```ts title="action-contract"
    await client.connection!.pause();
    const call = await client.mutations.addTodo({
      todo: { id: 'todo-offline', title: 'Draft', state: 'open', note: null },
      gone: [], status: null, tags: [],
    });
    console.log((await client.models.todo.get({ id: 'todo-offline' }))?.title);
    await client.connection!.resume();
    const outcome = await call.wait();
    if (outcome.error) console.error(outcome.error.code);
    ```

=== "Flutter"

    ```dart title="action-contract"
    await client.connection!.pause();
    final call = await client.mutations.addTodo(
      todo: const Todo(id: 'todo-offline', title: 'Draft', state: Status.open, note: null),
      gone: const [], status: null, tags: const [],
    );
    print((await client.models.todo.get(const TodoIdentity(id: 'todo-offline')))?.title);
    await client.connection!.resume();
    final outcome = await call.wait();
    if (outcome is CallFailure<AddTodoOutput>) print(outcome.error.code);
    ```

This assumes `GeneratedClient.open` was given `server`; otherwise call `client.connect` before resuming. The Mutation's create operand is visible locally while the connection is paused. Direct calls never queue; use `client.queries.enqueue` for a Query that should wait until the backend is reachable. Dart uses the same `pause`/`resume` methods.

The returned handle confirms the local commit. It does not mean the server has accepted the Mutation; `wait()` observes the terminal outcome. Display pending and rejected state using the record's [syncState](runtime.md#pending-work-and-recovery) when that distinction matters to the UI.

You can close and reopen the same local database without losing queued intent. A new client handle cannot retrieve a past result from client memory. Keep the same backend database as well: replacing its outcome/receipt/cursor history with an empty database is a reset, not a temporary network interruption. The tutorial's `offline` / `online` commands preserve both databases.

## Understand acceptance and rejection

After a durable call is accepted locally, the connection pushes its frozen request. A successful receipt completes the batch at once: the runtime applies server authority for changed records, removes completed queue entries and replays remaining local changes in one local transaction. One batch is in flight at a time. The handle's result stays the snapshot of its own invocation, even when later work in the batch changes the record.

If a handler rejects a call, AXTON removes any optimistic contribution and retains the rejection code locally. Later valid pending work may still affect the displayed record, so rollback is not necessarily a return to the value the user saw before all edits.

=== "TypeScript"

    ```ts
    const { rejections } = await client.models.entry.syncState({ id: 'entry-1' });
    console.log(rejections);
    // After handling the rejection in your UI:
    await client.dismissRejection(rejectionOrdinal);
    ```

=== "Flutter"

    ```dart
    final state = await client.models.entry.syncState(
      const EntryIdentity(id: 'entry-1'),
    );
    print(state.rejections);
    // After handling the rejection in your UI:
    await client.dismissRejection(rejectionOrdinal);
    ```

`rejectionOrdinal` is taken from the rejection you handled. Dismissing only clears the inbox entry. Retrying the business operation means making a new call after resolving its cause. `drop(ordinal)` is for eligible unsent work; it cannot cancel a request whose server outcome is unknown.

## Recover from connection failures

Provide `onError` to record background failures, and `refreshAuth` if your credentials can expire. Records that could not be applied also reach `onError`, as an `AxtonReport` with a `kind`: `readFailed` when the server could not read the record, `skipped` when the local schema refused it, `conflict`, or `diverged` when a pending edit no longer applies to newer server state. A diverged edit is still sent, and `models.<name>.syncState(identity)` marks it `diverged` until the server answers it. Let the runtime retry frozen work; do not make a new call merely because the original request timed out. The backend may already have committed it and retained its outcome.

Use `wake()` after an application event that should prompt another scheduling check. Use `resume()` after explicitly pausing. A closed connection cannot resume; create a new one with `client.connect` or reopen the client. Direct requests use a finite timeout; an `unknown` execution status can mean the backend committed but the client did not observe the response.

On connection or reconnection, AXTON establishes the WebSocket subscription and receives the current position of each channel. A channel with no saved position takes the acknowledged one as its starting point and downloads nothing older. For a channel that has one, reconnecting is not a new starting point: if the saved position is already current it streams at once; otherwise it sends one HTTP pull for all channels from their saved positions, holding changes that arrive meanwhile, then continues with WebSocket updates. A position never moves backwards, and a server position below saved progress is reported as an error instead of silently resetting the channel. Both sources use the same Rust page processing: each page applies as one transaction, covered pages are discarded, overlapping pages apply their unseen changes, and gaps trigger HTTP recovery from saved progress. Subscription changes replace the session; pages from replaced or canceled sessions cannot update local data.

## Authentication and account changes

Authenticate requests on the backend and check business permissions in handlers and loaders. `devAuth` is only for the local example. In production, your `authenticate` callback should verify your existing application's credentials and return its user ID.

Use a separate local database per signed-in user. On an account change, stop and close the old client before opening the other user's database. Changing only the transport token leaves the old user's cached records and client identity in place.

When permissions change, touch the affected records on the backend so every channel they belong to delivers them again. A loader can then return null to withdraw a record; removing a record from a channel only stops later deliveries and withdraws nothing already delivered. Unsubscribing removes nothing: it stops that channel's delivery and keeps the records, their stamps and any pending edits in place. It is not a cache wipe or an authorization mechanism.

## Diagnose pending work

| Observation | Check |
| --- | --- |
| Empty local query after opening | Desired channel, running connection, loader output and read permission |
| `queued` with failed prerequisites | Host callback failure; reset its readiness to pending and run it again |
| `frozen` after a network failure | Connectivity/authentication; retain the frozen bytes for retry |
| `frozen` long after the network recovered | Either the server refused the batch on identity or order grounds (401/403/409 `client.owner_mismatch`/`gap`/`overlap`) — the code reaches `onError` and the batch is resent as is because the server never ran it — or a received receipt was refused locally (it named another client or batch, or omitted an accepted record): check `onError` and the backend's loaders |
| Server values do not update | Whether the record was added to the channel (`channel(name).todo.add`) and is still a member, and whether the handler touched every record it changed beyond its Model inputs (`touch.todo`) |
| Local client fails after another process wrote | One active client per SQLite file; close/reopen the stale instance |
| Empty local data after an app update | `syncState().schema.rebuilt`: the schema was incompatible and a fresh database is synchronising from the beginning; `syncState().schema.pending` means the old file is still sending its last changes, call `rebuild()` when it reaches 0 ([local storage](storage.md#change-the-schema)) |

See [runtime APIs](runtime.md) for controls and [compatibility and recovery](storage.md) for storage constraints.
