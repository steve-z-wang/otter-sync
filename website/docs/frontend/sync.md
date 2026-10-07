# Sync, offline work and recovery

## Follow one Stream

Open each Store with one Stream and a fresh format-5 SQLite file. Opening binds the local database before connecting. Credentials belong to the connection; refreshing a token does not change the Store's identity.

`await client.bootstrap()` waits for every complete required Bootstrap unit at the finite captured head. It is useful for initial data and supported schema rematerialization. A Query or Fetch reads named records; it does not replace Bootstrap. Model watches report committed local state throughout delivery.

## Submit and settle

The first await of a named Mutation returns its durable `Call` after the local transaction commits. Its declared Model changes appear optimistically. A callback returns the typed input and may use `tx.models` for device-only companion writes before optimism is applied. A callback failure rolls back both contributions.

`await call.wait()` observes terminal settlement. Server acceptance can precede installation of the required Stream authority, so acceptance alone does not complete the Call. The runtime can recover saved settlement targets through a finite owned plan without rerunning the public Bootstrap handler or enrolling new records. The result is the invocation's saved snapshot; later local edits may make the current Model view different.

A refusal removes that call's owned optimism and companions while retaining later independent work. Several named Mutations in one local transaction commit together and settle independently. Direct `tx.models` writes never enter the remote queue.

## Work offline

Pause the connection to keep local work available while delaying network delivery:

```ts title="v04-sdk"
await client.connection!.pause();
const call = await client.mutations.publish(async tx => {
  await tx.models.draft.delete({ id: 'draft-1' });
  return { entry: { id: 'entry-1', text: 'Draft' }, call: 'publish' };
});
await client.connection!.resume();
const outcome = await call.wait();
```

Dart uses the same typed invoker with `.withTransaction((tx) async => input)`. A plain typed input uses `.publish(input)`.

The same database can reopen offline with the same binding and projection generation. Frozen Mutation requests survive reopen and supported schema evolution under retained contracts. Query and Fetch are finite requests; they do not queue for later delivery. `store: false` returns the read snapshot without changing the cache. Each invocation has a fresh request and snapshot; there are no saved Query-result controls.

## Connection failures

Set `onError` and optional `refreshAuth` on the connection. Retry uses the exact durable Mutation request; making another business call after a timeout can duplicate work. Use `wake()` to prompt scheduling, `resume()` after a pause, and `close()` to release the connection. Reconfiguration through `client.connect` changes transport, while the Store binding remains fixed.

Admission refusal stops the connection and reports `AdmissionRefused`; connect again after the application can satisfy the backend's admission policy. Query and Fetch timeouts report whether execution is known or unknown. Background errors retain owned pending work.

The bound Stream reconnects from its committed delivery cursor. HTTP catch-up and live pages share atomic commit units. Successful independent prefixes can commit before a later unit fails; neither a failed unit nor a captured plan head advances progress. Request limits reduce after eligible page failures to permit a smaller independent prefix, but cannot split a required constraint group.

## Accounts and cache authority

Use a separate database for each viewer and close the old Store before switching accounts. Changing credentials alone cannot authorize a different binding. A database has one physical owner; a competing open fails with `store_in_use`.

Tracking is delivery interest. Viewer Loaders decide readable content. Stream content and canonical absence carry authority; ordinary Query/Fetch cache writes have null authority and cannot replace current protected Stream state. Membership Remove retains the Model and its guards while releasing live-content protection; a true Stream deletion retains deletion protection. Explicit reset changes the Store incarnation and refuses pending work unless `discardPending` is chosen.

References support navigation and declared cascades; missing or unloaded parents are valid partial-cache states. For `onTargetDelete: delete`, an ordinary incoming child that names a currently Stream-deleted parent is suppressed. The backend must publish a changed or deleted child's own canonical state. Cache presence alone grants no product permission.

## Pending work

Observe `syncState`, `rejections`, `failures` and `outbound` for durable pending work. Dismissing a refusal clears its inbox entry. A new business attempt creates a new named Mutation. `drop` only removes eligible unsent work; it cannot cancel an operation with an unknown server outcome. Repairs can drop a failed call and submit its replacement in one transaction. See [unsent work](runtime.md#unsent-work) and [prerequisites](runtime.md#prerequisites).
