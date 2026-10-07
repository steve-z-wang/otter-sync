# Uplink

## Steps

### Local

1. Enqueue
   - Persist **MutationQueue**, **MutationQueueOperation** and local changes in one transaction.
2. Select
   - **MutationBatcher** fixes the batch.
3. Send
   - **MutationRequester** returns the acknowledgement.
4. Acknowledge
   - **MutationAcknowledgementHandler** saves results, rolls back rejections and advances **Store.lastAcknowledgedBatchId**.

### Cloud

1. Receive
   - **MutationBatchProcessor** validates the Store, Stream and batch sequence.
   - For the last completed batch, return its saved results.
2. Process
   - Resume from **Store.progress** and execute remaining Mutations in order.
   - **MutationProcessor** commits each Mutation's changes, **MutationResult** and progress together; a rejected Mutation's changes are rolled back.
3. Reply
   - Return the complete batch acknowledgement from **MutationResult**.

## Protocol

**MutationRequest**
- **storeId**: framework-generated, persistent identity of this Store instance.
- **stream**: the Client's bound Stream.
- **batchId**: the fixed batch number.
- **mutations**: ordered by **MutationQueue.id**.
  - **id**: **MutationQueue.id**.
  - **name**: **MutationQueue.name**.
  - **operations**: server-bound operations, ordered by **step**; device-only operations stay local.

## Classes

### Local

**MutationDispatcher**
1. Get a batch.
   - Restore rows whose **batchId** exceeds **Store.lastAcknowledgedBatchId**.
   - Otherwise, call **MutationBatcher**; if empty, wait for work.
2. Await **MutationRequester**.
   - On transport failure or unknown outcome, wait and retry the same batch.
3. Await **MutationAcknowledgementHandler**.
4. After it commits, repeat.

**MutationBatcher**
1. In one transaction:
   - Select **MutationQueue** rows with **batchId** and **rejectionCode** both null, ordered by **id**.
   - If empty, return no batch.
   - Assign **batchId** = **Store.lastAcknowledgedBatchId** + 1.
2. Commit and return the batch.
   - Keep its members and **MutationQueueOperation** rows unchanged until acknowledged.

**MutationRequester**
1. Build the request from **MutationQueue** and **MutationQueueOperation**.
2. Send, await the response and return the acknowledgement or transport failure.

**MutationAcknowledgementHandler**
1. In one transaction:
   - Validate the batch ID and one result per Mutation.
   - Accepted: save **MutationQueue.syncCursor**.
   - Rejected: set **MutationQueue.rejectionCode** and **rejectionMessage**, then rebuild affected rows without that Mutation's operations.
   - Update **Store.lastAcknowledgedBatchId**.
2. Commit.
   - Retain rejected **MutationQueue** and **MutationQueueOperation** rows until dismissed; exclude them from sending, settlement and optimistic replay.

### Cloud

**MutationBatchProcessor**
1. Validate the Cloud **Store.stream** and batch sequence.
   - For the last completed batch, return the acknowledgement from **MutationResult**.
2. Resume after **Store.progress** and process remaining Mutations through **MutationProcessor**.
3. Once all Mutations finish, assemble the acknowledgement from **MutationResult**.

**MutationProcessor**
1. Begin a transaction, lock the Cloud **Store** row and recheck batch sequence and progress; skip already processed Mutations.
2. Execute the Mutation within a savepoint and publish its changes.
   - Lock **Stream** rows and reserve one cursor per affected Stream.
   - All records published by the same Mutation in the same Stream share exactly one cursor.
   - On rejection, roll back to the savepoint.
3. Save **MutationResult** and advance **Store.progress**.
   - For the final Mutation, update **Store.lastProcessedBatchId** and reset **Store.progress** to 0.
4. Commit business changes, Stream positions, result and progress together.

# Downlink

## Steps

### Initial setup

1. Handshake
   - **DownlinkEngine** calls **StreamConnection** to open the WebSocket and perform the handshake.
   - **StreamConnection** sends **stream**.
   - Cloud validates access and returns **head**.
2. Set the starting cursor
   - **DownlinkEngine** persists **head** as **Store.startCursor** if null; retain it across reconnects.
3. Wake the engine
   - **StreamConnection** passes **head** to **DownlinkEngine** to drive Downlink work.

### Bootstrap

1. Check progress
   - **DownlinkEngine** checks whether the non-null **Store.bootstrapCursor** equals **Store.startCursor**; if so, Bootstrap is complete.
2. Request
   - **DownlinkEngine** calls **DeltaRequester** to send an HTTP Delta page request with the Bootstrap flag, after **Store.bootstrapCursor** (or 0 before the first page), through **Store.startCursor**.
   - Cloud returns only the Models selected for Bootstrap by the schema.
   - **DeltaRequester** adds the response to **DownlinkQueue** and wakes **DeltaWorker**.
3. Apply
   - **DeltaWorker** selects queued deliveries that connect to committed Bootstrap progress and calls **DeltaApplier**.
   - **DeltaApplier** commits returned data and **Store.bootstrapCursor** together, preserving pending local work; remove completed deliveries from **DownlinkQueue** only after commit.
   - Continue until **Store.startCursor**; on failure, resume from committed progress.

### Sync

1. Receive
   - **StreamConnection** adds WebSocket deliveries to **DownlinkQueue** and wakes **DeltaWorker**.
2. Check coverage
   - **DeltaWorker** compares delivery coverage with **Store.cursor**.
   - If a gap exists, or the known **head** exceeds available delivery coverage, **DeltaWorker** asks **DownlinkEngine** to call **DeltaRequester** for missing changes.
   - **DeltaRequester** adds responses to **DownlinkQueue** and wakes **DeltaWorker**.
3. Apply
   - **DeltaWorker** selects deliveries that connect to committed progress and calls **DeltaApplier**.
   - **DeltaApplier** applies changes in delivery order; the whole page need not commit together.
   - Commit authority, settle accepted Mutations whose **syncCursor** is covered, replay remaining local operations and advance **Store.cursor** together.
   - Advance only through completed delivery; never past unfinished changes at the same cursor.
4. Continue
   - Remove completed deliveries from **DownlinkQueue** only after commit.
   - **DeltaWorker** continues from committed progress; **DownlinkEngine** manages delivery retries.

## Protocol

**HandshakeRequest**
- **stream**

**HandshakeResponse**
- **head**

HTTP and WebSocket deliveries identify Bootstrap or Sync and declare their covered cursor range. Record cursor jumps alone do not indicate missing delivery.

Pending: WebSocket connection lifecycle and Delta request details.

## Classes

**DownlinkEngine**
1. Read Store progress; if disconnected, call **StreamConnection** to establish the connection and handshake.
2. Save **Store.startCursor** on first initialization and notify **DeltaWorker** of the received **head**.
3. Call **DeltaRequester** for Bootstrap pages and missing coverage reported by **DeltaWorker**.
4. Deliver incoming data into **DownlinkQueue**; manage waiting, retries, reconnection and shutdown.

**DeltaWorker**
1. Run on an independent thread with its own start and stop lifecycle.
2. Wait for queued delivery or updated **head**; check coverage against the corresponding committed cursor.
3. Report gaps to **DownlinkEngine** and retain later deliveries while awaiting repair.
4. Select applicable deliveries and call **DeltaApplier**.
5. After commit, remove completed deliveries and continue; report Bootstrap progress to **DownlinkEngine** for further requests.

**StreamConnection**
1. Open or close the WebSocket as directed by **DownlinkEngine**; perform the handshake.
2. Report connection status and **head** notifications to **DownlinkEngine**.
3. Add WebSocket deliveries to **DownlinkQueue** and wake **DeltaWorker**.

**DownlinkQueue**
1. Hold Bootstrap HTTP responses, Sync HTTP responses and WebSocket deliveries.
   - **after**: exclusive start cursor.
   - **through**: inclusive end cursor.
   - **bootstrap**: defaults to false; true for delivery that advances Bootstrap progress.
   - **changes**: changes covering the declared range; may be empty.
2. Organize deliveries by covered cursor range, distinguishing Bootstrap and Sync progress; expose deliveries that connect to the corresponding committed cursor.
3. Retain pending deliveries until applied and committed; capacity limits control incoming delivery.

**DeltaRequester**
1. On **DownlinkEngine**'s request, build and send the HTTP Delta request; Bootstrap requests carry the Bootstrap flag.
2. Add the response to **DownlinkQueue** and wake **DeltaWorker**; report transport failures to **DownlinkEngine** for retry.

**DeltaApplier**
1. On **DeltaWorker**'s call, apply queued delivery in local transactions, preserving pending local work and settling covered accepted Mutations during Sync.
2. Commit data and the corresponding **Store.bootstrapCursor** or **Store.cursor** together; never advance past incomplete delivery.
3. Use the shared Core/Runtime post-commit notification path to rerun watches and publish changed results. Rollback emits no committed changes; observer failure does not undo a committed delivery.

# Tables

## Local

**Store**
- **lastAcknowledgedBatchId**: 4
- **startCursor**: initially null; fixed starting cursor captured from the initial handshake.
- **bootstrapCursor**: initially null; committed Bootstrap progress from 0 through **startCursor**. Bootstrap is complete when this non-null cursor equals **startCursor**.
- **cursor**: committed normal Downlink progress.

**MutationQueue**
- **id**: 1
- **name**: 1
- **batchId** (strictly increasing across batches): 2
- **syncCursor** (settle after the Stream commits through this cursor): 4
- **rejectionCode** (null unless rejected): 4
- **rejectionMessage** (optional error description): 4

**MutationQueueOperation**
- **mutationId** (references **MutationQueue.id**): 1
- **step**: 1
- **model**: 1
- **identity**: 1
- **operation**: 1
- **value**: 1

**X**
- Schema-defined columns: 1

**before X**
- Same columns as **X**: 1

## Cloud

**Store**
- **id**: request **storeId**.
- **stream**: bound Stream.
- **lastProcessedBatchId**: last completed batch; initially 0.
- **progress**: number of processed Mutations in batch **lastProcessedBatchId** + 1; initially 0.

**MutationResult**
- **storeId**: references Cloud **Store.id**.
- **batchId**
- **mutationId**: request Mutation ID; primary key with **storeId** and **batchId**.
- **syncCursor**: accepted Mutation's Stream cursor.
- **rejectionCode**: null unless rejected.
- **rejectionMessage**: optional error description.

Retain results for the last completed batch and the batch in progress.

### Stream storage

Names omit the axton prefix. StreamMember and StreamLog are combined below.

**Stream** — Stream progress.
- **stream**: primary key.
- **head**: latest published cursor.

**Record** — Model + identity registry; domain content stays in application tables.
- **id**: internal record ID.
- **model**
- **identity_key**: canonical identity text; unique with **model**.
- **identity**: JSON generated from **identity_key**.

**StreamRecord** — Durable tracking and latest delivery position; retain rows for null Loader results and removals.
- **stream**: references **Stream.stream**.
- **record_id**: references **Record.id**; primary key with **stream**.
- **cursor**: shared by records published in the same Mutation transaction in this Stream; not unique; null before first publication.
- **kind**: upsert or remove; null with **cursor**.

**PublicationFence** — Shared publication coordination row.
- **id**: always 1.

# Rust ↔ Other Languages

- SDKs expose language-facing APIs and perform network I/O.
- Rust Core owns the entire execution flow: scheduling, queues, retries, transactions, persistence, apply and reactive notifications.
- Preserve the existing cross-language protocol and bindings: SDKs submit requests, execute Rust-issued effects and route results and events. Synchronization decisions stay in Rust.
