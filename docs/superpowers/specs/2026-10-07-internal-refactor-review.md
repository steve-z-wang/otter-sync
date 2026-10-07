# AXTON internal refactor: design review

## 1. Scope and evidence

This reviews the [frozen design](2026-10-07-uplink-downlink-design.md) against AXTON v0.4.2, commit `98891f9be99aa89fe237612377d65e5acafce9a1`. It informs the [proposed specification](2026-10-07-internal-refactor-spec.md) and [implementation plan](../plans/2026-10-07-internal-refactor.md). It does not change the original or describe implemented behavior.

The notes worktree starts from v0.4.1. Implementation must start from v0.4.2 or a newer integrated main, carrying these documents forward without resetting the notes.

Evidence inspected: the protocol contract, PostgreSQL storage/publication driver, SQLite queue/authority/settlement code, Rust runtime and bindings, JS/Dart facades and compiler emitter. No implementation tests were run for this documentation review.

## 2. Confirmed direction

- One Client exclusively owns one SQLite file and one Stream.
- Rust owns scheduling, persistence, retries, delivery, settlement and watch notifications. SDKs expose typed APIs and execute network effects through the existing bridge.
- Uplink uses an immutable, persisted, strictly increasing Batch. Only one Batch awaits acknowledgement. Cloud commits each Mutation independently and resumes by progress.
- Every publication from one Mutation into one Stream shares one cursor. Tracking is explicit; ordinary read results do not enroll records.
- Downlink separates network orchestration from apply. HTTP and WebSocket deliveries enter a range queue; a worker commits complete units and wakes observers afterward.
- Bootstrap uses the fixed first-handshake boundary and its own durable progress. It selects schema-marked Models. Normal Sync includes all published Model types.
- Remove public StoreIdentity configuration and Query once/refresh/invalidate. Preserve Fetch/Query storage choice and cursor-null read semantics.

## 3. Findings and recommended treatment

### R1 — A compacted log is not a frozen Bootstrap snapshot

Example: handshake captures S=40. A selected record is at 20, then moves to 45 before the Bootstrap page is read. Scanning only current rows at or below 40 misses it. B=40 does not prove that it was loaded.

Current 0.4.2 addresses this with immutable identity manifests and a fixed tail. Simply deleting those tables breaks the guarantee. See [the protocol contract](../../engineering/architecture/protocol/0.4.md#g2-finite-bootstrap) and `crates/server/src/protocol_v04.rs`.

**Recommendation:** retain finite snapshot staging, but unify it with the delivery mechanism: a transient DeliveryPlan freezes current Loader results at H. Bootstrap proves coverage through S while also carrying selected records that have moved beyond S. The plan head and page offset are delivery metadata, not a fourth Store cursor. Plan expiration restarts from committed Bootstrap progress; it never declares completion. This is an internal refinement proposed here, not a previously approved detail.

Cost: staging a large Bootstrap still costs server storage and a consistent read transaction. Moving work into a generic plan does not make that cost disappear. Validate this tradeoff before deleting the old manifest implementation.

### R2 — One cursor per Mutation does not solve compacted constraint transfers

Example: locally A owns a unique value x. Cloud releases x from A at 2, gives x to B at 3, and changes A again at 5. Latest-only storage contains B@3 and A@5. Applying B alone first violates uniqueness even though the final server state is valid.

Current PublicationGroup storage helps close such deliveries. Removing it requires another way to assemble a valid final projection; a sorted queue alone is insufficient. See `packages/postgres/src/sql.mts` and `crates/client/src/progress04.rs`.

**Recommendation:** build transient atomic units from the complete compacted range. Conservatively group records of Models joined by schema constraints/cascades; all changed rows of a Model with a unique constraint belong to one unit. Preserve whole same-cursor groups as well. Stage oversized units across transport pages and commit once complete. There is no retained publication-history table.

Cost: a busy Model can create a large unit. Incremental commits are available between units, not guaranteed for every network page. Capacity failure must be explicit, never a partial commit or dropped constraint. This replacement needs a correctness and capacity gate before PublicationGroup is removed.

### R3 — A receipt cursor alone does not prove owned data was reconciled

An accepted Mutation may affect an untracked record, only another Stream, or a record whose earlier authority was omitted from this Store's Bootstrap. C reaching a number cannot supply missing content.

Current 0.4.2 has tracked target evidence and call-owned private readback. Preserve that behavior while removing the duplicate Call master row. `crates/core/src/protocol_v04.rs::SettlementTarget` and `crates/client/src/settlement04.rs` own the current distinction.

**Recommendation:** keep one syncCursor per accepted Mutation plus a target manifest in its result. Stream targets need installed per-key evidence; nonmembers use receipt-owned null-cursor snapshots. These snapshots finalize only their Mutation's operations and do not track, advance C or override current Stream authority. A materialization request for an explicitly tracked but locally missing target enters the same Downlink queue without advancing coverage by itself.

### R4 — Operations must represent the entire typed input

The six illustrated Operation columns cover Model writes, but not scalar arguments, omitted/null/empty-list distinctions, slot identity or device-only ownership. The compiler currently supports those inputs.

**Recommendation:** keep one input source in MutationQueueOperation. Add a schema input path and an argument variant for non-Model input. A null input path identifies a local companion. Keep the Mutation descriptor version. Do not retain a second serialized input on MutationQueue, and do not use fake Models/IDs for scalar arguments.

### R5 — Before images cannot replace local ordering evidence

If Mutation M edits X, a later direct local write edits X again, and M is rejected, rebuilding only from before X plus pending Mutations loses the later write. Accepted companions also outlive their owning queue operations.

**Recommendation:** retain the ordered local-write journal and minimal record evidence. Merge duplicate per-Mutation bookkeeping, not facts with different lifetimes. A Stream cursor is not a new independent per-row stamp.

The latest user rule is that an explicit direct local operation clears the current authority marker. Historical Stream evidence must remain to reject duplicate old deliveries, while it must not permanently block Query/Fetch cache fills. This differs from 0.4.2's special retention of current tombstone protection after direct writes and requires an explicit regression test.

### R6 — Local receipt failure must not change a server outcome

An accepted server outcome must never become a fabricated rejection because local apply failed. The existing protocol persists receipts before fallible reconciliation. The new fixed-Batch protocol also guarantees replay of its last complete acknowledgement until the client sends the next Batch.

**Recommendation:** preserve the original atomic acknowledgement step: save accepted outcomes, rebuild rejected ownership, and advance lastAcknowledgedBatchId together. If it fails, advance nothing and retry the same saved response or Batch. Server replay prevents another business execution. Do not send the next Batch before this commit. Successful acceptance still awaits separate Downlink settlement, as already agreed.

A proposed extra receipt/rejection-reconciliation phase was removed during self-review: it would allow more progress after local failure, but it is not necessary for correctness and adds another partially completed state.

### R7 — A batch number needs immutable identity and authentication

Cloud progress alone cannot detect a different body retried under the same Batch number. A Store ID also does not authorize a Stream.

**Recommendation:** authenticate each request; bind the server Store to its principal and Stream; persist the current/last Batch digest and member count. Verify them before replay/resume. Keep results for the last completed and current Batch. Transport errors retry; business rejections are saved per Mutation. Delete Record.stamp only after its current locking role is replaced by the retained publication fence and ordered row locking.

### R8 — Breaking changes need an explicit boundary

These changes affect public API, wire shape and local persistence. They cannot be shipped as a wire-compatible 0.4 patch.

**Recommendation:** use a new wire discriminator and a new local storage format. First implement and test fresh Stores; reject older files without altering them. Keep the old release available to drain/export existing work. An in-place 0.4 migration is a separate adoption gate, not an excuse to silently wipe data or retain two permanent engines.

## 4. Issue alignment

| Issue | Treatment |
| --- | --- |
| [SYN-16](https://linear.app/capsoapp/issue/SYN-16) | Included: remove required public backend/viewer/contract configuration; no renamed replacement. |
| [SYN-17](https://linear.app/capsoapp/issue/SYN-17) | Included: remove Query once/refresh/invalidate and their persistent result cache. Each invocation executes a read; Bootstrap supplies finite initial loading. Do not add another generic one-time-load API in this refactor. |
| [SYN-18](https://linear.app/capsoapp/issue/SYN-18) | Separate: broad typed-input/Patch syntax redesign. Preserve existing typed API behavior except the two specified removals. |
| [SYN-19](https://linear.app/capsoapp/issue/SYN-19) | Its old call-ID transport proposal is superseded by the later confirmed Batch design. Its duplicate-record cleanup goal remains applicable. No issue text changed in this review. |

## 5. Review result

The public responsibilities and main lifecycle are coherent. The abbreviated table sketch is not yet a safe deletion list. R1–R7 explain the additional facts needed to preserve existing guarantees.

The companion specification makes concrete recommendations for those facts and labels the material refinements. Its first implementation gate is to prove moving-record Bootstrap, compacted unique transfers, owned settlement and crash recovery before removing the existing protections. This review does not claim those proposed mechanisms have passed implementation tests.

## 6. Material decisions to discuss

1. **Bootstrap completion and its storage cost.** Recommended meaning: initial coverage through S with a finite current projection, including selected rows that moved beyond S. This needs stable transfer staging; three Store cursors alone cannot make mutable paginated reads a snapshot. Approving this means accepting temporary cloud snapshot storage, not only a different class name.
2. **Removing publication history while retaining database constraints.** Recommended meaning: complete final-state units may span transport pages, and a large constrained component may require a large atomic commit. Shared cursor numbers alone are insufficient. This is a throughput/latency tradeoff; prove capacity before removing the old groups.
3. **Adoption scope.** Recommended first target: a new Store format and breaking wire boundary, with old files refused intact. It is not an in-place upgrade for users with pending 0.4 work. A coordinated adoption procedure is required before release; no data deletion is authorized by this document.

Preserving receipt target evidence, direct-write ordering and correlation registries maintains existing behavior. Those are engineering obligations, not new product decisions to delegate to the user. The atomic acknowledgement sequence remains as originally confirmed. All three material recommendations above remain proposals; the original is unchanged.
