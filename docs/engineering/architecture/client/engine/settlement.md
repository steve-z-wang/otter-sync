# Settlement

## 1. Introduction and Goals

A local write is shown to the user before the server has seen it. Settlement is the moment that optimism ends: the client learns the server's answer and replaces the optimistic row with the authoritative one, or rolls the write back. Its job is to do this exactly once per mutation, from the receipt alone, in one local transaction that cannot leave the queue and the records disagreeing. The receipt carries the server's final content for every record the batch changed, so no channel is awaited and no subscription is required.

The rest of the engine sets settlement up. [Local operations](local-operations/README.md) keeps a *before image* (the last server-known row) under every record with pending mutations; [Push](push/README.md) freezes mutations into numbered batches; [Pull](pull.md) applies server pages through the same authority applier settlement uses.

## 3. Context and Scope

Settlement is triggered by one event and works entirely inside the engine's transaction:

| Event | What arrives | What settlement does |
| --- | --- | --- |
| A receipt for the batch in flight ([Protocol / Push](../../protocol/push.md)) | rejections and the final authority of every record the accepted operations changed | stages the authority beneath the queue, records rejections, removes the completed operations, replays what remains, remembers the completion |

State it owns: `axton_client.last_completed_push` (the sequence of the last completed batch), `axton_client.push_models` (the read contracts the batch in flight declared) and `axton_rejection` (the durable inbox of rejected mutations). It deletes rows from the queue tables owned by [Queue](push/queue.md) and rewrites records through the authority applier and the replay logic of [Local operations](local-operations/README.md). Call completions are returned in memory after commit; successful business results have no local history table.

## 5. Building Block View

- **Acknowledgement**: `acknowledge` in [client/push.rs](../../../../../crates/client/src/push.rs) validates the receipt and runs the transition below; `mark_rejected` records rejections and their lifecycle dependents.
- **Authority applier**: `stage_authority` and `rebuild_held` in [client/authority.rs](../../../../../crates/client/src/authority.rs), shared with [Pull](pull.md). It compares by stamp and stages content beneath pending operations or writes it to a clean row; the caller decides when to replay.
- **Replay**: `rebuild` in [client/mutate.rs](../../../../../crates/client/src/mutate.rs) writes the before image plus the remaining operations, and drops the before image when nothing pending touches the record.

## 6. Runtime View

### Validating the receipt

A receipt must name this client and the batch in flight. A receipt for a sequence at or below `last_completed_push` is a duplicate and changes nothing, whatever it carries; one for any other sequence, or for another client, is refused. Call completions must match the frozen call IDs and order, with failure codes matching rejected ordinals. A Model result must contain every field declared when the call was frozen. After compatible schema growth, current fields supplied by the server are validated; absent newly introduced nullable or defaulted fields are added to the transient result. Its rejections must name ordinals of the batch, and its records must cover every record the accepted optimistic operations targeted: a receipt that omits one cannot complete the batch and is refused, leaving the frozen batch for retry. Authority the client cannot decode (a state its schema refuses) is refused the same way. Nothing below runs until all of this holds.

### The atomic transition

1. **Snapshot the queue.** The batch's mutations, the records every operation touches, and which of them are wire targets.
2. **Fold companions.** Accepted local-only companions, and the cascade deletes of companion deletes, settle as they always have: folded into the before image of records the server did not report. A record the receipt covers takes the server's authority instead.
3. **Stage authority.** Each receipt record goes through the applier while the queue still says which records hold a base. A newer stamp lands beneath the pending operations (in the before image) or directly in a clean row; an equal stamp compares against that base, not the optimistic row, so a page that already delivered the same change is recognized as the same authority rather than a conflict; an older stamp is ignored. A deletion stages the deletion of declared descendants too. Every record staged beneath pending operations is remembered for replay. Each record is staged in its own savepoint, as on a page: one this client cannot apply (a state its schema refuses, a local constraint it violates) is rolled back, reported as `skipped` with the batch sequence, and keeps its previous authority and stamp; the rest of the receipt lands and the batch completes.
4. **Record rejections.** Rejected mutations and their lifecycle dependents get their inbox entries and leave the queue.
5. **Remove the completed operations.** The accepted mutations' rows go, with their operations, dependencies and prerequisites.
6. **Replay once.** Every record the batch touched or that was staged beneath pending operations is rebuilt from its before image plus whatever is still queued; the before image is dropped where nothing pending remains. Queued deletes are extended to descendants that appeared. A clean row that received authority in step 3 is left alone.
7. **Remember the completion.** `last_completed_push` becomes the batch sequence and the frozen declaration is released; the transaction commits. The returned report carries transient call completions, including unsent lifecycle dependents removed by rejection.

Any failure rolls the whole transition back: the batch stays in flight, the records keep their optimism, and the next cycle resends the same bytes.

### Whichever arrives first

The receipt and a page for the same change carry the same authority at the same stamp. When the page comes first, the base is updated and the visible row rebuilt at once as base plus the pending edits ([Writes](local-operations/writes.md)); the receipt then finds an equal stamp with equal content, rewrites nothing, and still completes the batch. When the receipt comes first, the page finds the same and rewrites nothing, and still advances its cursor. Newer authority that a page delivered before the receipt is never regressed by the receipt's older stamp; the operation still completes over it.

```
receipt first                                        page first
─────────────                                        ──────────
receipt: authority @12 staged under the edit        page a 4→5: base = server row @12
   completed op removed, row rebuilt = server row       visible = server row + pending edit replayed
page a 4→5: same stamp, same content → no write     receipt @12: same stamp → no write
   cursor → 5                                           completed op removed, row rebuilt = server row
visible = server row, pending 0                      visible = server row, pending 0
```

Because the pending edit is replayed over the new base rather than discarded, the user never sees the server value overwrite the local edit and the edit reappear later.

### Replacing the optimistic row

- *Accepted.* The base becomes the visible row, any still-pending later mutations are replayed on top, and the base is dropped once nothing pending touches the record (guarantee A1). A pending create has no base until its receipt arrives; the receipt's authority is its first base, so the created record survives with the server's content.
- *Rejected.* The mutation and every mutation whose lifecycle depends on it (for example an edit of a record the rejected mutation created) are removed. Each gets a durable inbox entry with its code (`dependency.rejected` for the dependents) and the records it touched, and the touched records are rebuilt from their base, which undoes the optimistic change (guarantee P5). The application reads the inbox through `rejections()` or `record_status()` and clears entries with `dismiss_rejection`.

### Unsubscribing

Unsubscribing no longer touches settlement: it deletes the subscription row and nothing else ([Pull](pull.md)).

## 9. Architecture Decisions

**Completion from the receipt, not from a channel ([#55](https://github.com/zanminwang/axton/issues/55); supersedes [#52](https://github.com/zanminwang/axton/issues/52)).** The earlier contract stored the channel positions a receipt named and settled a batch only once subscribed channels reached them, dropping positions on unsubscribed channels. Acceptance without a subscribed channel therefore reverted an update and removed a create until some channel delivered the result, and settlement had to run in batch order behind waiting batches. Now the server reads every changed record back in the handler's transaction and the receipt carries it, so the batch completes on arrival with the server's content, with or without subscriptions, and no ordering rule is needed: one batch is in flight at a time. The stamp rule is shared with pages, so the receipt and the channel never disagree about which content is newer. The proposals of adding a per-operation required stamp, keeping accepted operations waiting for the channel, or flagging receipt-confirmed predictions as provisional were withdrawn.

**Stage before removing, replay once.** Authority is staged while the queue still identifies which records hold a base, and the visible rows are rebuilt only after the completed operations are gone. Replaying before removal would put the completed edit back on top of the server's row; removing before staging would lose track of a pending create's absent base. The applier never replays on its own for this reason; page application, whose queue state does not change, stages and replays in one step.

**A refused receipt is not partially applied.** Membership of the rejections, coverage of the accepted targets and the receipt's client and batch are checked before any row is touched, and the whole transition is one transaction. Such a receipt is a defect on the wire, and the honest outcome is a batch that stays in flight.

**A record that does not fit fails alone ([#95](https://github.com/zanminwang/axton/issues/95)).** The server stores a receipt and replays the same bytes on every retry, so refusing a whole receipt because one record's state does not fit would hold the queue forever. The record is skipped and reported instead, never silently: the application hears about it through `onError`, and the record is corrected the next time it is published.

## 10. Quality Requirements

- **A successful push completes from its receipt alone, with the server's content, with no subscription; the frozen bytes carry the declaration and survive restart until completion** (guarantee A3). Evidence: [sqlite/tests/settlement.rs](../../../../../crates/sqlite/tests/settlement.rs) `response_completes_without_a_subscription`, `frozen_batch_and_its_declaration_survive_restart_until_completed`; [sqlite/tests/query.rs](../../../../../crates/sqlite/tests/query.rs) `transport_pulls_only_subscribed_channels_and_the_receipt_completes_the_push`; [sqlite/tests/runtime_lanes.rs](../../../../../crates/sqlite/tests/runtime_lanes.rs) `a_receipt_applies_its_authority_and_leaves_reads_to_the_stream`.
- **Staging order: the authority lands beneath the pending operation before it is removed; a pending create survives with the server's content; clean extra authority is written and kept** (section 9). Evidence: `authority_is_staged_under_the_pending_operation_before_it_is_removed`, `accepted_create_survives_with_the_servers_content`, `clean_extra_authority_is_written_and_kept`.
- **Receipt and page in either order leave the same state; an equal stamp compares against the held base; newer channel authority is not regressed by an older receipt** (guarantee A4). Evidence: `channel_first_then_receipt_dedups_and_still_completes`, `receipt_first_then_channel_is_a_no_op_that_advances_the_cursor`, `newer_channel_authority_is_not_regressed_by_an_older_response`; [sqlite/tests/push.rs](../../../../../crates/sqlite/tests/push.rs) `pull_before_ack_and_later_local_edit_replay_in_order`, `offline_queue_and_frozen_bytes_survive_restart_and_receipt_completes_at_once`.
- **The server's value replaces the optimistic one, later local edits replay on top, companions on reported records do not outrank the server, and unrelated records are untouched** (guarantee A1). Evidence: `later_unsent_edit_replays_over_the_returned_authority`, `companions_settle_locally_except_where_the_server_answered`, `unrelated_records_are_unaffected`, `repeated_record_in_one_batch_completes_from_one_final_result`; `accepted_wire_rows_do_not_promote_companion_over_server_authority`, `accepted_companion_cascade_does_not_resurrect_descendants`.
- **A rejection rolls back the mutation and its lifecycle dependents beside an accepted one, and the reason survives restart until dismissed** (guarantee P5). Evidence: `failed_sibling_mutation_is_rolled_back_beside_the_accepted_one`, `rejection_cascades_to_lifecycle_dependents`, `all_rejected_batch_removes_optimism_and_keeps_direct_edits`; `rejection_removes_optimism_preserves_direct_truth_and_has_durable_inbox`; [crates/sim/tests/push.rs](../../../../../crates/sim/tests/push.rs) `p5_rejection_rolls_back_and_rejects_dependents`.
- **A receipt with the wrong envelope or coverage is refused whole and the batch stays frozen; a record that does not fit is skipped and reported and the batch completes; a duplicate changes nothing before or after reopen and never touches a later batch; a deletion keeps its stamp** (guarantee A5, D5). Evidence: `a_receipt_that_cannot_be_applied_is_refused_and_the_batch_stays_frozen`, `a_receipt_record_that_does_not_fit_is_skipped_and_the_batch_completes`, `duplicate_receipt_is_ignored_and_completion_survives_restart`, `deletion_authority_removes_the_row_and_retains_the_stamp`; `record_status_reports_phases_and_duplicate_ack_is_idempotent`.

- **A receipt whose authority lands under a pending edit that no longer replays reports the divergence; the edit stays queued and is still sent** (guarantee D8). Evidence: `divergence_is_reported_from_a_receipt_too`, `a_pending_update_over_a_deleted_base_diverges_and_is_still_sent`.

Executed 2026-09-16: `cargo test -p axton-sqlite --locked` and `cargo test -p axton-binding --locked` passed with the suites above. The runtime test that replaced the binding session test ran in `cargo test --workspace --locked` on 2026-09-26, 687 passed (owner, [#165](https://github.com/zanminwang/axton/pull/165)).

## 11. Risks and Technical Debt

**Accepted limitation.** Only lifecycle dependents are rejected with their parent; a sequence dependent of a rejected mutation is still sent. This matches guarantee P5 as written and is noted because the two dependency kinds are easy to confuse ([Dependencies](push/dependencies.md)).

**Accepted limitation.** The receipt carries the uploaded targets only ([#140](https://github.com/zanminwang/axton/issues/140)): records a handler touches beyond them, and a server-side cascade (a database `ON DELETE CASCADE`, for example), are not in it; a locally cascaded child whose parent's receipt state is `null` is deleted with it, but a child the server removed while the parent survived is corrected only when a channel delivers it.

**Resolved ([#122](https://github.com/zanminwang/axton/issues/122)): a replay failure is no longer silent.** When staged authority leaves a pending operation with nothing to apply to (an update over a deleted base, a create over an existing one), the visible row is the base, the mutation stays queued and is sent, `record_status` marks it `diverged` until it completes or is rejected, and the receipt's `ApplyReport` (or the page's) carries a `diverged` report with the ordinal ([Pull](pull.md)).

**Accepted consequence.** A record's authority in the receipt is an optional extra for records with no pending operation: it is written directly. The applier has no way to tell a record the client never held from one it deleted, and does not need one, because deleted records keep their stamp ([Pull](pull.md)).
