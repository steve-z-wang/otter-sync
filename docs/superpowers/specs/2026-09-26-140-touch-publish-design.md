# Channel membership, change declarations and explicit results

Status: reviewed implementation proposal for [#140](https://github.com/zanminwang/axton/issues/140). Replaces the earlier API-only proposal and the superseded same-name input/output binding. No implementation is included. Baseline: `origin/main` at `9cfb0b88506b001fa78e744af132245a7d0d0230`, inspected 2026-09-26.

## 1. Contract and ownership

Keep four responsibilities separate:

| Responsibility | Declaration | Framework work |
| --- | --- | --- |
| Input mutation target | `todo Todo.create/update/delete` | Register change, reconcile the optimistic target through protocol authority, distribute to its Channels |
| Additional changed record | `ctx.touch.project(identity)` | Advance its version and distribute to its Channels; no automatic caller readback |
| Persistent distribution relationship | `ctx.channel(name).todo.add/remove(identity)` | Maintain which Channels receive a record's current state and future changes |
| Business result | Explicit schema output plus handler return | Resolve handler-supplied Model identities through Loaders and produce `result` |

Rust owns target inference, settlement, membership reduction, fan-out, stamps, result assembly and replay. SDKs collect typed declarations and execute host effects. PostgreSQL persistence owns SQL inside the application's transaction. Preserve the [architecture](../../engineering/architecture.md) and [guarantees](../../engineering/guarantees.md), with the intentional changes below documented in their owning pages.

The user approved the responsibilities. This document chooses `.todo.add/remove()` and `touch` as the concrete API for implementation: resource before verb matches `client.models.todo.create()`, and `touch` remains a change declaration, not a network send. These are explicit design recommendations following the naming review, not claims that the spelling already shipped.

## 2. Public backend API

```ts
const channel = ctx.channel(`project:${projectId}`);
channel.todo.add({ id: todoId });
channel.todo.remove({ id: otherTodoId });

// Same API without a local variable.
ctx.channel(`project:${projectId}`).todo.add({ id: todoId });

// Business writes were already performed through ctx.tx.
ctx.touch.project({ id: projectId });

// Mixed sets use existing generated, explicitly typed references.
channel.add([Todo({ id: todoId }), Project({ id: projectId })]);
channel.remove([Todo({ id: oldTodoId })]);
```

`ctx.channel(name)` creates a transaction-scoped operation handle, with no network request or durable creation. It is not a client subscription and does not check whether the Channel already exists. Repeated calls address the same logical Channel; JavaScript object identity is not promised. No `getChannel`, `publish`, `attach`, `detach`, `changes.add`, or public `changes.records` alias remains in the new surface.

```ts
interface ModelMembership<Identity> {
  add(identity: Identity): void;
  remove(identity: Identity): void;
}
interface Channel {
  todo: ModelMembership<TodoIdentity>;
  project: ModelMembership<ProjectIdentity>;
  add(records: readonly RecordRef[]): void;
  remove(records: readonly RecordRef[]): void;
}
interface Touch {
  todo(identity: TodoIdentity): void;
  project(identity: ProjectIdentity): void;
}
interface MutationContext<Tx> {
  tx: Tx;
  userId: string;
  callId: string;
  channel(name: string): Channel;
  touch: Touch;
}
```

These interfaces are generated per schema, not hand-maintained Model lists. Generate a discriminated Model/identity union for `RecordRef` and narrow existing `Todo(identity)` constructors accordingly. Mixed methods deliberately accept explicit references only; do not depend on invisible input-object symbol tags. The short generated method `channel.todo.add(args.todo)` can select identity fields from a structurally compatible value, but must snapshot only identity fields. An untagged `{id}` cannot identify its Model in `channel.add([...])` and must fail. No callable function with generated Model properties is needed.

Use existing lower-first Model accessors. Detect duplicate accessor names. The Channel namespace reserves `add` and `remove`; emit a specific compiler diagnostic for Models mapping to those keys. This is a documented generated-API restriction, not silent overwriting. Use null-prototype dictionaries and safe own-property definition for other keys such as `__proto__` and `constructor`. Do not add further arbitrary reservations.

`touch`, `add`, and `remove` synchronously validate and collect owned intents; they return void. Copy canonical identity values, including composite and temporal identities, at the declaration. Later object/Date/array changes cannot retarget them. Empty mixed arrays are no-ops; missing, null, malformed or unknown references fail. Validate and snapshot the entire mixed call before appending any intents so a caught error cannot leave a partial declaration. Reuse identity codecs rather than serializing whole business records.

All handles close when the handler/external callback settles, including when it throws. Escaped references reject later operations. A handler returning successfully is not yet a committed operation: settlement and required Loader reads may still fail.

`backend.transaction(async ({tx, channel, touch}) => ...)` provides the same generated API, has no inferred inputs or client result, and preserves its arbitrary application return value. Still-supported legacy slot handlers expose the same declaration API. Query contexts expose neither touch nor mutable Channel handles; forged effects are rejected by Rust. This does not claim to sandbox arbitrary writes through an application-owned `tx`.

## 3. Explicit results and automatic input authority

```axton
mutation UpdateTodo(todo Todo.update)

mutation UpdateTodoAndRead(todo Todo.update) {
  todo Todo
  project Project
}
```

The first operation has no named business output. Its input still automatically registers a change and receives authoritative reconciliation; the generated return/result keeps the existing no-output convention (`void` in application types, null in its wire result).

The second operation's outputs are independent of its inputs, even when names and Models match:

```ts
return {
  todo: { id: anotherTodoId },
  project: { id: projectId },
};
```

If input Todo A is modified and output Todo B is selected, protocol authority confirms A and `result.todo` is the Loader snapshot of B. Missing explicit output fields must fail; never fill them from input fields. The handler returns identities for Models and actual values for scalar/enum outputs. Generated handler types and client result types differ accordingly. No output declaration implies modification or Channel enrollment.

Input and output names are unique within their own namespaces, not across them. Explicit optional outputs require a handler field containing an identity or null; list outputs require an identity array. Optional absent input operands and empty input lists add no input targets. Each concrete input target still receives reconciliation even without outputs.

Delete inputs also produce no implicit business result. They receive ordinary authoritative deletion reconciliation. A developer wanting a deleted ID as a business result declares scalar identity fields (including each component for a composite identity) and returns those values. Do not add a new deletion-result grammar. An explicit `Todo` output always means a Loader-resolved Model: a deleted record cannot satisfy a nonnullable `Todo`; use `Todo?` and return null when appropriate. Preserve normal nullable/list read validation.

Caller authority is the union of mandatory input-target authority and explicit Model-output authority selected by the existing `store` option. `store: false` suppresses optional output storage, not input reconciliation or the business result snapshot. Extra touches alone do not add caller authority and do not require that caller to declare or read the touched Model. All sync-participating references still need valid schema identities and registered Model Loaders; extra touches do not invoke those Loaders as the initiating caller.

If a touched record is also an input or an explicit output, those independent reasons require the appropriate read. Reuse compatible reads at the same identity, version, owner and snapshot; never mix retained result versions with current authority versions. An output-read failure or input reconciliation failure preserves existing operation rejection/rollback. A later subscriber Loader failure is a subscriber page error under existing isolation rules; it cannot retroactively reject the committed mutation.

Completion still follows committed local application of required authority. This issue does not redesign `call.wait()`, direct Query completion, the Rust client runtime, onStore, or query-once caching. Update their fixtures when generated result shapes change; saved results remain immutable.

## 4. Membership semantics

Membership is a persistent `(channel, model, canonical identity)` relationship, independent of active subscribers and backend processes. Adding an absent member distributes its current state without declaring a content change. Adding an existing member does nothing observable. Removing an existing member stops future distribution; removing a non-member does nothing observable. Neither removal nor client unsubscribe deletes local business data.

Membership declarations form an ordered intent list. For each record/Channel pair, the last add/remove determines desired membership. Compare that final state with membership at settlement start: a present member removed then re-added in one settlement is unchanged; an absent member added then removed is unchanged. Intermediate membership is never externally published.

Every changed record advances its stamp once per successful Mutation/external settlement. Inferred input targets and explicit touches deduplicate. Its final member Channels all get a new publication position. An unchanged newly added member also gets one position at its existing stamp, initialized at 1 only if absent. Deduplicate a newly added and changed pair to one position. Separate successful mutations remain separate changes even in one batch.

Example: one Todo advances from stamp 7 to 8 and is in A and B. It may receive A cursor 121 and B cursor 46, both at stamp 8. Do not advance its stamp twice, and do not modify any client's subscription cursor. Notify live subscribers only after commit; no connected subscriber is required for durable distribution.

Membership applies to a logical identity, including after business deletion. A touched/deleted record remains enrolled so offline subscribers can receive Loader-null deletion evidence. Recreating the same identity later resumes distribution to the same memberships. Applications wanting a new lifecycle use a new identity or explicitly remove old memberships. If a transaction deletes and removes a record from A, the final relationship wins: A receives no new deletion notification. To notify A of deletion, keep it enrolled. Do not fabricate a global deletion for Channel removal.

### Removal and historical delivery

Keep invalidation evidence physically separate from membership. Both ordinary delta scans and Bootstrap scans must filter out identities that are no longer members in the scan's database snapshot, BEFORE applying the page limit. Otherwise an old invalidation joined to the record's current stamp would continue exposing later content after removal.

Do not erase historical rows or rewind Channel heads on removal. Removed positions are holes: existing page rules advance to the head or bounded origin when no eligible rows remain. No removal event or client eviction is introduced. A read begun in an earlier snapshot or an already in-flight response may still arrive; removal is not revocation of previously readable data. Loader authorization remains authoritative.

Re-adding a removed record overwrites its invalidation position with a new cursor and publishes its current state. Bootstrap retains its fixed-origin history scan and fixed live completion barrier; a record moved above the origin is covered by live delivery. Refine D10 documentation to say that coverage is subject to membership in the scan snapshot, not permanent entitlement to every historically published identity. Do not claim a point-in-time collection snapshot or maintained query results.

## 5. Storage and concurrency

Add `axton_membership` separately from retained invalidations:

```sql
CREATE TABLE IF NOT EXISTS axton_membership (
 channel text NOT NULL REFERENCES axton_channel(channel),
 model text NOT NULL,
 identity_key text NOT NULL,
 PRIMARY KEY(model, identity_key, channel),
 FOREIGN KEY(model, identity_key) REFERENCES axton_record(model, identity_key)
);
CREATE INDEX IF NOT EXISTS axton_membership_channel
 ON axton_membership(channel, model, identity_key);
```

The record-first primary key serves reverse membership lookup; the Channel-first index serves filtered scans. Identity payload remains in intents/invalidation rows. Ensure a Channel row at head zero when first enrolling, then publish to allocate its first actual position. Do not derive membership from invalidation retention, or enroll all old invalidations implicitly.

New persistence operations, mirrored in Rust host types and TypeScript:

| Operation | Request fields | Answer |
| --- | --- | --- |
| `lockRecord` | model, identityKey | current stamp or null if no row |
| `memberships` | model, identityKey | sorted unique Channel strings |
| `setMembership` | channel, model, identityKey, present | unit |

`lockRecord` performs `UPDATE axton_record SET stamp=stamp ... RETURNING stamp`, NOT merely `SELECT FOR UPDATE`, and never creates a missing row. For changed records use existing `advanceStamp` instead; for records with a final add intent use existing `ensureStamp`. These existing upserts also write-lock the row. Process the full union of changed records and membership-intent records in canonical key order, acquiring one of these three guards before reading membership. Under PostgreSQL Repeatable Read, concurrent membership/touch transactions then conflict on a written record row and restart on serialization failure instead of observing a stale membership snapshot. A remove-only record with no metadata is a no-op, ordered before a concurrent first enrollment.

After all record guards, obtain initial memberships, reduce desired state, apply only net relationship changes, and collect publication pairs. Emit pairs sorted by Channel then canonical record identity, so Channel head locks are acquired in a consistent order. Set-member creation inserts the Channel row if needed; perform these creates in sorted Channel order as well. Preserve bounded 40001/40P01 retries of the entire application transaction. Other application lock ordering may still require retries; do not claim deadlock freedom.

All business writes, relation edits, stamps, positions, invalidations and saved-call outcomes share the existing transaction/savepoint. Retried call IDs return stored outcomes without restamping or republishing. Membership operations and wakes roll back together. Record guards are an internal implementation detail, not extra version increments.

## 6. Engine and host interfaces

Use one settlement intent shape for modern handlers, legacy handlers and external transactions:

```ts
interface MembershipIntent {
  channel: string;
  model: string;
  identity: object;
  present: boolean;
}
interface SettlementEffects {
  changes: RecordRef[];
  memberships: MembershipIntent[];
}
```

Keep private `changes`, replace host `publications` with `memberships`. Modern handled actions also carry their existing `outputs`; refusal/error variants remain. Internal `HostRequest::Publish` remains a low-level cursor/invalidation write, not the removed developer API. Require typed membership fields and reject old/malformed effect payloads; upgrade all host producers together. Query effect rejection checks both arrays.

Refactor Rust to distinguish `changed = inputs union extras`, `reconcile = inputs`, and explicit output reads. A shared `settle_changes` in `crates/server/src/settlement.rs` owns record guards, membership reduction, stamp allocation and publication. `readback.rs` reads only required input authority using allocated stamps; it no longer decides which extra changes return to the caller. The output assembler retains explicit result assembly and output store policy. External settlement uses the same fan-out without client reads. Legacy slots keep input-target reconciliation while extras stop being automatic caller authority.

Do not weaken authorization, read-version checks for actual reads, immutable call replay or failure isolation to make this separation easier. A touch-only extra unknown to the caller is valid; an explicit output requiring an unsupported read contract is still invalid. A reference to an unregistered backend Model/Loader remains invalid.

## 7. Coordinated prelaunch change

There are no production users to migrate. Ship a coordinated compiler/server/generated-fixture change rather than a compatibility facade for old public APIs. Keep operation-history compatibility checks intact: removing implicit outputs is genuinely a contract change, unlike merely renaming backend helpers.

For repository sample/fixture histories, regenerate reviewed prelaunch baselines and retained-version fixtures from their source under the new rules. Preserve deliberate version-evolution tests rather than deleting all history. Do not silently rewrite a caller's history when compilation runs: an old external history with an incompatible output snapshot must still receive the existing version diagnostic. Existing decoder support for old retained `inputIdentity` descriptors may remain for bounded compatibility tests; the compiler must emit none for newly compiled source. No additional migration service, automatic backfill or production data rewrite is in scope.

The new membership table starts empty on an existing development database. Reset/re-enroll development fixtures explicitly; never assume old publications mean perpetual membership. No operation version bump is required solely for the host helper rename; actual retained output-contract incompatibilities still follow the existing version rules.

Preserve in-process wake limitations (#62), no eviction (#139), no automatic change detection and no new frontend subscription API. Runtime memory handles remain callback-scoped; persistent memberships do not.

## 8. Acceptance and review evidence

Implementation must demonstrate:

1. Explicit output names can equal input names while referring to different identities; missing output never falls back to input. No-output and delete-input results generate no implicit fields.
2. Input authority is mandatory with or without result/store/Channel; extra touch alone is absent from caller authority and can target a Model the caller did not declare.
3. One change stamp reaches all member Channels once; no membership means no fan-out. Output-only reads and unchanged add do not bump an existing stamp.
4. Net membership reduction, duplicate add/remove, declaration order, snapshots and callback lifetimes behave as specified.
5. Real PostgreSQL interleavings cover touch/add/remove, including first enrollment, repeated membership-only changes at unchanged stamp, rollback and driver retries.
6. Removed records are filtered before LIMIT in delta and Bootstrap, empty/holey pages progress, re-add publishes current state, deleted/recreated identities obey persistent membership.
7. Saved call replay creates no new stamps, cursors or membership changes; live delivery wakes after successful commit only.
8. TS/Dart generated results, backend types, external callbacks, legacy contexts, simulation, PostgreSQL adapters and documentation agree.

Preparation evidence: current source and architecture inspected, spec/plan cross-reviewed, document links and whitespace checked. No runtime tests or implementation have run for this document. See the [implementation plan](../plans/2026-09-26-140-touch-publish.md) for executable checkpoints and the [handoff](../plans/2026-09-26-140-touch-publish-handoff.md) for assignment.
