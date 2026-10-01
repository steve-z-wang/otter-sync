# Scope membership API design

Status: proposed implementation contract, approved in conversation and awaiting review of this written spec. None of the new APIs below is implemented by this document.

Baseline: AXTON v0.2.0, commit `aba57d1441b8593724312b4d0a9ebdf7d4124d70`.

Existing behavior is documented in the [backend membership API](../../../website/docs/backend/api.md#channels), [client subscriptions](../../../website/docs/frontend/client-api.md#channels) and [v0.2 cutover contract](../../../website/docs/backend/deployment.md#channel-membership-cutover). This spec proposes the additional public API; it does not replace those current-behavior references.

## 1. Introduction and goals

Expose one consistent public vocabulary for synchronizing collections of records: **Scope**. A Scope can represent a user, an organization, or an application-defined audience. Applications choose those boundaries; AXTON does not require one Scope per screen or per business object.

Keep three responsibilities explicit:

- `add` and `remove` change which records a Scope holds.
- `tag(...).add/remove` edit server-side labels without changing that holding.
- `where(...)` selects currently held records for a membership or label operation.

`ctx.touch` remains the declaration that business content changed. Membership operations do not create, update or delete business rows. Labels do not grant access or retain records independently of membership.

Names follow their roles rather than one grammatical form: `scope` names a collection, `tag` and `where` construct editing/selection handles, and `add`, `remove` and `touch` declare effects. The postfix `.tag(...)` on an add declaration is an explicit convenience for labeling that addition. It has a different receiver and return type from `scope.tag(...)`; neither receiver exposes the other's unrelated operations. Do not introduce additional verbs such as `release` or `releaseTag` for effects already expressed by these operations.

## 2. Constraints

- Preserve v0.2.0 removal delivery, enrollment claims, local hold accounting and request fences. No tags, predicates or SQL travel to clients.
- Preserve the viewer Loader as the permission authority. A Scope name or tag is not authorization.
- Preserve named Mutations, transaction boundaries, saved-response replay and Load page atomicity.
- Support generated Models without introducing new reserved names such as `Tag`, `Name` or `Length`.
- Introduce the public spelling additively. Do not rename persisted tables, protocol fields or capability identifiers solely to rename Channel to Scope.
- This spec does not authorize a release, merge, Most Days migration or removal of application hooks.

## 3. Context and interfaces

### Scope and record operations

```ts
const scope = ctx.scope('User:alice');

scope.add.todo('A');
scope.add.todo(['A', 'B']);
scope.add.todo({ id: 'A' });

scope.remove.todo('A');
scope.remove.todo(['A', 'B']);

scope.add(Todo({ id: 'A' }));
scope.add([Todo({ id: 'A' }), Project({ id: 'P' })]);
scope.remove(Todo({ id: 'A' }));
scope.remove([Todo({ id: 'A' }), Project({ id: 'P' })]);
```

For a Model with one identity field, its generated method accepts that field's scalar value or the complete identity object. Composite identities require complete objects. Every record operation accepts one operand or a readonly array. Mixed operations require generated Model references, never an untyped `{ id }`.

`ctx.scope(name)` creates a transaction-bound handle, not a persisted Scope, subscription or network request. Names retain current Channel name validation. Record and label declarations are synchronous; database effects settle with the enclosing operation.

`add` ensures membership and preserves existing labels. Repeated addition is idempotent. `remove` withdraws the whole membership and clears its labels; an absent membership is a no-op. Neither operation changes the business record. Root `scope.add()` and `scope.remove()` without operands are invalid; empty record arrays do nothing.

### Add with labels

```ts
scope.add.todo(['A', 'B']).tag('journal:1');
scope.add.todo('C').tag(['X', 'Y']);
scope.add([Todo({ id: 'A' }), Project({ id: 'P' })]).tag('shared');
```

An add call returns a transaction-bound declaration handle naming exactly those records in exactly that Scope. Its `.tag(labels)` attaches labels to those records and returns the same handle for further `.tag(...)` calls. Ignoring the return value still declares the add. There is no explicit execute, await, commit or terminal call required.

The chained call ensures membership and adds labels inside the same transaction. On an existing member it unions labels without changing business content or publishing a label-only update. It never creates an additional retention reason.

The add handle copies operands when `add` is called. Each later `.tag(...)` appends a label attachment at its invocation position in declaration order; it does not rewrite an earlier operation or resurrect a removed member. Ordinary adjacent chaining therefore performs add, then label attachment. If a caller saves the handle, removes the member, and then calls `.tag(...)`, the missing-membership rule below applies. Handles expire with their originating callback.

### Label editing

```ts
const x = scope.tag('X');

x.add.todo('A');
x.add.todo(['A', 'B']);
x.remove.todo('A');
x.remove.todo(['A', 'B']);

x.add([Todo({ id: 'A' }), Project({ id: 'P' })]);
x.remove([Todo({ id: 'A' }), Project({ id: 'P' })]);

x.remove(); // Detach X from every current member of this Scope.
scope.tag(['X', 'Y']).add.todo('A');
scope.tag(['X', 'Y']).remove.todo('A');
```

`scope.tag(labels)` is a pure label-editing handle. It creates no label associations until a terminal call. It accepts one string or a nonempty readonly list; a list names each label to edit, not an AND/OR condition.

Label add requires each targeted record to be currently enrolled at that declaration's settlement position. A missing membership fails the enclosing handler or host operation and rolls back its effects; it is not silently skipped or enrolled. Label remove is idempotent, including for absent memberships. Removing the last label leaves the membership present. Tag-only edits allocate no content stamp or delivery cursor, invoke no Loader and produce no client event.

Label names retain the v0.2.0 rules: case-sensitive opaque nonblank strings, at most 256 UTF-8 bytes each, accepted spelling preserved. Copy and deduplicate arrays at invocation; accept at most 64 distinct labels per label operation. There is no separate create-tag call, tag reference count or direct-hold flag. Label handles cannot be reused outside their transaction callback. A label handle's argument-free `add()` is invalid because it names no records; its argument-free `remove()` explicitly requests whole-Scope detachment of the selected labels.

### Selection

```ts
scope.where({ tags: { all: ['X'] } }).remove();
scope.where({ tags: { only: ['X'] } }).remove();
scope.where({ tags: { all: ['X'], none: ['Y'] } }).remove();

scope.where.todo({ tags: { only: ['X'] } }).remove();

scope.where({ tags: { all: ['X'] } }).tag('Y').add();
scope.where({ tags: { all: ['X'] } }).tag('X').remove();
```

`where(predicate)` selects among present members of the current Scope. `where.todo(predicate)` additionally limits the selection to that generated Model. Building a selection freezes no database result and changes nothing; a terminal operation evaluates it at that operation's settlement position, after earlier declarations in the callback. Reusing a selection evaluates it again, against that later state.

| Predicate | Meaning |
| --- | --- |
| `tags.all: ['X', 'Y']` | Contains every listed label; other labels are allowed. |
| `tags.any: ['X', 'Y']` | Contains at least one listed label. |
| `tags.none: ['X', 'Y']` | Contains none of the listed labels. |
| `tags.only: ['X']` | The complete label set is exactly X. |
| `tags.only: []` | Has no labels. |
| `and: [predicate, ...]` | Every child matches. |
| `or: [predicate, ...]` | At least one child matches. |
| `not: predicate` | The child does not match. |

Sibling conditions combine with AND. Label order and duplicate labels do not affect matching. Reject empty predicates, empty `tags` objects, empty `and`/`or` groups and empty `all`/`any`/`none` lists. `only: []` remains the explicit valid selection of untagged members. Copy predicates at invocation. Reject malformed or excessively large inputs using a bounded, shared validator before recording effects; implementation must not allow unbounded predicate recursion or input bytes.

Selectors inspect membership and labels only. They do not query arbitrary Model fields, run application code, execute raw SQL or join business tables. There is no `where(...).add()` because its candidates are already held; `.tag(...).add()` edits their labels.

### Content authority

```ts
ctx.touch.todo('A');
ctx.touch.todo(['A', 'B']);
ctx.touch([Todo({ id: 'A' }), Project({ id: 'P' })]);
```

Touch has the same single/list and typed/mixed operand rules. It declares changed content, allocates one stamp per identity per enclosing operation, and publishes through all Scopes holding that identity under the existing settlement contract. A Model input to a Mutation already declares its authority; touch additionally affected records. Tag changes alone never imply touch.

Business deletion remains distinct from withdrawing a replica's holding. Touch a deleted business identity and keep it enrolled when subscribers need authoritative Loader absence. A record with no Loader remains device-only and cannot be added, removed, tagged, selected as a generated Model target, or touched through these publication interfaces.

### Client interfaces

```ts
const followed = await client.scopes.subscribe('User:alice');
await followed.bootstrap(); // Only when this application wants retained history.
await followed.unsubscribe();
```

`client.scopes` remains the canonical spelling already recommended in v0.2.0. Add `tx.scopes.subscribe/unsubscribe` as the corresponding spelling for local subscription intent, with the same transaction behavior as `tx.channels`. Client subscription operations neither edit server membership nor attach labels. `Subscription` handles and their existing status, bootstrap and lifecycle semantics remain unchanged.

Keep the rest of the framework's responsibility boundaries:

| Surface | Responsibility |
| --- | --- |
| `client.models.*` | Read, query and watch local state. |
| `client.transaction`, `tx.models.*` | Atomic local state changes; direct CRUD is device-only. |
| `client.mutations.*`, `tx.mutations.*` | Named durable business commands and their optimism. |
| `client.queries.*` | Finite request/response reads. |
| `client.loads.*` | Durable paged reads; handler enrollment is explicit. |
| `client.fetch.*` | Read one identity through its viewer Loader. |
| `connection`, `syncState`, failures and prerequisites | Existing lifecycle, status and recovery. |

Subscribe initialization still establishes the future-update starting point, rather than fetching all history. Initialize the subscription before a Load whose enrollment must receive later updates. Unsubscribe stops following and its bootstrap work; it does not delete cached Models. Tags stay entirely on the server.

## 4. Solution strategy

Use the existing server record catalog, Scope membership, label dictionary/associations and retained per-record delivery evidence. Public Scope names map to existing Channel identifiers. Label-only editing changes associations; selection expands matched members on the backend. Withdrawals continue to produce per-record removal evidence, not a tag deletion command sent to clients.

The new server effects need generated TypeScript declarations, validation, host operations, reducer support and PostgreSQL adapter support. Bulk selection and label updates should use set-based database operations; do not invoke a Loader for withdrawals or issue one statement per association when a batch operation suffices. Matching and mutation must use the same application transaction and ordered effective membership state. Failures roll back the whole enclosing unit rather than committing an arbitrary prefix.

The client wire target stays the v0.2.0 membership contract, including its existing `channel-membership-v1` capability. No new client tag table, expression evaluator, SQL execution surface, reference-counted labels, retention policy or extra cursor per label is introduced. Wire compatibility is a requirement to prove during implementation, not a tested claim of this design document.

## 6. Runtime view

### Overlapping labels

Initially A has X and Y, B has only X, and C has only Y. To withdraw only the records held under X alone, then remove the obsolete label from surviving records:

```ts
// Declare both inside the same handler or host transaction.
scope.where({ tags: { only: ['X'] } }).remove();
scope.tag('X').remove();
```

The result is A/Y present, B absent, C/Y present. Only B produces a withdrawal. A's label edit is server-only. Reversing the calls changes the predicate's input: after X is detached, `only: ['X']` matches nothing. `all: ['X'], none: ['Y']` is not equivalent to `only: ['X']`; a record carrying X and an unknown Z must survive an exact-only-X withdrawal.

### Transactions and reduction

Mutation and native Load handlers use the existing Serializable transaction/retry contract. Host operations run inside the transaction their caller owns and retain the existing adapter's isolation responsibilities. No external side effect runs inside a replayable handler; use an application outbox.

Within a callback, declarations settle in call order and membership events reduce under the existing v0.2.0 rules: initially absent add/remove emits no event; existing remove/add emits one upsert; final withdrawal emits identity-only removal; label-only edits emit none. Add-with-labels is still atomic when initial enrollment needs a Loader and a later validation, Loader, save or commit step fails.

Removing a record from one Scope releases only that holding. Another current Scope holding the same Model/identity preserves the local replicated base. A last-hold release uses the existing local ledger, absence evidence and request epoch fences, preserving pending/device-local work under the v0.2.0 contract. This is not a business delete and does not invoke `onStore` or schema cascades. Dependent Models must be explicitly enrolled and withdrawn by the application's publication policy; labels provide grouping, not inferred ownership.

### Capability boundaries

| Context | Allowed effects |
| --- | --- |
| Mutation/legacy slot handler; host transaction/publication | Membership add/remove, label edit, selection and touch. |
| Load handler | Membership add, chained labels, and explicit label add, only for identities the current page returns. |
| Query handler or any viewer Loader | No Scope effects or touch. |
| Client transaction / `onStore` | Local subscription intent only; no server Scope effects. |

Load handlers have no remove, where, bulk label detach or touch surface. A replay of a saved Load page performs no new enrollment or label editing and cannot reverse a later withdrawal. A fresh traversal may enroll again. Preserve all current enrollment/page size limits and atomic output admission; new APIs do not broaden which identities a Load can enroll.

## 9. Decisions and compatibility

Prefer Scope for the public collection vocabulary, verb-first generated Model operations for clarity, and explicit label editing independent of membership. Chained `.tag(...)` makes enrollment with labels one expression without overloading standalone label-add with enrollment.

Preserve the v0.2.0 spellings through compatibility facades:

- `ctx.channel(name)`, `channel.todo.add/remove`, mixed-array add/remove, and add options retain their current meanings. Legacy add options still enroll and union labels.
- Legacy `channel.remove({ tag: 'X' })` continues to withdraw every whole member containing X, including members with additional labels. It is equivalent to a new `scope.where({ tags: { all: ['X'] } }).remove()`, never to label detachment.
- `client.channels` and `tx.channels` remain subscription aliases. Existing status fields and saved/wire shapes are not silently renamed.

The new canonical `ctx.scope` facade exposes the operations in this spec. Compatibility does not require that legacy and canonical handles have identical object shapes, only that they address the same persisted membership and preserve old behavior. Generated function namespaces must handle Models whose names overlap JavaScript function properties; do not solve collisions by reserving new Model names.

Most Days adoption is separate. Its existing records may have no labels, and its publication and Load enrollment paths must apply one consistent backend policy before label-based cleanup replaces a hook. Backfill or explicit legacy handling is required. A fresh one-shot read without enrollment does not acquire a Scope holding; this design does not guarantee future cleanup of every such cache entry. Authoritative business-deletion hooks remain useful.

## 10. Verification requirements

Implementation acceptance must cover:

- Generated single/list parity, complete composite identities, mixed references, empty lists, immutable operand capture and Model/function-name collisions.
- Ignored add handles still enrolling; chained labels on new/existing members; repeated chaining; delayed chaining after removal failing without resurrection; expired handles rejected.
- Label add missing-membership rollback; last-label removal keeping membership; idempotent detach; no Loader, stamp, cursor or client event for label-only edits.
- Each predicate, exact-set matching with an extra Z, untagged matching, boolean combinations, invalid/bounded predicates, Model filtering and declaration-order selection.
- Real PostgreSQL selection/label effects, rollback and concurrent membership/touch/label operations using the supported transaction contract.
- Ordered final-event reduction, two-Scope holds, last-hold withdrawal, stale read fencing, pending/device-local preservation, reopen and duplicate/reordered delivery through the existing client machinery.
- Load returned-identity restrictions, failed-page rollback and saved replay performing no effects.
- v0.2.0 compatibility facades, unchanged capability/wire behavior, synchronized TypeScript/Dart subscription spelling, affected guide/snippet checks and appropriate e2e coverage.

This document records design and source inspection only. No implementation code, runtime tests, release work or Most Days changes have been performed for this proposal.
