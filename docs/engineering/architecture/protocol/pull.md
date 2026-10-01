# Pull

Engine behavior: [Client Pull](../client/engine/pull.md), [Server Pull](../server/engine/pull.md).

## 3. Context and Scope

`POST /sync/pull` accepts `{capabilities, models, cursors}` for delta delivery or `{capabilities, mode: "bootstrap", scope, models, after, until}` for one historical page. The SDK advertises `scope-membership-v1`; [Common](common.md) owns capability admission. `models` declares every generated Model's read version; authentication supplies the viewer. A durable subscription must have an initialized position before delta pull names it.

A delta response remains `{cursors: {scope: {from, to, head}}, changes}`. Each change names its Scope and log cursor:

```json
{"cursors":{"U":{"from":1,"to":3,"head":3}},"changes":[
  {"scope":"U","cursor":2,"kind":"upsert","model":"Entry","identity":{"id":"a"},"stamp":1,"state":{"text":"A"}},
  {"scope":"U","cursor":3,"kind":"remove","model":"Entry","identity":{"id":"b"}}
]}
```

An upsert carries ordinary stamped authority. Stamped `state: null` means authoritative absence; an `error` upsert is a Loader diagnostic and does not erase the base. A remove carries only `scope`, `cursor`, `kind`, `model` and `identity`; it has no stamp, state, error or tags and calls no Loader.

Bootstrap answers `{mode: "bootstrap", scope, from, to, until, head, changes}` with the same Scope changes, at most 50. It covers `(from, to]` within the fixed origin `until`; `to == until` is terminal. The final `head` is the ordinary-delivery completion barrier.

## 5. Building Block View

- Each Scope range satisfies `from ≤ to ≤ head`. Every event satisfies `from < cursor ≤ to`; each Scope/record pair and Scope position occurs at most once. A shared record may appear once in each Scope. Its body is loaded once in the server transaction, preserving each pair's evidence.
- Scans read the compacted log before applying the 50-event limit, including removals and without joining live membership. If another retained row exists, continue from the last emitted position; otherwise advance to the observed head, or the fixed bootstrap bound, across compacted gaps. A pair compacted above bootstrap's bound belongs to ordinary delivery.
- Delivery cursors advance by whole committed pages. The per-record membership cursor is local ordering evidence, never a second polling cursor. Content stamps independently order authority across paths.
- Clients merge all membership evidence before applying any positive body or releasing any base. Older evidence is inert; conflicting equal evidence aborts the transaction. Last-hold release evicts replication without Model cascade, pending-write deletion or domain writes. Another hold keeps the base.
- Historical identity/run checks and the delivery gap gate still apply. Membership evidence, content, progress and release commit atomically. Tags never enter client storage or the wire.

Errors: malformed requests and positions ahead of the head are `400 request.invalid`; unsupported retained Model versions are `409 model_version_unsupported`; missing required capability is `426 protocol.unsupported`; infrastructure failures are server errors. Loader errors are per-record diagnostics.

## 10. Quality Requirements

The shared [scope-membership fixture](../../../../fixtures/protocol/scope-membership.json) pins provenance, duplicate-pair refusal, safe counters, identity-only removal, authoritative-null and diagnostic distinctions, bootstrap and enrollment claims. Server delivery coverage lives in [stamp.rs](../../../../crates/server/tests/stamp.rs), [bootstrap.rs](../../../../crates/server/tests/bootstrap.rs) and [live.rs](../../../../crates/server/tests/live.rs); client holds and release coverage lives in [scope_members.rs](../../../../crates/sqlite/tests/scope_members.rs). These are evidence locations, not a claim that final cross-runtime acceptance has run.

## 11. Risks and Technical Debt

The page limit remains 50 per Scope. An error body is corrected by later authority rather than retried automatically. Removal logs remain retained; a future floor/snapshot mechanism must preserve offline convergence before pruning them.
