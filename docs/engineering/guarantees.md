# Guarantees

The requirements for protocol 5. A passing component test does not establish every cross-runtime guarantee; [coverage review](testing/review.md) records evidence and gaps. The [0.3 contract](guarantees-0.3.md) and protocol-4 pages are historical references, not supported file migrations or alternate public APIs.

Each client owns one physical SQLite file, one Store and one Stream. Different files may follow the same Stream. Authentication and publication of every change affecting Loader content or visibility are application responsibilities.

## Failure isolation

Independent calls have independent outcomes. A local transaction, declared dependency or required authority group defines a shared atomic boundary. Failure rolls back that boundary, never unrelated calls. A network page can contain several units: earlier committed units survive a later failure, but progress cannot cross the failed unit. A fresh smaller request may recover an independent prefix; required groups are never split.

The bound API has no custom `onStore` callbacks. Local transactions, Mutation companions, observers and declared relations remain.

## L. Local writes

| ID | Required behavior |
| --- | --- |
| L1 | Reads show the settled base with pending operations replayed in order. Transactions see their own writes; other readers see committed state. |
| L2 | Committed Models, queued Mutations, rejections, saved requests and completions survive reopen. |
| L3 | Transaction failure rolls back its writes and queued calls. Nested savepoints can roll back their scope without discarding the outer transaction. |
| L4 | Direct writes are device-only and carry no Stream content version. They release live-content protection but retain historical evidence and true Stream tombstones. Older Mutation settlement cannot retimestamp companions or undo later independent writes. Newer Stream positions replace settled direct state; older/equal duplicates do not. |
| L5 | Local deletion follows declared cascades in the initiating rollback scope. Device cascades manufacture no authoritative child version; backends publish canonical child deletions explicitly. |

Pending optimism changes the projection without clearing base protection. Ordinary reads may refill local absence, but not current Stream tombstones. Compatible same-position rematerialization updates the base while preserving surviving direct and pending layers.

## P. Push

| ID | Required behavior |
| --- | --- |
| P1 | Frozen-request retry returns its saved backend outcome without rerunning handlers, preparation, tracking or publication. |
| P2 | Authenticated Store context, Mutation identity and exact frozen intent reject foreign or conflicting replay before business work. Each immutable Batch contains named Mutations with independently committed backend outcomes. |
| P3 | Unready prerequisites block dependent calls; lifecycle and sequence dependencies retain their declared meaning. Independent ready work can proceed. |
| P4 | Frozen intent, call identity and original descriptor survive retries, reopen and supported retained-schema reconciliation unchanged. |
| P5 | Rejection removes only that Mutation's optimism and companions, rejects lifecycle dependents and retains a readable refusal. Other pending work replays. |
| P6 | A Mutation-level refusal rolls back its savepoint and effects, preserving successful independent calls. Infrastructure failure rolls back the enclosing delivery/receipt and stays retryable. |
| P7 | Unsupported Mutation versions are per-call refusals. Backend acceptance followed by local failure remains accepted-awaiting-settlement; it never becomes business rejection. |

Dropping an unsent call is an explicit local decision. Dismissal, retry and drop within a transaction share its rollback scope. Frozen accepted work is never silently discarded.

## Q. Call outcomes

| ID | Required behavior |
| --- | --- |
| Q1 | Named Mutations atomically commit typed input, optimism, callback-owned local writes and durable intent. Query/Fetch are direct reads, not queued Mutations. |
| Q2 | Durable calls have one immutable backend outcome; business work, publications and saved outcome commit together. Replay allocates no new position or membership. |
| Q3 | Awaiting a Mutation invocation waits for local acceptance and returns a Call. `Call.wait()` waits for backend outcome and the initiating Store's local settlement. Waiting before the enclosing commit fails promptly. |
| Q4 | Query/Fetch await response validation and durable completion. `store:true` also waits for permitted Model writes; false writes no Models/authority and waits for no unrelated Stream progress. |
| Q5 | Results hold the invocation's declared nullable/list/scalar snapshot, which may differ from the Store. Protected cache no-ops still return that snapshot successfully. |
| Q6 | Post-commit observer/diagnostic exceptions cannot replace outcomes, reexecute handlers or become transport failures. |
| Q7 | Mutation input targets are mandatory settlement obligations. Declared Model outputs remain immutable invocation results; returning them alone neither enrolls them nor installs them in the Store. Explicit Stream publication controls their later authoritative delivery. |
| Q8 | Returning/storing a Model never enrolls it. Tracking is explicit and commits with the read's saved outcome. Query handlers have no framework business-write publication lane. |

For Query/Fetch, default and explicit true are equivalent; false installs no Models. Every invocation has a fresh request and snapshot; public once, refresh and saved-result invalidation controls are removed.

## T. Backend transactions

| ID | Required behavior |
| --- | --- |
| T1 | Framework-owned transactions use Serializable isolation. Publication and versioned materialization acquire the persisted namespace fence before relevant work, maintaining position/content correspondence. |
| T2 | Serialization conflict retries the entire transaction/handler within bounded driver retries, discarding previous output. Exhaustion is infrastructure failure, not business rejection. |

Read-only Queries remain concurrent. Explicit tracking upgrades the read to the fence and may force a whole-handler retry. Application-owned transactions must acquire that fence before relevant business work; later publication cannot repair an unfenced snapshot.

Handlers and preparation tolerate retries. Record external effects transactionally and perform them after commit. Publish every Loader projection/visibility dependency; materialize volatile external/time inputs or explicitly change projection generation.

## A. Authority and settlement

Only Stream delivery establishes authority. Current content/deletion protection, retained per-identity evidence (`G`) and completely committed Stream prefix (`C`) are separate facts. They are protocol metadata, not application fields or independent row stamps.

| ID | Required behavior |
| --- | --- |
| A1 | Authority replaces settled older state; later pending operations replay over the base. |
| A2 | `C` advances only across a proven contiguous committed prefix. Required companions may be ahead of that prefix, bounded by the observed head; installing them advances `G`, never an invented `C`. |
| A3 | Mutation completion requires committed execution acknowledgment plus real target Stream evidence or permitted call-owned private settlement. Untracked/no-op/other-Stream calls can finish without enrolling targets or bumping a shared cursor. |
| A4 | Receipt-before-Stream and Stream-before-receipt converge. Receipts cannot replace newer authority, but their outstanding obligations still complete after sufficient evidence commits. |
| A5 | Admission checks exact frozen intent, Store identity, Stream and retained descriptor from this database. Execution acknowledgment persists before fallible local settlement; queue retirement and terminal completion commit together. |

Receipt-owned Materialize can obtain current authority for an otherwise missing historical target. It runs no public Bootstrap handler, enrolls nothing and advances no `C`. A later membership Remove may enable saved private fallback at the original operation order, subject to original generation/current guards. Acceptance cannot revive work superseded by newer Stream content.

## D. Distribution

Convergence assumes correct publication, valid contracts and resumed delivery. Device-only content and deliberate cache-only reads are outside the authority comparison.

| ID | Required behavior |
| --- | --- |
| D1 | Stores with the same Stream and compatible viewer/materialization context converge after delivery and settlement. Their queues, observers and progress remain independent. |
| D2 | Higher Stream positions replace older authority; older/equal duplicates cannot undo direct state. Compatible same-position rematerialization updates the base without erasing surviving local layers. |
| D3 | Explicit track enrolls missing pairs with real positions. Repeated live pairs do not move the cursor. Invalidation updates affected existing pairs without enrolling them. |
| D4 | Content and position come from one fenced canonical view. Preparation publications settle before final reads; replay runs no preparation. |
| D5 | True Stream absence retains protected identity/position evidence. Older Stream content and cursor-null cache reads cannot revive it. Newer child authority survives an older parent cascade. |
| D6 | Membership Remove changes holding evidence, releasing current live protection while retaining content, historical authority and genuine deletion evidence. It is not canonical absence. |
| D7 | Loader/constraint/commit failure cannot advance progress across its unresolved unit. Earlier committed units remain; fresh smaller requests can recover independent prefixes. Persistent required-group failure remains observable. |
| D8 | Invalid envelopes, context mismatch, unsupported schema, divergence and capacity/constraint failures report diagnostics without inventing absence, completion or progress. |
| D9 | Binding/incarnation persist offline and across reopen/reconnect. The initial boundary commits once; ACK alone proves neither delivery nor Bootstrap coverage. Reconnection never replaces committed progress. |
| D10 | Handshake commits initial head S and C=S. B remains absent until the last complete Bootstrap unit commits. Bootstrap and Sync capture finite authority heads; completion is neither perpetual freshness nor every historical transition. |

Bootstrap selects initial Models through concrete `@@bootstrap` declarations after explicit application enrollment. Companions share an atomic unit but earn no ordinal coverage. Later Stream delivery includes unmarked Models normally. Compatible rematerialization also includes Store-held authoritative identities and preserves local work. Rows, evidence and unit coverage commit together.

Query/Fetch Model snapshots always carry `cursor:null`. Storage checks current protection at commit, not whether an ID ever appeared in the Stream. Declared delete-cascade references to a currently Stream-deleted parent suppress stale child cache writes without inventing child authority. Missing or device-deleted parents do not imply that guard.

## N. Native Loads

Public Load jobs/schema declarations and N1–N8 are retired. The [0.3 contract](guarantees-0.3.md#n-native-loads) retains internal compatibility requirements. Protocol 5 uses Bootstrap, Query and Model Fetch.

## R. Resilience

| ID | Required behavior |
| --- | --- |
| R1 | Local reads/writes continue offline. Bound open requires no handshake; pending work resumes when connectivity recovers. |
| R2 | Lost, duplicate, delayed and reordered messages preserve admission/commit rules. Progress requires resumed delivery. |
| R3 | Reopen retains committed rows, context, evidence, manifest coverage, frozen work and completions. A crash cannot expose half a required local unit; close/reopen alone proves no arbitrary crash boundary. |
| R4 | A physical SQLite file has one runtime owner across aliases, hardlinks and processes. Duplicate open fails before schema coordination. Different files may follow the same Stream. |

Explicit reset retains binding/file ownership, changes incarnation and atomically retires replica state. It refuses pending work unless explicitly discarded, reports abandoned Calls and fences old effects. Supported schema reconciliation retains complete old descriptors; incompatible storage changes fail explicitly.

Batch replay retains current/last-completed outcomes and prunes the older set on valid next-Batch admission. Immutable delivery staging expires after five minutes; expiry proves no coverage. Capacity limits fail explicitly. [Protocol 5](architecture/protocol/0.5.md), [client](architecture/client/storage/protocol5.md) and [server](architecture/server/protocol5.md) own mechanisms and bounds; [adoption](protocol5-adoption.md) owns release gates and limits.
