# Internal ownership and retired compiler cleanup

**Status:** Proposed. Exploration and planning only; implementation requires approval.

**Baseline:** `71b14195b897bc8a42f19e0a83d59dfe8fc76677`, the merged Protocol/SDK refactor. Packages remain at 0.5.4 and the sync discriminator is 5.

## Goal

Remove unnecessary internal routes and unreachable retired compiler code while preserving the framework's APIs, messages, persistence and behavior.

## Findings

| Candidate | Evidence | Decision |
| --- | --- | --- |
| `client/src/runtime/protocol.rs` | Only re-exports `axton_protocols::client_bridge`; runtime admission uses this extra hop. | Remove the file and import/re-export directly from the owning crate. |
| `server/src/host.rs` | Only re-exports `backend_interface`; current engine modules still import through it. | Remove it; import Host invocation from `backend_interface` and messages from `axton_protocols::server_bridge`. |
| `server/src/error.rs` | Only re-exports Server bridge errors. | Remove it; preserve the existing server-root `Error`/`code` exports directly from Protocols. |
| `server/src/stream_members.rs` | Only re-exports Server bridge member DTOs. | Remove it; import those DTOs from their owner. |
| Current compiler Load validation/emission | `validate()` rejects nonempty source `Declarations.loads` before its later `validate_load` loop. `Validated.loads`, `validate::Load`, and Generate's Load emitters therefore have no reachable successful source-compilation path. | Remove this unreachable successful-compilation representation. Preserve the rejection path and historical descriptor support. |
| Old comments in runtime/interface/queue | Comments mention a deleted Load worker, store hooks, frozen Push ledger methods and nonpersisted completions; the nearby implementation now uses the format-5 queue/status/completion paths. | Remove orphan comments and describe the actual nearby invariant. |

This is source inspection, not executed test evidence. No implementation changes have been made.

## Approach

Use explicit owner imports and delete obsolete forwarding files. Retain a small set of intentional entry-point exports for actual callers. Simplify the compiler's validated current schema so it cannot represent a successful retired Load declaration.

A global rename of every `05`, `v05` or subscription-derived identifier would touch active state and interfaces without removing a responsibility. Keeping all forwarding modules would preserve the unnecessary paths. The proposed cleanup removes demonstrated redundancy and leaves live functionality intact.

## Owner boundaries

- `axton_protocols::client_bridge` owns Client bridge messages and local/query DTOs.
- `axton_protocols::server_bridge` owns Host requests/responses, member DTOs and structured errors.
- `crates/server/src/backend_interface.rs` owns `Host`, `HostResult` and `HostExt::call_typed`, with explicit imports of the bridge types it uses.
- `crates/client/src/runtime/` owns task admission, effects, transactions, observers and scheduling.
- The compiler's current Parse → Validate → Generate path accepts the supported schema. Historical JSON validation/reconciliation is a distinct retained responsibility.

Server runtime files consume protocol DTOs from Protocols rather than from a Host trait module. The backend interface stops glob re-exporting the entire Server bridge. Client runtime may continue its existing `runtime::{Input, Event, ...}` entry-point exports directly from Client bridge; the forwarding file adds no value.

Preserve the current server-root `Host`, `HostResult`, `Error`, `code` and entry functions; client-root Client/query/model/status exports; and `axton_client::v05`. These are deliberate in-repository entry points, not duplicate implementations. Removing them would create unrelated caller churn.

## Compiler deletion boundary

Remove from `crates/compiler/src/validate.rs`:

- `Validated.loads` and its documentation;
- `validate::Load`;
- the `LoadDecl` import used only by `validate_load`;
- `validate_load` and the later Load collection/assignment after the retirement gate;
- namespace comments that imply current generated Load methods.

Remove from `crates/compiler/src/generate.rs`:

- the validated `Load` import;
- conditional emission of `loads` from `Validated` in `descriptors()` and `schema()`;
- the private `load()` and `loads()` emitters.

Keep the exact current retirement diagnostic: `Load declarations were removed in 0.4; use Bootstrap or a named Query`. Keep Parse's recognition of retired Load syntax so callers still receive that useful diagnostic. Keep the CLI's rejection of retired Load history flags. `OperationNames`, `method_name` and `ROUTE_MEMBERS` also validate current Mutation/Query names; retain these shared helpers and existing diagnostics when removing `validate_load`.

Keep `crates/core/src/loads.rs`, `Schema.loads`, `Schema::load`, `validate_loads`, `compiler/history.rs::reconcile_load_history` and their tests. They validate retained JSON contracts. The compiler history fixture explicitly constructs saved Load descriptors from Query descriptors; it does not need the unreachable validated Load type.

No current generated descriptor gains a `loads` key, and no retained artifact loses one. Generated current outputs and historical fixtures must remain unchanged.

## Explicitly retained functionality

- `queue.rs` and `mutation_queue.rs` both operate current format-5 tables: projection/local-write ordering and durable input/Batch ownership are distinct responsibilities. Do not merge them just because their names overlap.
- `bootstrap.rs`, `subscriptions.rs` and SDK subscription-named helpers still serve current bound Store status/Bootstrap observers. Correct misleading comments; preserve fields, variants and serialized messages.
- `store05`, `sync05`, `settlement05`, `Publication05` and the `protocol05` host envelope are active code. Their names do not prove obsolete implementations.
- Query storage protection, tombstones, direct writes, companions, cascades, prerequisites, reset, file ownership, schema reconciliation, settlement and observer-after-commit rules remain unchanged.
- TypeScript/Dart protocol mirrors belong to independently shipped language packages. This task neither deletes them nor introduces cross-language code generation.
- Negative legacy-config/file admission tests are current guards, not obsolete tests.

## Scope boundary

No SDK API/export change, generated API redesign, serialization/error change, native ABI change, table/SQL change, dependency-version change, storage migration, Capso change, package publication or application/backend deployment. Documentation edits follow the existing website publication workflow after merge.

Keep original design notes and past specs/plans unchanged. SYN-24 crash diagnostics and SYN-25 release inventory filtering are separate changes. Do not fold in engine algorithms, class splitting, mass naming changes or unrelated dead-code hunts.

If verification uncovers another live caller or a contract dependency, preserve that contract and adjust the internal import. A behavioral defect is reported separately with a reproduction; it is not repaired by weakening an assertion in this refactor.

## Acceptance

- The four forwarding files and their module declarations are gone; no code/docs reference the deleted paths as current owners.
- Protocols still depends only on shared Core/schema and codec dependencies, never runtime/storage/native implementations.
- Current source Load declarations and old CLI flags retain their diagnostics; historical descriptor validation/reconciliation continues to pass.
- Existing protocol fixture bytes, SDK exports, generated outputs, Cargo dependency pins, native symbols and persistence layout remain unchanged.
- Rust tests/lints, native build, affected SDK/host tests and the complete joined host gate pass on the final branch. Evidence identifies the tested commit and any unexecuted platform checks.
- Review distinguishes intentional entry-point exports from redundant internal hops and confirms the retained-context Host behavior.

## Delivery

One internal cleanup PR. Rust facade removal, compiler cleanup and comment correction are separate reviewable commits. It may be prepared alongside the documentation PR. Merge the documentation PR first, then rebase this PR and update only source-owner references it actually changes. Before that merge, inspect changed links locally; the complete current-document path audit belongs to final integration after the documentation cleanup.

Implementation steps: [plan](../plans/2026-10-09-internal-cleanup.md).
