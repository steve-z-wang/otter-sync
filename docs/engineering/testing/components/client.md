# Client tests

Verify local projection, durable intent, protected read caching and settlement against [Client architecture](../../architecture/client/README.md).

| Rule | Inspected assertions | Boundary |
| --- | --- | --- |
| A rejection preserves later direct work and its terminal completion across reopen; accepted private work waits for coverage. | [protocol05_settlement.rs](../../../../crates/sqlite/tests/protocol05_settlement.rs) checks visible text, completion, pending work and reopen in `rejected_owner_preserves_later_direct_and_completion_after_reopen` and `accepted_private_result_waits_for_coverage_then_settles_later_direct`. | Real SQLite, fixture acknowledgements. |
| Null-cursor reads do not replace protected authority or delete optimistic content; `store: false` changes neither rows nor evidence. | [protocol05_authority.rs](../../../../crates/sqlite/tests/protocol05_authority.rs) asserts the content and current/history evidence in `pending_optimism_does_not_release_base_protection_and_null_reads_never_delete` and `cache_mode_false_changes_no_rows_or_evidence_and_true_is_best_effort_without_g`. | Client authority/cache admission; no network. |
| Queries read the local projection and refuse write SQL. | [query.rs](../../../../crates/sqlite/tests/query.rs) checks relation queries, optimistic SQL results and write refusal. | Real SQLite query surface. |
| Retired storage is refused intact, while a Remove advances coverage without erasing content/history. | [stream_upgrade.rs](../../../../crates/sqlite/tests/stream_upgrade.rs) compares the original catalog, rows and database bytes and checks retained content and evidence. | Active negative admission and canonical Remove evidence. |
| Prerequisite readiness gates batches; restart, backoff and late callback fencing are separate runtime concerns. | [runtime_prerequisites.rs](../../../../crates/sqlite/tests/runtime_prerequisites.rs) uses a scripted Host and timer, asserts no freeze while unready and freeze after Ready. | No real HTTP or wall-clock service. |

Run `cargo test -p axton-client -p axton-sqlite --locked`. For joined transport and persistence evidence use [Connection](../integration/connection.md).

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/components/client.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
