# Protocol tests

Verify the [Sync](../../architecture/protocols/sync.md), [Client bridge](../../architecture/protocols/client-bridge.md) and [Server bridge](../../architecture/protocols/server-bridge.md) carriers separately from runtime execution.

| Rule | Inspected assertions | Limit |
| --- | --- | --- |
| Immutable Batch digest binds context, order and descriptor; malformed counters, duplicate operations and protocol 4 input are refused. | [sync.rs](../../../../crates/protocols/tests/sync.rs): `a1_canonical_batch_binds_exact_order_context_and_descriptor`, `strict_envelopes_counters_and_duplicates`. | Pure encoding/admission checks. Retired input rejection remains current evidence. |
| Fragment arrival grants no commit; complete manifests bind payload, context and coverage. | Same suite: `delivery_manifest_binds_coverage_payload_parts_and_complete_units` asserts unchanged progress after errors, no ready unit for a partial fragment and coverage only after complete commit. | Pure staging model; no SQLite transaction. |
| Cursor-null reads and owned materialization retain their context/owner boundaries. | Same suite: `handshake_and_reads_correlate_store_stream_request_and_modes`, `owned_materialization_has_no_ranges_and_rejects_foreign_keys_or_owner`. | Carrier validation, not transport. |
| Retained Host fixture covers request and response variants. | [server_bridge.rs](../../../../crates/protocols/tests/server_bridge.rs): fixture/serde operation-set equality, guard cardinality, duplicate tracking and invalid Handler/Loader outcomes. | Host carrier validation; real adapter behavior is integration evidence. |

Run `cargo test -p axton-protocols --locked`. Shared fixtures live in [protocol fixtures](../../../../fixtures/protocol).

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/components/protocol.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
