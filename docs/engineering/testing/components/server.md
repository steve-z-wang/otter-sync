# Server tests

Verify [Server runtime](../../architecture/server/README.md) admission and finite planning independently of database adapter behavior.

| Rule | Inspected assertions | Limit |
| --- | --- | --- |
| Canonical slot input is checked before execution; descriptor fingerprints record intent rather than current schema authority. | [protocol_v05.rs](../../../../crates/server/tests/protocol_v05.rs) checks bad typed input refusal, changed descriptor admission, field reorder and nullable widening. | Validation only; no Handler execution or database. |
| Bootstrap can freeze content ahead of its requested prefix without claiming that newer position. | [delivery_plan.rs](../../../../crates/server/tests/delivery_plan.rs) asserts observed head 45, coverage through 40, content cursor 45 and one canonical Loader call. | Scripted Host. |
| Live progress waits for all fragments of a frozen unit. | Same suite: `live_coverage_waits_for_every_frozen_fragment` checks cursor stays 40 until the third fragment advances it to 45. | Live state machine with fixture responses, no socket. |
| Old server configuration is refused. | [host_contract.rs](../../../../crates/server/tests/host_contract.rs). | Active negative configuration admission. |

Independent member execution/replay and publication/savepoints require [PostgreSQL evidence](../integration/persistence.md). Run `cargo test -p axton-server --locked`.

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/components/server.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
