# Failure and recovery

[scenario05.rs](../../../../crates/sim/src/scenario05.rs) reopens the same SQLite file after Queued, Frozen, SavedAcceptance, Covered and Settled boundaries. [scenario05.rs](../../../../crates/sim/tests/scenario05.rs) permutes receipt/authority order and private versus Stream targets, then asserts exact retry bytes and durable completion. Close/reopen does not establish arbitrary process-kill or interrupted-commit durability.

The pure progress scenarios in [protocol05_coverage.rs](../../../../crates/sim/tests/protocol05_coverage.rs) serialize incomplete staging, test expiry without progress and verify replacement delivery. They do not inject partial network bytes or database failure.

Actual SQLite process-exit cases belong to [protocol05_settlement.rs](../../../../crates/sqlite/tests/protocol05_settlement.rs); joined HTTP/PostgreSQL behavior belongs to [adoption gates](../../protocol5-adoption.md). No current action-list shrinker exists. Preserve the tested commit, named scenario and fixture/order when reproducing failures.

Run `cargo test -p axton-sim --test scenario05 --locked`.

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/simulation/recovery.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
