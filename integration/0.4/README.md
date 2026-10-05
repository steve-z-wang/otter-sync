# Independent 0.4 scenarios

`scenarios.json` contains eleven hand-authored scenarios and 122 steps. Each expected transition is independent of production code. `cargo test -p axton-sim --test oracle04 --locked` checks them against the separate requirements model; this is preparation, not engine acceptance.

[`docs/engineering/testing/0.4.md`](../../docs/engineering/testing/0.4.md) defines the event/snapshot vocabulary, requirement mapping, adapter seam and remaining PostgreSQL/SQLite/SDK evidence. Existing 0.3 integration runners are retained until their obligations have actual replacement evidence.
