# Independent 0.4 scenarios

`scenarios.json` contains eleven hand-authored scenarios and 122 steps. Each expected transition is independent of production code. `cargo test -p axton-sim --test oracle04 --locked` checks them against the separate requirements model; this is preparation, not engine acceptance.

[`docs/engineering/testing/0.4.md`](../../docs/engineering/testing/0.4.md) defines the event/snapshot vocabulary, requirement mapping, adapter seam and remaining PostgreSQL/SQLite/SDK evidence. Existing 0.3 integration runners are retained until their obligations have actual replacement evidence.


`native-scenarios.json` separately authors two production-compatible traces (fourteen steps). `cargo test -p axton-sim --test native04 --locked` compares their complete normalized snapshots through the actual binding actor and SQLite. The original symbolic scenarios remain unchanged.

`bash integration/0.4/run.sh` creates disposable PostgreSQL and drives real HTTP/WebSocket → forked Node native actor → SQLite. `scripts/test.sh` includes this runner. It requires the normal host toolchain, local listening permission and writable application-data ownership metadata. Its eighteen fixed cases include actual SIGKILL/reopen, delayed receipts, private and tracked settlement, and retained historical Remove. See the testing document for normalization, exact evidence boundaries and outstanding SDK/package obligations.

`cargo test -p axton-sim --test seeded04 --locked` adds eight finite local-native seeds and bounded failure reduction. `AXTON04_SEED` selects a decimal seed; `AXTON04_REPLAY` reads a saved failure artifact's reduced independent scenario. `cargo test -p axton-sim --test measure04 --locked -- --nocapture` reports actual local-commit and authored-carrier manifest observations; `AXTON04_MEASUREMENTS` saves JSON. These limited domains and measurement exclusions are documented in the testing guide.
