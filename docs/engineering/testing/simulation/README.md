# Simulation

Current simulation uses deterministic protocol-5 examples with distinct boundaries: real SQLite recovery with fixture acknowledgements, pure planning/progress models, and a server member-engine Host seam. It does not run a generated-action network arena or shrinker.

- [Scenarios](scenarios.md) — Named assertions and each harness boundary.
- [Invariants](invariants.md) — Explicit complete-unit and authority properties.
- [Failure and recovery](recovery.md) — Persisted reopen boundaries and expiry.

[Integration](../integration/README.md) owns real PostgreSQL, transport and native-language boundaries. Prior harness descriptions and execution records remain in [history](../../history/pre-protocol5/testing/simulation/README.md).

Run `cargo test -p axton-sim --locked`.
