# Simulation invariants

The current [protocol05_coverage.rs](../../../../crates/sim/tests/protocol05_coverage.rs) checks explicit protocol properties through deterministic examples: partial or expired fragments leave progress unchanged; complete units alone advance coverage; transitive dependencies remain one unit; owned materialization advances no range; publication overflow leaves input heads unchanged.

The overlay harness clones its candidate map, checks unique values, then installs the candidate projection. That is model evidence, not a real database transaction. The many-unit scenario checks ordered coverage and rejects a late low cursor hidden behind earlier progress.

There is no current generated sequence invariant runner or shrinker. The older invariant suite and its execution records are historical. [rng.rs](../../../../crates/sim/src/rng.rs) tests deterministic sequences and bounded choices only. Use [named scenarios](scenarios.md) and [real persistence integration](../integration/persistence.md) for their stated boundaries.

Run `cargo test -p axton-sim --test protocol05_coverage --locked`.

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/simulation/invariants.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
