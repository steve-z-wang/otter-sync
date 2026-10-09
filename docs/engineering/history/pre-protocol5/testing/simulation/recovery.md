# Failure and recovery

Explore delayed, duplicated, reordered or dropped messages, rejected mutations, subscription changes and client restart. These are logical faults chosen by the harness; a restart reopens a client's SQLite file, not an operating-system power failure.

Existing entry points: [resilience scenarios](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/tests/resilience.rs), [network queues](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/src/net.rs) and [action execution](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/src/step.rs).

```sh
cargo test -p axton-sim --test resilience --locked
```

A generated failure reports its seed, step, error and traces. Keep the commit, client count, run settings and trace when reporting it. [Replay and shrinking](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/src/shrink.rs) replay an action list and remove actions while preserving the failure identity.

Preserve a minimal failing trace as a named regression before fixing its cause.

**Process-failure injection: deferred.** The harness's `Crash`/`Restart` reopens a client's SQLite file between actions; it does not interrupt a commit, kill the process mid-page, or fail the server's persistence mid-batch. Adding those faults would need a fault-injecting store (interrupting between SQLite statements) and a failing host persistence, which is new harness machinery rather than a test. Decision for this round: record the boundary and keep R3 evidence limited to what the table below says; do not report close/reopen as arbitrary crash coverage. Reopen this when a defect points at a commit boundary or when the store gains a fault-injection seam ([#68](https://github.com/zanminwang/axton/issues/68)).

## Coverage review

Reviewed 2026-09-14; not executed.

| Fault | How the harness injects it | What is covered | Limits |
| --- | --- | --- | --- |
| Drop, duplicate, delay, reorder | queue operations on one network queue ([net.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/src/net.rs)) | R2 through the random runner; P1 through `Drop` of a receipt | Faults act on whole messages; there is no partial delivery or corruption. |
| Client crash and restart | `Crash` drops the client handle, `Restart` reopens the same SQLite file | R3 at every step boundary of a round trip (`r3_crash_after_every_step_loses_nothing`); L2 and P4 across restart | A step is not a commit. Every client transaction is one SQLite commit, so a crash between two commits inside one action (for example between two changes of a page) is not reachable. An operating-system kill during a commit is out of scope; SQLite's own durability is assumed. |
| Server failure | `FailNext` makes the next handler throw; the host rolls its tables back, stamps and publications included | P6 abort path | No server crash mid-batch, no persistence failure, no partial commit, no loader refusal or failure (those are in [readback.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/server/tests/readback.rs)). The host's savepoints are snapshots; real savepoint semantics are PostgreSQL-only. |
| Rejection | `RejectNext` makes the next handler reject | P5 | none |
| Subscription change | `Subscribe`, `Unsubscribe`; the stepwise `unsubscribe cannot remove content` check runs after every `Unsubscribe` | D6; A2 `a2_page_from_a_previous_subscription_is_stale_not_a_gap` | The in-flight-page-after-resubscribe case ([#32](https://github.com/zanminwang/axton/issues/32)) is fixed and its scenario enabled. Unsubscribing removes nothing, so there is no cleanup path to fault. |
| Membership move | `MoveMembership` with parent-and-child follow-through | D4 in random runs and the named `d4_parent_and_child_move_scopes_together_without_deletes` | Executed 2026-09-15 by `cargo test -p axton-sim --locked`. |

Shrinking replays candidate traces and keeps a removal only if the failure key (the invariant name or error text) is unchanged, so a minimized trace exposes the same failure, not merely some failure. A trace that references a crashed client after a removal is rejected rather than accepted. No test asserts that a shrunk trace still reproduces the original; that property follows from the key check, which was read, not exercised.
