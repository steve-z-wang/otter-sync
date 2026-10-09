# Simulation

Simulation is a testing method and environment. AXTON uses it to exercise the real Rust client and server together through a controlled network. Clients use temporary SQLite files; the server uses an in-memory host. This makes message ordering and restarts reproducible without running a network server or PostgreSQL.

- [Scenarios](scenarios.md) — Named examples of overall behavior.
- [Invariants](invariants.md) — Checks across generated sequences.
- [Failure and recovery](recovery.md) — Fault injection, replay and shrinking.

Implementation: [crates/sim](../../../../crates/sim). Behavioral contract: [guarantees](../../guarantees.md). The harness does not establish real PostgreSQL isolation, FFI lifetimes or socket behavior; those need [Integration tests](../integration/README.md).

It also has no live session and no Downlink worker: pulls, receipts and bounded historical pages are actions on the arena, applied through the same public client calls the worker makes. Both pull modes reach the server through its one `process_pull` entry point, so a bootstrap page here is the same contract the HTTP route serves; the worker's own scheduling - rotation, backoff, one request in flight, fencing a late answer - is asserted in the SQLite harness instead ([Client tests](../components/client.md)).

```sh
cargo test -p axton-sim --locked
```
