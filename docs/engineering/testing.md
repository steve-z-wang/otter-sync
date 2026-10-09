# Testing

**Contents**

- [What we test](#what-we-test)
  - [Guarantees](#guarantees)
  - [Component contracts](#component-contracts)
- [Test responsibilities](#test-responsibilities)
- [Code map](#code-map)

## What we test

### Guarantees

[Guarantees](guarantees.md) describe the bound Store contract, including offline durability and convergence after delivery resumes. Tests check these requirements under their stated conditions. [Protocol-5 adoption](protocol5-adoption.md) separates current host, capacity and installed-artifact gates.

### Component contracts

Each [component](architecture.md) defines its own responsibilities, interfaces and rules, such as the compiler rejecting an invalid schema or a binding preserving values across languages. Tests check these contracts, including how components work together at their boundaries.

## Test responsibilities

The sections above define what to verify. This tree assigns responsibilities to AXTON's tests. One guarantee or component contract may need several kinds of evidence: simulation can explore message ordering, while integration tests check real database transactions.

Component, integration and end-to-end describe the scope of a test. Simulation describes a method and environment; AXTON uses it to exercise the Rust client and server together under controlled faults.

- **[Component tests](testing/components/README.md)** — Verify a component's own rules.
  - **[Schema](testing/components/schema.md)** — Valid descriptors, types and relationships.
  - **[Protocol](testing/components/protocol.md)** — Shared messages, encoding and validation.
  - **[Compiler](testing/components/compiler.md)** — Parsing, validation and generated output.
  - **[Client](testing/components/client.md)** — Local operations, dependencies, batches, page application, receipt completion and the runtime's task scheduling.
  - **[Server](testing/components/server.md)** — Request handling, handler/loader calls and publication.
- **[Simulation](testing/simulation/README.md)** — Verify overall Rust sync behavior across clients and a server.
  - **[Scenarios](testing/simulation/scenarios.md)** — Named examples of the behavior promised by guarantees.
  - **[Invariants](testing/simulation/invariants.md)** — Explicit properties checked by deterministic protocol-5 examples.
  - **[Failure and recovery](testing/simulation/recovery.md)** — Delivery faults, restart and reproducible failures.
- **[Integration tests](testing/integration/README.md)** — Verify real boundaries and their contracts.
  - **[Storage and persistence](testing/integration/persistence.md)** — Database transactions, durability and concurrency.
  - **[SDKs and bindings](testing/integration/bindings.md)** — Generated types, conversion, callbacks and native lifetimes.
  - **[Connection](testing/integration/connection.md)** — HTTP/WebSocket handshakes, cancellation and reconnect.
- **[End-to-end tests](testing/end-to-end.md)** — Verify complete paths from a generated client through the backend to local state.

[Strategy](testing/strategy.md) explains how to choose the tests and environment. [Running tests](testing/running.md) lists commands and prerequisites.

[Coverage review](history/pre-protocol5/testing/review.md) records current gaps and the scope of the next testing issue. These pages define the intended responsibilities; they do not certify complete coverage.

## Code map

| Responsibility | Current source |
| --- | --- |
| Schema | [core contracts](../../crates/core/tests/contracts.rs) |
| Protocols | [protocol fixtures/tests](../../crates/protocols/tests) |
| Compiler | [compiler tests](../../crates/compiler/tests) |
| Client and SQLite | [SQLite tests](../../crates/sqlite/tests), including protocol05 suites, query, prerequisites, store and stream admission |
| Server | [server tests](../../crates/server/tests) for admission and finite plans |
| Simulation | [scenario05](../../crates/sim/tests/scenario05.rs), [protocol05_coverage](../../crates/sim/tests/protocol05_coverage.rs), [mutation_versions05](../../crates/sim/tests/mutation_versions05.rs) |
| Persistence | [PostgreSQL suite](../../integration/persistence/server), with driver boundaries described in [persistence evidence](testing/integration/persistence.md) |
| Joined native transport and capacity | [v05-sdk](../../integration/v05-sdk/README.md) |
| SDK and binding boundaries | [bindings](../../integration/bindings), [generated API](../../integration/generated-api), [Dart tests](../../packages/frontend/dart/test) |

Current evidence pages identify inspected assertions and their limits. Their source inspection on 2026-10-09 is not an execution result. Historical execution records are retained in [history](history/pre-protocol5/README.md).
