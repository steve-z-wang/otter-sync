# Testing

**Contents**

- [What we test](#what-we-test)
  - [Guarantees](#guarantees)
  - [Component contracts](#component-contracts)
- [Test responsibilities](#test-responsibilities)
- [Code map](#code-map)

## What we test

### Guarantees

[Guarantees](guarantees.md) describe promises about the Rust sync core as a whole, such as committed offline writes surviving restart and subscribed clients eventually converging. Tests check whether these promises hold under their stated conditions.

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
  - **[Invariants](testing/simulation/invariants.md)** — Properties checked across generated operation sequences.
  - **[Failure and recovery](testing/simulation/recovery.md)** — Delivery faults, restart and reproducible failures.
- **[Integration tests](testing/integration/README.md)** — Verify real boundaries and their contracts.
  - **[Storage and persistence](testing/integration/persistence.md)** — Database transactions, durability and concurrency.
  - **[SDKs and bindings](testing/integration/bindings.md)** — Generated types, conversion, callbacks and native lifetimes.
  - **[Connection](testing/integration/connection.md)** — HTTP/WebSocket handshakes, cancellation and reconnect.
- **[End-to-end tests](testing/end-to-end.md)** — Verify complete paths from a generated client through the backend to local state.

[Strategy](testing/strategy.md) explains how to choose the tests and environment. [Running tests](testing/running.md) lists commands and prerequisites.

[Coverage review](testing/review.md) records current gaps and the scope of the next testing issue. These pages define the intended responsibilities; they do not certify complete coverage.

## Code map

Current test locations. Some suites support more than one responsibility.

| Test area | Code location |
| --- | --- |
| Component / Schema | [core contracts](../../crates/core/tests/contracts.rs), [compiler/tests](../../crates/compiler/tests) |
| Component / Protocol | [core contracts](../../crates/core/tests/contracts.rs) |
| Component / Compiler | [compiler/tests](../../crates/compiler/tests) |
| Component / Client | Engine scenarios and live session transitions in [sqlite/tests](../../crates/sqlite/tests), the runtime in [runtime.rs](../../crates/sqlite/tests/runtime.rs) and [runtime_lanes.rs](../../crates/sqlite/tests/runtime_lanes.rs); scheduling tests in [client/connection.rs](../../crates/client/src/connection.rs) |
| Component / Server | [server/tests](../../crates/server/tests) (`readback.rs` for the push readback, `host_contract.rs` for the twelve host operations, `stamp.rs` for stamps and pages) |
| Simulation / Scenarios | [sim/tests](../../crates/sim/tests) |
| Simulation / Invariants | Checks in [sim/src/invariants.rs](../../crates/sim/src/invariants.rs); runner in [sim/tests/invariants.rs](../../crates/sim/tests/invariants.rs) |
| Simulation / Failure and recovery | [resilience.rs](../../crates/sim/tests/resilience.rs), [net.rs](../../crates/sim/src/net.rs), [shrink.rs](../../crates/sim/src/shrink.rs) |
| Integration / Storage and persistence | SQLite contracts in [store.rs](../../crates/sqlite/tests/store.rs), [ddl.rs](../../crates/sqlite/tests/ddl.rs) and [rebuild.rs](../../crates/sqlite/tests/rebuild.rs); PostgreSQL in [integration/persistence](../../integration/persistence) |
| Integration / SDKs and bindings | [bindings/common/tests](../../bindings/common/tests), [integration/bindings](../../integration/bindings), [packages/dart/test](../../packages/dart/test), [integration/generated-api](../../integration/generated-api) |
| Integration / Connection | Client tests in [live.test.mjs](../../integration/bindings/client-js/live.test.mjs) and [live_test.dart](../../packages/dart/test/live_test.dart); server tests in [runtime.test.mjs](../../integration/persistence/server/runtime.test.mjs) |
| End-to-end | [integration/e2e](../../integration/e2e); device smoke tests in [integration/platform](../../integration/platform) |
