# Testing

**Contents**

- [What we test](#what-we-test)
  - [Guarantees](#guarantees)
  - [Component contracts](#component-contracts)
- [Test responsibilities](#test-responsibilities)
- [Code map](#code-map)

## What we test

### Guarantees

[Guarantees](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/guarantees.md) describe the bound Store contract, including offline durability and convergence after delivery resumes. Tests check these requirements under their stated conditions. [Protocol-4 acceptance](testing/0.4.md) separates independent symbolic expectations, actual native/SQLite traces, production PostgreSQL process tests and language/package evidence. Historical compatibility tests are not substitutes for those paths.

### Component contracts

Each [component](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/architecture.md) defines its own responsibilities, interfaces and rules, such as the compiler rejecting an invalid schema or a binding preserving values across languages. Tests check these contracts, including how components work together at their boundaries.

## Test responsibilities

The sections above define what to verify. This tree assigns responsibilities to AXTON's tests. One guarantee or component contract may need several kinds of evidence: simulation can explore message ordering, while integration tests check real database transactions.

Component, integration and end-to-end describe the scope of a test. Simulation describes a method and environment; AXTON uses it to exercise the Rust client and server together under controlled faults.

- **[Component tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/testing/components/README.md)** — Verify a component's own rules.
  - **[Schema](testing/components/schema.md)** — Valid descriptors, types and relationships.
  - **[Protocol](testing/components/protocol.md)** — Shared messages, encoding and validation.
  - **[Compiler](testing/components/compiler.md)** — Parsing, validation and generated output.
  - **[Client](testing/components/client.md)** — Local operations, dependencies, batches, page application, receipt completion and the runtime's task scheduling.
  - **[Server](testing/components/server.md)** — Request handling, handler/loader calls and publication.
- **[Simulation](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/testing/simulation/README.md)** — Verify overall Rust sync behavior across clients and a server.
  - **[Scenarios](testing/simulation/scenarios.md)** — Named examples of the behavior promised by guarantees.
  - **[Invariants](testing/simulation/invariants.md)** — Properties checked across generated operation sequences.
  - **[Failure and recovery](testing/simulation/recovery.md)** — Delivery faults, restart and reproducible failures.
- **[Integration tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/testing/integration/README.md)** — Verify real boundaries and their contracts.
  - **[Storage and persistence](testing/integration/persistence.md)** — Database transactions, durability and concurrency.
  - **[SDKs and bindings](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/testing/integration/bindings.md)** — Generated types, conversion, callbacks and native lifetimes.
  - **[Connection](testing/integration/connection.md)** — HTTP/WebSocket handshakes, cancellation and reconnect.
- **[End-to-end tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/testing/end-to-end.md)** — Verify complete paths from a generated client through the backend to local state.

[Strategy](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/testing/strategy.md) explains how to choose the tests and environment. [Running tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/engineering/testing/running.md) lists commands and prerequisites.

[Coverage review](testing/review.md) records current gaps and the scope of the next testing issue. These pages define the intended responsibilities; they do not certify complete coverage.

## Code map

Current test locations. Some suites support more than one responsibility.

| Test area | Code location |
| --- | --- |
| Component / Schema | [core contracts](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/core/tests/contracts.rs), [compiler/tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/compiler/tests) |
| Component / Protocol | [core contracts](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/core/tests/contracts.rs) |
| Component / Compiler | [compiler/tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/compiler/tests) |
| Component / Client | Engine scenarios and live session transitions in [sqlite/tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/sqlite/tests), the runtime in [runtime.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sqlite/tests/runtime.rs) and [runtime_lanes.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sqlite/tests/runtime_lanes.rs); scheduling tests in [client/connection.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/client/src/connection.rs) |
| Component / Server | [server/tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/server/tests) (`readback.rs` for the push readback, `host_contract.rs` for the twelve host operations, `stamp.rs` for stamps and pages) |
| Simulation / Scenarios | [sim/tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/sim/tests) |
| Simulation / Invariants | Checks in [sim/src/invariants.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/src/invariants.rs); runner in [sim/tests/invariants.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/tests/invariants.rs) |
| Simulation / Failure and recovery | [resilience.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/tests/resilience.rs), [net.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/src/net.rs), [shrink.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sim/src/shrink.rs) |
| Integration / Storage and persistence | SQLite contracts in [store.rs](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/crates/sqlite/tests/store.rs), [ddl.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sqlite/tests/ddl.rs) and [rebuild.rs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/crates/sqlite/tests/rebuild.rs); PostgreSQL in [integration/persistence](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/persistence) |
| Integration / SDKs and bindings | [bindings/common/tests](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/bindings/common/tests), [integration/bindings](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/bindings), [packages/frontend/dart/test](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/packages/frontend/dart/test), [integration/generated-api](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/generated-api) |
| Integration / Connection | Client tests in [live.test.mjs](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/bindings/client-js/live.test.mjs) and [live_test.dart](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/packages/frontend/dart/test/live_test.dart); server tests in [runtime.test.mjs](https://github.com/zanminwang/axton/blob/8ad6b3efbf999f148b6dfe7278d7a2c6dd91b643/integration/persistence/server/runtime.test.mjs) |
| End-to-end | [integration/e2e](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/e2e); device smoke tests in [integration/platform](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/platform) |
