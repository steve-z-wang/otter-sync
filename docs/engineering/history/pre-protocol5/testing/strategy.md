# Testing strategy

Start with the behavior a change must preserve. Read the owning component's architecture document, especially its quality requirements and known risks, then choose where that behavior can be observed and asserted.

## Responsibilities

| Test area | What it checks |
| --- | --- |
| [Component](components/README.md) | One component obeys its rules, including invalid inputs and failure cases. |
| [Simulation](simulation/README.md) | The Rust sync core behaves correctly across clients, message orders and recovery. |
| [Integration](integration/README.md) | Real storage, language and network boundaries satisfy the contracts the core relies on. |
| [End-to-end](end-to-end.md) | The assembled system completes a user-visible path. |

These are AXTON's testing responsibilities. Component, integration and end-to-end describe test scope; simulation provides a controlled environment for exercising the Rust core across components.

Choose by the property, not the test file's directory. Client engine tests currently use the SQLite crate as a harness; a mocked WebSocket test cannot establish actual socket behavior. Several test areas may support one guarantee or component contract when each checks a different failure mode.

## Guarantees and component rules

Keep top-level [guarantees](../guarantees.md) focused on overall, observable behavior of the Rust sync core. Simulation is the primary tool for exploring those behaviors across operation sequences. Real-boundary tests provide additional evidence when a behavior depends on database or transport semantics.

Detailed compiler, SDK, storage and connection rules belong in their [component documents](../architecture.md). Link their test evidence there; a tested component rule does not automatically need a new top-level guarantee.

## Adding or changing a test

1. State the expected behavior and the condition that could violate it.
2. Choose the smallest scope and an environment that can expose the failure, including real dependencies where their behavior matters.
3. Create a named, reproducible scenario with assertions on observable outcomes.
4. Run the relevant tests and record the command and result. Link important coverage gaps to their owning component.

Use named scenarios for known cases and generated sequences to explore interactions. Preserve a minimized regression when a generated run exposes a defect. Do not treat test counts, a fixed percentage split or a passing run as a complete coverage assessment.

[Coverage review](review.md) records the starting gaps for the next testing issue. That work must inspect assertions and define required checks per change; measured run times should be recorded only after execution.

Background: [ISTQB's test levels](https://astqb.org/2-2-test-levels-and-test-types/) distinguish component and system scopes; [FoundationDB's testing approach](https://apple.github.io/foundationdb/testing.html) combines simulation with live performance and hardware failure tests. AXTON's directory groups are a project-specific choice.
