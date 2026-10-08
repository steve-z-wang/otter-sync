# End-to-end tests

Verify complete application paths through generated clients, the real backend, PostgreSQL, HTTP/WebSocket and local SQLite. Component and symbolic tests cannot establish these assembled boundaries. The [0.3 reference](end-to-end-0.3.md) records retired Load/hook/subscription scenarios; current consumers use bound Stores and named calls.

After the prerequisites in [Running tests](running.md):

```sh
bash integration/e2e/run.sh
bash integration/e2e/todo-run.sh
bash integration/v05-sdk/run-host.sh
bash integration/action-e2e/run.sh
bash integration/release/verify-installed.sh
```

Build native artifacts using the prerequisites first. The runners generate their fixture interfaces and start disposable PostgreSQL clusters; the E2E/To-do and installed-package runners also build their required artifacts. Application fixture teardown closes clients/listeners, drains admitted database transactions, then disconnects the pool. Unexpected backend or teardown errors fail the fixture; an ordinary canceled WebSocket handshake is classified separately.

## Application paths

| Path | Owning tests | Assertions |
| --- | --- | --- |
| Generated Node and Dart clients | [round-trip](../../../integration/e2e/round-trip.test.mjs), [console client](../../../integration/e2e/fixtures/round-trip/client.mts), [parity](../../../integration/e2e/parity.test.mjs) | Offline optimism/reopen; lost accepted response without handler rerun; independent local writes during delayed HTTP; real multi-page catch-up; committed observers; dependent Calls; pause/resume; normalized values and runtime parity. |
| One client/file/Stream | [binding lifetime](../../../integration/e2e/subscriptions.test.mjs) | Binding mismatch refusal, retained progress/coverage across reconnect and reopen, independent physical files. |
| Finite initialization | [Bootstrap](../../../integration/e2e/bootstrap.test.mjs) | Empty and multi-unit Bootstrap ranges, moving identities, canonical tombstones, frozen plan ownership, immutable captured coverage and completion only after complete units. A Loader refusal leaves coverage incomplete; releasing the fault completes the same run. |
| Finite protocol-5 transport and recovery | [host runner](../../../integration/v05-sdk/README.md), [generated clients](../../../integration/v05-sdk/client.mjs) | Immutable Bootstrap coverage and captured tail, owned rematerialization with retained Model/Mutation history, Query/Fetch Store modes, explicit Query tracking, stale-read protection, exact Batch retry, independent Call fates and durable local settlement/reopen. |
| Public To-do example | [backend scenarios](../../../integration/e2e/todo.test.mjs), [UI source harness](../../../integration/e2e/todo-ui.test.mjs) | Alice/Bob convergence; credential, primary-key, missing-target and ownership refusals; offline dependent Calls; backend restart; no-op completion; immutable result while Store content changes. The deterministic UI harness checks the actual hook source, not a device. |
| Typed business operations | [Action E2E](../../../integration/action-e2e/README.md) | Named Mutation input/output, Query storage and saved results, versioned Loaders and real Dart execution. Retired routes have explicit compiler-negative checks; retained-context rematerialization is checked by the protocol-5 host. |
| Installed packages | [scratch npm host](../../../integration/release/installed/installed.test.mts), [scratch Dart client](../../../integration/release/installed_dart/bin/network.dart) | Installed CLI-generated APIs, bundled native loading without development library paths, offline/reopen, Bootstrap, normalized Mutation settlement and Query/Fetch mode selection against a real backend. No fixture imports repository SDK source. |

## Evidence and limits

The runners above exercise this checkout's current protocol-5 interfaces. Historical Load jobs, durable once/refresh mappings and the protocol-4 transport runner are retired; their earlier results do not establish current behavior. The [protocol-4 reference](0.4.md) preserves that release's evidence.

Each runner proves its named scenarios. Passing an individual runner does not establish the [full host gate](running.md#full-host-gate), installed release artifacts, capacity limits or macOS/Linux CI. The finite host freezes Bootstrap coverage and the captured tail; it does not prove convergence under arbitrary server schedules.

SDK adapter tests do not establish Expo or Flutter device startup; [platform smoke tests](../../../integration/platform/README.md) own that boundary. Close/reopen is not a crash test. An explicitly killed-process case proves only its named process boundary, not power loss during SQLite COMMIT. The deterministic [protocol-5 scenarios](../../../crates/sim/src/scenario05.rs) author cloud responses and complement, rather than replace, the real PostgreSQL host.
