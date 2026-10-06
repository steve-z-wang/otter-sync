# End-to-end tests

Verify complete application paths through generated clients, the real backend, PostgreSQL, HTTP/WebSocket and local SQLite. Component and symbolic tests cannot establish these assembled boundaries. The [0.3 reference](end-to-end-0.3.md) records retired Load/hook/subscription scenarios; current consumers use bound Stores and named calls.

After the prerequisites in [Running tests](running.md):

```sh
bash integration/e2e/run.sh
bash integration/e2e/todo-run.sh
bash integration/load-e2e/run.sh
bash integration/action-e2e/run.sh
bash integration/release/verify-installed.sh
```

Build native artifacts using the prerequisites first. The runners generate their fixture interfaces and start disposable PostgreSQL clusters; the E2E/To-do and installed-package runners also build their required artifacts. Application fixture teardown closes clients/listeners, drains admitted database transactions, then disconnects the pool. Unexpected backend or teardown errors fail the fixture; an ordinary canceled WebSocket handshake is classified separately.

## Application paths

| Path | Owning tests | Assertions |
| --- | --- | --- |
| Generated Node and Dart clients | [round-trip](../../../integration/e2e/round-trip.test.mjs), [console client](../../../integration/e2e/fixtures/round-trip/client.mts), [parity](../../../integration/e2e/parity.test.mjs) | Offline optimism/reopen; lost accepted response without handler rerun; independent local writes during delayed HTTP; real multi-page catch-up; committed observers; dependent Calls; pause/resume; normalized values and runtime parity. |
| One client/file/Stream | [binding lifetime](../../../integration/e2e/subscriptions.test.mjs) | Binding mismatch refusal, retained progress/coverage across reconnect and reopen, independent physical files. |
| Finite initialization | [Bootstrap](../../../integration/e2e/bootstrap.test.mjs) | Empty and paged manifests, moving identities, canonical absence, exact saved manifest ownership, immutable captured tail and completion only after coverage. A healthy prefix commits while a later Loader refuses; releasing the fault completes the same run. |
| Named historical reads and Mutation recovery | [read fixture](../../../integration/load-e2e/README.md), [tests](../../../integration/load-e2e/load.test.mts) | Query/Fetch default/false, durable once and refresh, reverse completion with Stream protection, no implicit enrollment, viewer/file isolation, exact frozen retry after actual SIGKILL, companion acceptance/refusal and multi-Model read failure without partial cache. The directory retains its historical name; it exposes no Load-job API. |
| Public To-do example | [backend scenarios](../../../integration/e2e/todo.test.mjs), [UI source harness](../../../integration/e2e/todo-ui.test.mjs) | Alice/Bob convergence; credential, primary-key, missing-target and ownership refusals; offline dependent Calls; backend restart; no-op completion; immutable result while Store content changes. The deterministic UI harness checks the actual hook source, not a device. |
| Typed business operations | [Action E2E](../../../integration/action-e2e/README.md) | Named Mutation input/output, Query storage and saved results, versioned Loaders and real Dart execution. Retired routes have explicit compiler-negative checks; retained-context replay is checked by the separate protocol transport suite. |
| Installed packages | [scratch npm host](../../../integration/release/installed/installed.test.mts), [scratch Dart client](../../../integration/release/installed_dart/bin/network.dart) | Installed CLI-generated APIs, bundled native loading without development library paths, offline/reopen, Bootstrap, normalized Mutation settlement and Query/Fetch mode selection against a real backend. No fixture imports repository SDK source. |

## Executed evidence and limits

On 2026-10-05, the current E2E runner passed ten cases (round-trip three, binding one, Bootstrap five, parity one); the named-read runner passed ten with Dart analysis clean; To-do passed eight. The Action runner passed thirteen, including three analyzed/executed Dart hosts; the actual UI source harness passed one. Current installed npm/backend and external generated Dart verification passed, plus seven loader/launcher checks. Final source-frozen full-host and macOS/Linux CI remain separate release gates; passing an individual runner does not establish them.

The [protocol-4 transport suite](0.4.md#actual-native-adapter-and-transport-runner) separately observes authority, receipts, coverage and injected COMMIT faults through real PostgreSQL and native actor children. Its fixed scenarios and bounded local-native seeds do not claim arbitrary server schedule coverage. SDK adapter tests do not establish Expo or Flutter device startup; [platform smoke tests](../../../integration/platform/README.md) own that boundary. Close/reopen is not a crash test; only the explicitly killed-process cases prove those named process boundaries, not power loss during SQLite COMMIT.
