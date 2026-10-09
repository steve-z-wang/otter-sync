# SDK and binding tests

Verify that generated APIs and native calls preserve types, values, errors, callbacks and resource ownership. Link engine behavior to its existing tests; exercise it here when crossing the boundary introduces a distinct failure mode.

Existing entry points are the [runtime actor tests](../../../../bindings/common/tests/runtime.rs), the [C ABI tests](../../../../bindings/mobile/src/lib.rs), [JavaScript tests](../../../../integration/bindings/client-js), [Dart tests](../../../../packages/frontend/dart/test) and [generated API fixtures](../../../../integration/generated-api). The runtime's own scheduling and lifecycle rules are tested once in Rust ([Runtime](../../architecture/client/runtime.md)); these tests cover what crossing the carrier adds.

After the prerequisites and native build in [Running tests](../running.md):

```sh
cargo test -p axton-binding -p axton-mobile --locked
node --test integration/bindings/client-js/*.test.mjs
node --test integration/bindings/client-react-native/*.test.mjs
bash integration/generated-api/verify.sh
```

The generated API runner checks TypeScript positive and negative cases, analyzes Dart, and executes generated clients. Dart native tests also need the library-path environment described in the running guide.

Next review: negative type coverage per language, callback failures, native lifetimes and shared cross-language scenarios. A shared Rust engine alone does not establish SDK equivalence.

## React Native checks

[Mobile adapter tests](../../../../integration/bindings/client-react-native) exercise stream lifetime, rollback/isolation on real SQLite through the host carrier, and native-style HTTP/WebSocket authentication, subscription ordering, cancellation and bounded recovery. [C carrier tests](../../../../bindings/mobile/src/lib.rs) exercise malformed input and handle lifetime. These host checks do not establish iOS execution. The separate [simulator harness](../../../../integration/platform/react-native/README.md) owns actual Expo/Swift/Rust, offline process-restart and two-client evidence.

## Current coverage

| Boundary | Owning tests | Evidence limit |
| --- | --- | --- |
| Native runtime admission, completion delivery, observers and close | [runtime actor](../../../../bindings/common/tests/runtime.rs), [mobile C ABI](../../../../bindings/mobile/src/lib.rs) | Actual native runtime and local SQLite; mobile C ABI tests run on the host. |
| SDK task correlation, callback scope, savepoints, rollback and provisional Calls | [JavaScript transactions](../../../../integration/bindings/client-js/transaction.test.mjs), [Bridge](../../../../integration/bindings/client-js/runtime-bridge.test.mjs), [Dart client](../../../../packages/frontend/dart/test/client_test.dart), [Dart Bridge](../../../../packages/frontend/dart/test/runtime_bridge_test.dart) | Native storage with controlled backend responses; assembled backend behavior belongs to the host gate. |
| HTTP/socket effects, cancellation, token refresh and prerequisites | [JavaScript connection](../../../../integration/bindings/client-js/connection.test.mjs), [prerequisites](../../../../integration/bindings/client-js/prerequisite.test.mjs), [Dart connection](../../../../packages/frontend/dart/test/connection_test.dart), [Dart prerequisites](../../../../packages/frontend/dart/test/prerequisite_test.dart) | Controlled transport and timers exercise SDK effects, not PostgreSQL publication. |
| Fresh Query/Fetch snapshots, storage modes and terminal close | [JavaScript queries](../../../../integration/bindings/client-js/fresh-queries.test.mjs), [Fetch](../../../../integration/bindings/client-js/fetch.test.mjs), [Dart Query](../../../../packages/frontend/dart/test/query_test.dart), [Dart Fetch](../../../../packages/frontend/dart/test/fetch_test.dart) | Host boundary checks; current reads have no durable once/refresh job API. |
| Bound Stream Bootstrap handles and committed SQL observation | [Bootstrap](../../../../integration/bindings/client-js/bootstrap.test.mjs), [Stream handles](../../../../integration/bindings/client-js/subscriptions.test.mjs), [SQL watch](../../../../integration/bindings/client-js/watch-sql.test.mjs), [Dart handles](../../../../packages/frontend/dart/test/subscription_handles_test.dart), [Dart SQL watch](../../../../packages/frontend/dart/test/watch_sql_test.dart) | Bootstrap handle sharing/lifecycle and committed callbacks; no per-screen Stream registration. |
| Refusals, failed tasks and durable completion lookup | [JavaScript unsent work](../../../../integration/bindings/client-js/unsent.test.mjs), [Dart unsent work](../../../../packages/frontend/dart/test/unsent_test.dart), [Dart Calls](../../../../packages/frontend/dart/test/call_handles_test.dart) | SDK representation and lifecycle; frozen retry and cloud settlement use the assembled host. |
| Generated positive/negative types and native execution | [generated API runner](../../../../integration/generated-api/verify.sh), [Action contract](../../../../integration/action-contract) | Compile-time restrictions and host execution; unsupported old files are refused, not upgraded. |
| PostgreSQL, HTTP/WebSocket, SQLite and generated TypeScript/Dart together | [protocol-5 host](../../../../integration/v05-sdk/README.md), [application paths](../end-to-end.md) | Real backend and host-native libraries; installed artifacts and devices have separate gates. |

React Native counterparts live in [client-react-native](../../../../integration/bindings/client-react-native). They preserve the same transaction, Bootstrap, read, prerequisite, unsent-work and SQL-watch boundaries through the mobile adapter on the host. Passing them does not establish Expo or Flutter device startup. See [platform smoke tests](../../../../integration/platform/README.md).
