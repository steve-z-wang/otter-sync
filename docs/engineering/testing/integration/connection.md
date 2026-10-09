# Connection integration

Verify native HTTP/WebSocket/timer behavior through the [joined protocol-5 host gate](../../protocol5-adoption.md), driven by [run-host.sh](../../../../integration/v05-sdk/run-host.sh) and [README.md](../../../../integration/v05-sdk/README.md). This is separate from scripted runtime effects and pure carrier validation.

[delivery_plan.rs](../../../../crates/server/tests/delivery_plan.rs) inspects live fragment coverage without sockets; [runtime_prerequisites.rs](../../../../crates/sqlite/tests/runtime_prerequisites.rs) inspects cancellation, timer-driven backoff and late answers with a scripted Host. Neither establishes real transport behavior. [read-liveness.test.mjs](../../../../integration/persistence/server/read-liveness.test.mjs) establishes the application-read versus concurrent-write boundary under the database driver matrix, not all reconnect schedules.

Use [Running tests](../running.md) for prerequisites. Host-native evidence does not certify iOS/Android artifacts or device behavior.

Evidence below was inspected on 2026-10-09; these suites were not executed for this documentation change. Prior execution records remain in [history](../../history/pre-protocol5/testing/integration/connection.md). Source inspection supplies neither a new passing result nor a complete-coverage claim.
