# @axtonjs/client

> **Alpha.** AXTON is alpha software: its API is unstable and it is not ready for production use.

AXTON's local-first client for Node.js. See [client setup](https://github.com/zanminwang/axton/blob/main/website/docs/frontend/setup.md) for the public API.

Each client is one Rust-owned runtime on its own thread
([#134](https://github.com/zanminwang/axton/issues/134)). The package reaches it
through a synchronous carrier - the Node addon here, the Expo module in
[React Native](https://github.com/zanminwang/axton/blob/main/packages/client-react-native/README.md): `runtimeSubmit` only admits a
task, a wake says that events were published, and `bridge.mts` drains and
dispatches them on the JavaScript thread, settling each Promise from its
`taskCompleted`. Rust owns scheduling, transactions, the connection lanes,
retries, deadlines, credential refresh, Call outcomes and subscription status.
The package keeps the language objects (Promises, Call handles, subscription
handles, transaction contexts) and the platform adapters the runtime asks for
as effects: HTTP, the WebSocket, timers, `refreshAuth`, prerequisite handlers
and transaction callbacks. See
[SDK bindings](https://github.com/zanminwang/axton/blob/main/docs/engineering/architecture/sdks/bindings.md) for the
contract.
