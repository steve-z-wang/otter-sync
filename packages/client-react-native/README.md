# AXTON for React Native

React Native host integration for the Rust-owned client runtime. The generated TypeScript models, Mutations and Queries are shared with Node, and so is the TypeScript Bridge over the runtime ([client-js](../client-js/README.md)). The carrier, transaction scope, and HTTP/WebSocket transport use the mobile environment.

The package is currently used from an AXTON repository checkout. It is not a published standalone npm distribution. The initial native target is an arm64 iOS simulator with an Expo native build. Android, browser/WASM, physical-device execution and app-store packaging are not verified by this integration.

## Install and build

Use the [integration app](../../integration/platform/react-native/README.md) for a complete, locked example. From an Expo app in this repository, add file dependencies on this package and [the native module](native-module/README.md), then rebuild the native app. Expo Go does not contain the AXTON module.

Build the Rust library before installing CocoaPods for a local file dependency:

```sh
rustup target add aarch64-apple-ios-sim
bash packages/client-react-native/native-module/scripts/build-ios.sh simulator
```

The integration app's Metro configuration includes the repository source and `.mts` modules, and resolves React Native/Expo dependencies from the app. Its Babel configuration applies the TypeScript transform to `.mts` files, which the Expo preset otherwise treats as plain JavaScript. Reuse both configurations when consuming these local packages. Do not substitute Node polyfills or import `packages/client-js/index.mts` in a mobile bundle.

Generate the application's client with `--client-runtime` pointing to this package's `index.ts`, or to `@axton/client-react-native` when package resolution is configured. The compiler owns generated files.

```ts
import { databasePath } from '@axton/client-react-native';
import { GeneratedClient } from './generated/client';

const client = await GeneratedClient.open({
  path: await databasePath('app.sqlite'),
  server: { url: 'http://127.0.0.1:4242', token: 'demo-user' },
  connection: { onError: error => console.error(error) },
});
await client.scopes.subscribe('book:demo');
```

The URL above is for a simulator using a backend on its host. Configure a reachable address and the application's real authentication for other environments. Local HTTP permission belongs to the development app configuration.

`databasePath(name = 'axton.sqlite'): Promise<string>` resolves a basename in Application Support and creates the parent directory. It rejects paths and invalid basenames. Keep the same filename when reopening a client; the engine persists client identity and queued work there. Different installations have separate app containers.

## Runtime and carrier

Each client is one Rust runtime on its own thread, reached through the [native module](native-module/README.md)'s carrier: `runtimeSubmit` only admits a task, the runtime posts `axtonWake` on the main queue when it has published events, and the Bridge drains and dispatches them on the JavaScript thread. Rust owns scheduling, transactions, the connection lanes, retries, deadlines, credential refresh and subscription status; the package supplies only the platform adapters the runtime asks for as effects: HTTP `fetch`, the native `WebSocket`, timers, the application's `refreshAuth` and prerequisite handlers, and its transaction callbacks. See [SDK bindings](../../docs/engineering/architecture/sdks/bindings.md) for the contract.

## Client behavior

Generated reads, queries, watches, direct writes, Mutations, Queries, transactions, and channel subscriptions use the same ports as Node. A durable `client.mutations` call commits its optimistic changes and queue entry together before returning its `Call`; network delivery proceeds independently. Public transactions are for local model reads and direct writes only. See [client API](../../website/docs/frontend/client-api.md) for the shared generated API.

Always await each operation inside a transaction. A failed command poisons that transaction even if the callback catches its error. Unawaited operations prevent commit; queued work drains before rollback, and escaped transaction objects reject further calls. The mobile raw transaction does **not** expose nested `savepoint`; Node's existing savepoint API remains available on Node. A captured `client.mutations` or `client.queries` call during an active public transaction fails promptly with `transaction_active`. React Native applies this guard to unrelated concurrent mutation calls too; retry those after the public transaction settles. Because React Native cannot tell the callback's own calls from unrelated ones, it does not guard the outer client's other calls (reads, `transaction`, subscriptions): from inside a callback they wait behind the transaction that waits for them, so use `tx` there. Node refuses every outer-client call from its callback with `transaction_active`.

`Client` also exposes inspection/control methods used by the generated facade: `status`, `recordStatus`, `pendingTasks`, `setReadiness`, `runPrerequisites`, `drop`, `dismissRejection`, `readSql`, connection lifecycle, and `close`. These retain the current engine contracts, including the existing limitations of migration options. Do not create a new client on every React render. Dispose watches and call `close` when the owning session ends. A watch whose first query fails reports that error to its own `onError` and registers nothing; a later re-run that fails is reported to the connection's `onError`, and the watch stays.

The transport authenticates HTTP and native WebSocket requests with bearer tokens, hands raw frames to the runtime as socket effect results, aborts on the runtime's cancellation, and bounds pending frame delivery to 64. Rust validates acknowledgements and owns catch-up, page application, gap recovery and overflow recovery. Node and React Native share the same effect executor; ordinary live pages do not cause polling. Native WebSocket errors lack a structured HTTP status; automatic `refreshAuth` based on status 401 is available for HTTP errors, while applications must also manage credentials for native socket failures. The adapter does not infer status from error message wording. The transport derives the WebSocket address with the global `URL` class, which Expo polyfills; a bare React Native app without Expo needs its own `URL` polyfill.

Foreground execution on an arm64 iOS simulator is verified by the integration harness (see its recorded evidence); iOS background execution and delivery while the process is suspended are not promised. Offline relaunch testing must use an embedded JS bundle rather than depend on Metro.

## Verification

Host tests cover the mobile transaction adapter, the shared runtime using real SQLite through the Node carrier, and the native-style transport over real HTTP/WebSocket. They do not substitute for iOS runtime validation:

```sh
node --test integration/bindings/client-react-native/*.test.mjs
```

The separate [simulator harness](../../integration/platform/react-native/README.md) checks the actual Expo/Swift/Rust boundary and two-client offline/restart/reconnect behavior. See its recorded evidence for tested versions and results.
