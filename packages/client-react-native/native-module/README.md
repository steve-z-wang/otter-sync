# `@axton/native`

Reusable iOS Expo module that carries AXTON's Rust-owned client runtime
([#134](https://github.com/zanminwang/axton/issues/134)) for the shared
TypeScript Bridge. See [SDK bindings](../../../docs/engineering/architecture/sdks/bindings.md)
for the carrier contract.

The Expo module is named `AxtonNative` and exposes:

```ts
interface AxtonNativeModule {
  runtimeOpen(request: string): string;
  runtimeSubmit(runtimeId: string, message: string): void;
  runtimeDrain(runtimeId: string): string;
  runtimeDetach(runtimeId: string): void;
  // event "axtonWake": { runtimeId: string }
  databasePath(name: string): Promise<string>;
}
```

The `runtime*` functions are synchronous and never wait for a task:
`runtimeOpen` starts a runtime on its own thread and answers its id (it throws
the engine message for a malformed request), `runtimeSubmit` admits one
envelope (it throws `client_closed` once the runtime is gone), `runtimeDrain`
answers the published events as JSON array text, and `runtimeDetach` stops the
runtime's wakes for good. The runtime's thread signals new events through the
`axtonWake` event, posted on the main queue; the package's `index.ts`
registers one listener and routes each wake to its runtime's Bridge. The
module detaches every runtime it still holds when it is destroyed, so no wake
can reach a freed module. SQLite, scheduling, retries and deadlines stay on
the runtime's thread; the module adds no queue of its own.

`databasePath` accepts one basename and returns a stable path in the app's
Application Support directory, creating that directory when needed.

The pod's prepare step builds the Apple Silicon simulator slice. Run
`bash scripts/build-ios.sh simulator` directly to rebuild it. The device slice
is configured with `bash scripts/build-ios.sh device`, but remains unverified
until it is built and exercised on a device.
