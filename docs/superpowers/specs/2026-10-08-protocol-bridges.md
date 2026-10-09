# Protocol and SDK boundaries

Extract the Client bridge and Server bridge contracts and make the source layout follow the agreed component tree. The baseline is released 0.5.4 (`4252ae3ef041321f4b79b00e944e522e15be4d1e`). Package versioning follows the existing release process; this task does not reserve a version number.

```text
AXTON
├── Schema
├── Protocols
│   ├── Sync
│   ├── Client bridge
│   └── Server bridge
├── Compiler
├── Frontend SDK
│   ├── API
│   └── Bindings
├── Backend SDK
│   ├── API
│   └── Bindings
├── Client runtime
│   ├── Frontend interface
│   ├── Runtime
│   ├── Engine
│   ├── Storage
│   └── Connection
└── Server runtime
    ├── Backend interface
    ├── Engine
    ├── Persistence
    └── Connection
```

## Code ownership

- `crates/protocols`: `sync`, `client_bridge`, `server_bridge`. Message types, encoding, decoding and pure contract validation; depends on shared schema/core definitions, never on either runtime, SQLite, native bindings or a transport.
- `crates/client`: consumes protocol types. Its frontend interface and runtime retain task admission, transactions, effects, observers and scheduling.
- `crates/server`: consumes protocol types. Its backend interface retains the `Host` implementation contract and typed invocation adapter; the Engine retains orchestration and publication.
- `packages/frontend/{client-js,dart,client-react-native}`: existing public packages, with API and bindings implementation separated inside them.
- `packages/backend/{server,postgres}`: existing backend and persistence packages. Server API construction and its native/host bindings are separate modules.
- Native Node/Dart/mobile ABI carriers remain shared adapters in `bindings/`. SDK boundaries do not require duplicating a shared native library.

The existing Sync contract moves alongside the two bridge contracts so the Protocols component has one source owner. Small DTOs referenced by the bridge contracts move with them. Runtime-facing re-exports preserve existing client and server Rust paths where possible; there is no reverse runtime dependency.

## Source layout

```text
crates/
├── core/src/schema.rs                  # Shared schema descriptors and normalization
├── protocols/src/
│   ├── sync.rs                        # Client ↔ Server contracts and pure helpers
│   ├── sync/{delivery,mutation}.rs    # Existing pure planning helpers
│   ├── client_bridge/
│   │   ├── mod.rs                     # Input, Command, Event, effect and callback messages
│   │   ├── model.rs                   # Local operation, readiness and report DTOs
│   │   └── query.rs                   # QuerySpec and ordering DTOs
│   └── server_bridge/
│       ├── mod.rs                     # HostRequest, responses and pure validation
│       ├── members.rs                 # Tracking and position DTOs
│       └── error.rs                   # Structured cross-language error carrier
├── compiler/src/                      # Parse, Validate, Generate
├── client/src/
│   ├── frontend_interface.rs          # Client entry points and language-task dispatch
│   ├── runtime/                       # Tasks, transactions, effects and observers
│   ├── engine.rs                      # Local operation execution
│   ├── store.rs                       # Storage abstraction
│   └── sync05/                        # Durable Uplink and Downlink execution
└── server/src/
    ├── backend_interface.rs           # Host trait and typed invocation adapter
    ├── mutation_batch.rs              # Mutation processing
    ├── delivery_plan.rs               # Authority delivery
    └── live.rs                        # Live connection decisions

packages/
├── frontend/
│   ├── client-js/{api,bindings}/
│   ├── dart/lib/src/{api,bindings}/
│   └── client-react-native/{api,bindings}/
├── backend/
│   ├── server/{api,bindings}/
│   └── postgres/                     # Persistence adapters
├── native/                           # Shared native artifact package
└── cli/                              # Compiler distribution

bindings/
├── common/                           # Shared actor and C ABI
├── node/                             # Node native carrier
├── dart/                             # Dart native carrier
└── mobile/                           # Mobile native carrier
```

This is component ownership, not a requirement to introduce a class or crate for every documentation node. Shared core primitives and native distribution remain shared. Rust client/server package identities can remain stable while files move. SDK package-root entry points retain their existing exports and forward to the API modules; internal source imports follow the new owners.

## Protocol / Interface / Bindings boundary

| Layer | Owns | Does not execute |
| --- | --- | --- |
| Protocols | Messages, DTOs, field semantics, encoding/decoding, pure validation | I/O, application callbacks, transaction or synchronization scheduling |
| Rust frontend interface | Submit validated tasks to ClientRuntime and expose their outcomes | Native platform I/O |
| Rust backend interface | Invoke the Host under the retained execution context and classify its replies | Application business logic |
| Frontend SDK bindings | Native carrier calls, Promise/Future and callback routing, HTTP/WebSocket/timer effects | A second synchronization or settlement policy |
| Backend SDK bindings | Native Engine calls and dispatch to handlers, Loaders and persistence | Independent transaction or authority policy |

Client bridge retains requestId, effectId, transactionId, nested scope, companionId, callId and observerId as distinct existing identities. Runtime continues to admit/cancel them and determine completion; extraction creates no new queue or message broker. Server bridge retains HostRequest and its exact response variants. The host callback uses the same database transaction/session supplied by its caller.

TypeScript and Dart keep language-side representations and codecs where their package can build and ship them. Existing mirrored contracts and shared fixtures remain checked against Rust; automatic protocol-code generation is outside this change.

## Preserved behavior

Keep all message spellings, validation, error codes, field nullability, correlation IDs, C ABI functions, SDK exports and published package names. Rust continues to own scheduling, retry, Store transactions, authority and settlement. Backend handlers and Loaders retain their transaction and authenticated context. One Client owns one physical file and Stream.

Carry forward the 0.5.3 Bootstrap collection fix in the Server SDK and the 0.5.4 PostgreSQL batching fix, including their regression tests. This is an ownership and layout change, not a rewrite of those implementations.

There is no new protocol discriminator, database migration, Store rebuild, Capso change, package publication or deployment in this task. Previously frozen design/specification documents remain untouched.

Repository-internal Rust imports such as `axton_core::v05` move to `axton_protocols::sync`. The Rust crates are unpublished workspace components. Existing runtime entry paths can re-export protocol types without retaining duplicate definitions or creating a dependency cycle. Public npm/pub.dev imports, serialized data and native symbols remain unchanged.

## Acceptance

Run the existing JSON fixture and malformed-message checks from the extracted contracts, independently compile the protocol crate, then run workspace tests and strict linting. Rebuild native artifacts and verify TypeScript, Dart, native client bindings and the real backend host callback. Update live component trees, code maps, links, generator paths, CI and package/release tooling; verify installation of the moved packages outside the checkout.

The acceptance report must separately state local host results, CI results and any unexecuted platform checks. A passing host gate does not establish an iOS/Android device result. Release Please prepares the patch release after integration; the package version does not change Sync's protocol discriminator 5.
