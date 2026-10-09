# Frontend Bindings

## 1. Introduction and Goals

Connect a language client to one Rust actor. Bindings submit Client bridge messages, drain events, route request/effect/callback identities, load native libraries and execute platform effects. They do not decide Stream authority, retries or settlement.

One actor owns each Client and physical SQLite file. The shared ABI submits tasks, drains effect/outcome messages and detaches the actor; platform Bridges execute native networking, timers and callbacks. Rust validates protocol-5 delivery and queue state and emits observer/terminal events only after their deciding commit. Pending network or callback effects do not hold another Client’s writer. Close/detach cancel effects and fence late replies.

## 5. Building Block View

Language implementations: [Node bindings](../../../../packages/frontend/client-js/bindings), [Dart bindings](../../../../packages/frontend/dart/lib/src/bindings) and [React Native bindings](../../../../packages/frontend/client-react-native/bindings). The shared [actor](../../../../bindings/common/src/actor.rs) and [C ABI](../../../../bindings/common/src/ffi.rs) remain under `bindings/`, with Node/Dart/mobile carriers. [Client bridge](../protocols/client-bridge.md) owns message shape; Rust ClientRuntime owns its execution.

## 10. Quality Requirements

Public Promise/Future completion comes from the runtime outcome, not mailbox admission. Callback zones, native finalizers, cancellation and physical-file ownership retain their existing behavior. SDK tests and joined native gates verify them; installed-package checks verify nested modules and bundled library loading outside this checkout.
