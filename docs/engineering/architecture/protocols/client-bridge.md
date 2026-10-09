# Client bridge protocol

## 1. Introduction and Goals

Define the JSON messages between a Frontend SDK and its Rust ClientRuntime: task/transaction commands, callback/effect results, effects, observer changes, diagnostics and terminal outcomes. An ABI admission acknowledgement means the message entered the mailbox; the SDK await completes from the matching terminal event.

## 3. Context and Scope

`requestId` routes a submitted command, `effectId` routes an effect, `transactionId` and `scope` admit transaction work, `companionId` admits Mutation-local callbacks, `callId` identifies a durable Mutation and `observerId` routes watch events. Their allocation, cancellation and completion are runtime responsibilities. Extraction preserves these identities and all null/omitted-field distinctions.

## 5. Building Block View

[Client bridge](../../../../crates/protocols/src/client_bridge/mod.rs) owns messages and decoding, with local operation/report/readiness DTOs and QuerySpec ordering DTOs. The [frontend interface](../../../../crates/client/src/frontend_interface.rs) exposes Client operations; [ClientRuntime](../../../../crates/client/src/runtime) admits and executes commands. [Frontend Bindings](../frontend-sdk/bindings.md) carry messages and run requested platform effects.

## 10. Quality Requirements

Malformed routable commands must fail their request without leaving an SDK waiter hanging; malformed envelopes report diagnostics. Existing protocol tests move with the definitions. Runtime/SDK tests retain transaction lifetime, nested scope, cancellation, Call completion and observer coverage.
