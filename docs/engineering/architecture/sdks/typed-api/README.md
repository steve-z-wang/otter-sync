# Typed API

The typed API has two halves that never meet at runtime but share one compiled schema:

- [Client](client.md) — The generic client runtime for TypeScript and Dart plus generated Model, Mutation, Query and transaction classes.
- [Server](server.md) — The TypeScript backend runtime: `createBackend`, handlers, loaders, publishing and the generated typed signatures.

The compiler emits both halves from the same `.model` files ([Compiler / Generate](../../compiler/generate.md)), so the client operation input/output contract and retained backend handler registration agree.

## Code map

| Part | Code location |
|---|---|
| Client | [client-js](../../../../../packages/frontend/client-js), [dart](../../../../../packages/frontend/dart/lib); model-specific classes are compiler output |
| Server | [server/index.mts](../../../../../packages/backend/server/index.mts); typed signatures are compiler output |
