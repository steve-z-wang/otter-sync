# Architecture

- [Bound protocol 4](protocol/0.4.md) — Current admission, delivery coverage and settlement contract.
- [Bound client](client/protocol4.md) / [bound server](server/protocol4.md) — Runtime implementation and persistence responsibilities.
- [Current guarantees](../guarantees.md) — Required behavior; the separate [0.3 reference](../guarantees-0.3.md) covers internal compatibility paths.
- [Overview](../architecture.md) — Component responsibilities, graph and code map.
- [Schema](schema/README.md) — User-written, language-independent definitions of models, fields, types, identities and mutations.
- [Protocol](protocol/README.md) — Language-independent push, pull, receipt and subscription message formats.
- [Compiler](compiler/README.md) — Compile schemas and generate typed interfaces.
- [SDKs](sdks/README.md) — Convert typed calls to runtime tasks and outcomes back; execute the effects the runtime asks for.
- [Client runtime](client/README.md) — Local state, storage and sync.
- [Server runtime](server/README.md) — Sync protocol and backend execution.
