# Generate

## 1. Introduction and Goals

Generate renders runtime descriptors and typed TypeScript/Dart APIs from the same validated definitions. Client, backend and application code therefore agree on Model and operation shapes.

## 3. Context and Scope

Input is `Validated` from [Validate](validate.md). [Descriptor generation](../../../../crates/compiler/src/generate.rs) produces the client schema and backend operation/Model contracts. Emitters consume that descriptor after the CLI reconciles retained history.

| File | Content | Consumer |
| --- | --- | --- |
| `schema.json` | Local Model schema, read versions, requirements, prerequisites and retained compatibility policies | [Client frontend](../client/frontend-interface.md) |
| `backend.json` | Backend schema, retained operation and Model contracts, Loader names, relations and constraints | [Backend interface](../server/backend-interface.md) |
| `generated.ts` | Model types, codecs, local Model APIs, typed Mutation input/callback scopes, Query and Fetch facades | [Typed client](../frontend-sdk/api.md) |
| `client.ts` | Bound `GeneratedClient` over the selected runtime | [Typed client](../frontend-sdk/api.md) |
| `backend.ts` | Versioned Mutation/Query/Loader handlers, typed tracking/publication contexts and `createBackend` | [Typed server](../backend-sdk/api.md) |
| `generated.dart` | Dart Model types, codecs and bound client/transaction facades | [Typed client](../frontend-sdk/api.md) |
| `history/models.json` | Retained Model read contracts beside the input schema | [Validate](validate.md) |
| `history/actions.json` | Retained named Mutation/Query contracts, when operation history exists | [Validate](validate.md) |
| `history/mutations.json` | Retained compatibility slot-Mutation contracts | [Validate](validate.md) |

Load declarations and Load history CLI options are rejected in 0.4. Generated clients expose local `models`, named `mutations`, direct `queries`, Model `fetch` and `bootstrap()`. They have no Load jobs, queued Query facade, direct Mutation facade, incoming-store hooks or multi-Stream subscription API.

## 5. Building Block View

[generate.rs](../../../../crates/compiler/src/generate.rs) owns descriptors; [emit.rs](../../../../crates/compiler/src/emit.rs) renders Model/operation types and facades. [current_operations.rs](../../../../crates/compiler/src/current_operations.rs) selects each name's latest retained version once per emitted surface. [emit_transactions.rs](../../../../crates/compiler/src/emit_transactions.rs) uses that selection for typed Mutation callback and transaction scopes. [emit_stream.rs](../../../../crates/compiler/src/emit_stream.rs) renders backend tracking/publication types; retained internal type names such as `LoadStream` do not expose Load jobs.

The [CLI](../../../../crates/compiler/src/main.rs) concatenates input `.model` files, reconciles histories, checks the schema fence and stages each output before renaming it into place. Current-kind selection does not discard history: backend handlers retain each version under its own kind, and retained wire contracts remain in descriptors.

## 6. Runtime View

Generated local Model APIs forward to runtime ports. Named Mutations accept typed input or a callback returning that input; the callback exposes local companion writes. TypeScript supports both invocation forms directly, while Dart uses the typed invoker's `withTransaction`. The same Mutation forms are available inside an outer transaction. Invocation returns a `Call`; `wait()` observes backend outcome and local settlement.

Queries and Model Fetch use boolean `store`, default true, and await their invocation result and permitted cache commit. Query results use the SDK's shared options; Fetch is generated from each concrete Model's identity and read version. These facades neither enqueue reads nor implicitly track returned Models. The [typed client](../frontend-sdk/api.md) owns those behaviors and runtime completion rules.

Schema inheritance is expanded by validation before emission. Each concrete Model has its own identity, version and inherited fields. `@@bootstrap` is emitted as Model selection metadata; it does not fix membership in a Stream or prevent later delivery of unmarked Models. Tracking and Loader preparation remain backend responsibilities.

## 9. Architecture Decisions

History belongs beside the application's schema and is committed separately from generated output. Default locations are `history/models.json`, `history/actions.json` when operations are tracked, and `history/mutations.json` for retained compatibility contracts. `--model-history`, `--action-history` and `--mutation-history` override these locations. Their corresponding `--initialize-*-history` options explicitly initialize missing history with version 1. A schema without operations does not create new operation history; existing history is retained. Load history options are unsupported.

Model outputs retain their declared Model read version and cardinality. Backend handlers return selected identities; the server resolves those identities through the retained viewer Loader. Generated result types represent the invocation snapshot rather than promising equality with current Store content. Returning a Model is separate from explicit tracking/publication.

Current client methods follow the latest version's kind. An older Mutation followed by a Query remains a retained backend Mutation contract but is absent from the current transaction facade. TypeScript, Dart and transaction emission share this selection so they cannot independently classify the same name. Member deprecation notices are language hints and change no runtime descriptor or history.

## 10. Quality Requirements

- Descriptor generation and CLI output are deterministic; compatible histories retain old contracts. Evidence: [compiler](../../../../crates/compiler/tests/compiler.rs), [history](../../../../crates/compiler/tests/history.rs) and [CLI tests](../../../../crates/compiler/tests/cli.rs).
- Current kind and retained versions remain distinct across TypeScript, Dart and backend emission. Evidence: operation kind-change tests in [compiler](../../../../crates/compiler/tests/compiler.rs) and current transaction tests in [transactions](../../../../crates/compiler/tests/transactions.rs).
- Generated TypeScript/Dart accept valid use and reject wrong identity, patch, filter, enum, input and scope types. Evidence: [generated API checks](../../../../integration/generated-api), [operation contract checks](../../../../integration/action-contract) and [0.4 SDK checks](../../../../integration/v04-sdk).
- Model inheritance, Bootstrap metadata, defaults and the current bound facades reach generated output. Evidence: [inheritance](../../../../crates/compiler/tests/inheritance.rs), [defaults](../../../../crates/compiler/tests/defaults.rs) and [0.4 facade tests](../../../../crates/compiler/tests/v04_facade.rs).

## 11. Risks and Technical Debt

Generated code still renders retained compatibility contracts alongside current API types. Changes must preserve historical codecs and backend version dispatch while keeping retired public entry points absent. Passing emitter string assertions alone does not establish installed-package or runtime behavior; those are checked by the integration runners above.
