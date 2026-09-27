# Schema compiler reference

The compiler reads sorted `.model` files and generates client and backend interfaces for the same contract. State transitions execute in Rust; generated code performs typed construction, conversion and forwarding. Start with [define a schema](define.md) for a walkthrough.

## Command

```sh
cargo run -p axton-compiler -- compile INPUT_DIR OUTPUT_DIR \
  --backend-runtime BACKEND_IMPORT \
  --client-runtime CLIENT_IMPORT
```

| Argument / option | Meaning |
| --- | --- |
| `INPUT_DIR` | Directory containing `.model` files; sorted and compiled together |
| `OUTPUT_DIR` | Destination for generated artifacts |
| `--backend-runtime SPEC` | TypeScript backend import; default `@axton/server` |
| `--client-runtime SPEC` | TypeScript client import; default `@axton/client` |
| `--mutation-history FILE` | Override the retained mutation history path; default `INPUT_DIR/history/mutations.json` |
| `--initialize-mutation-history` | Allow a missing explicitly selected mutation history file; only version 1 declarations |
| `--model-history FILE` | Override the retained model history path; default `INPUT_DIR/history/models.json` |
| `--initialize-model-history` | Allow a missing explicitly selected model history file; only version 1 declarations |
| `--action-history FILE` | Override retained Mutation and Query history; default `INPUT_DIR/history/actions.json` when the schema declares either |
| `--initialize-action-history` | Allow a missing explicitly selected Mutation and Query history; new operations must start at version 1 |
| `--schema-fence FILE` | Published schema to check; default existing output `schema.json` |

For source-checkout use, supply runtime paths relative to the output directory; see the [working command](define.md#generate-from-a-source-checkout). Default package names are not evidence of published packages. Unknown syntax or incompatible contracts fail with a diagnostic that names the file and line of the offending declaration. Validation runs before generated artifacts are replaced.

## Outputs

| File | Contents |
| --- | --- |
| `schema.json` | Client descriptor with Model read versions, Mutation and Query contracts and requirements |
| `backend.json` | Backend descriptor with each retained operation version's kind and input/output contracts, and Model read contracts; legacy mutation inputs remain where used |
| `generated.ts` | TypeScript records, identities, patches, Model facades and Mutation/Query bindings |
| `client.ts` | Schema-bound `GeneratedClient`, channels and runtime re-exports |
| `backend.ts` | Typed `Mutations`, `Queries`, `Loaders`, inputs, `CallRejected`, record references and a bound `createBackend` |
| `generated.dart` | Dart Models, patches, Mutation/Query bindings and generated client |

History is not generated output: `INPUT_DIR/history/mutations.json` retains legacy mutation inputs where used, `INPUT_DIR/history/models.json` retains Model reads, and `INPUT_DIR/history/actions.json` retains each Mutation and Query version's kind, inputs and outputs when the schema declares any. A refused compile leaves all histories and outputs untouched. Generated operation methods and the bound backend factory are executable.

Dart output imports `package:axton/axton.dart`. Commit the history used to generate released clients; regenerating from an empty history loses compatibility information. A history left at the superseded `OUTPUT_DIR/mutation-history.json` is read once, rewritten at the new default and reported on stderr; the old file stays where it is and you can delete it after committing the new one.

## Fields and identities

| Declaration | TypeScript | Dart | Meaning |
| --- | --- | --- | --- |
| `String` | `string` | `String` | Text |
| `UUID` | `string` | `String` | UUID represented as text |
| `Int` | `number` | `int` | Integer within the supported JSON safe range |
| `Float` | `number` | `double` | Finite number |
| `Bool` / `Boolean` | `boolean` | `bool` | Boolean |
| `DateTime` | `Date` | `DateTime` | Converted to/from UTC wire text |
| Enum name | Generated enum type | Generated enum type | One declared value |
| `T?` | Nullable type | Nullable type | Field can be null |
| `T[]` | Array | List | List of scalar/enum values |

`@deprecated` or `@deprecated(reason: "…")` after a field, an enum value or a mutation slot marks it deprecated, as in GraphQL: generated TypeScript carries `@deprecated` JSDoc and generated Dart carries `@Deprecated`, so editors and the Dart analyzer flag uses. Nothing else changes: the member stays in the schema and in every contract, and removing it still follows the versioning rules below.

`@@id(field,...)` defines identity, including composite keys. Identity fields must be nonnullable. `@@version(n)` names the Model read contract returned by a Loader; it defaults to 1 and is independent of Mutation and Query versions (see [History and compatibility](#history-and-compatibility)). Model names starting with `axton_` or `sqlite_` (in any letter case) are reserved. Names used by generated clients and runtimes, including `Call`, `CallError`, `Mutations`, `Queries`, `GeneratedClient` and `GeneratedTransaction`, are also reserved, as are the generated backend's type names `Channel`, `Touch`, `ModelMembership`, `RecordRef`, `HandlerCall`, `TransactionCall`, `MutationContext` and `QueryContext`. `@@unique(field,...)` declares a unique group that the client's local database enforces; your application database needs its own constraint ([What your backend owns](../backend/api.md#what-your-backend-owns)). Generated patches exclude identity fields. Complete records contain nullable fields; an optional patch field is a separate concept.

TypeScript omission leaves a patch field unchanged; null clears a nullable field. Dart uses `Present<T>` to distinguish supplied values from omission. Generated TypeScript is intended for `exactOptionalPropertyTypes`.

### Creation defaults

```text
enum Status { open closed }
model Todo {
  id String @default(uuid())
  title String @default("")
  done Boolean @default(false)
  priority Int @default(0)
  status Status @default(open)
  createdAt DateTime @default(now())
  note String? @default("inbox")
  @@id(id)
}
```

`@default(value)` on a stored Model field supplies the value when a fresh create omits that field. It applies to a local `create` (inside a transaction too) and to the `Model.create` operands of a Mutation on either route, including optional and list operands. It never applies to updates, deletes, reads, Loader records, server data or migrations: an update that omits a field leaves it unchanged, and a Loader record missing a required field is still an error.

| Default | Allowed on | Value |
| --- | --- | --- |
| `"text"` | `String`, `UUID`, `DateTime` | Normalized like any value of the field: a UUID is lowercased, a date-time becomes UTC with milliseconds |
| `true` / `false` | `Bool` / `Boolean` | The boolean |
| A number, such as `0`, `-1.5` or `1e3` | `Int` (safe integers), `Float` | The number |
| An enum value name, such as `open` | Enum fields | That enum value |
| `uuid()` | `String`, `UUID` | A new lowercase UUID v4 |
| `now()` | `DateTime` | The client's current UTC time, to the millisecond |

An explicit value always wins, and an explicit `null` for a nullable field stays `null`; it never requests the default. A nullable field may have a non-null default. `@default(null)`, defaults on list or relation fields, on operation inputs or outputs, unknown functions, function arguments and values the field type rejects are compile errors.

The client generates `uuid()` and `now()` values once, before the create is written locally or sent, so the local row, the queued call, every retry and the backend handler see the same values; reopening the app regenerates nothing. `now()` is the device clock and is not a trusted server timestamp; set server-side times in your handler. To know an identity before submitting, supply it yourself.

Generated create inputs make only defaulted fields optional: `TodoCreate` in TypeScript, and in Dart `TodoCreate` (a defaulted nullable field takes `Present(...)`, so omission and an explicit null differ) or any complete `Todo`, both accepted as `TodoCreateInput`. (Dart `TodoCreate` was previously an alias of `Todo`; code that assigns one to the other, or declares a handler's create argument as `TodoCreate`, now uses `Todo`.) Full `Todo` records and backend handler arguments stay complete: handlers receive the expanded values.

A default is creation policy, not stored data. Adding, changing or removing one needs no version bump and rewrites no existing rows or queued calls. It never fills historical records: adding a required field still needs a new `@@version` even with a default, and the local database is rebuilt for it.

## Relations

```text
model Book {
  id String
  comments Comment[]
  @@id(id)
}
model Comment {
  id String
  bookId String
  book Book @reference(via: [bookId], onTargetDelete: delete)
  @@id(id)
}
```

A reference names the local fields matching the target identity. `onTargetDelete` accepts `none` (default) or `delete`. The cascade runs on the client only: deleting a `Book` locally deletes its `Comment` rows locally, and those deletes are never sent. A handler that deletes a book must delete its comments itself and touch them, leaving them in their Channels so subscribers receive the deletions ([What your backend owns](../backend/api.md#what-your-backend-owns)). Inverse declarations generate navigation without storing another copy of the relationship. Singular inverses require a unique foreign key. Named references/inverses can disambiguate multiple relations; see the [parser tests](https://github.com/zanminwang/axton/blob/main/crates/compiler/tests/compiler.rs) for validated examples.

## Mutations and Queries

`mutation Name(inputs) { outputs }` and `query Name(inputs) { outputs }` each define one generated input/output pair. A Mutation may change business state or perform external effects; a Query reads without business side effects. Braces may be omitted when there are no explicit outputs. Ordinary inputs use scalar or enum types; `String?` is a required argument whose value can be null. Mutation Model operands use `Model.create`, `Model.update<fields>` or `Model.delete`, optionally followed by `?` or `[]`. An optional Model operand may be omitted or null; a list has zero or more elements. Omitted update fields stay omitted, while explicit null clears a nullable field. TypeScript flattens operands into `ModelCreate`, `ModelUpdate<K>` and `ModelDelete`; Dart uses `Present<T>` to represent supplied patch fields.

A Query takes ordinary inputs only: a Model operand or `@sequence` on a Query is refused with a diagnostic at its source line. The older keyword `action` is refused with a diagnostic naming `mutation` and `query`. Mutations and Queries share one namespace, after lower-camel normalization; `call` is reserved as a Mutation name and `enqueue` as a Query name.

Model operands imply no result: the backend's authority for each target is always applied to the caller's local Models, and every output is declared explicitly. Inputs and outputs are separate namespaces, so an output may share an input's name and select a different record; a missing output fails the call instead of falling back to the input. A deleted record cannot satisfy a nonnullable Model output; declare `Todo?`, or scalar outputs holding each identity field, to return one. The versioned backend handler receives `{ ctx, args }`, separating trusted framework context from caller-supplied values; a Query's context has no `touch` or `channel` ([backend handlers](../backend/api.md#handlers)). Explicit scalar/enum outputs are supplied by the handler; explicit Model outputs are selected with identity objects containing exactly the Model's `@@id` fields. For a composite identity, every key field is required. A nullable output field is present with null when absent; a list preserves order and duplicates; an operation with no outputs returns void. Nullable lists, nested lists, nullable list elements and duplicate output names are invalid.

The generated TypeScript and Dart client exposes each Mutation under `client.mutations` and each Query under `client.queries`, each with a durable and a direct route; the [client reference](../frontend/client-api.md#mutations-and-queries) lists the four methods, their return types and when each resolves. A durable call returns a `Call<Output>` handle with only `status` and `wait()`; a direct call returns the `Output`. Local submission errors reject before a handle exists; terminal business failures appear in `wait()` as `CallError`; direct-call failures reject with `CallError`. Neither namespace is available inside an application-owned local transaction. Standalone `client.models` CRUD is local-only; `tx.models` has local reads and CRUD without operation calls or watch.

Model results are resolved through the shared Loader path. A returned Model is the snapshot for that invocation, even if a later call in the same batch changes the row or local optimism changes the current `client.models` view. Batch-final records settle the local authoritative base and cannot reconstruct an earlier per-call result. The backend persists each outcome atomically with business writes and retains it without TTL or automatic pruning, for Queries as well as Mutations. Live client handles keep business results in memory, while pending and completion state remains durable. Whether explicit Model outputs also update local Models is chosen per call with the client's `store` option, not in the schema; choosing it or the delivery route never requires a new version. [#143](https://github.com/zanminwang/axton/issues/143) tool behavior is separate.

## History and compatibility

Mutations and Queries retain every version's kind and input and output contract, including the Model read version selected for each Model output. A new operation starts at version 1 and `@version(n)` selects a newer one. Changing an output's name, type, cardinality or identity source requires a newer version; an incompatible input change also requires a bump. Changing a published version's kind, from Mutation to Query or back, requires a newer version; changing the delivery route or `store` choice does not. Earlier versions stay in `history/actions.json` and the generated backend registration interfaces, each under its own kind: a name retained as Mutation v1 and Query v2 registers `mutations.name` for v1 and `queries.name: { v2 }`. Renaming an `action` declaration to `mutation` keeps its retained contract and needs no new version. A schema with neither kind does not need this history.

The generated `Mutations` and `Queries` interfaces group retained versions under one key per operation, such as `addTodo: { v1, v2 }`; input types keep names such as `AddTodoInput` for the latest version and `AddTodoV1Input` for older ones.

Same-version changes must be backward compatible. Adding nullable create fields or additional permitted patch/enum values can be compatible. Required create fields, field removal, changed types, reordered slots, changed bindings and changed dependency policy require a new version. Versions cannot decrease, and a retained Mutation or Query cannot disappear.

Models are versioned separately, by the shape of the records a client reads. Within one model version you may add a nullable field; an older client ignores it and a newer client reads it as `null` from older records. Adding a required field, renaming, removing or retyping a field, or adding a value to an enum the model returns, requires `@@version(n+1)` on the model. The compiler keeps the older version's fields and enum values in `history/models.json` exactly as published, so a value added later never reaches the older contract, and `backend.json` lists every retained version. Changing a model's identity is refused at any version. The generated `Loaders` interface groups the retained versions under one key per model, such as `entry: { v1, v2 }`, with `Entry` for the current version and `EntryV1` for the older record shape ([loaders](../backend/api.md#loaders)).

An explicitly selected missing history requires `--initialize-mutation-history`, which refuses an existing history and declarations above version 1. The schema fence prevents removing published model/field names. Broader identity/type/nullability compatibility fences remain incomplete; a passing compile is not a guarantee that any local database migration is supported.

See [local migration](../frontend/runtime.md#opening-and-schema-changes) and separately migrate your backend tables.

## Verify generated APIs

Build the root native libraries, then run:

```sh
bash integration/generated-api/verify.sh
```

The checks cover Rust parser/history/CLI behavior, TypeScript positive and expected-error fixtures, Node native integration, Dart analysis and Dart native integration.
