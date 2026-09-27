# Define a schema and generate interfaces

The schema connects local Models and backend operations, Mutations and Queries, to typed client and handler interfaces. Your backend's tables and business logic can have a different shape.

## Define records and a Mutation

Create `models/entry.model`:

```text
model Entry {
  id String
  text String
  note String?
  @@id(id)
}

mutation EditEntry(entry Entry.update<text,note>) { updated Entry? }
```

`@@id(id)` defines identity. `String?` is nullable. `EditEntry` declares an update operand named `entry`; callers can change `text` and `note` but not identity through that patch. The `entry` input is not part of the result: once the call completes, the backend's version of that record is already applied locally. The only result is the explicit nullable `updated` output, which the handler selects by identity.

Fields can declare [creation defaults](reference.md#creation-defaults), filled in only when a new record omits them:

```text
model Note {
  id String @default(uuid())
  text String @default("")
  createdAt DateTime @default(now())
  @@id(id)
}
```

## Generate from a source checkout

For the To-do example, run from the repository root:

```sh
cargo run -p axton-compiler -- compile \
  examples/todo/models examples/todo/generated/node \
  --backend-runtime ../../../../packages/server/index.mts \
  --client-runtime ../../../../packages/client-js/index.mts
```

The runtime import paths are relative to the generated output directory. Adjust them when generating into another directory; [examples/todo/generate.sh](https://github.com/zanminwang/axton/blob/main/examples/todo/generate.sh) also emits the React Native client from the same schema. Follow [getting started](../getting-started.md) to build the required native artifacts. Packages are not currently published; the default package specifiers are not a registry installation guide.

The compiler writes TypeScript and Dart clients, typed backend interfaces, descriptors and retained history. See the [compiler reference](reference.md) for every output and option. Do not edit generated files by hand.

## Declare Mutations and Queries

Declare each backend operation by its business intent. A **Mutation** may change business state or perform external effects, such as sending email. A **Query** reads without business side effects. Both use `Name(inputs) { outputs }` with the same input and output types; how a call is delivered is chosen by the client method, not by the schema ([routes](../frontend/client-api.md#mutations-and-queries)).

This standalone schema is a small declaration example, separate from the [integration schema](https://github.com/zanminwang/axton/blob/main/integration/action-contract/schema.model) used in the [frontend examples](../frontend/client-api.md#mutations-and-queries).

```text
model Todo {
  id String
  title String
  note String?
  @@id(id)
}
mutation AddTodo(todo Todo.create, removed Todo.delete[], note String?) {
  relatedTodo Todo?
  labels String[]
}
mutation DeleteTodo(todo Todo.delete)
mutation SendEmail(to String, body String) { messageId String }
query SearchTodos(text String, cursor String?) {
  todos Todo[]
  nextCursor String?
}
query FindTodo(id String) { found Todo? }
```

`AddTodo`'s result holds only its declared outputs: its handler supplies `relatedTodo` as a `TodoIdentity` object or `null`, plus `labels`. The created `todo` and the deleted `removed` records are inputs, not results; the caller has the backend's version of each locally once the call completes. An output may even share an input's name and name another record. `DeleteTodo` returns void; to return a deleted ID, declare it as scalar outputs such as `{ id String }` and return the value. `SendEmail` changes no Model, but it performs an external effect, so it is a Mutation; its handler returns the plain `messageId`. An operation with no outputs returns void. The required `note` argument accepts a string or `null`; omitting it is invalid.

`SearchTodos` selects a list of identities resolved through the same Loader as Model results; `nextCursor` is an ordinary value your handler computes, and the framework does not infer pagination from it. `FindTodo` selects `found` by identity. A Query accepts ordinary scalar, enum and list inputs and returns ordinary or Model outputs. A Model `create`, `update` or `delete` operand, or `@sequence`, on a Query is a compile error at its source line.

Mutations and Queries share one namespace, so a name is declared once across both. A Mutation cannot be named `call` and a Query cannot be named `enqueue`, because those names hold the other delivery route in the generated client.

## Connect the generated layers

| Generated interface | Your use |
| --- | --- |
| `client.models.entry` | Local get, query and watch |
| `client.mutations.editEntry` | Apply inferred local optimism and queue `EditEntry` |
| `client.mutations.call.editEntry` | Execute `EditEntry` directly and return its final output |
| `client.queries.searchTodos` | Execute a Query directly and return its output (`queries.enqueue` queues it instead) |
| `Mutations<Tx>.editEntry`, `Queries<Tx>.searchTodos` | Implement authoritative business logic for each operation |
| `Loaders<Tx>.entry` | Return current records from your backend |
| Backend `Entry(identity)` | Name a record in a mixed Channel list, `channel(name).add([Entry({ id })])` |

An update operand is one flat object: the identity fields plus the changed fields it allows (`EntryUpdate<'text' | 'note'>` in TypeScript), both in the client call and in the handler's `args`. Dart exposes a typed update operand whose changed fields use `Present`.

See [generated client usage](../frontend/client-api.md) and [backend usage](../backend/api.md) for complete examples.

## Group several changes into one Mutation

Declare several named operands in the same `mutation`. For example:

```text
model Project {
  id String
  title String
  @@id(id)
}
model Task {
  id String
  projectId String
  title String
  @@id(id)
}
mutation CreateProject(project Project.create, tasks Task.create[])
```

`CreateProject` becomes one typed client call and one backend handler. Its inferred Model operations apply together locally on the durable route, and its backend business writes share one savepoint. The list operand supplies zero or more complete task records. If you need a declared relationship as well, add a reference; a field named `projectId` alone does not create one automatically.

Several `client.mutations` calls commit locally one at a time and have separate backend outcomes. Choose one Mutation with several operands when the business operation must be accepted or rejected as one unit.

## Relations, prerequisites and ordering

Declare a forward reference with `@reference(via: [field])` and an inverse with the related model type. Relations generate local navigation methods and let the runtime enforce the declared contract. The [relations fixture](https://github.com/zanminwang/axton/blob/main/fixtures/compiler/relations.model) shows `Book.comments` and `Comment.book`.

Prerequisites declare host work that must finish before sending a durable call. For example, a schema can declare `prerequisite Uploaded(key String)` and use `@requires(Uploaded(key: self))` on a field. Your application provides the async callback through [runPrerequisites](../frontend/runtime.md#prerequisites). Prerequisites are not automatically implemented uploads.

Operand bindings and `@sequence(after: [...])` before a Mutation express operation dependencies. Use the [compiler's tested declarations](https://github.com/zanminwang/axton/blob/main/crates/compiler/tests/compiler.rs) as syntax examples for these advanced features; they affect scheduling, not just generated types.

## Evolve the contract

Keep `history/actions.json` and `history/models.json` beside your `.model` files and commit them. Regenerating retains prior contracts: each queued call keeps its operation's input and output contract, and each published Model version keeps the record shape its readers expect.

A compatible change can keep the same version; incompatible input or output changes to a Mutation or Query require `@version(n)` with a newer version. The kind is part of the retained contract: turning a published Mutation into a Query, or back, also requires a newer version. A breaking change to the records a Model returns (a required field, a rename, a removal, a type change, a new enum value) requires a newer `@@version(n)` on the Model. Implement every retained handler version exposed by the generated backend interface. Do not delete history to silence a compatibility error.

There are three separate responsibilities: compiler compatibility checks, the client's local database, and migration of your backend database. The client handles its database on its own at open, applying a compatible change in place and rebuilding an incompatible one beside the old file; the compile and your backend migration are yours. Read [compiler compatibility](reference.md#history-and-compatibility) and [opening and schema changes](../frontend/runtime.md#opening-and-schema-changes) before shipping a schema change.
