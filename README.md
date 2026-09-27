<h1 align="center">
  <img src="assets/branding/axton-logo.png" alt="AXTON" width="560">
</h1>

<p align="center">
  A schema-driven framework for building local-first apps with your own backend.
</p>

<p align="center">
  <a href="https://github.com/zanminwang/axton/actions/workflows/verify.yml"><img src="https://github.com/zanminwang/axton/actions/workflows/verify.yml/badge.svg?branch=main" alt="Verify"></a>
  <a href="https://github.com/zanminwang/axton/actions/workflows/docs.yml"><img src="https://github.com/zanminwang/axton/actions/workflows/docs.yml/badge.svg?branch=main" alt="Documentation"></a>
</p>

<p align="center">
  <a href="#build-with-axton">Quick start</a> ·
  <a href="website/docs/schema/reference.md">Schema</a> ·
  <a href="website/docs/frontend/setup.md">Client</a> ·
  <a href="website/docs/backend/setup.md">Backend</a>
</p>

## Why AXTON?

- **Schema-driven.** Define your models and local mutations in a schema. AXTON handles the local state changes.
- **Type-safe end to end.** Get typed client calls and backend read/write interfaces from the same schema.
- **Works offline.** Read and write local SQLite without a connection. AXTON persists changes and syncs in the background.
- **Your backend.** Implement your own read and write logic and choose your database. No vendor cloud service required.

## Why local-first?

Local-first apps read and write data on the device, so everyday interactions don't wait for a network round trip. Users can keep working through a slow or missing connection, with changes saved locally and synchronized when the network is available.

## How it works

![AXTON architecture: local state and background sync](website/docs/assets/architecture.svg)

Clients push mutations over HTTP. On connection, they catch up from saved progress over HTTP, then receive ongoing record updates over WebSocket. AXTON manages this as one connection.

On your server, **handlers** process writes and **loaders** read records to send to clients. After a handler runs, AXTON reads the records the mutation targeted back through your loaders and returns them to the client in the receipt. A **channel** is a set of records other clients subscribe to: add a record to it once, and every later change to that record reaches its subscribers.

Writes update local SQLite immediately, so reads see changes before sync completes. Changes to local data update query subscriptions (`watch`). If the backend rejects a mutation, its local changes roll back.

## Current support

| Layer | Supported today |
| --- | --- |
| Frontend / client | [TypeScript](website/docs/frontend/setup.md) · [Flutter](website/docs/frontend/setup.md) |
| Backend | [TypeScript](website/docs/backend/setup.md) |
| Database | [PostgreSQL](website/docs/backend/database.md) through `pg`, Prisma or Drizzle |

The TypeScript client and backend currently run on Node.js. The clients use native runtimes; browser support is not yet implemented. See [platform validation](website/docs/frontend/platforms.md) for tested environments.

Need another language, runtime, or database adapter? [Request support](https://github.com/zanminwang/axton/issues/new). More integrations can be added.

## Build with AXTON

### 1. Define your models and mutations

Write your data models and local write operations (mutations) in a `.model` file:

```
model Todo {
  id    String
  title String
  done  Boolean
  @@id(id)
}

mutation AddTodo(todo Todo.create)
query SearchTodos(text String) { todos Todo[] }
```

A `mutation` may change business state; a `query` only reads. The compiler generates the client used below and the backend's `Mutations`, `Queries` and `Loaders` interfaces. See the [schema compiler guide](website/docs/schema/reference.md) for generation commands.

### 2. Read and write locally

<details markdown="1">
<summary>Open the client</summary>

```ts
// Open the local database and connect.
import { GeneratedClient } from "./generated/client.ts";

const client = await GeneratedClient.open({
  path: "local.sqlite",
  server: {
    url: "http://127.0.0.1:4242",
    token: "demo-user",
  },
});

```

</details>

```ts
// Read.
const open = await client.models.todo.query({ where: { done: false } });

// Watch this query and render again when local data changes.
client.models.todo.watch({ where: { done: false } }, (todos) => render(todos));

// Run a Mutation. It commits locally and returns once it is queued;
// wait() yields the backend result or rejection.
const call = await client.mutations.addTodo({
  todo: { id: "t1", title: "Buy milk", done: false },
});
const { error } = await call.wait();

// Run a Query. It asks the backend directly and returns its result.
const { todos } = await client.queries.searchTodos({ text: "milk" });

// Receive record changes published to the "todos" channel.
await client.channels.subscribe("todos");
```

AXTON sends queued Mutations when the network allows, retries failed sync requests, and fetches changed records from your subscribed channels. `client.mutations.call` waits for the backend instead, and `client.queries.enqueue` queues a Query.

### 3. Implement handlers and loaders for your backend

This example uses Prisma with PostgreSQL through the [included `prisma` shim](website/docs/backend/database.md).

```ts
// Handle a write using your database transaction.
const mutations: Mutations<Tx> = {
  async addTodo({ ctx, args }) {
    await ctx.tx.todo.create({ data: args.todo });

    // The new todo is read back for the caller regardless. Adding it to the
    // "todos" channel once sends it, and every later change, to subscribers.
    ctx.channel("todos").todo.add(args.todo);
  },
};

// Answer a read. A Query's context has no touch or channel.
const queries: Queries<Tx> = {
  async searchTodos({ ctx, args }) {
    const rows = await ctx.tx.todo.findMany({
      where: { title: { contains: args.text } },
      select: { id: true },
    });
    return { todos: rows };
  },
};

// Read the requested records from your database for sync.
const loaders: Loaders<Tx> = {
  todo: ({ ids, tx }) =>
    Promise.all(ids.map((identity) => tx.todo.findUnique({ where: identity }))),
};
```

<details markdown="1">
<summary>Imports and server setup</summary>

Add these imports and the transaction type before the handlers and loaders above:

```ts
import { PrismaClient, type Prisma } from "@prisma/client";
import { prisma } from "./packages/postgres/index.mts";
import {
  createBackend,
  devAuth,
  type Mutations,
  type Queries,
  type Loaders,
} from "./generated/backend.ts";

type Tx = Prisma.TransactionClient;
```

Start the server after defining the handlers and loaders:

```ts
// Start the server.
const backend = createBackend({
  database: prisma(new PrismaClient()),
  authenticate: devAuth(),
  mutations,
  queries,
  loaders,
});
await backend.listen({ port: 4242 });
```

</details>

## How AXTON compares with other sync frameworks

AXTON generates local operations from your schema and gives you typed interfaces to implement backend reads and writes.

| Project | How local updates work | How backend writes work | How you define the read / sync path | Required backend database |
| --- | --- | --- | --- | --- |
| **AXTON** | Generated local operations from your schema | You implement the business logic through a generated, typed write interface | You mark changes and define the read path through a generated typed read interface. | No fixed database |
| [Replicache](https://doc.replicache.dev/byob/local-mutations) | You write local update functions | Your write API runs the requested operations | You implement a [read API](https://doc.replicache.dev/reference/server-pull) that returns data changes | No fixed database |
| [Zero](https://zero.rocicorp.dev/docs/mutators) | You write local update functions | Your server functions handle each write | You define [queries](https://zero.rocicorp.dev/docs/queries); Zero syncs matching database rows | PostgreSQL with database replication enabled |
| [PowerSync](https://docs.powersync.com/intro/powersync-philosophy) | You update local SQLite | Your write API processes the changes | You define sync rules to select which database records reach each client | A supported database with change tracking enabled |
| [Electric](https://electric.ax/docs/sync/guides/writes) | You choose how to update local state | You choose how writes reach your backend | You define [shapes](https://electric.ax/docs/sync/guides/shapes) to select Postgres rows for sync | PostgreSQL with database replication enabled |
| [InstantDB](https://www.instantdb.com/docs) | You update records through the SDK | Instant applies writes using your permission rules | You query through the SDK; Instant keeps the results up to date | Instant's database backend |

AXTON needs a database adapter that provides consistent transactions and stores sync metadata. Replicache also requires [consistent transaction snapshots](https://doc.replicache.dev/byob/remote-database).

Instant Cloud is closed to new signups and will shut down on August 31, 2027. You can still host Instant yourself. See the [official announcement](https://www.instantdb.com/essays/instant_team_joins_openai).

## Project status

AXTON is an early alpha. Packages have not been published, and a license has not yet been added. Existing integrations must follow the [AXTON rename notes](docs/engineering/brand-rename.md) before upgrading from a pre-rename build.

[Schema guide](website/docs/schema/reference.md) · [Client guide](website/docs/frontend/setup.md) · [Backend guide](website/docs/backend/setup.md)
