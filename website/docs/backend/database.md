# Database

AXTON's backend runs on PostgreSQL. Your business tables, AXTON's six metadata tables and every sync operation share one database transaction, so a push commits business writes, stamps, Channel memberships, publications and the receipt together. Local client storage is SQLite regardless.

`@axton/postgres` (`packages/postgres`) holds every statement AXTON runs, the metadata migration and one small driver interface. You pick the shim for the tool your application already uses to talk to PostgreSQL; handlers and loaders receive that tool's own transaction object.

## Pick a shim

| Your tool | Shim | `tx` in handlers and loaders |
| --- | --- | --- |
| [node-postgres](https://node-postgres.com/) | `pg(pool)` | the `PoolClient` |
| [Prisma](https://www.prisma.io/) | `prisma(client)` | `Prisma.TransactionClient` |
| [Drizzle](https://orm.drizzle.team/) over node-postgres | `drizzle(db)` | the Drizzle transaction |

```ts
import { prisma } from '../../packages/postgres/index.mts';

const database = prisma(db, { retries: 3, timeout: 20_000 });
// Pass database to generated createBackend({ database, ... }).
```

`db` is your Prisma client; `pg(pool)` takes a `pg.Pool` and `drizzle(db)` the database returned by `drizzle-orm/node-postgres`. The import above uses the To-do example's directory depth. Every shim accepts the same options: `retries` (serialization-failure retries after the first attempt, default 3) and `timeout` (milliseconds, default 20,000, applied where the tool has a transaction timeout). Each runs its transactions at Repeatable Read and retries PostgreSQL `40001` / `40P01` (Prisma `P2034`, or `P2010` carrying one of those codes); other failures propagate immediately. A retry runs your whole body again, so arrange irreversible side effects through your own outbox.

## Apply the migration

Apply [migration.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migration.sql) to your database with your deployment's migration process before accepting sync traffic. It creates the tables prefixed `axton_` (`axton_client`, `axton_call`, `axton_channel`, `axton_record`, `axton_invalidation`, `axton_membership`) and nothing else: your business tables and the database itself are yours to create. Applied to a database from before Channel membership existed, it adds an empty `axton_membership` table; AXTON never infers membership from earlier deliveries, so add your records to their Channels again ([Channels](api.md#channels)). The [getting-started runner](../getting-started.md) handles a disposable database for the example.

## The driver interface

A shim is about thirty lines: it binds two methods to its tool's transaction type.

```ts
interface PostgresDriver<Tx> {
  /** BEGIN … COMMIT, ROLLBACK on throw, bounded retry on 40001/40P01; Repeatable Read. */
  transaction<R>(body: (tx: Tx) => Promise<R>): Promise<R>;
  /** Run one statement inside tx; `$1…` placeholders; rows as plain objects. */
  query(tx: Tx, sql: string, params: readonly unknown[]): Promise<Record<string, unknown>[]>;
}
```

For a PostgreSQL tool without a shipped shim, write these two methods and pass `persistence(driver)` as the `database` option. `params` may contain strings, numbers, bigints and JSON values; a tool that cannot send a bigint sends it as text, as the `pg` and `drizzle` shims do. `withRetries` and `RETRYABLE_SQLSTATES` are exported for the retry loop.

| Export | Returns |
| --- | --- |
| `pg(pool, options?)`, `prisma(client, options?)`, `drizzle(db, options?)` | The `database` option: the tool's transaction runner plus AXTON's persistence bound to each transaction |
| `pgDriver`, `prismaDriver`, `drizzleDriver` | The bare `PostgresDriver` of each shim |
| `persistence(driver)` | The `database` option built on any driver |
| `PostgresDriver<Tx>`, `DriverOptions` | The interface and the options every shim accepts |

Run the driver conformance suite ([driver-conformance.test.mjs](https://github.com/zanminwang/axton/blob/main/integration/persistence/server/driver-conformance.test.mjs)) against a new driver: it proves claim locking, receipt replay, stamp allocation, `ensureStamp` under concurrency, publication stamp validation, the scan join, savepoints, serialization retry and rollback on a real database, once per shim.

## What the persistence does

The driver runs AXTON's statements; the operations they answer are the persistence half of the [backend interface](https://github.com/zanminwang/axton/blob/main/docs/engineering/architecture/server/backend-interface.md). `claim` locks the client row with `SELECT … FOR UPDATE`, so retries of one client serialize and a retried `(clientId, sequence)` replays its stored receipt. `claimCall` and `saveCall` store each Mutation's and Query's immutable outcome by call ID in the application's transaction with business writes. A duplicate call ID replays that outcome without re-running the handler or Loader. `advanceStamp` and `ensureStamp` allocate a record's stamp with atomic upserts; `lockRecord`, `memberships` and `setMembership` guard a record and maintain its Channel memberships; `publish` allocates channel cursors and `scan` joins each invalidation of a current member with the record's current stamp. Per-call savepoints use real `SAVEPOINT` statements. Counters are `bigint` in the database and narrowed to JavaScript safe integers on the way out.

There is no TTL or automatic pruning for saved call responses or client rows. Invalidation rows also grow with records × channels ([#61](https://github.com/zanminwang/axton/issues/61)).
