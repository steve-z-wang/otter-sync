# Database

AXTON's backend runs on PostgreSQL. Business writes, explicit publications, Stream membership and durable Mutation receipts share the application transaction. Local client storage is SQLite regardless.

`@axtonjs/postgres` (`packages/postgres`) holds every statement AXTON runs, the metadata migration and one small driver interface. You pick the shim for the tool your application already uses to talk to PostgreSQL; handlers and loaders receive that tool's own transaction object.

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

`db` is your Prisma client; `pg(pool)` takes a `pg.Pool` and `drizzle(db)` the database returned by `drizzle-orm/node-postgres`. The import above uses the To-do example's directory depth. The package exports `pg` and `prisma` from its root and from `@axtonjs/postgres/pg` and `@axtonjs/postgres/prisma`; `drizzle` imports `drizzle-orm`, so it is exported only from `@axtonjs/postgres/drizzle` (`packages/postgres/src/drizzle.mts`), and the migration is `@axtonjs/postgres/migration.sql`. Every shim accepts the same options: `retries` (serialization-failure retries after the first attempt, default 3) and `timeout` (milliseconds, default 20,000, applied where the tool has a transaction timeout). Each runs its transactions at Serializable, with no option to choose another level, and retries PostgreSQL `40001` / `40P01` (Prisma `P2034`, or `P2010` carrying one of those codes, including a Prisma 7 driver adapter's write conflict); other failures propagate immediately. Serializable means a handler needs no row locks to stay correct: when two transactions would produce an outcome no serial order could, PostgreSQL aborts one with `40001` and the shim runs it again after a short random wait (below 20 ms, then 40, then 80). A retry runs your whole body again, so arrange irreversible side effects through your own outbox. When the retries run out, the call fails as a server error, never a rejection, and a queued call is sent again later.

## Apply the migration

Apply [migration.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migration.sql) to a fresh database using your deployment migration process before sync traffic. Apply the whole file transactionally, for example `psql -v ON_ERROR_STOP=1 -f migration.sql`. It installs Store, MutationResult, StreamRecord, immutable DeliveryPlan/DeliveryUnit metadata and the persisted publication fence. Business tables remain application-owned. Fresh DDL refuses installed older layouts rather than creating parallel empty truth. Existing databases follow the [forward migration chain](https://github.com/zanminwang/axton/blob/v0.3.0/website/docs/backend/database.md#stream-forward-migration).

## The driver interface

A shim is about thirty lines: it binds two methods to its tool's transaction type.

```ts
interface PostgresDriver<Tx> {
  /** BEGIN … COMMIT, ROLLBACK on throw, bounded retry on 40001/40P01; Serializable. */
  transaction<R>(body: (tx: Tx) => Promise<R>): Promise<R>;
  /** Run one statement inside tx; `$1…` placeholders; rows as plain objects. */
  query(tx: Tx, sql: string, params: readonly unknown[]): Promise<Record<string, unknown>[]>;
}
```

For a PostgreSQL tool without a shipped shim, write these two methods and pass `persistence(driver)` as the `database` option. `params` may contain strings, numbers, bigints and JSON values; a tool that cannot send a bigint sends it as text, as the `pg` and `drizzle` shims do. `withRetries` (with the shims' jittered wait, `retryDelay`) and `RETRYABLE_SQLSTATES` are exported for the retry loop.

| Export | Returns |
| --- | --- |
| `pg(pool, options?)`, `prisma(client, options?)`, `drizzle(db, options?)` | The `database` option: the tool's transaction runner plus AXTON's persistence bound to each transaction |
| `pgDriver`, `prismaDriver`, `drizzleDriver` | The bare `PostgresDriver` of each shim |
| `persistence(driver)` | The `database` option built on any driver |
| `PostgresDriver<Tx>`, `DriverOptions` | The interface and the options every shim accepts |

The retained 0.3 driver conformance suite ([driver-conformance.test.mjs](https://github.com/zanminwang/axton/blob/main/integration/persistence/server/driver-conformance.test.mjs)) exercises legacy persistence: it proves claim locking, receipt replay, stamp allocation, `ensureStamp` under concurrency, Stream range reservation, kept positions, removal scans, savepoints, serialization retry and rollback on a real database, once per shim.

## Protocol 5 persistence

The migration chain adds `axton_store`, `axton_mutation_result`, `axton_stream_record`, `axton_delivery_plan` and `axton_delivery_unit`. Store rows bind authenticated principal, Stream and immutable Batch progress. Each member result commits with its business transaction. StreamRecord holds explicit membership and current positions; returning a Model does not create one.

The persisted publication fence and canonical Stream locks keep Loader snapshots consistent with reserved positions. Each transaction reserves at most one cursor per affected Stream. Preparation settles before final reads. Delivery plans retain immutable header, part digests and payloads; continuation reauthorizes and expiry cleans staging without coverage.

Apply the forward migration chain before protocol-5 traffic. Existing server tables are not permission to open protocol-4 local files with protocol 5, and documentation completion does not authorize legacy-table deletion. Run the protocol-v05 Batch and delivery suites through the PostgreSQL runner when changing a shim. They cover real replay, isolation, finite units, expiry and capacity rollback.
