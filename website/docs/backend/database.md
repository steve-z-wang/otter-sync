# Database

AXTON's backend runs on PostgreSQL. Your business tables, AXTON's eight metadata tables and every sync operation share one database transaction, so a push commits business writes, stamps, Channel memberships, publications and the receipt together. Local client storage is SQLite regardless.

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

Apply [migration.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migration.sql) to a new database with your deployment's migration process before accepting sync traffic. Apply the whole file at once (for example `psql -v ON_ERROR_STOP=1 -f migration.sql`): its trigger functions are dollar-quoted, so it cannot be split on semicolons. It creates the tables prefixed `axton_` (`axton_client`, `axton_call`, `axton_channel`, `axton_record`, `axton_channel_member`, `axton_channel_tag`, `axton_channel_member_tag`, `axton_channel_log`) and nothing else: your business tables and the database itself are yours to create. A database installed by AXTON 0.1.x upgrades with [migrations/2026-09-30-channel-members.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migrations/2026-09-30-channel-members.sql) instead: stop every older AXTON writer, then apply it the same way. It keeps existing memberships and delivery positions, leaves the old `axton_membership` and `axton_invalidation` tables in place, fails whole on inconsistent data and changes nothing when run again. The [getting-started runner](../getting-started.md) handles a disposable database for the example.

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

Run the driver conformance suite ([driver-conformance.test.mjs](https://github.com/zanminwang/axton/blob/main/integration/persistence/server/driver-conformance.test.mjs)) against a new driver: it proves claim locking, receipt replay, stamp allocation, `ensureStamp` under concurrency, Channel range reservation, kept positions, removal scans, savepoints, serialization retry and rollback on a real database, once per shim.

## What the persistence does

The driver runs AXTON's statements; the operations they answer are the persistence half of the [backend interface](https://github.com/zanminwang/axton/blob/main/docs/engineering/architecture/server/backend-interface.md). `claim` locks the client row with `SELECT … FOR UPDATE`, so retries of one client serialize and a retried `(clientId, sequence)` replays its stored receipt. `claimCall` and `saveCall` store each Mutation's, Query's and Model Fetch's immutable outcome by call ID (for a Fetch, the full Model snapshot, even with `store: false`) in the application's transaction with business writes. A duplicate call ID replays that outcome without re-running the handler or Loader. `advanceStamp` and `ensureStamp` allocate a record's stamp with atomic upserts (a stored Fetch calls `ensureStamp` before its Loader, so it leaves a stamp row even for an identity the Loader answers `null`); `lockRecord` guards a record and `memberships` answers the Channels it belongs to; `lockChannels` locks a settlement's Channels in canonical order, `readChannelMembers` answers the members its declarations name or its tag selectors match, and `applyChannelMembers` writes the final members and tags and reserves one cursor range per Channel for their positions; `scan` reads retained `upsert` and `remove` log rows without joining live membership. Upserts resolve the current stamp and Loader; removals use centralized record identity and call no Loader. Per-call savepoints use real `SAVEPOINT` statements. Counters are `bigint` in the database and narrowed to JavaScript safe integers on the way out.

There is no TTL or automatic pruning for saved call responses or client rows. Channel log rows are never pruned either: one row per record ever represented in a Channel, removals included ([#61](https://github.com/zanminwang/axton/issues/61)).


## Channel persistence constraints

Membership is unique per Channel/record. Tags are unique per Channel/name, with indexed member associations; a constraint trigger rejects cross-Channel associations and member/tag Channel ownership is immutable. Surrogate bigint IDs remain internal and are never narrowed to JavaScript numbers. Content stamps and delivery cursors remain positive safe integers.

Custom hosts implement `lockChannels`, `readChannelMembers` and `applyChannelMembers`, including final tags, consecutive cursor reservations, kept upsert positions, compacted removals and the same Channel/tag constraints. They must preserve the transaction boundary and lock order; the adapter never commits the application's transaction. Channel locks precede record guards in canonical order. A changed touch recipient set retries the whole transaction; multiple Mutation settlements within one push can still deadlock and must use the bounded whole-transaction retry.

The adapter batches up to 1,000 deltas per statement inside the same transaction. Removing N tagged members costs O(N) row/index/WAL work and O(N) delivered identities, without per-record Loader reads. Bulk adds still perform one engine `ensureStamp` guard per record. Enrolling Loads hold Channel locks across their Loader reads: slow Loaders therefore delay competing membership writers.

The forward upgrade preserves live memberships, converts retained absent positions to removals, and allocates positions above the head for previously unlogged members. Apply it whole under stopped older writers. Even a repeated upgrade that changes no data takes an `ACCESS EXCLUSIVE` lock on `axton_record`; schedule it as migration work. Keep old tables and removal logs: no retention floor or snapshot replacement is supplied. Coordinate server and client cutover using the [0.2 release boundary](deployment.md#channel-membership-cutover).
