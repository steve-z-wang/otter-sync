# Database

AXTON's backend runs on PostgreSQL. Your business tables, AXTON's six metadata tables and every sync operation share one database transaction, so a push commits business writes, stamps, Stream memberships, publications and the receipt together. Local client storage is SQLite regardless.

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

Apply [migration.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migration.sql) to a fresh database using your deployment migration process before sync traffic. Apply the whole file transactionally, for example `psql -v ON_ERROR_STOP=1 -f migration.sql`. It installs six metadata tables: `axton_client`, `axton_call`, `axton_stream`, `axton_record`, `axton_stream_member` and `axton_stream_log`. Business tables remain application-owned. Fresh DDL refuses installed older layouts rather than creating parallel empty truth. Existing databases follow the [forward migration chain](#stream-forward-migration).

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

Run the driver conformance suite ([driver-conformance.test.mjs](https://github.com/zanminwang/axton/blob/main/integration/persistence/server/driver-conformance.test.mjs)) against a new driver: it proves claim locking, receipt replay, stamp allocation, `ensureStamp` under concurrency, Stream range reservation, kept positions, removal scans, savepoints, serialization retry and rollback on a real database, once per shim.

## What the persistence does

`claim` locks a client row so a repeated batch replays its receipt. `claimCall`/`saveCall` retain immutable Mutation, Query, Fetch and Load outcomes in the application transaction; saved IDs replay without rerunning handlers, Loaders or declarations. Existing single-record stamp operations remain for read paths. Delivery settlement uses bulk `readTracking` and `guardRecords`, then `lockStreams` and `applyStreamMembers`. `scan` reads compacted upsert/removal positions: upserts resolve current stamps and viewer Loaders; retained removals use identity and invoke no Loader.

## Stream persistence constraints

Tracking is unique per Stream/record and survives Loader absence. Streams own ordered heads; catalog IDs stay internal bigints, while stamps/cursors are positive safe integers. There are no tag dictionaries or joins in the fresh layout and no public withdrawal operation. Historical removal logs and client holdings remain supported.

Custom hosts answer `readTracking({records, pairs})` with the deduplicated union of all holders for named records and existing explicit candidates. `guardRecords` accepts canonically ordered mixed `advance`/`ensure`/`lock` records and returns request-aligned stamps (null only for absent lock metadata). `lockStreams` precedes record guards in canonical UTF-8 order; `applyStreamMembers` writes final pairs, reserves grouped head ranges and validates matching positions. See [host interfaces](https://github.com/zanminwang/axton/blob/main/docs/engineering/architecture/server/backend-interface.md#9-architecture-decisions).

The adapter chunks set-based SQL at 1,000 items. Host round trips and statement counts scale with chunks, rather than one call/statement per record. Row, lock, WAL and log work still scale with affected records and pairs. Mixed guard acquisition order spans chunks, retains no-op write conflict fencing and cannot be established merely by sorting returned rows. A changed global recipient set requires whole-transaction retry; locks are never extended out of order. Caller-owned transactions retain their commit/retry responsibilities.

Saved outcomes, tracking and logs have no TTL or automatic pruning. Tracking is durable interest, not permission or a retention guarantee. Loader errors retain local content; `null` is authority absence, never automatic server tracking removal.

## Stream forward migration

Stop old writers before the complete migration transaction. Scope installations apply [2026-10-01-streams.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migrations/2026-10-01-streams.sql); Channel installations first apply [2026-09-30-scopes.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migrations/2026-09-30-scopes.sql). Earlier v0.1 installations first apply [2026-09-30-channel-members.sql](https://github.com/zanminwang/axton/blob/main/packages/postgres/migrations/2026-09-30-channel-members.sql). Apply current DDL only after the installed layout has been upgraded. Older-layout migrations are not rerun after cutover.

The forward upgrade preserves tracking pairs, heads, catalog IDs, stamps, retained removals, receipts and calls. It rewrites only top-level framework `memberships[*].scope` claim keys, never opaque names or business JSON. The Stream migration retires tag-only tables. All changes roll back on inconsistency and reapplication is idempotent. Coordinate matching backend, adapter, tooling and clients through [stream-authority-v1 cutover](deployment.md#stream-membership-cutover); source version `0.2.0` is not a registry release decision.

After prior layout upgrades, keep writers and live sessions stopped while applying [local-authority repair](https://github.com/zanminwang/axton/blob/main/packages/postgres/migrations/2026-10-01-local-authority.sql). It restores historical removed pairs and publishes newer current authority to all tracking viewers without rewriting saved business bytes. Reapplication allocates nothing. Deploy coordinated authority-capable admission before resuming traffic; never reset/rewind cursors or turn Remove into null ([cutover](deployment.md#stream-membership-cutover)). Server tracking remains durable; clients have no holding table.
