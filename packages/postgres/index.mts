/**
 * AXTON's PostgreSQL persistence: the framework tables (`migration.sql`),
 * every statement AXTON runs, and one driver interface a PostgreSQL access
 * tool binds with two methods. `pg`, `prisma` and `drizzle` are the shipped
 * shims; `persistence(driver)` builds the `database` option from any other.
 */
export type { PostgresDriver, DriverOptions } from "./src/driver.mts";
export {
  RETRYABLE_SQLSTATES,
  RETRY_BACKOFF_BASE_MS,
  RETRY_BACKOFF_CAP_MS,
  retryDelay,
  withRetries,
} from "./src/driver.mts";
export { persistence, answer } from "./src/persistence.mts";
export { pg, pgDriver, type PgClient, type PgPool } from "./src/pg.mts";
export {
  prisma,
  prismaDriver,
  type PrismaTransaction,
  type PrismaClientLike,
} from "./src/prisma.mts";
export {
  drizzle,
  drizzleDriver,
  bindDrizzle,
  type DrizzleTransaction,
  type DrizzleDatabase,
} from "./src/drizzle.mts";
