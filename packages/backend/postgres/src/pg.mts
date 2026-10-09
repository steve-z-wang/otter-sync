import type { DriverOptions, PostgresDriver } from "./driver.mts";
import { withRetries } from "./driver.mts";
import { persistence } from "./persistence.mts";
import { isRetryableTransactionError } from "@axtonjs/server";

/** The part of a `pg` client AXTON uses; `pg.PoolClient` satisfies it. */
export interface PgClient {
  query(
    sql: string,
    params?: readonly unknown[],
  ): Promise<{ rows: Record<string, unknown>[] }>;
  release(error?: Error | boolean): void;
}
/** The part of a `pg` pool AXTON uses; `pg.Pool` satisfies it. */
export interface PgPool {
  connect(): Promise<PgClient>;
}

/** `pg` bigint parameters are sent as text; everything else as is. */
const bind = (params: readonly unknown[]): unknown[] =>
  params.map((p) => (typeof p === "bigint" ? p.toString() : p));

/** A driver over a node-postgres pool. Handlers and loaders receive the `PoolClient`. */
export function pgDriver(
  pool: PgPool,
  options: DriverOptions = {},
): PostgresDriver<PgClient> {
  return {
    transaction: (body) =>
      withRetries(
        async () => {
          const client = await pool.connect();
          // A connection whose ROLLBACK failed is in an unknown state: hand
          // the error to `release` so the pool discards it instead of reusing it.
          let broken: Error | undefined;
          try {
            await client.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
            try {
              const result = await body(client);
              await client.query("COMMIT");
              return result;
            } catch (error) {
              await client.query("ROLLBACK").catch((rollback: unknown) => {
                broken =
                  rollback instanceof Error
                    ? rollback
                    : new Error(String(rollback));
              });
              throw error;
            }
          } finally {
            client.release(broken);
          }
        },
        isRetryableTransactionError,
        options.retries ?? 3,
      ),
    query: async (client, sql, params) =>
      (await client.query(sql, bind(params))).rows,
  };
}

/** `createBackend({ database: pg(pool) })`. */
export function pg(pool: PgPool, options: DriverOptions = {}) {
  return persistence(pgDriver(pool, options));
}
