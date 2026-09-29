import { sql as drizzleSql, type SQL, type SQLChunk } from "drizzle-orm";
import type { DriverOptions, PostgresDriver } from "./driver.mts";
import { withRetries } from "./driver.mts";
import { persistence } from "./persistence.mts";
import { isRetryableTransactionError } from "../../server/retryable.mts";

/** The part of a Drizzle node-postgres transaction AXTON uses. */
export interface DrizzleTransaction {
  execute(query: SQL): Promise<{ rows: Record<string, unknown>[] }>;
}
/** The part of a Drizzle database AXTON uses; `drizzle(pool)` from `drizzle-orm/node-postgres` satisfies it. */
export interface DrizzleDatabase<Tx extends DrizzleTransaction> {
  transaction<R>(
    body: (tx: Tx) => Promise<R>,
    options: { isolationLevel: "serializable" },
  ): Promise<R>;
}

/**
 * Turn `$n` placeholders into a Drizzle `sql` template so the tool binds the
 * parameters itself; bigints go as text like the `pg` shim sends them.
 */
export function bindDrizzle(text: string, params: readonly unknown[]): SQL {
  const chunks: SQLChunk[] = [];
  const pattern = /\$(\d+)/g;
  let last = 0;
  for (const match of text.matchAll(pattern)) {
    chunks.push(drizzleSql.raw(text.slice(last, match.index)));
    const value = params[Number(match[1]) - 1];
    chunks.push(
      drizzleSql.param(typeof value === "bigint" ? value.toString() : value),
    );
    last = match.index + match[0].length;
  }
  chunks.push(drizzleSql.raw(text.slice(last)));
  return drizzleSql.join(chunks, drizzleSql.raw(""));
}

/** A driver over a Drizzle node-postgres database. Handlers and loaders receive the Drizzle transaction. */
export function drizzleDriver<Tx extends DrizzleTransaction>(
  db: DrizzleDatabase<Tx>,
  options: DriverOptions = {},
): PostgresDriver<Tx> {
  return {
    transaction: (body) =>
      withRetries(
        () => db.transaction(body, { isolationLevel: "serializable" }),
        isRetryableTransactionError,
        options.retries ?? 3,
      ),
    query: async (tx, text, params) =>
      (await tx.execute(bindDrizzle(text, params))).rows,
  };
}

/** `createBackend({ database: drizzle(db) })`. */
export function drizzle<Tx extends DrizzleTransaction>(
  db: DrizzleDatabase<Tx>,
  options: DriverOptions = {},
) {
  return persistence(drizzleDriver(db, options));
}
