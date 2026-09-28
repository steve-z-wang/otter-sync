/**
 * What AXTON needs from a PostgreSQL access tool: one transaction runner and
 * one statement runner inside that transaction. Every AXTON statement lives in
 * `sql.mts`; a tool shim (`pg`, `prisma`, `drizzle`) only has to bind these
 * two methods to its own transaction type, which handlers and loaders keep
 * receiving unchanged.
 */
export interface PostgresDriver<Tx> {
  /**
   * Run `body` in one transaction at SERIALIZABLE: commit when it resolves,
   * roll back when it throws, and retry the whole body a bounded number of
   * times on a serialization failure (SQLSTATE 40001 or 40P01). The body may
   * therefore run more than once; the last failure is thrown when the retries
   * run out.
   */
  transaction<R>(body: (tx: Tx) => Promise<R>): Promise<R>;
  /**
   * Run one statement inside `tx`. `sql` uses `$1…$n` placeholders; `params`
   * may contain strings, numbers, bigints and JSON-serialisable objects. Rows
   * come back as plain objects keyed by column name; a statement that returns
   * no rows resolves to an empty array.
   */
  query(
    tx: Tx,
    sql: string,
    params: readonly unknown[],
  ): Promise<Record<string, unknown>[]>;
}

/** The PostgreSQL serialization failures a transaction runner retries. */
export const RETRYABLE_SQLSTATES: ReadonlySet<string> = new Set([
  "40001",
  "40P01",
]);

/** Run `attempt` up to `retries + 1` times while `isRetryable(error)` holds. */
export async function withRetries<R>(
  attempt: () => Promise<R>,
  isRetryable: (error: unknown) => boolean,
  retries: number,
): Promise<R> {
  for (let n = 0; ; n++) {
    try {
      return await attempt();
    } catch (error) {
      if (!isRetryable(error) || n >= retries) throw error;
    }
  }
}

export interface DriverOptions {
  /** Serialization-failure retries after the first attempt. Default 3. */
  retries?: number;
  /** Transaction timeout in milliseconds where the tool supports one. Default 20000. */
  timeout?: number;
}
