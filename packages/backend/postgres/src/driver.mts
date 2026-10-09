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

/**
 * The first retry waits below this many milliseconds; each later one doubles
 * the bound, so with the default three retries a call waits under 140 ms in
 * all (20 + 40 + 80) before its last failure is reported. Chosen by
 * measurement: on near-empty framework tables, where page-level predicate
 * locks make transactions on disjoint rows conflict, a shorter wait left
 * noticeably more calls exhausting their retries ([Persistence
 * §11](../../../../docs/engineering/architecture/server/persistence.md)).
 */
export const RETRY_BACKOFF_BASE_MS = 20;
/** No single retry waits 400 milliseconds or more, however many `retries` allow. */
export const RETRY_BACKOFF_CAP_MS = 400;

/**
 * How long to wait before retry number `retry` (0 for the first): full
 * jitter, a uniformly random delay below `min(cap, base * 2^retry)`.
 * Transactions that failed serialization together would otherwise restart
 * together and collide again; spreading them apart lets them commit in turn.
 */
export function retryDelay(
  retry: number,
  random: () => number = Math.random,
): number {
  const ceiling = Math.min(
    RETRY_BACKOFF_CAP_MS,
    RETRY_BACKOFF_BASE_MS * 2 ** retry,
  );
  return Math.floor(random() * ceiling);
}

/**
 * Run `attempt` up to `retries + 1` times while `isRetryable(error)` holds,
 * waiting `delay(n)` milliseconds before retry `n` (default `retryDelay`).
 */
export async function withRetries<R>(
  attempt: () => Promise<R>,
  isRetryable: (error: unknown) => boolean,
  retries: number,
  delay: (retry: number) => number = retryDelay,
): Promise<R> {
  for (let n = 0; ; n++) {
    try {
      return await attempt();
    } catch (error) {
      if (!isRetryable(error) || n >= retries) throw error;
    }
    const wait = delay(n);
    if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
  }
}

export interface DriverOptions {
  /** Serialization-failure retries after the first attempt. Default 3. */
  retries?: number;
  /** Transaction timeout in milliseconds where the tool supports one. Default 20000. */
  timeout?: number;
}
