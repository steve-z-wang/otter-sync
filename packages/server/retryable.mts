/** Transaction faults which must reach the adapter's whole-transaction retry loop. */
export function isRetryableTransactionError(error: unknown): boolean {
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (current && typeof current === "object" && !seen.has(current)) {
    seen.add(current);
    const value = current as {
      code?: string;
      kind?: string;
      meta?: { code?: string; driverAdapterError?: unknown };
      cause?: unknown;
    };
    if (
      value.code === "40001" ||
      value.code === "40P01" ||
      value.code === "P2034"
    )
      return true;
    if (
      value.code === "P2010" &&
      (value.meta?.code === "40001" || value.meta?.code === "40P01")
    )
      return true;
    // Prisma 7 driver adapters: a raw query's P2010 carries the adapter's
    // error in `meta.driverAdapterError`, whose structured `cause` names a
    // write conflict or (`kind: "postgres"`) the SQLSTATE `code` checked above.
    if (value.kind === "TransactionWriteConflict") return true;
    if (value.meta?.driverAdapterError !== undefined) {
      if (isRetryableTransactionError(value.meta.driverAdapterError))
        return true;
    }
    current = value.cause;
  }
  return false;
}
