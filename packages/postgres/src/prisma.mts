import type { DriverOptions, PostgresDriver } from "./driver.mts";
import { withRetries } from "./driver.mts";
import { persistence } from "./persistence.mts";
import { isRetryableTransactionError } from "@axtonjs/server";

/** The part of a Prisma interactive transaction AXTON uses; `Prisma.TransactionClient` satisfies it. */
export interface PrismaTransaction {
  $queryRawUnsafe<T = unknown>(sql: string, ...values: any[]): Promise<T>;
}
/** The part of a Prisma client AXTON uses; `PrismaClient` satisfies it. */
export interface PrismaClientLike<Tx extends PrismaTransaction> {
  $transaction<R>(
    body: (tx: Tx) => Promise<R>,
    options: { isolationLevel: "Serializable"; timeout: number },
  ): Promise<R>;
}

/** A driver over a Prisma client. Handlers and loaders receive the interactive transaction client. */
export function prismaDriver<Tx extends PrismaTransaction>(
  client: PrismaClientLike<Tx>,
  options: DriverOptions = {},
): PostgresDriver<Tx> {
  return {
    transaction: (body) =>
      withRetries(
        () =>
          client.$transaction(body, {
            isolationLevel: "Serializable",
            timeout: options.timeout ?? 20000,
          }),
        isRetryableTransactionError,
        options.retries ?? 3,
      ),
    // Prisma binds `$n` itself and maps bigint/jsonb columns to JS values.
    query: (tx, sql, params) =>
      tx.$queryRawUnsafe<Record<string, unknown>[]>(sql, ...params),
  };
}

/** `createBackend({ database: prisma(client) })`. */
export function prisma<Tx extends PrismaTransaction>(
  client: PrismaClientLike<Tx>,
  options: DriverOptions = {},
) {
  return persistence(prismaDriver(client, options));
}
