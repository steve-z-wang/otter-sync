import type { Database } from "../../packages/backend/server/index.mts";
// Test lifecycle observation only: preserve transaction semantics and drain
// already admitted backend work before disposing its PostgreSQL connection.
export function drainedDatabase<Tx>(database: Database<Tx>) {
  const pending = new Set<Promise<unknown>>();
  const observed: Database<Tx> = {
    ...database,
    transaction<R>(body: (tx: Tx) => Promise<R>) {
      const result = database.transaction(body);
      pending.add(result);
      void result.then(
        () => pending.delete(result),
        () => pending.delete(result),
      );
      return result;
    },
  };
  return {
    database: observed,
    async drain() {
      while (pending.size) await Promise.allSettled([...pending]);
    },
  };
}
