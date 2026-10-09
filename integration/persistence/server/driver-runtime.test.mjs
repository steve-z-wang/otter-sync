import test from "node:test";
import assert from "node:assert/strict";
import {
  pgDriver,
  withRetries,
  retryDelay,
  RETRY_BACKOFF_BASE_MS,
  RETRY_BACKOFF_CAP_MS,
} from "../../../packages/backend/postgres/index.mts";
test("pg: a connection whose ROLLBACK fails is released as broken, a healthy one is released for reuse", async () => {
  const releases = [];
  const fakePool = (failRollback) => ({
    connect: async () => ({
      query: async (sql) => {
        if (sql === "ROLLBACK" && failRollback)
          throw new Error("connection lost");
        return { rows: [] };
      },
      release: (arg) => releases.push(arg),
    }),
  });
  await assert.rejects(
    () =>
      pgDriver(fakePool(true)).transaction(async () => {
        throw new Error("body failed");
      }),
    /body failed/,
  );
  assert.ok(
    releases.at(-1) instanceof Error,
    "the broken connection is discarded",
  );
  await assert.rejects(
    () =>
      pgDriver(fakePool(false)).transaction(async () => {
        throw new Error("body failed");
      }),
    /body failed/,
  );
  assert.equal(
    releases.at(-1),
    undefined,
    "a rolled-back connection goes back to the pool",
  );
  assert.equal(await pgDriver(fakePool(false)).transaction(async () => 7), 7);
  assert.equal(releases.at(-1), undefined);
});

test("a retry waits a jittered, doubling, capped delay first; the last failure and a non-retryable one are thrown at once", async () => {
  assert.deepEqual([RETRY_BACKOFF_BASE_MS, RETRY_BACKOFF_CAP_MS], [20, 400]);
  assert.deepEqual(
    [0, 1, 2, 3, 4, 5, 9].map((n) => retryDelay(n, () => 0.999999)),
    [19, 39, 79, 159, 319, 399, 399],
    "just below min(cap, base * 2^n)",
  );
  assert.deepEqual(
    [0, 1, 2].map((n) => retryDelay(n, () => 0)),
    [0, 0, 0],
    "full jitter reaches zero",
  );
  for (let n = 0; n < 200; n++) {
    const d = retryDelay(n % 6);
    assert.ok(
      Number.isInteger(d) && d >= 0 && d < Math.min(400, 20 * 2 ** (n % 6)),
    );
  }
  const events = [];
  const conflict = Object.assign(new Error("conflict"), { code: "40001" });
  const failing = (failures) => async () => {
    events.push("attempt");
    if (failures-- > 0) throw conflict;
    return "done";
  };
  const delay = (n) => {
    events.push(`wait ${n}`);
    return 1;
  };
  const retryable = (error) => error?.code === "40001";
  assert.equal(await withRetries(failing(2), retryable, 3, delay), "done");
  assert.deepEqual(events, [
    "attempt",
    "wait 0",
    "attempt",
    "wait 1",
    "attempt",
  ]);
  events.length = 0;
  await assert.rejects(
    () => withRetries(failing(9), retryable, 2, delay),
    (error) => error === conflict,
  );
  assert.deepEqual(
    events,
    ["attempt", "wait 0", "attempt", "wait 1", "attempt"],
    "no wait after the last attempt",
  );
  events.length = 0;
  await assert.rejects(
    () =>
      withRetries(
        async () => {
          events.push("attempt");
          throw new Error("other");
        },
        retryable,
        3,
        delay,
      ),
    /other/,
  );
  assert.deepEqual(
    events,
    ["attempt"],
    "a non-retryable failure is thrown without waiting",
  );
});

test("the pg driver retries only serialization failures, a bounded number of times", async () => {
  const attempts = [];
  const failing = (codes) => ({
    async connect() {
      return {
        async query(sql) {
          if (sql === "COMMIT") {
            const code = codes.shift();
            if (code) {
              attempts.push(code);
              throw Object.assign(new Error(code), { code });
            }
          }
          return { rows: [] };
        },
        release() {},
      };
    },
  });
  assert.equal(
    await pgDriver(failing(["40001", "40P01"])).transaction(async () => "body"),
    "body",
  );
  assert.deepEqual(attempts, ["40001", "40P01"]);
  await assert.rejects(
    () =>
      pgDriver(failing(["40001", "40001", "40001", "40001"])).transaction(
        async () => {},
      ),
    (error) => error.code === "40001",
  );
  await assert.rejects(
    () => pgDriver(failing(["23505"])).transaction(async () => {}),
    (error) => error.code === "23505",
  );
  await assert.rejects(
    () =>
      pgDriver(failing(["40001", "40001"]), { retries: 1 }).transaction(
        async () => {},
      ),
    (error) => error.code === "40001",
  );
});

import { prisma, prismaDriver } from "../../../packages/backend/postgres/index.mts";
import { isRetryableTransactionError } from "../../../packages/backend/server/bindings/retryable.mts";
test("the Prisma driver retries only serialization failures, a bounded number of times, and reports the last one", async () => {
  const attempts = [];
  const bodies = [];
  const failing = (codes) => ({
    async $transaction(body, options) {
      attempts.push(options);
      const code = codes.shift();
      await body({ attempt: attempts.length });
      bodies.push(attempts.length);
      if (code) throw Object.assign(new Error(`fail ${code.code}`), code);
      return "committed";
    },
  });
  const conflict = { code: "P2034" };
  const rawConflict = { code: "P2010", meta: { code: "40001" } };
  const deadlock = { code: "P2010", meta: { code: "40P01" } };
  const unique = { code: "P2002" };
  assert.equal(
    await prismaDriver(failing([conflict, rawConflict, deadlock])).transaction(
      async () => "body",
    ),
    "committed",
    "the fourth attempt succeeds within the default of three retries",
  );
  assert.equal(attempts.length, 4);
  assert.deepEqual(bodies, [1, 2, 3, 4], "the body runs once per attempt");
  assert.deepEqual(attempts[0], {
    isolationLevel: "Serializable",
    timeout: 20000,
  });
  attempts.length = 0;
  bodies.length = 0;
  await assert.rejects(
    () =>
      prismaDriver(
        failing([conflict, conflict, conflict, conflict]),
      ).transaction(async () => {}),
    (error) => error.code === "P2034" && error.message === "fail P2034",
  );
  assert.equal(
    attempts.length,
    4,
    "three retries after the first attempt, then the failure is reported",
  );
  attempts.length = 0;
  await assert.rejects(
    () =>
      prismaDriver(failing([conflict, conflict]), { retries: 1 }).transaction(
        async () => {},
      ),
    (error) => error.code === "P2034",
  );
  assert.equal(
    attempts.length,
    2,
    "retries is the number of additional attempts",
  );
  attempts.length = 0;
  await assert.rejects(
    () => prismaDriver(failing([unique])).transaction(async () => {}),
    (error) => error.code === "P2002",
  );
  assert.equal(
    attempts.length,
    1,
    "a non-serialization failure is not retried",
  );
  attempts.length = 0;
  const bundled = prisma(failing([conflict]), { retries: 1, timeout: 5 });
  await bundled.transaction(async () => {});
  assert.deepEqual(
    attempts.map((o) => o.timeout),
    [5, 5],
    "prisma() passes retries and timeout to the runner",
  );
});
test("a Prisma 7 driver-adapter write conflict is retryable; other adapter errors are not", () => {
  const adapter = (cause) =>
    Object.assign(new Error("Raw query failed"), {
      code: "P2010",
      meta: { driverAdapterError: { name: "DriverAdapterError", cause } },
    });
  assert.equal(
    isRetryableTransactionError(adapter({ kind: "TransactionWriteConflict" })),
    true,
  );
  assert.equal(
    isRetryableTransactionError(adapter({ kind: "postgres", code: "40001" })),
    true,
  );
  assert.equal(
    isRetryableTransactionError(adapter({ kind: "postgres", code: "40P01" })),
    true,
  );
  assert.equal(
    isRetryableTransactionError(adapter({ kind: "UniqueConstraintViolation" })),
    false,
  );
  assert.equal(
    isRetryableTransactionError(adapter({ kind: "postgres", code: "23505" })),
    false,
  );
});
test("retry classifier retains raw SQLSTATE and wrapped Prisma failures", () => {
  for (const code of ["40001", "40P01", "P2034"])
    assert.equal(isRetryableTransactionError({ code }), true);
  assert.equal(
    isRetryableTransactionError({ code: "P2010", meta: { code: "40001" } }),
    true,
  );
  assert.equal(
    isRetryableTransactionError({ code: "P2010", meta: { code: "40P01" } }),
    true,
  );
  assert.equal(isRetryableTransactionError({ cause: { code: "40P01" } }), true);
  assert.equal(
    isRetryableTransactionError({ code: "P2010", meta: { code: "23505" } }),
    false,
  );
});
