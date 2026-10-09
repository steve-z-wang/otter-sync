// Bulk host operations must use bounded PostgreSQL round trips, without changing
// transaction ownership or the positional host contract.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { answer } from "../../../packages/backend/postgres/src/persistence.mts";
import { pgDriver } from "../../../packages/backend/postgres/src/pg.mts";
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const base = pgDriver(pool);
const ddl = await readFile(
  new URL("../../../packages/backend/postgres/migration.sql", import.meta.url),
  "utf8",
);
after(() => pool.end());
let serial = 0;
async function fixture(body) {
  const schema = `bulk_${process.pid}_${++serial}`;
  const tx = await pool.connect();
  try {
    await tx.query(`CREATE SCHEMA ${schema}`);
    await tx.query(`SET search_path TO ${schema}`);
    await tx.query(ddl);
    await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    let queries = 0;
    const driver = {
      ...base,
      query: (...args) => {
        queries++;
        return base.query(...args);
      },
    };
    const call = (op, fields) =>
      answer(driver, tx, { op: "protocol05", request: { op, ...fields } });
    const measure = async (op, fields) => {
      queries = 0;
      const start = performance.now();
      const result = await call(op, fields);
      return { result, queries, ms: Math.round(performance.now() - start) };
    };
    await body({ tx, call, measure, schema });
    await tx.query("COMMIT");
  } finally {
    await tx.query("ROLLBACK");
    await tx.query(`DROP SCHEMA ${schema} CASCADE`);
    tx.release();
  }
}
const key = (id, model = "Entry") => ({
  model,
  identityKey: JSON.stringify({ id }),
});
const keys = (n) =>
  Array.from({ length: n }, (_, i) => key(String(i).padStart(5, "0")));
const delta = (k, stream = "User:a", publish = true) => ({
  ...k,
  identity: JSON.parse(k.identityKey),
  stream,
  publish,
});

test("2055 identities settle and read in bounded SQL batches", () =>
  fixture(async ({ tx, measure }) => {
    const records = keys(2055);
    const guards = await measure("guardRecords", {
      records: records.map((k) => ({ ...k, mode: "ensure" })),
    });
    assert.deepEqual(
      guards.result,
      records.map(() => true),
    );
    const applied = await measure("applyStreamMembers", {
      deltas: records.map((k) => delta(k)),
    });
    assert.deepEqual(
      applied.result,
      records.map((k) => ({
        ...k,
        stream: "User:a",
        cursor: 1,
        kind: "upsert",
      })),
    );
    const unchanged = await measure("applyStreamMembers", {
      deltas: records.map((k) => delta(k, "User:a", false)),
    });
    assert.deepEqual(unchanged.result, applied.result);
    const reads = await measure("readPositions", {
      stream: "User:a",
      records: [...records].reverse(),
    });
    assert.deepEqual(reads.result, [...applied.result].reverse());
    const targets = await measure("targetPositions", {
      stream: "User:a",
      records,
    });
    assert.deepEqual(targets.result, applied.result);
    assert.equal(
      (await tx.query("SELECT head FROM axton_stream")).rows[0].head,
      "1",
    );
    console.log(
      JSON.stringify({
        identities: records.length,
        guards: { queries: guards.queries, ms: guards.ms },
        applied: { queries: applied.queries, ms: applied.ms },
        unchanged: { queries: unchanged.queries, ms: unchanged.ms },
        reads: { queries: reads.queries, ms: reads.ms },
        targets: { queries: targets.queries, ms: targets.ms },
      }),
    );
    assert.ok(
      guards.queries <= 4,
      `guardRecords used ${guards.queries} queries`,
    );
    assert.ok(
      applied.queries <= 6,
      `applyStreamMembers used ${applied.queries} queries`,
    );
    assert.ok(
      unchanged.queries <= 4,
      `unchanged applyStreamMembers used ${unchanged.queries} queries`,
    );
    assert.ok(
      reads.queries <= 4,
      `readPositions used ${reads.queries} queries`,
    );
    assert.ok(
      targets.queries <= 4,
      `targetPositions used ${targets.queries} queries`,
    );
  }));

test("mixed canonical guards across batches retain missing locks and Unicode identities", () =>
  fixture(async ({ tx, call }) => {
    const records = keys(2055);
    await tx.query(
      "INSERT INTO axton_record(model,identity_key) SELECT 'Entry',v->>'identityKey' FROM jsonb_array_elements($1::jsonb) v",
      [JSON.stringify(records.slice(1000, 1500))],
    );
    const guards = records.map((k, i) => ({
      ...k,
      mode: i < 1000 || i >= 2000 ? "ensure" : "lock",
    }));
    assert.deepEqual(
      await call("guardRecords", { records: guards }),
      records.map((_, i) => i < 1500 || i >= 2000),
    );
    assert.equal(
      (await tx.query("SELECT count(*) n FROM axton_record")).rows[0].n,
      "1555",
    );
    const unicode = ["é", "\ue000", "😀"].map((id) => ({
      ...key(id, "Unicode"),
      mode: "ensure",
    }));
    assert.deepEqual(await call("guardRecords", { records: unicode }), [
      true,
      true,
      true,
    ]);
    await assert.rejects(
      call("guardRecords", { records: [...unicode].reverse() }),
      /canonical/,
    );
    await assert.rejects(
      call("guardRecords", { records: [guards[0], guards[0]] }),
      /repeats/,
    );
    await assert.rejects(
      call("guardRecords", {
        records: [
          { ...key("unwritten"), mode: "ensure" },
          { ...key("z"), mode: "bad" },
        ],
      }),
      /invalid guard mode/,
    );
    assert.equal(
      (
        await tx.query(
          "SELECT count(*) n FROM axton_record WHERE identity_key=$1",
          [key("unwritten").identityKey],
        )
      ).rows[0].n,
      "0",
    );
  }));

test("position batches retain duplicates, caller order, missing targets and remove positions", () =>
  fixture(async ({ tx, call }) => {
    const [a, b, absent] = keys(3);
    await call("guardRecords", {
      records: [a, b].map((k) => ({ ...k, mode: "ensure" })),
    });
    await call("applyStreamMembers", { deltas: [delta(a), delta(b)] });
    await tx.query(
      "UPDATE axton_stream_record SET kind='remove' WHERE record_id=(SELECT id FROM axton_record WHERE identity_key=$1)",
      [b.identityKey],
    );
    const records = Array.from({ length: 2055 }, (_, i) =>
      i % 3 === 0 ? b : i % 3 === 1 ? absent : a,
    );
    const result = await call("targetPositions", { stream: "User:a", records });
    assert.deepEqual(
      result,
      records.map((k) =>
        k === absent
          ? null
          : {
              ...k,
              stream: "User:a",
              cursor: 1,
              kind: k === b ? "remove" : "upsert",
            },
      ),
    );
    await assert.rejects(
      call("readPositions", { stream: "User:a", records }),
      /missing Stream position/,
    );
    assert.deepEqual(
      await call("readPositions", { stream: "User:a", records: [b, a, b] }),
      [result[0], result[2], result[0]],
    );
  }));

test("member batches correlate mixed published and existing pairs across streams", () =>
  fixture(async ({ tx, call }) => {
    const records = keys(2055);
    await call("guardRecords", {
      records: records.map((k) => ({ ...k, mode: "ensure" })),
    });
    await tx.query(
      "INSERT INTO axton_stream VALUES('User:a',10),('User:b',20)",
    );
    const first = await call("applyStreamMembers", {
      deltas: records.map((k) => delta(k)),
    });
    const deltas = records.map((k, i) =>
      delta(k, i % 2 === 0 ? "User:b" : "User:a", i % 2 === 0),
    );
    assert.deepEqual(
      await call("applyStreamMembers", { deltas }),
      deltas.map((d) => ({
        model: d.model,
        identityKey: d.identityKey,
        stream: d.stream,
        cursor: d.publish ? 21 : 11,
        kind: "upsert",
      })),
    );
    assert.deepEqual(
      await call("applyStreamMembers", { deltas: [delta(records[0])] }),
      [first[0]],
    );
    await assert.rejects(
      call("applyStreamMembers", { deltas: [deltas[0], deltas[0]] }),
      /repeats/,
    );
    await assert.rejects(
      call("applyStreamMembers", {
        deltas: [delta(key("missing"), "User:a", false)],
      }),
      /missing canonical record metadata/,
    );
    await tx.query(
      "UPDATE axton_stream_record SET kind='remove' WHERE stream='User:a' AND record_id=(SELECT id FROM axton_record WHERE identity_key=$1)",
      [records[1].identityKey],
    );
    await assert.rejects(
      call("applyStreamMembers", {
        deltas: [delta(records[1], "User:a", false)],
      }),
      /no live position/,
    );
    await call("applyStreamMembers", { deltas: [delta(records[1])] });
    assert.equal(
      (
        await tx.query(
          "SELECT kind FROM axton_stream_record WHERE stream='User:a' AND record_id=(SELECT id FROM axton_record WHERE identity_key=$1)",
          [records[1].identityKey],
        )
      ).rows[0].kind,
      "upsert",
    );
  }));

for (const prior of [false, true])
  test(`bulk publication savepoint preserves ${prior ? "prior reservation" : "first creation rollback"}`, () =>
    fixture(async ({ tx, call }) => {
      const [keep, undo, later] = keys(3);
      await tx.query("DROP TABLE IF EXISTS pg_temp.axton_publication_cursor");
      await call("guardRecords", {
        records: [keep, undo, later].map((k) => ({ ...k, mode: "ensure" })),
      });
      if (prior) await call("applyStreamMembers", { deltas: [delta(keep)] });
      await tx.query("SAVEPOINT publication");
      await call("applyStreamMembers", { deltas: [delta(undo)] });
      await tx.query("ROLLBACK TO SAVEPOINT publication");
      const result = await call("applyStreamMembers", {
        deltas: [delta(later)],
      });
      assert.equal(result[0].cursor, 1);
      assert.equal(
        (await tx.query("SELECT head FROM axton_stream WHERE stream='User:a'"))
          .rows[0].head,
        "1",
      );
      assert.equal(
        (await tx.query("SELECT count(*) n FROM axton_stream_record")).rows[0]
          .n,
        prior ? "2" : "1",
      );
    }));

test("bulk publication keeps namespace reservations distinct and counters exact at the safe limit", () =>
  fixture(async ({ tx, call, schema }) => {
    const k = key("a");
    await call("guardRecords", { records: [{ ...k, mode: "ensure" }] });
    await tx.query(
      "INSERT INTO axton_stream VALUES('User:a',9007199254740990)",
    );
    assert.equal(
      (await call("applyStreamMembers", { deltas: [delta(k)] }))[0].cursor,
      Number.MAX_SAFE_INTEGER,
    );
    assert.equal(
      (await call("readPositions", { stream: "User:a", records: [k] }))[0]
        .cursor,
      Number.MAX_SAFE_INTEGER,
    );
    const other = `${schema}_other`;
    await tx.query(`CREATE SCHEMA ${other}`);
    await tx.query(`SET LOCAL search_path TO ${other}`);
    await tx.query(ddl);
    await call("guardRecords", { records: [{ ...k, mode: "ensure" }] });
    assert.equal(
      (await call("applyStreamMembers", { deltas: [delta(k)] }))[0].cursor,
      1,
    );
    await tx.query(`SET LOCAL search_path TO ${schema}`);
    assert.equal(
      (await call("applyStreamMembers", { deltas: [delta(k)] }))[0].cursor,
      Number.MAX_SAFE_INTEGER,
    );
    await tx.query(`DROP SCHEMA ${other} CASCADE`);
    await tx.query("COMMIT");
    await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    await assert.rejects(
      call("applyStreamMembers", { deltas: [delta(k)] }),
      /head counter overflow/,
    );
    await tx.query("ROLLBACK");
    await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    assert.equal(
      (await tx.query("SELECT head FROM axton_stream WHERE stream='User:a'"))
        .rows[0].head,
      String(Number.MAX_SAFE_INTEGER),
    );
  }));

test("empty bulk operations preserve empty positional responses", () =>
  fixture(async ({ call }) => {
    assert.deepEqual(await call("guardRecords", { records: [] }), []);
    assert.deepEqual(await call("applyStreamMembers", { deltas: [] }), []);
    for (const op of ["readPositions", "targetPositions"])
      assert.deepEqual(await call(op, { stream: "User:a", records: [] }), []);
  }));

test("[prisma] 2055 identities settle in the default interactive transaction", () =>
  fixture(async ({ tx, schema }) => {
    const { createRequire } = await import("node:module");
    const { prismaDriver } =
      await import("../../../packages/backend/postgres/src/prisma.mts");
    const { PrismaClient } = createRequire(import.meta.url)(
      process.env.AXTON_PRISMA_CLIENT ?? "../../bindings/node/generated/client",
    );
    const client = new PrismaClient();
    const records = keys(2055);
    let queries = 0;
    const driver = prismaDriver(client);
    const counted = {
      ...driver,
      query: (...args) => {
        queries++;
        return driver.query(...args);
      },
    };
    try {
      await driver.transaction(async (transaction) => {
        await transaction.$queryRawUnsafe(`SET LOCAL search_path TO ${schema}`);
        const call = (op, fields) =>
          answer(counted, transaction, {
            op: "protocol05",
            request: { op, ...fields },
          });
        assert.deepEqual(
          await call("guardRecords", {
            records: records.map((k) => ({ ...k, mode: "ensure" })),
          }),
          records.map(() => true),
        );
        const result = await call("applyStreamMembers", {
          deltas: records.map((k) => delta(k)),
        });
        assert.deepEqual(
          result,
          records.map((k) => ({
            ...k,
            stream: "User:a",
            cursor: 1,
            kind: "upsert",
          })),
        );
        assert.deepEqual(
          await call("readPositions", { stream: "User:a", records }),
          result,
        );
      });
      assert.ok(
        queries <= 14,
        `default Prisma transaction used ${queries} host SQL queries`,
      );
      assert.equal(
        (await tx.query("SELECT count(*) n FROM axton_stream_record")).rows[0]
          .n,
        "2055",
      );
    } finally {
      await client.$disconnect();
    }
  }));

for (const mode of ["ensure", "lock"])
  test(`bulk ${mode} guards retain Serializable MVCC conflict detection`, () =>
    fixture(async ({ tx, call, schema }) => {
      const records = keys(1001);
      await call("guardRecords", {
        records: records.map((k) => ({ ...k, mode: "ensure" })),
      });
      await tx.query("COMMIT");
      await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
      await tx.query("SELECT count(*) FROM axton_record");
      const writer = await pool.connect();
      try {
        await writer.query(`SET search_path TO ${schema}`);
        await writer.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
        await answer(base, writer, {
          op: "guardRecords",
          records: records.map((k) => ({ ...k, mode })),
        });
        await writer.query("COMMIT");
        await assert.rejects(
          call("guardRecords", {
            records: records.map((k) => ({ ...k, mode })),
          }),
          (error) => error.code === "40001",
        );
        await tx.query("ROLLBACK");
        await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
        assert.deepEqual(
          await call("guardRecords", {
            records: records.map((k) => ({ ...k, mode })),
          }),
          records.map(() => true),
        );
      } finally {
        await writer.query("ROLLBACK");
        writer.release();
      }
    }));
