// Fresh-layout adoption and real MVCC conflicts, independent of a native addon.
import test, { after } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { Pool } from "pg";
import { answer } from "../../../packages/backend/postgres/index.mts";
import { pgDriver } from "../../../packages/backend/postgres/src/pg.mts";
import * as SQL from "../../../packages/backend/postgres/src/sql.mts";
const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const driver = pgDriver(pool);
const ddl = await readFile(
  new URL("../../../packages/backend/postgres/migration.sql", import.meta.url),
  "utf8",
);
after(() => pool.end());
let serial = 0;
async function namespace(body) {
  const schema = `cleanup_${process.pid}_${++serial}`;
  const admin = await pool.connect();
  try {
    await admin.query(`CREATE SCHEMA ${schema}`);
    await admin.query(`SET search_path TO ${schema}`);
    await body(admin, schema);
  } finally {
    await admin.query(`DROP SCHEMA ${schema} CASCADE`);
    admin.release();
  }
}
test("fresh namespace has only canonical identity, Store/result, StreamRecord and plan facts", () =>
  namespace(async (tx) => {
    await tx.query(ddl);
    await tx.query(ddl);
    const names = (
      await tx.query(
        "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename",
      )
    ).rows.map((r) => r.tablename);
    assert.deepEqual(names, [
      "axton_delivery_plan",
      "axton_delivery_unit",
      "axton_mutation_result",
      "axton_publication_fence",
      "axton_record",
      "axton_store",
      "axton_stream",
      "axton_stream_record",
    ]);
    const columns = (
      await tx.query(
        "SELECT table_name,column_name FROM information_schema.columns WHERE table_schema=current_schema() AND column_name IN ('stamp','held')",
      )
    ).rows;
    assert.deepEqual(columns, []);
    const ensured = await answer(driver, tx, {
      op: "protocol05",
      request: {
        op: "guardRecords",
        records: [
          { model: "Entry", identityKey: '{"id":"a"}', mode: "ensure" },
          { model: "Entry", identityKey: '{"id":"b"}', mode: "lock" },
        ],
      },
    });
    assert.deepEqual(ensured, [true, false]);
    await tx.query("INSERT INTO axton_stream VALUES('User:a',1)");
    await tx.query(SQL.ENSURE_IDENTITY, ["Entry", '{"id":"b"}']);
    await tx.query(
      "INSERT INTO axton_stream_record SELECT 'User:a',id,1,'upsert' FROM axton_record",
    );
    assert.equal(
      (
        await tx.query(
          "SELECT count(*) n FROM axton_stream_record WHERE cursor=1",
        )
      ).rows[0].n,
      "2",
    );
  }));
test("legacy namespace installation and Store admission refuse before any writes", () =>
  namespace(async (tx) => {
    await tx.query(
      "CREATE TABLE axton_record(model text,identity_key text,stamp bigint NOT NULL); INSERT INTO axton_record VALUES('Entry','{\"id\":\"keep\"}',7)",
    );
    const snapshot = async () => ({
      tables: (
        await tx.query(
          "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename",
        )
      ).rows,
      columns: (
        await tx.query(
          "SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema=current_schema() ORDER BY table_name,ordinal_position",
        )
      ).rows,
      rows: (await tx.query("SELECT * FROM axton_record")).rows,
    });
    const before = await snapshot();
    await assert.rejects(tx.query(ddl), /fresh namespace/);
    assert.deepEqual(await snapshot(), before);
    await assert.rejects(
      answer(driver, tx, {
        op: "protocol05",
        request: {
          op: "claimStore",
          storeId: "s",
          principal: "a",
          stream: "User:a",
        },
      }),
      /fresh namespace/,
    );
    assert.deepEqual(await snapshot(), before);
  }));
async function staleWaiter(sql, params, { insert = false } = {}) {
  await namespace(async (admin, schema) => {
    await admin.query(ddl);
    if (!insert)
      await admin.query(SQL.ENSURE_IDENTITY, ["Entry", '{"id":"x"}']);
    const writer = await pool.connect(),
      waiter = await pool.connect();
    try {
      for (const tx of [writer, waiter]) {
        await tx.query(`SET search_path TO ${schema}`);
        await tx.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
      }
      await waiter.query("SELECT count(*) FROM axton_record"); // snapshot predates publication
      await writer.query(sql, params);
      let completed = false;
      const pending = waiter.query(sql, params).then(
        () => {
          completed = true;
          return null;
        },
        (e) => {
          completed = true;
          return e;
        },
      );
      // Observe the actual blocked database session before releasing its writer.
      let blocked = false;
      for (let i = 0; i < 100; i++) {
        const rows = (
          await admin.query(
            "SELECT wait_event_type FROM pg_stat_activity WHERE pid=$1",
            [waiter.processID],
          )
        ).rows;
        if (rows[0]?.wait_event_type === "Lock") {
          blocked = true;
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      assert.equal(blocked, true, "waiter reached the concurrent row lock");
      assert.equal(completed, false);
      await writer.query("COMMIT");
      const error = await pending;
      assert.equal(
        error?.code,
        "40001",
        "stale snapshot must retry the whole transaction",
      );
    } finally {
      for (const tx of [writer, waiter]) {
        await tx.query("ROLLBACK");
        tx.release();
      }
    }
  });
}
test("publication UPDATE id=id rejects a pre-existing-snapshot waiter", () =>
  staleWaiter(SQL.PUBLICATION_FENCE, []));
test("identity no-op write lock rejects a pre-existing-snapshot waiter", () =>
  staleWaiter(SQL.LOCK_IDENTITY, ["Entry", '{"id":"x"}']));
test("identity ensure rejects a pre-existing-snapshot waiter on a new identity", () =>
  staleWaiter(SQL.ENSURE_IDENTITY, ["Entry", '{"id":"x"}'], { insert: true }));

test("fresh DDL whole-file and split prepared statements produce identical catalog", () =>
  namespace(async (whole, wholeSchema) => {
    const { sqlStatements } =
      await import("../../../packages/backend/postgres/src/statements.mts");
    assert.deepEqual(
      sqlStatements(
        "SELECT 'a;b'; DO $f$ BEGIN PERFORM 1; END $f$; -- ignored;\n",
      ),
      ["SELECT 'a;b'", "DO $f$ BEGIN PERFORM 1; END $f$"],
    );
    await whole.query(ddl);
    await namespace(async (split) => {
      for (let round = 0; round < 2; round++)
        for (const sql of sqlStatements(ddl))
          await split.query({ text: sql, values: [] });
      const catalog = async (tx) => ({
        columns: (
          await tx.query(
            "SELECT table_name,column_name,data_type,is_nullable,is_identity,is_generated FROM information_schema.columns WHERE table_schema=current_schema() ORDER BY 1,2",
          )
        ).rows,
        constraints: (
          await tx.query(
            "SELECT c.relname tbl,conname,pg_get_constraintdef(p.oid) body FROM pg_constraint p JOIN pg_class c ON c.oid=p.conrelid WHERE connamespace=current_schema()::regnamespace ORDER BY 1,2",
          )
        ).rows,
      });
      assert.deepEqual(await catalog(split), await catalog(whole));
    });
  }));

test("guards validate the whole request before writes and acquire mixed keys in canonical order", () =>
  namespace(async (admin, schema) => {
    await admin.query(ddl);
    const request = (records) => ({ op: "guardRecords", records });
    const record = (id, mode = "ensure") => ({
      model: "Order",
      identityKey: JSON.stringify({ id }),
      mode,
    });
    await assert.rejects(
      answer(driver, admin, request([record("a"), record("a")])),
      /repeats/,
    );
    await assert.rejects(
      answer(driver, admin, request([record("z"), record("a")])),
      /canonical/,
    );
    assert.equal(
      (await admin.query("SELECT count(*) n FROM axton_record")).rows[0].n,
      "0",
    );
    await answer(driver, admin, request([record("a"), record("z")]));
    const holder = await pool.connect(),
      writer = await pool.connect(),
      observer = await pool.connect();
    try {
      for (const tx of [holder, writer, observer])
        await tx.query(`SET search_path TO ${schema}`);
      await holder.query("BEGIN");
      await holder.query(SQL.LOCK_IDENTITY, [
        "Order",
        JSON.stringify({ id: "a" }),
      ]);
      await writer.query("BEGIN");
      const pending = answer(
        driver,
        writer,
        request([record("a"), record("m"), record("z", "lock")]),
      );
      let blocked = false;
      for (let i = 0; i < 100; i++) {
        blocked = (
          await admin.query(
            "SELECT cardinality(pg_blocking_pids($1))>0 blocked",
            [writer.processID],
          )
        ).rows[0].blocked;
        if (blocked) break;
        await new Promise((r) => setTimeout(r, 5));
      }
      assert.equal(blocked, true);
      await observer.query("BEGIN");
      await observer.query("SET LOCAL lock_timeout='200ms'");
      await observer.query(SQL.LOCK_IDENTITY, [
        "Order",
        JSON.stringify({ id: "z" }),
      ]);
      await observer.query("ROLLBACK");
      await holder.query("COMMIT");
      assert.deepEqual(await pending, [true, true, true]);
      await writer.query("COMMIT");
    } finally {
      for (const tx of [holder, writer, observer]) {
        await tx.query("ROLLBACK");
        tx.release();
      }
    }
  }));
