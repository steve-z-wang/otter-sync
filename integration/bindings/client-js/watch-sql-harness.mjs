// Watched read-only SQL over several Models (#184) through the real native
// runtime: SQLite names the tables a statement reads, and the runtime re-runs
// it only after a commit that writes one of them. Shared by the Node and
// React Native suites, which pass their own Transaction adapter; not a test
// file.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createClient } from "../../../packages/client-js/runtime.mts";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const entry = JSON.parse(
  await readFile(
    new URL("../../../fixtures/schemas/entry.json", import.meta.url),
    "utf8",
  ),
);
const text = (name, nullable = false) => ({
  name,
  nullable,
  type: { kind: "scalar", name: "string" },
});
/** Entry, with Media and Person belonging to an Entry, and a lone Note. */
const schema = {
  ...entry,
  models: [
    ...entry.models,
    {
      name: "Media",
      identity: ["id"],
      fields: [text("id"), text("entryId"), text("url"), text("caption", true)],
    },
    {
      name: "Person",
      identity: ["id"],
      fields: [text("id"), text("entryId"), text("name")],
    },
    { name: "Note", identity: ["id"], fields: [text("id"), text("body")] },
  ],
};
/** A Journal page: each Entry with its Media and its Person. */
const JOURNAL =
  "SELECT e.id AS entry, e.text AS text, m.url AS media, p.name AS person " +
  "FROM Entry e JOIN Media m ON m.entryId = e.id JOIN Person p ON p.entryId = e.id ORDER BY e.id";
/** Every re-run publishes: its `random()` column differs. */
const PROBE = "SELECT count(*) AS n, random() AS r FROM Entry";
const put = (model, id, values) => ({
  model,
  op: "create",
  identity: { id },
  values,
});
const change = (model, id, values) => ({
  model,
  op: "update",
  identity: { id },
  values,
});
async function until(predicate, what) {
  const deadline = Date.now() + 5000;
  while (!predicate()) {
    if (Date.now() > deadline) throw Error(`${what} timed out`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

async function withClient(Transaction, body) {
  const directory = await mkdtemp(join(tmpdir(), "axton-watch-sql-"));
  const Client = createClient(native, Transaction, () => {
    throw Error("no network");
  });
  const client = await Client.open({
    path: join(directory, "client.sqlite"),
    schema,
  });
  try {
    await body(client);
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
}

/**
 * Register the watchSql scenarios. `exactGuard`: the host knows the
 * callback's async context, so a call from inside it is refused with
 * `transaction_active`; otherwise it waits behind the transaction.
 */
export function watchSqlTests(test, Transaction, { exactGuard }) {
  const run = (body) => withClient(Transaction, body);

  test("watchSql re-emits a join over three Models after a commit to each, and never for another table", async () => {
    await run(async (client) => {
      await client.direct(put("Entry", "e", { text: "first" }));
      await client.direct(put("Media", "m", { entryId: "e", url: "a.jpg" }));
      await client.direct(put("Person", "p", { entryId: "e", name: "Ann" }));
      const page = [];
      const probe = [];
      const errors = [];
      const stopPage = client.watchSql(
        JOURNAL,
        [],
        (rows) => page.push(rows),
        (error) => errors.push(error),
      );
      const stopProbe = client.watchSql(
        PROBE,
        undefined,
        (rows) => probe.push(rows[0].n),
        (error) => errors.push(error),
      );
      await until(() => page.length === 1 && probe.length === 1, "the first rows");
      const row = (text, media, person) => ({ entry: "e", text, media, person });
      assert.deepEqual(page, [[row("first", "a.jpg", "Ann")]]);
      await client.direct(change("Entry", "e", { text: "second" }));
      await until(() => page.length === 2 && probe.length === 2, "the Entry commit");
      await client.direct(change("Media", "m", { url: "b.jpg" }));
      await until(() => page.length === 3, "the Media commit");
      await client.direct(change("Person", "p", { name: "Bea" }));
      await until(() => page.length === 4, "the Person commit");
      assert.deepEqual(page.slice(1), [
        [row("second", "a.jpg", "Ann")],
        [row("second", "b.jpg", "Ann")],
        [row("second", "b.jpg", "Bea")],
      ]);
      // A commit to an unrelated Model re-runs neither; one that leaves the
      // join's answer unchanged publishes nothing.
      await client.direct(put("Note", "n", { body: "aside" }));
      await client.direct(change("Media", "m", { caption: "unselected" }));
      await settle();
      assert.equal(page.length, 4);
      assert.deepEqual(probe, [1, 1], "only the Entry commit re-ran the probe");
      stopPage();
      stopProbe();
      await client.direct(change("Entry", "e", { text: "after" }));
      await settle();
      assert.equal(page.length, 4, "a stopped watch hears nothing");
      assert.deepEqual(errors, []);
    });
  });

  test("watchSql binds parameters and refuses a write, an engine table and a missing table through onError", async () => {
    await run(async (client) => {
      await client.direct(put("Entry", "e", { text: "bound" }));
      const bound = [];
      const stop = client.watchSql(
        "SELECT text FROM Entry WHERE id = ?",
        ["e"],
        (rows) => bound.push(rows),
      );
      await until(() => bound.length === 1, "the bound rows");
      assert.deepEqual(bound, [[{ text: "bound" }]]);
      stop();
      for (const sql of [
        "DELETE FROM Entry RETURNING id",
        "SELECT count(*) AS n FROM axton_record",
        "SELECT id FROM Missing",
      ]) {
        const delivered = [];
        const refused = await new Promise((resolve) =>
          client.watchSql(sql, [], (rows) => delivered.push(rows), resolve),
        );
        assert.ok(refused instanceof Error, sql);
        assert.deepEqual(delivered, [], sql);
      }
    });
  });

  test(
    exactGuard
      ? "watchSql inside a transaction callback is refused with transaction_active"
      : "watchSql from inside a transaction callback waits for the commit and sees only committed rows",
    async () => {
      await run(async (client) => {
        await client.direct(put("Entry", "e", { text: "before" }));
        const rows = [];
        let refusal;
        await client.transaction(async (tx) => {
          await tx.direct(change("Entry", "e", { text: "inside" }));
          client.watchSql(
            "SELECT text FROM Entry",
            [],
            (value) => rows.push(value),
            (error) => (refusal = error),
          );
          await settle();
          assert.deepEqual(rows, [], "nothing is delivered inside the callback");
        });
        if (exactGuard) {
          await until(() => refusal !== undefined, "the refusal");
          assert.equal(refusal.message, "transaction_active");
          await settle();
          assert.deepEqual(rows, []);
        } else {
          await until(() => rows.length === 1, "the committed rows");
          assert.deepEqual(rows, [[{ text: "inside" }]]);
          assert.equal(refusal, undefined);
        }
      });
    },
  );

  test("closing the client ends a watched statement", async () => {
    const directory = await mkdtemp(join(tmpdir(), "axton-watch-sql-"));
    const Client = createClient(native, Transaction, () => {
      throw Error("no network");
    });
    const client = await Client.open({
      path: join(directory, "client.sqlite"),
      schema,
    });
    try {
      const rows = [];
      const errors = [];
      client.watchSql(
        "SELECT count(*) AS n FROM Entry",
        [],
        (value) => rows.push(value),
        (error) => errors.push(error),
      );
      await until(() => rows.length === 1, "the first rows");
      await client.close();
      await settle();
      assert.deepEqual(rows, [[{ n: 0 }]], "the last rows are not delivered again");
      assert.deepEqual(errors, []);
      await assert.rejects(client.direct(put("Entry", "e", { text: "x" })));
    } finally {
      await client.close();
      await rm(directory, { recursive: true, force: true });
    }
  });
}
