// Real default-v5 native clients, PostgreSQL transactions and transport faults.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import { GeneratedClient } from "./client.ts";
import { host } from "./host.mts";
import { wait } from "../protocol-fixture.mjs";
const sqlite = (path, sql) =>
  promisify(execFile)("python3", [
    "-c",
    "import sqlite3,sys;db=sqlite3.connect(sys.argv[1]);db.executescript(sys.argv[2]);db.close()",
    path,
    sql,
  ]);
const open = (path, url, owner = "alice") =>
  GeneratedClient.open({
    path,
    stream: `User:${owner}`,
    ...(url ? { connection: { url, token: owner } } : {}),
  });
const item = (id, title, project = "project") => ({ id, title, project });

// The parent kills this process only after the actual saved acceptance and
// failed deferred SQLite COMMIT have both been observed over IPC.
if (process.argv[2] === "commit-fault-child") {
  const path = process.argv[3],
    url = process.argv[4];
  const client = await open(path);
  await client.mutations.publishItem({
    item: item("commit-fault", " accepted "),
    private: true,
  });
  await client.models.item.update(
    { id: "commit-fault" },
    { title: "later-direct" },
  );
  await sqlite(
    path,
    "CREATE TABLE fault_parent(id INTEGER PRIMARY KEY);CREATE TABLE fault_marker(parent INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER fail_settlement_commit BEFORE UPDATE OF reconciled ON axton_mutation_queue WHEN NEW.reconciled=1 BEGIN INSERT INTO fault_marker VALUES(99);END;",
  );
  const diagnostics = [];
  await client.connect({
    url,
    token: "alice",
  }, { onError: (error) => diagnostics.push(String(error)) });
  await wait(async () => {
    const [row] = await client.readSql(
      "SELECT result,reconciled FROM axton_mutation_queue WHERE id=1",
    );
    return (
      row?.result !== null &&
      row?.reconciled === 0 &&
      (
        await client.readSql(
          "SELECT last_acknowledged_batch_id AS n FROM axton_store",
        )
      )[0].n === 1
    );
  }, "saved acceptance before failed deferred settlement COMMIT");
  await wait(
    () => diagnostics.some((message) => message.includes("FOREIGN KEY")),
    "actual deferred FOREIGN KEY COMMIT refusal reported",
  );
  // Verify the failed settlement left the independently committed receipt intact.
  await wait(async () => {
    const status = await client.syncState();
    return (
      status.pending === 1 &&
      (await client.models.item.get({ id: "commit-fault" })).title ===
        "later-direct"
    );
  }, "pending accepted Mutation preserves newer direct content");
  const proof = {
    storeId: client.clientId,
    queue: await client.readSql(
      "SELECT result,reconciled FROM axton_mutation_queue WHERE id=1",
    ),
    markers: await client.readSql("SELECT count(*) AS n FROM fault_marker"),
    local: await client.models.item.get({ id: "commit-fault" }),
  };
  process.send?.({ type: "acceptedAwaiting", proof });
  // Keep the native owner alive until SIGKILL; no close or graceful process exit.
  await new Promise(() => {});
} else {
  test(
    "parent Stream tombstone cascades locally and fences a delayed ordinary child Fetch",
    { timeout: 120000 },
    async () => {
      const h = await host(),
        dir = await mkdtemp(join(tmpdir(), "axton-cascade05-"));
      let client, held, pending;
      try {
        await h.backend.transaction(async ({ tx, streams }) => {
          await tx.query(
            "INSERT INTO behavior_parent VALUES('parent','alive');INSERT INTO behavior_child VALUES('child','parent','cached-child')",
          );
          streams(["User:alice"]).track.parent("parent");
        });
        client = await open(join(dir, "db"), h.proxy.url);
        await client.bootstrap();
        assert.equal(
          (await client.fetch.child({ id: "child" })).title,
          "cached-child",
        );
        held = h.proxy.holdResponse(
          (x) =>
            x.path === "/sync/fetch" &&
            JSON.parse(x.body).invocation?.key?.model === "Child",
        );
        pending = client.fetch.child({ id: "child" });
        pending.catch(() => {});
        await held.arrived;
        await h.backend.transaction(async ({ tx, invalidate }) => {
          await tx.query("DELETE FROM behavior_parent WHERE id='parent'");
          invalidate.parent("parent");
        });
        await wait(
          async () =>
            (await client.models.parent.get({ id: "parent" })) === null &&
            (await client.models.child.get({ id: "child" })) === null,
          "actual parent authority installs its device cascade",
        );
        held.release();
        assert.equal(
          (await pending).title,
          "cached-child",
          "caller retains its earlier invocation snapshot",
        );
        assert.equal(
          await client.models.child.get({ id: "child" }),
          null,
          "late ordinary child read cannot revive current parent tombstone",
        );
        const evidence = await client.readSql(
          "SELECT model,evidence FROM axton_authority WHERE model IN ('Parent','Child')",
        );
        assert.equal(
          JSON.parse(evidence.find((row) => row.model === "Parent").evidence)
            .current.deleted,
          true,
        );
        const childEvidence = evidence.find((row) => row.model === "Child");
        if (childEvidence)
          assert.deepEqual(
            JSON.parse(childEvidence.evidence).history,
            {},
            "device cascade invents no child Stream authority position",
          );
        assert.equal(
          (
            await h.pool.query(
              "SELECT count(*)::int AS n FROM behavior_child WHERE id='child'",
            )
          ).rows[0].n,
          1,
          "device cascade is not server FK deletion",
        );
        assert.equal(
          (
            await h.pool.query(
              "SELECT count(*)::int AS n FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.model='Child'",
            )
          ).rows[0].n,
          0,
          "ordinary Fetch does not enroll the child",
        );
      } finally {
        held?.release();
        await pending?.catch(() => {});
        await client?.close();
        await h.close();
        await rm(dir, { recursive: true, force: true });
      }
    },
  );

  test(
    "actual deferred SQLite COMMIT refusal survives SIGKILL and keeps later direct content",
    { timeout: 120000 },
    async () => {
      const h = await host(),
        dir = await mkdtemp(join(tmpdir(), "axton-commit05-"));
      const path = join(dir, "db");
      let child, client;
      try {
        child = spawn(
          process.execPath,
          [
            "--experimental-strip-types",
            new URL(import.meta.url).pathname,
            "commit-fault-child",
            path,
            h.proxy.url,
          ],
          { stdio: ["ignore", "pipe", "pipe", "ipc"] },
        );
        let stderr = "";
        child.stderr.on("data", (bytes) => (stderr += bytes));
        const proof = await new Promise((resolve, reject) => {
          const timer = setTimeout(
            () => reject(Error(`child acceptance timeout: ${stderr}`)),
            45000,
          );
          child.on("message", (message) => {
            if (message.type === "acceptedAwaiting") {
              clearTimeout(timer);
              resolve(message.proof);
            }
          });
          child.on("exit", (code, signal) => {
            clearTimeout(timer);
            reject(
              Error(`child exited before SIGKILL: ${code}/${signal} ${stderr}`),
            );
          });
        });
        assert.equal(proof.queue[0].reconciled, 0);
        assert.equal(JSON.parse(proof.queue[0].result).item.title, "accepted");
        assert.equal(
          proof.markers[0].n,
          0,
          "failed deferred COMMIT rolled back its marker and settlement",
        );
        assert.equal(proof.local.title, "later-direct");
        const exited = new Promise((resolve) =>
          child.once("exit", (code, signal) => resolve({ code, signal })),
        );
        assert.equal(child.kill("SIGKILL"), true);
        assert.deepEqual(await exited, { code: null, signal: "SIGKILL" });
        await sqlite(path, "DROP TRIGGER fail_settlement_commit;");
        client = await open(path, h.proxy.url);
        assert.equal(client.clientId, proof.storeId);
        await wait(
          async () =>
            (
              await client.readSql(
                "SELECT reconciled FROM axton_mutation_queue WHERE id=1",
              )
            )[0]?.reconciled === 1,
          "saved acceptance reconciles after process death",
        );
        assert.equal(
          (await client.models.item.get({ id: "commit-fault" })).title,
          "later-direct",
        );
        assert.equal((await client.syncState()).pending, 0);
        assert.equal(
          (
            await h.pool.query(
              "SELECT count(*)::int AS n FROM behavior_execution WHERE id='commit-fault'",
            )
          ).rows[0].n,
          1,
          "saved result never repeats its cloud side effect",
        );
        assert.equal(
          (await client.readSql("SELECT count(*) AS n FROM fault_marker"))[0].n,
          0,
        );
      } finally {
        if (child && child.exitCode === null && child.signalCode === null) {
          const exited = new Promise((resolve) => child.once("exit", resolve));
          child.kill("SIGKILL");
          await exited;
        }
        await client?.close();
        await h.close();
        await rm(dir, { recursive: true, force: true });
      }
    },
  );

  test(
    "independent server commits transfer a UNIQUE value through one actual v5 range",
    { timeout: 120000 },
    async () => {
      const h = await host(),
        dir = await mkdtemp(join(tmpdir(), "axton-unique05-"));
      let client;
      try {
        await h.backend.transaction(async ({ tx, streams }) => {
          await tx.query(
            "INSERT INTO behavior_item VALUES('zz','unique','X'),('aa','unique','Y')",
          );
          streams(["User:alice"]).track.item(["zz", "aa"]);
        });
        client = await open(join(dir, "db"), h.proxy.url);
        await client.bootstrap();
        assert.equal((await client.models.item.get({ id: "zz" })).title, "X");
        assert.equal((await client.models.item.get({ id: "aa" })).title, "Y");
        assert.equal(
          (
            await client.readSql(
              "SELECT count(*) AS n FROM sqlite_master WHERE type='index' AND name='Item_title_unique'",
            )
          )[0].n,
          1,
        );
        await client.connection.pause();
        await h.backend.transaction(async ({ tx, invalidate }) => {
          await tx.query("UPDATE behavior_item SET title='Z' WHERE id='zz'");
          invalidate.item("zz");
        });
        await h.backend.transaction(async ({ tx, invalidate }) => {
          await tx.query("UPDATE behavior_item SET title='X' WHERE id='aa'");
          invalidate.item("aa");
        });
        await client.connection.resume();
        await wait(
          async () =>
            (await client.models.item.get({ id: "aa" })).title === "X" &&
            (await client.models.item.get({ id: "zz" })).title === "Z",
          "constraint-connected rows commit without transient collision",
        );
        assert.equal((await client.syncState()).pending, 0);
        assert.equal(
          (
            await client.readSql(
              'SELECT count(DISTINCT title) AS n FROM "Item"',
            )
          )[0].n,
          2,
        );
      } finally {
        await client?.close();
        await h.close();
        await rm(dir, { recursive: true, force: true });
      }
    },
  );

  test(
    "authenticated viewer projections stay separate and a confused Store context never enters a Loader",
    { timeout: 120000 },
    async () => {
      const h = await host({ projected: true }),
        dir = await mkdtemp(join(tmpdir(), "axton-viewers05-"));
      let alice, bob;
      try {
        await h.backend.transaction(async ({ tx, streams }) => {
          await tx.query(
            "INSERT INTO behavior_item VALUES('shared','auth','canonical')",
          );
          streams(["User:alice", "User:bob"]).track.item("shared");
        });
        alice = await open(join(dir, "alice"), h.proxy.url, "alice");
        bob = await open(join(dir, "bob"), h.proxy.url, "bob");
        await Promise.all([alice.bootstrap(), bob.bootstrap()]);
        assert.equal(
          (await alice.models.item.get({ id: "shared" })).title,
          "alice:canonical",
        );
        assert.equal(
          (await bob.models.item.get({ id: "shared" })).title,
          "bob:canonical",
        );
        assert.notEqual(alice.clientId, bob.clientId);
        await alice.fetch.item({ id: "shared" });
        const request = h.proxy
          .requests("/sync/fetch")
          .find((x) => x.request.storeId === alice.clientId).request;
        const before = h.loaderCalls;
        const response = await fetch(h.proxy.url + "/sync/fetch", {
          method: "POST",
          headers: {
            authorization: "Bearer bob",
            "content-type": "application/json",
          },
          body: JSON.stringify({ ...request, requestId: "confused-viewer" }),
        });
        assert.equal(response.ok, false, await response.text());
        assert.equal(
          h.loaderCalls,
          before,
          "refused principal/Store pairing never invokes product code",
        );
        assert.equal(
          (
            await h.pool.query(
              "SELECT principal FROM axton_store WHERE id=$1",
              [alice.clientId],
            )
          ).rows[0].principal,
          "alice",
        );
        assert.equal(
          (await alice.models.item.get({ id: "shared" })).title,
          "alice:canonical",
        );
        assert.equal(
          (await bob.models.item.get({ id: "shared" })).title,
          "bob:canonical",
        );
      } finally {
        await alice?.close();
        await bob?.close();
        await h.close();
        await rm(dir, { recursive: true, force: true });
      }
    },
  );

  test(
    "Store reset fences an earlier ordinary response without changing a same-Stream peer",
    { timeout: 120000 },
    async () => {
      const h = await host(),
        dir = await mkdtemp(join(tmpdir(), "axton-reset05-"));
      let first, second, held, pending;
      try {
        await h.pool.query(
          "INSERT INTO behavior_item VALUES('reset-row','reset','canonical')",
        );
        first = await open(join(dir, "first"), h.proxy.url);
        second = await open(join(dir, "second"), h.proxy.url);
        await Promise.all([first.bootstrap(), second.bootstrap()]);
        await second.fetch.item({ id: "reset-row" });
        await second.models.item.update(
          { id: "reset-row" },
          { title: "peer-only" },
        );
        const originalId = first.clientId;
        held = h.proxy.holdResponse(
          (x) =>
            x.path === "/sync/fetch" &&
            JSON.parse(x.body).storeId === originalId,
        );
        pending = first.fetch.item({ id: "reset-row" });
        const refused = assert.rejects(pending);
        await held.arrived;
        await first.resetStore();
        held.release();
        await refused;
        assert.equal(
          await first.models.item.get({ id: "reset-row" }),
          null,
          "pre-reset response cannot write new Store lifecycle",
        );
        assert.equal(
          (await second.models.item.get({ id: "reset-row" })).title,
          "peer-only",
        );
        assert.notEqual(
          (await first.readSql("SELECT id FROM axton_store"))[0].id,
          originalId,
        );
        assert.equal(
          (await first.fetch.item({ id: "reset-row" })).title,
          "canonical",
        );
        assert.equal(
          (await second.models.item.get({ id: "reset-row" })).title,
          "peer-only",
        );
      } finally {
        held?.release();
        await pending?.catch(() => {});
        await first?.close();
        await second?.close();
        await h.close();
        await rm(dir, { recursive: true, force: true });
      }
    },
  );

  test(
    "invalid Query identity output and malformed wire input refuse without changing held state or running a handler",
    { timeout: 120000 },
    async () => {
      const h = await host(),
        dir = await mkdtemp(join(tmpdir(), "axton-input05-"));
      let client;
      try {
        await h.pool.query(
          "INSERT INTO behavior_item VALUES('validation-row','validation','canonical')",
        );
        client = await open(join(dir, "db"), h.proxy.url);
        await client.bootstrap();
        assert.equal(
          (await client.queries.findItem({ id: "validation-row" })).item.title,
          "canonical",
        );
        h.invalidIdentity = true;
        await assert.rejects(
          client.queries.findItem({ id: "validation-row" }, { store: false }),
        );
        h.invalidIdentity = false;
        assert.equal(
          (await client.models.item.get({ id: "validation-row" })).title,
          "canonical",
        );
        const request = h.proxy
          .requests("/sync/actions")
          .find((x) => x.request.invocation?.name === "FindItem").request;
        const queries = h.queryCalls,
          loaders = h.loaderCalls;
        const response = await fetch(h.proxy.url + "/sync/actions", {
          method: "POST",
          headers: {
            authorization: "Bearer alice",
            "content-type": "application/json",
          },
          body: JSON.stringify({
            ...request,
            requestId: "malformed-args",
            invocation: { ...request.invocation, args: { id: 42 } },
          }),
        });
        const answer = await response.json();
        assert.equal(
          response.ok && answer.outcome?.kind === "succeeded",
          false,
        );
        assert.equal(
          h.queryCalls,
          queries,
          "input refusal occurs before product handler",
        );
        assert.equal(h.loaderCalls, loaders);
        assert.equal(
          (await client.models.item.get({ id: "validation-row" })).title,
          "canonical",
        );
      } finally {
        h.invalidIdentity = false;
        await client?.close();
        await h.close();
        await rm(dir, { recursive: true, force: true });
      }
    },
  );

  test(
    "Loader refusal rejects a whole multi-Model Query and commits no partial cache",
    { timeout: 120000 },
    async () => {
      const h = await host(),
        dir = await mkdtemp(join(tmpdir(), "axton-read-atomic05-"));
      let client;
      try {
        await h.pool.query(
          "INSERT INTO behavior_item VALUES('i1','failure','first'),('i2','failure','second');INSERT INTO behavior_tag VALUES('t1','failure','tag')",
        );
        client = await open(join(dir, "db"), h.proxy.url);
        await client.bootstrap();
        h.failItems = true;
        await assert.rejects(
          client.queries.projectItems({ project: "failure" }),
        );
        assert.deepEqual(await client.models.item.query(), []);
        assert.deepEqual(await client.models.tag.query(), []);
        h.failItems = false;
        const result = await client.queries.projectItems({
          project: "failure",
        });
        assert.equal(result.items.length, 2);
        assert.equal(result.tags.length, 1);
        assert.equal((await client.models.item.query()).length, 2);
        assert.equal((await client.models.tag.query()).length, 1);
        assert.equal(
          (
            await h.pool.query(
              "SELECT count(*)::int AS n FROM axton_stream_record",
            )
          ).rows[0].n,
          0,
          "ordinary read still has no implicit enrollment",
        );
      } finally {
        h.failItems = false;
        await client?.close();
        await h.close();
        await rm(dir, { recursive: true, force: true });
      }
    },
  );
}
