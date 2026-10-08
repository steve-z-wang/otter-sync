import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, symlink, link, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, execFile } from "node:child_process";
import { promisify } from "node:util";
import { createRequire } from "node:module";
import { Client } from "../../packages/client-js/index.mts";
import { Bridge } from "../../packages/client-js/bridge.mts";
import {
  GeneratedClient,
  schema,
  liveModels,
  makeMutations,
} from "./client.ts";
import { GeneratedClient as RolloverClient } from "./rollover/client.ts";
import { GeneratedClient as VersionedClient } from "./versioned/client.ts";
import { host } from "./server.mjs";
const until = async (probe, label) => {
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    if (await probe()) return;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw Error(`Timed out: ${label}`);
};
const sqliteFault = (path, sql) =>
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
const publish = (client, id, text, mode = "normal") =>
  client.mutations.publish({ entry: { id, text }, call: mode });

if (process.argv[2] === "enqueue-child") {
  const client = await open(process.argv[3]);
  await client.transaction(async (tx) => {
    await tx.models.draft.create({ id: "restart-companion", text: "keep" });
    await tx.mutations.publish({
      entry: { id: "restart-entry", text: " normalized restart " },
      call: "private",
    });
  });
  // Exit without close: WAL + committed optimism + queue survive the missing waiter.
  process.exit(73);
} else if (process.argv[2] === "accepted-child") {
  const client = await open(process.argv[3], process.argv[4], "killed");
  await (await publish(client, "killed-accepted", " accepted ")).wait();
  throw Error("accepted response should remain held");
} else {
  test("ported reads: SIGKILL after cloud acceptance replays identical frozen Batch without repeating execution", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-killed-accepted-"));
    let child, client;
    try {
      h.holdAcknowledgements();
      const path = join(dir, "db");
      child = spawn(
        process.execPath,
        [new URL(import.meta.url).pathname, "accepted-child", path, h.url],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let errors = "";
      child.stderr.on("data", (bytes) => (errors += bytes));
      const exited = new Promise((resolve) =>
        child.once("exit", (code, signal) => resolve({ code, signal })),
      );
      await until(
        () => h.heldAcknowledgementCount > 0,
        "accepted cloud response held",
      );
      assert.ok(h.executions.length > 0);
      assert.ok(h.executions.every((id) => id === "killed-accepted"));
      // SQL may retry before acceptance; replay must add no handler attempt afterward.
      const acceptedExecutions = [...h.executions];
      const frozen = h.batches[0];
      child.kill("SIGKILL");
      assert.deepEqual(await exited, { code: null, signal: "SIGKILL" }, errors);
      h.releaseAcknowledgements();
      client = await open(path, h.url, "killed");
      await until(
        async () => (await client.syncState()).pending === 0,
        "killed client recovery",
      );
      assert.ok(h.batches.length >= 2);
      assert.equal(h.batches.at(-1), frozen);
      assert.deepEqual(h.executions, acceptedExecutions);
      assert.equal(
        (await client.models.entry.get({ id: "killed-accepted" })).text,
        "accepted",
      );
      assert.equal(
        (
          await h.pool.query(
            "SELECT count(*)::int n FROM sdk05_entry WHERE id='killed-accepted'",
          )
        ).rows[0].n,
        1,
      );
    } finally {
      if (child?.exitCode === null && child.signalCode === null)
        child.kill("SIGKILL");
      h.releaseAcknowledgements();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("ported reads: multi-Model Query Loader failure installs neither Model; false and nullable arguments stay read-only", async () => {
    const h = await host({ mixed: true }),
      dir = await mkdtemp(join(tmpdir(), "axton-mixed-read-"));
    let client;
    try {
      await h.pool.query(
        "INSERT INTO sdk05_entry VALUES('mixed-entry','entry','mixed'),('mixed-snapshot','snapshot','mixed')",
      );
      client = await Client.open({
        schema: h.mixedSchema,
        path: join(dir, "db"),
        stream: "User:mixed",
        connection: { url: h.url, token: "mixed" },
      });
      const query = (store) =>
        client.invokeQuery("Mixed", 1, { id: null }, (value) => value, {
          store,
        });
      h.failSnapshot(true);
      await assert.rejects(query(true));
      for (const model of ["Entry", "Snapshot"])
        assert.deepEqual(await client.querySpec(model), []);
      h.failSnapshot(false);
      const result = await query(false);
      assert.equal(result.entry.text, "entry");
      assert.equal(result.snapshot.text, "snapshot");
      for (const model of ["Entry", "Snapshot"])
        assert.deepEqual(await client.querySpec(model), []);
      assert.deepEqual(await query(true), result);
      for (const model of ["Entry", "Snapshot"])
        assert.equal((await client.querySpec(model)).length, 1);
      assert.equal(
        (
          await h.pool.query(
            "SELECT count(*)::int n FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id' IN ('mixed-entry','mixed-snapshot')",
          )
        ).rows[0].n,
        0,
      );
    } finally {
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("Batch ownership: delayed historical settlement survives disjoint queued work and reopen", async () => {
    const h = await host({ unbootstrapped: true }),
      dir = await mkdtemp(join(tmpdir(), "axton-batch-window-"));
    let client;
    try {
      await h.backend.transaction(async ({ tx, streams }) => {
        await tx.query(
          "INSERT INTO sdk05_entry VALUES('window-owned','canonical','window'),('window-independent','independent','window')",
        );
        streams(["User:window"]).track.entry("window-owned");
      });
      const path = join(dir, "db");
      client = await Client.open({
        path,
        schema: h.mixedSchema,
        stream: "User:window",
        connection: { url: h.url, token: "window" },
      });
      await until(
        async () =>
          (await client.readSql("SELECT start_cursor FROM axton_store"))[0]
            .start_cursor !== null,
        "handshake prefix",
      );
      h.holdSettlement();
      await makeMutations(client).publish({
        entry: { id: "window-owned", text: "optimistic" },
        call: "noop",
      });
      await until(
        () => h.heldSettlementCount > 0,
        "historical owned request parked before server admission",
      );
      const first = (
        await client.readSql(
          "SELECT batch_id,sync_cursor,targets,reconciled FROM axton_mutation_queue WHERE id=1",
        )
      )[0];
      assert.equal(first.reconciled, 0);
      assert.equal(JSON.parse(first.targets)[0].kind, "stream");
      await makeMutations(client).publish({
        entry: { id: "window-independent", text: "second optimism" },
        call: "noop",
      });
      // Keep the owned request in flight while the actor can schedule disjoint work.
      await new Promise(resolve => setTimeout(resolve, 300));
      await client.close();
      client = undefined;
      client = await Client.open({
        path,
        schema: h.mixedSchema,
        stream: "User:window",
        connection: { url: h.url, token: "window" },
      });
      await until(
        () => h.heldSettlementCount > 1,
        "owned request rebuilt after reopen",
      );
      assert.equal(
        (
          await h.pool.query(
            "SELECT count(*)::int n FROM axton_mutation_result WHERE store_id=$1 AND batch_id=$2",
            [client.clientId, first.batch_id],
          )
        ).rows[0].n,
        1,
        "next Batch must preserve the unresolved owner",
      );
      h.releaseSettlement();
      await until(
        async () => (await client.syncState()).pending === 0,
        "both disjoint calls settle after ownership release",
      );
      assert.equal(
        (await liveModels(client).entry.get({ id: "window-owned" })).text,
        "canonical",
      );
      assert.equal(
        (await liveModels(client).entry.get({ id: "window-independent" })).text,
        "independent",
      );
      assert.equal(
        h.requests
          .filter((r) => r.route === "/sync/materialize")
          .map((r) => JSON.parse(r.body))
          .filter((r) => r.owner.kind === "settlement")
          .every((r) => r.owner.batchId === first.batch_id),
        true,
      );
      assert.equal(h.errors.some(error => error.includes("unknown settlement owner")), false, h.errors.join("\n"));
    } finally {
      h.releaseSettlement();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("ported reads: private Model-target no-op settles optimism to existing canonical state without enrollment", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-private-noop-"));
    let client;
    try {
      await h.pool.query(
        "INSERT INTO sdk05_entry VALUES('private-noop','existing canonical','noop')",
      );
      client = await open(join(dir, "db"), undefined, "noop");
      const call = await publish(
        client,
        "private-noop",
        "optimistic value",
        "noop",
      );
      assert.equal(
        (await client.models.entry.get({ id: "private-noop" })).text,
        "optimistic value",
      );
      await client.connect({ url: h.url, token: "noop" });
      const result = await call.wait();
      assert.equal(result.error, null);
      assert.equal(result.result.entry.text, "existing canonical");
      assert.equal(
        (await client.models.entry.get({ id: "private-noop" })).text,
        "existing canonical",
      );
      assert.equal((await client.syncState()).pending, 0);
      assert.equal(
        (
          await h.pool.query(
            "SELECT text FROM sdk05_entry WHERE id='private-noop'",
          )
        ).rows[0].text,
        "existing canonical",
      );
      assert.equal(
        (
          await h.pool.query(
            "SELECT count(*)::int n FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id'='private-noop'",
          )
        ).rows[0].n,
        0,
      );
      const outcome = (
        await h.pool.query(
          "SELECT result FROM axton_mutation_result WHERE store_id=$1",
          [client.clientId],
        )
      ).rows[0].result.outcome;
      assert.equal(outcome.kind, "accepted");
      assert.equal(outcome.syncCursor, 0);
      assert.equal(outcome.targets[0].kind, "private");
      assert.equal(outcome.targets[0].record.cursor, null);
      assert.equal(outcome.targets[0].record.state.text, "existing canonical");
    } finally {
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A1/A2/A4/A8/A11/A15: offline Batch, partial refusal, lost response, local ownership and multi-device completion", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-"));
    let a, b;
    try {
      a = await open(join(dir, "a"));
      b = await open(join(dir, "b"));
      assert.notEqual(a.clientId, b.clientId);
      let accepted, refused, privateCall;
      await a.transaction(async (tx) => {
        accepted = await tx.mutations.publish({
          entry: { id: "joined-accepted", text: " canonical " },
          call: "multi",
        });
        refused = await tx.mutations.publish(async (local) => {
          await local.models.draft.create({
            id: "refused-companion",
            text: "undo",
          });
          return {
            entry: { id: "joined-refused", text: "refuse" },
            call: "normal",
          };
        });
        privateCall = await tx.mutations.publish(async (local) => {
          await local.models.draft.create({
            id: "accepted-companion",
            text: "keep",
          });
          return {
            entry: { id: "joined-private", text: " private " },
            call: "private",
          };
        });
      });
      assert.equal(
        (await a.models.entry.get({ id: "joined-accepted" })).text,
        " canonical ",
      );
      await a.models.entry.update(
        { id: "joined-private" },
        { text: "later direct" },
      );
      h.loseNext();
      await a.connect({ url: h.url, token: "alice" });
      await b.connect({ url: h.url, token: "alice" });
      const [yes, no, priv] = await Promise.all([
        accepted.wait(),
        refused.wait(),
        privateCall.wait(),
      ]);
      assert.equal(yes.error, null);
      assert.equal(yes.result.entry.text, "canonical");
      assert.equal(no.error.code, "publish.refused");
      assert.equal(priv.result.entry.text, "private");
      assert.equal(await a.models.entry.get({ id: "joined-refused" }), null);
      assert.equal(
        (await a.models.entry.get({ id: "joined-private" })).text,
        "later direct",
      );
      assert.equal(await a.models.draft.get({ id: "refused-companion" }), null);
      assert.equal(
        (await a.models.draft.get({ id: "accepted-companion" })).text,
        "keep",
      );
      assert.equal(h.batches.length >= 2, true);
      assert.equal(
        h.batches[0],
        h.batches[1],
        "lost response must retry exact Batch bytes",
      );
      assert.deepEqual(h.executions, [
        "joined-accepted",
        "joined-refused",
        "joined-private",
      ]);
      const submitted = JSON.parse(h.batches[0]);
      assert.equal(submitted.protocol, 5);
      assert.equal(submitted.mutations.length, 3);
      await b.bootstrap();
      assert.equal(
        (await b.models.entry.get({ id: "joined-accepted" })).text,
        "canonical",
      );
      assert.equal(
        await b.models.entry.get({ id: "joined-private" }),
        null,
        "private settlement did not enroll another device",
      );
      const members = (
        await h.pool.query(
          "SELECT s.stream,s.cursor,r.identity FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id' LIKE 'joined-%' ORDER BY s.stream,s.cursor",
        )
      ).rows;
      assert.deepEqual(
        members.map((x) => [x.stream, x.identity.id]),
        [
          ["User:alice", "joined-accepted"],
          ["User:bob", "joined-accepted"],
        ],
      );
      const other = await publish(b, "device-b-entry", "device b");
      assert.equal((await other.wait()).error, null);
      assert.equal(JSON.parse(h.batches.at(-1)).storeId, b.clientId);
    } finally {
      await a?.close();
      await b?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A3/A10: child exits after local commit, reopened Store settles without original waiter", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-reopen-"));
    let client;
    try {
      const path = join(dir, "db");
      const child = spawn(
        process.execPath,
        [new URL(import.meta.url).pathname, "enqueue-child", path],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let error = "";
      child.stderr.on("data", (bytes) => (error += bytes));
      assert.equal(
        await new Promise((resolve) => child.on("exit", resolve)),
        73,
        error,
      );
      client = await open(path);
      const id = client.clientId;
      assert.equal(
        (await client.models.entry.get({ id: "restart-entry" })).text,
        " normalized restart ",
      );
      assert.equal(
        (await client.models.draft.get({ id: "restart-companion" })).text,
        "keep",
      );
      await client.connect({ url: h.url, token: "alice" });
      await until(
        async () =>
          (
            await client.readSql(
              "SELECT reconciled FROM axton_mutation_queue WHERE id=1",
            )
          )[0]?.reconciled === 1,
        "reopened durable settlement",
      );
      assert.equal(
        (await client.models.entry.get({ id: "restart-entry" })).text,
        "normalized restart",
      );
      await client.close();
      client = await open(path);
      assert.equal(client.clientId, id);
      const persisted = await client.readSql(
        "SELECT result,reconciled FROM axton_mutation_queue WHERE id=1",
      );
      assert.equal(persisted[0].reconciled, 1);
      assert.equal(
        JSON.parse(persisted[0].result).entry.text,
        "normalized restart",
      );
      assert.deepEqual(h.executions, ["restart-entry"]);
      await client.close();
      client = undefined;
      const native = createRequire(import.meta.url)(
        "../../bindings/node/axton-node.node",
      );
      const { bridge } = await Bridge.open(native, {
        path,
        schema,
        stream: "User:alice",
      });
      try {
        const completed = await bridge.task({
          kind: "callCompletion",
          callId: `${id}:1`,
        });
        assert.equal(completed.outcome.status, "succeeded");
        assert.equal(completed.outcome.result.entry.text, "normalized restart");
      } finally {
        await bridge.close();
      }
    } finally {
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A3: actual SQLite acknowledgement failure retries unchanged Batch without duplicate cloud execution", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-ack-fault-"));
    let client;
    try {
      const path = join(dir, "db");
      client = await open(path);
      const call = await publish(
        client,
        "ack-fault-entry",
        " canonical ack ",
        "private",
      );
      await sqliteFault(
        path,
        "CREATE TRIGGER task7_ack_failure BEFORE UPDATE OF last_acknowledged_batch_id ON axton_store BEGIN SELECT RAISE(ABORT,'task7 acknowledgement commit failure');END;",
      );
      await client.connect({ url: h.url, token: "alice" });
      await until(
        () => h.batches.length >= 2,
        "acknowledgement failure causes exact retry",
      );
      assert.equal(h.batches[0], h.batches[1]);
      assert.deepEqual(h.executions, ["ack-fault-entry"]);
      assert.equal(
        (
          await client.readSql(
            "SELECT last_acknowledged_batch_id FROM axton_store",
          )
        )[0].last_acknowledged_batch_id,
        0,
      );
      assert.equal(
        (
          await client.readSql(
            "SELECT sync_cursor FROM axton_mutation_queue WHERE id=1",
          )
        )[0].sync_cursor,
        null,
      );
      await sqliteFault(path, "DROP TRIGGER task7_ack_failure;");
      assert.equal((await call.wait()).result.entry.text, "canonical ack");
      assert.deepEqual(h.executions, ["ack-fault-entry"]);
    } finally {
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A3/A4: saved acceptance survives SQLite settlement failure and reopening without original waiter", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-settle-fault-"));
    let client;
    try {
      const path = join(dir, "db");
      client = await open(path);
      const id = client.clientId;
      await publish(
        client,
        "settle-fault-entry",
        " canonical settle ",
        "private",
      );
      await client.models.entry.update(
        { id: "settle-fault-entry" },
        { text: "later local" },
      );
      await sqliteFault(
        path,
        "CREATE TRIGGER task7_settlement_failure BEFORE UPDATE OF reconciled ON axton_mutation_queue WHEN NEW.reconciled=1 BEGIN SELECT RAISE(ABORT,'task7 settlement commit failure');END;",
      );
      await client.connect({ url: h.url, token: "alice" });
      await until(
        async () =>
          (
            await client.readSql(
              "SELECT last_acknowledged_batch_id FROM axton_store",
            )
          )[0].last_acknowledged_batch_id === 1,
        "saved acceptance boundary",
      );
      const saved = (
        await client.readSql(
          "SELECT sync_cursor,result,reconciled FROM axton_mutation_queue WHERE id=1",
        )
      )[0];
      assert.equal(saved.reconciled, 0);
      assert.equal(JSON.parse(saved.result).entry.text, "canonical settle");
      assert.equal(
        (await client.models.entry.get({ id: "settle-fault-entry" })).text,
        "later local",
      );
      await client.close();
      client = undefined;
      await sqliteFault(path, "DROP TRIGGER task7_settlement_failure;");
      client = await open(path, h.url);
      assert.equal(client.clientId, id);
      await until(
        async () =>
          (
            await client.readSql(
              "SELECT reconciled FROM axton_mutation_queue WHERE id=1",
            )
          )[0].reconciled === 1,
        "reopened settlement",
      );
      assert.equal(
        (await client.models.entry.get({ id: "settle-fault-entry" })).text,
        "later local",
      );
      assert.deepEqual(h.executions, ["settle-fault-entry"]);
    } finally {
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A5/A9/A12/A13: zero Bootstrap, fresh Query snapshots, direct-delete refill and Store aliases", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-reads-"));
    let client;
    try {
      const path = join(dir, "db");
      client = await open(path, h.url, "reader");
      await client.bootstrap();
      const progress = await client.readSql(
        "SELECT start_cursor,bootstrap_cursor,cursor FROM axton_store",
      );
      assert.equal(progress[0].start_cursor, 0);
      assert.equal(progress[0].bootstrap_cursor, 0);
      assert.equal(progress[0].cursor, 0);
      for (const [alias, make] of [
        ["symlink", symlink],
        ["hardlink", link],
      ]) {
        const name = join(dir, alias);
        await make(path, name);
        await assert.rejects(
          open(name, h.url, "reader"),
          /in.use|locked|already|ownership/,
        );
      }
      await h.pool.query(
        "INSERT INTO sdk05_entry VALUES('snapshot-read','snapshot','reader')",
      );
      assert.equal(
        (await client.queries.find({ id: "snapshot-read" }, { store: false }))
          .entry.text,
        "snapshot",
      );
      assert.equal(
        await client.models.entry.get({ id: "snapshot-read" }),
        null,
      );
      await h.pool.query(
        "UPDATE sdk05_entry SET text='fresh' WHERE id='snapshot-read'",
      );
      assert.equal(
        (await client.queries.find({ id: "snapshot-read" }, { store: false }))
          .entry.text,
        "fresh",
      );
      assert.equal(
        h.queries.filter((x) => x === "snapshot-read").length,
        2,
        "every Query must invoke its handler",
      );
      assert.equal(
        (await client.fetch.entry({ id: "snapshot-read" }, { store: false }))
          .text,
        "fresh",
      );
      assert.equal(
        await client.models.entry.get({ id: "snapshot-read" }),
        null,
      );
      assert.equal(
        (await client.queries.find({ id: "snapshot-read" })).entry.text,
        "fresh",
      );
      await client.models.entry.delete({ id: "snapshot-read" });
      assert.equal(
        await client.models.entry.get({ id: "snapshot-read" }),
        null,
      );
      assert.equal(
        (await client.queries.find({ id: "snapshot-read" })).entry.text,
        "fresh",
      );
      assert.equal(
        (await client.models.entry.get({ id: "snapshot-read" })).text,
        "fresh",
      );
      const memberships = (
        await h.pool.query(
          "SELECT s.* FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id'='snapshot-read'",
        )
      ).rows;
      assert.deepEqual(memberships, [], "Query/Fetch never enroll implicitly");
      assert.equal((await client.queries.find({ id: "missing" })).entry, null);
      await assert.rejects(
        client.queries.find({ id: "refused-query" }),
        (error) =>
          error.code === "find.denied" && error.execution === "rejected",
      );
      assert.equal(await client.fetch.entry({ id: "missing" }), null);
      await assert.rejects(
        client.queries.find({ id: "snapshot-read" }, { once: true }),
        /option|once/,
      );
      await assert.rejects(
        GeneratedClient.open({
          path: join(dir, "identity"),
          stream: "User:reader",
          connection: {
            url: h.url,
            token: "reader",
            identity: { viewer: "reader" },
          },
        }),
        /identity/,
      );
    } finally {
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A10/A11/A12: close fences an unfinished Query response before any cache commit", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-close-read-"));
    let client;
    try {
      await h.pool.query(
        "INSERT INTO sdk05_entry VALUES('close-read','snapshot','close')",
      );
      const path = join(dir, "db");
      client = await open(path, h.url, "close");
      await client.bootstrap();
      h.holdReads();
      const pending = client.queries.find({ id: "close-read" });
      const rejected = assert.rejects(
        pending,
        (error) => error.code === "action.unavailable",
      );
      await until(
        () => h.heldReadCount === 1,
        "Query response held after server read",
      );
      await Promise.race([
        client.close(),
        new Promise((_, reject) =>
          setTimeout(
            () => reject(Error("close waited for held Query response")),
            1000,
          ),
        ),
      ]);
      await rejected;
      h.releaseReads();
      client = await open(path, undefined, "close");
      assert.equal(
        await client.models.entry.get({ id: "close-read" }),
        null,
        "late closed-connection result wrote reopened Store",
      );
    } finally {
      h.releaseReads();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A5/A6/A7/A11: actual multi-part same-cursor Bootstrap commits one complete unit", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-fragments-"));
    let client, stop;
    try {
      await h.backend.transaction(async ({ tx, streams }) => {
        await tx.query(
          "INSERT INTO sdk05_entry SELECT 'fragment-'||n,'payload-'||n,'fragments' FROM generate_series(1,1001) n",
        );
        streams(["User:fragments"]).track.entry(
          Array.from({ length: 1001 }, (_, i) => "fragment-" + (i + 1)),
        );
      });
      client = await open(join(dir, "db"), undefined, "fragments");
      const sizes = [];
      stop = client.watchSql('SELECT COUNT(*) AS n FROM "Entry"', [], (rows) =>
        sizes.push(rows[0].n),
      );
      await client.connect({ url: h.url, token: "fragments" });
      await client.bootstrap();
      await until(
        async () =>
          (await client.readSql('SELECT COUNT(*) AS n FROM "Entry"'))[0].n ===
          1001,
        "complete Bootstrap unit committed",
      );
      assert.ok(sizes.includes(1001));
      assert.ok(
        sizes.every((size) => size === 0 || size === 1001),
        "fragment published partial Model state",
      );
      const pulls = h.requests
        .filter((request) => request.route === "/sync/pull")
        .map((request) => JSON.parse(request.body));
      assert.ok(
        pulls.filter((request) => request.continuation !== null).length >= 2,
        "actual transport did not continue fragmented unit",
      );
      const state = (
        await client.readSql(
          "SELECT start_cursor,bootstrap_cursor FROM axton_store",
        )
      )[0];
      assert.equal(state.start_cursor, 1);
      assert.equal(state.bootstrap_cursor, 1);
    } finally {
      stop?.();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A10/A11/A12: parked storing Fetch before initial Stream start closes with its retained failure code", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-parked-fetch-"));
    let client, connection;
    try {
      h.holdHandshake();
      client = await open(join(dir, "db"), undefined, "parked-fetch");
      connection = await client.connect({ url: h.url, token: "parked-fetch" });
      await until(
        () => h.requests.some((request) => request.route === "/sync/handshake"),
        "initial handshake held",
      );
      const failure = assert.rejects(
        client.fetch.entry({ id: "not-held" }),
        (error) =>
          error.code === "fetch.unavailable" && error.execution === "unknown",
      );
      assert.equal(
        await client.fetch.entry({ id: "not-held" }, { store: false }),
        null,
      );
      await client.models.draft.create({ id: "local", text: "independent" });
      assert.equal(
        (await client.models.draft.get({ id: "local" })).text,
        "independent",
      );
      assert.equal(
        h.requests.filter(
          (request) =>
            request.route === "/sync/fetch" && JSON.parse(request.body).store,
        ).length,
        0,
      );
      await connection.close();
      await failure;
    } finally {
      h.releaseHandshake();
      await connection?.close();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A5/A9: first storing read waits for durable Stream start while store false remains independent", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-first-read-"));
    let client;
    try {
      await h.backend.transaction(async ({ tx, streams }) => {
        await tx.query(
          "INSERT INTO sdk05_entry VALUES('first-read','A','first')",
        );
        streams(["User:first"]).track.snapshot("first-read");
      });
      h.holdHandshake();
      client = await open(join(dir, "first"), undefined, "first");
      await client.connect({ url: h.url, token: "first" });
      await until(
        () => h.requests.some((request) => request.route === "/sync/handshake"),
        "initial handshake request waiting",
      );
      const storing = client.queries.peek({ id: "first-read" });
      assert.equal(
        (await client.queries.peek({ id: "first-read" }, { store: false }))
          .entry.text,
        "A",
      );
      assert.equal(
        h.requests.filter(
          (request) =>
            request.route === "/sync/actions" &&
            JSON.parse(request.body).store === true,
        ).length,
        0,
        "storing Query must not read before initial S/C",
      );
      assert.equal(
        await client.models.snapshot.get({ id: "first-read" }),
        null,
        "non-storing read wrote cache",
      );
      await h.backend.transaction(async ({ tx, invalidate }) => {
        await tx.query("UPDATE sdk05_entry SET text='B' WHERE id='first-read'");
        invalidate.snapshot("first-read");
      });
      h.releaseHandshake();
      assert.equal((await storing).entry.text, "B");
      assert.equal(
        (await client.models.snapshot.get({ id: "first-read" })).text,
        "B",
      );
    } finally {
      h.releaseHandshake();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A9: authoritative deletion wins over a genuinely earlier Query response", async () => {
    const h = await host(),
      dir = await mkdtemp(join(tmpdir(), "axton-sdk05-stale-"));
    let client;
    try {
      client = await open(join(dir, "db"), h.url, "stale");
      await client.bootstrap();
      const call = await publish(client, "stale-entry", "before");
      assert.equal((await call.wait()).error, null);
      h.holdReads();
      const delayed = client.queries.find({ id: "stale-entry" });
      await until(
        () => h.heldReadCount === 1,
        "server completed old Query snapshot",
      );
      await h.backend.transaction(async ({ tx, invalidate }) => {
        await tx.query("DELETE FROM sdk05_entry WHERE id='stale-entry'");
        invalidate.entry("stale-entry");
      });
      await until(
        async () =>
          (await client.models.entry.get({ id: "stale-entry" })) === null,
        "current authoritative absence",
      );
      h.releaseReads();
      assert.equal(
        (await delayed).entry.text,
        "before",
        "caller still receives its own earlier snapshot",
      );
      assert.equal(
        await client.models.entry.get({ id: "stale-entry" }),
        null,
        "ordinary stale snapshot resurrected current authority",
      );
    } finally {
      h.releaseReads();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A9/A12: generated Query handler explicitly tracks through its authenticated Stream", async () => {
    const h = await host();
    const dir = await mkdtemp(join(tmpdir(), "axton-sdk05-query-track-"));
    let client;
    try {
      await h.backend.transaction(async ({ tx }) => {
        await tx.query(
          "INSERT INTO sdk05_entry VALUES('explicit-track-query','tracked deliberately','query-track')",
        );
      });
      client = await open(join(dir, "db"), h.url, "query-track");
      await client.bootstrap();
      const result = await client.queries.find({ id: "explicit-track-query" });
      assert.equal(result.entry.text, "tracked deliberately");
      const rows = (
        await h.pool.query(
          "SELECT s.stream FROM axton_stream_record s JOIN axton_record r ON r.id=s.record_id WHERE r.identity->>'id'='explicit-track-query'",
        )
      ).rows;
      assert.deepEqual(rows, [{ stream: "User:query-track" }]);
      await h.backend.transaction(async ({ tx, invalidate }) => {
        await tx.query(
          "UPDATE sdk05_entry SET text='later authority' WHERE id='explicit-track-query'",
        );
        invalidate.entry("explicit-track-query");
      });
      await until(
        async () =>
          (await client.models.entry.get({ id: "explicit-track-query" }))
            ?.text === "later authority",
        "explicit Query holding receives later authority",
      );
    } finally {
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A14: desired schema activates only after owned complete materialization and includes newly selected held keys", async () => {
    let h = await host();
    const dir = await mkdtemp(join(tmpdir(), "axton-sdk05-schema-"));
    let client;
    try {
      await h.backend.transaction(async ({ tx, streams }) => {
        await tx.query(
          "INSERT INTO sdk05_entry VALUES('schema-entry','old entry','schema'),('schema-snapshot','old snapshot','schema')",
        );
        streams(["User:schema"]).track.entry("schema-entry");
        streams(["User:schema"]).track.snapshot("schema-snapshot");
      });
      const path = join(dir, "db");
      client = await open(path, h.url, "schema");
      await client.bootstrap();
      assert.equal(
        (await client.queries.peek({ id: "schema-snapshot" })).entry.text,
        "old snapshot",
      );
      const before = (
        await client.readSql(
          "SELECT materialization,start_cursor,bootstrap_cursor,cursor FROM axton_store",
        )
      )[0];
      await client.close();
      client = undefined;
      await h.backend.transaction(async ({ tx, invalidate }) => {
        await tx.query(
          "UPDATE sdk05_entry SET text='new content' WHERE owner='schema'",
        );
        invalidate.entry("schema-entry");
        invalidate.snapshot("schema-snapshot");
      });
      await h.close();
      const config = JSON.parse(
        await readFile(new URL("./backend.json", import.meta.url), "utf8"),
      );
      h = await host({
        rollover: true,
        materializations: {
          [before.materialization]: {
            schema: config.schema,
            projectionGeneration: "1",
          },
        },
      });
      h.holdSchema();
      client = await RolloverClient.open({
        path,
        stream: "User:schema",
        connection: { url: h.url, token: "schema" },
      });
      await until(
        () => h.heldSchemaCount > 0,
        "owned schema materialization response",
      );
      const pending = (
        await client.readSql(
          "SELECT materialization,desired_materialization,bootstrap_cursor FROM axton_store",
        )
      )[0];
      assert.equal(pending.materialization, before.materialization);
      assert.notEqual(pending.desired_materialization, before.materialization);
      assert.equal(pending.bootstrap_cursor, before.bootstrap_cursor);
      const owned = h.requests
        .filter((request) => request.route === "/sync/materialize")
        .map((request) => JSON.parse(request.body));
      assert.ok(
        owned.some(
          (request) =>
            request.owner.kind === "schema" &&
            request.owner.previousMaterialization === before.materialization &&
            request.models.Snapshot === 1,
        ),
        "new Bootstrap Model missing owned schema request",
      );
      h.releaseSchema();
      await until(async () => {
        const row = (
          await client.readSql(
            "SELECT materialization,desired_materialization FROM axton_store",
          )
        )[0];
        return row.materialization === row.desired_materialization;
      }, "desired schema enabled");
      assert.equal(
        (await client.models.entry.get({ id: "schema-entry" })).text,
        "new content",
      );
      assert.equal(
        (await client.models.snapshot.get({ id: "schema-snapshot" })).text,
        "new content",
      );
      assert.equal(
        (await client.readSql("SELECT bootstrap_cursor FROM axton_store"))[0]
          .bootstrap_cursor,
        before.bootstrap_cursor,
        "owned schema data fabricated range coverage",
      );
    } finally {
      h.releaseSchema();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A14: retained version 1 and desired version 2 re-materialize a new field at the same cursor", async () => {
    let h = await host();
    const dir = await mkdtemp(join(tmpdir(), "axton-sdk05-versioned-"));
    let client;
    try {
      await h.backend.transaction(async ({ tx, streams }) => {
        await tx.query(
          "INSERT INTO sdk05_entry VALUES('versioned-entry','same row','versioned')",
        );
        streams(["User:versioned"]).track.entry("versioned-entry");
      });
      const path = join(dir, "db");
      client = await open(path, h.url, "versioned");
      await client.bootstrap();
      const before = (
        await client.readSql(
          "SELECT materialization,bootstrap_cursor,cursor FROM axton_store",
        )
      )[0];
      const evidence = JSON.parse(
        (
          await client.readSql(
            "SELECT evidence FROM axton_authority WHERE model='Entry'",
          )
        )[0].evidence,
      );
      assert.equal(
        (await client.models.entry.get({ id: "versioned-entry" })).text,
        "same row",
      );
      await client.close();
      client = undefined;
      await h.close();
      const config = JSON.parse(
        await readFile(new URL("./backend.json", import.meta.url), "utf8"),
      );
      h = await host({
        versioned: true,
        materializations: {
          [before.materialization]: {
            schema: config.schema,
            projectionGeneration: "1",
          },
        },
      });
      h.holdSchema();
      client = await VersionedClient.open({
        path,
        stream: "User:versioned",
        connection: { url: h.url, token: "versioned" },
      });
      await until(
        () => h.heldSchemaCount > 0,
        "versioned owned materialization held",
      );
      const pending = (
        await client.readSql(
          "SELECT materialization,desired_materialization FROM axton_store",
        )
      )[0];
      assert.equal(pending.materialization, before.materialization);
      assert.notEqual(pending.desired_materialization, before.materialization);
      const owned = h.requests
        .filter((request) => request.route === "/sync/materialize")
        .map((request) => JSON.parse(request.body));
      assert.ok(
        owned.some(
          (request) =>
            request.owner.kind === "schema" &&
            request.owner.previousMaterialization === before.materialization &&
            request.keys.some(
              (key) =>
                key.model === "Entry" && key.identity.id === "versioned-entry",
            ),
        ),
      );
      h.releaseSchema();
      await until(async () => {
        const row = (
          await client.readSql(
            "SELECT materialization,desired_materialization FROM axton_store",
          )
        )[0];
        return row.materialization === row.desired_materialization;
      }, "versioned schema activated");
      const row = await client.models.entry.get({ id: "versioned-entry" });
      assert.equal(row.text, "same row");
      assert.equal(row.note, "new schema");
      const after = JSON.parse(
        (
          await client.readSql(
            "SELECT evidence FROM axton_authority WHERE model='Entry'",
          )
        )[0].evidence,
      );
      assert.equal(
        after.membership.cursor,
        evidence.membership.cursor,
        "field rematerialization restamped unchanged record",
      );
      assert.equal(
        (await client.readSql("SELECT bootstrap_cursor FROM axton_store"))[0]
          .bootstrap_cursor,
        before.bootstrap_cursor,
        "owned versioned rematerialization fabricated Bootstrap coverage",
      );
      assert.equal(
        (
          await h.pool.query(
            "SELECT head FROM axton_stream WHERE stream='User:versioned'",
          )
        ).rows[0].head,
        String(before.cursor),
      );
    } finally {
      h.releaseSchema();
      await client?.close();
      await h.close();
      await rm(dir, { recursive: true, force: true });
    }
  });
  test("A11/A12: generated Dart crosses HTTP/WebSocket, SQLite and PostgreSQL", async () => {
    assert.ok(process.env.AXTON_DART, "Dart must participate in the host gate");
    const h = await host();
    try {
      const result = await promisify(execFile)(
        process.env.AXTON_DART,
        ["run", "client.dart", h.url],
        { cwd: new URL(".", import.meta.url).pathname, timeout: 60000 },
      );
      assert.match(result.stdout, /Dart protocol5 actual host: PASS/);
    } finally {
      await h.close();
    }
  });
}
