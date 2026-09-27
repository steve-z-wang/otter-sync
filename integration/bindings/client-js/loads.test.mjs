// Native Loads through the real native runtime (#173): Rust persists every
// job, batches its pages, decides once reuse and projects its status; the host
// executes the `load` HTTP effect and keeps only language handles. This file
// drives the handle API end to end over a scripted backend.
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { AxtonReport } from "../../../packages/client-js/connection.mts";
import { LoadError } from "../../../packages/client-js/loads.mts";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-js/transaction.mts";
import {
  PROJECT,
  args,
  backend,
  deferred,
  failed,
  page,
  schema,
  until,
} from "./loads-harness.mjs";

const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
async function harness(body, { Tx = Transaction } = {}) {
  const directory = await mkdtemp(join(tmpdir(), "axton-loads-"));
  const path = join(directory, "db");
  const { server, connection } = backend();
  const Client = createClient(native, Tx, connection);
  const open = () => Client.open({ path, schema });
  const client = await open();
  const clients = [client];
  try {
    await body({
      client,
      server,
      open: async () => {
        const opened = await open();
        clients.push(opened);
        return opened;
      },
      connect: (c = client, options = {}) =>
        c.connect({ url: "http://unused", token: "token" }, options),
    });
  } finally {
    for (const c of clients) await c.close();
    await rm(directory, { recursive: true, force: true });
  }
}
const code = (expected) => (error) =>
  error instanceof LoadError && error.code === expected;

test("a start is accepted offline and its wait resolves after the final page committed", async () => {
  await harness(async ({ client, server, connect }) => {
    const job = await client.startLoad("Entries", 1, args);
    assert.match(job.id, /^[0-9a-f-]{36}$/);
    assert.equal(job.status.id, job.id);
    assert.equal(job.status.name, "Entries");
    assert.equal(job.status.version, 1);
    assert.equal(job.status.phase, "waiting", "offline work waits");
    assert.equal(job.status.pages, 0);
    assert.equal(job.status.error, null);
    assert.ok(Object.isFrozen(job.status));
    const seen = [];
    const stop = job.watch((status) => seen.push(status));
    assert.deepEqual(
      seen.map((s) => s.phase),
      ["waiting"],
      "the current status at once",
    );
    // Ordinary starts never share: the same args make another job.
    const other = await client.startLoad("Entries", 1, args);
    assert.notEqual(other.id, job.id);
    // Two pages: the continuation goes out as the next page's `continuation`.
    server.answer = (intent) =>
      intent.continuation === null
        ? page(intent, [["a", "A"]], { state: { after: "a" } })
        : page(intent, [["b", "B"]]);
    let settled = false;
    const waited = job.wait().then(() => (settled = true));
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(settled, false, "nothing loads while offline");
    assert.equal(server.batches.length, 0);
    const connection = await connect();
    await waited;
    await other.wait();
    assert.equal(job.status.phase, "complete");
    assert.equal(job.status.pages, 2);
    assert.deepEqual(await client.read("Entry", { id: "b" }), {
      id: "b",
      text: "B",
      note: null,
    });
    const mine = server.intents.filter((i) => i.loadId === job.id);
    assert.deepEqual(
      mine.map((i) => i.continuation),
      [null, { state: { after: "a" } }],
    );
    assert.deepEqual(mine[0].args, args);
    assert.equal(mine[0].name, "Entries");
    assert.notEqual(mine[0].callId, mine[1].callId, "each page has its call");
    for (const intent of mine) {
      assert.equal("once" in intent, false, "options never reach the wire");
      assert.equal("refresh" in intent, false);
    }
    assert.equal(seen.at(-1).phase, "complete");
    assert.ok(seen.some((s) => s.phase === "loading"));
    for (let i = 1; i < seen.length; i++)
      assert.notDeepEqual(seen[i], seen[i - 1], "only distinct snapshots");
    stop();
    // A stopped listener hears nothing more while its own job still changes:
    // the batch is held until the listener is stopped.
    const gate = deferred();
    server.gate = gate;
    const third = await client.startLoad("Entries", 1, args);
    const heard = [];
    const stopThird = third.watch((status) => heard.push(status.phase));
    assert.deepEqual(heard, [third.status.phase], "the current status at once");
    assert.notEqual(heard[0], "complete");
    stopThird();
    server.gate = undefined;
    gate.resolve();
    await third.wait();
    assert.equal(third.status.phase, "complete", "the handle itself moved on");
    assert.equal(heard.length, 1, "a stopped listener hears nothing");
    // A complete job waits locally.
    await job.wait();
    await connection.close();
  });
});

test("once shares active and complete jobs, refresh joins or restarts, invalidation is offline", async () => {
  await harness(async ({ client, server, connect }) => {
    const once = (options = {}) =>
      client.startLoad("Entries", 1, args, { once: true, ...options });
    const first = await once();
    const joined = await once();
    assert.equal(joined.id, first.id, "an active once job is shared");
    assert.notStrictEqual(joined, first, "each caller has its own handle");
    // Canonical key: the same UUID in another case is the same arguments.
    const upper = await client.startLoad(
      "Entries",
      1,
      { projectId: PROJECT.toUpperCase(), since: null },
      { once: true },
    );
    assert.equal(upper.id, first.id);
    // Refresh of an active job joins it; it never starts a duplicate.
    const refreshedActive = await once({ refresh: true });
    assert.equal(refreshedActive.id, first.id);
    server.gate = deferred();
    const connection = await connect();
    await until(() => server.batches.length === 1, "the first batch");
    assert.equal(
      server.intents.filter((i) => i.loadId === first.id).length,
      1,
      "one request carries the shared job",
    );
    server.gate.resolve();
    server.gate = undefined;
    await Promise.all([first.wait(), joined.wait(), refreshedActive.wait()]);
    const requests = server.batches.length;
    // A complete hit applies nothing and asks for nothing, offline too.
    await connection.close();
    const hit = await once();
    assert.equal(hit.id, first.id);
    assert.equal(hit.status.phase, "complete");
    assert.equal(hit.status.pages, 1);
    await hit.wait();
    assert.equal(server.batches.length, requests);
    // Refresh of a complete job starts again from the first page, as new work.
    const refreshed = await once({ refresh: true });
    assert.notEqual(refreshed.id, first.id);
    assert.equal(refreshed.status.pages, 0);
    assert.equal((await once()).id, refreshed.id, "the mapping moved");
    assert.equal(
      first.status.phase,
      "complete",
      "the old job keeps its completion",
    );
    // Invalidation needs no connection and cancels nothing.
    await client.invalidateLoad("Entries", args);
    const fresh = await once();
    assert.notEqual(fresh.id, refreshed.id);
    assert.equal(
      refreshed.status.phase,
      "waiting",
      "the invalidated job runs on",
    );
    // A no-argument Load reuses by the empty argument set.
    const recent = await client.startLoad("Recent", 1, {}, { once: true });
    assert.equal(
      (await client.startLoad("Recent", 1, {}, { once: true })).id,
      recent.id,
    );
    await client.invalidateLoad("Recent", {});
    assert.notEqual(
      (await client.startLoad("Recent", 1, {}, { once: true })).id,
      recent.id,
    );
    assert.equal(server.batches.length, requests, "all of it offline");
    // An invalidated job that completes later cannot restore its mapping.
    const again = await connect();
    await Promise.all([refreshed.wait(), fresh.wait()]);
    assert.equal((await once()).id, fresh.id);
    await again.close();
  });
});

test("invalid options and arguments are refused before any job exists", async () => {
  await harness(async ({ client }) => {
    const before = (await client.listLoads()).length;
    for (const options of [
      { refresh: true },
      { once: "yes" },
      { once: true, refresh: 1 },
      { store: false },
      { once: true, cursor: "x" },
      "once",
      null,
    ])
      await assert.rejects(
        client.startLoad("Entries", 1, args, options),
        code("load.invalid_options"),
        JSON.stringify(options),
      );
    await assert.rejects(client.startLoad("Nope", 1, {}), code("load.unknown"));
    // Arguments the Load's inputs refuse fail with a coded reason.
    await assert.rejects(
      client.startLoad("Entries", 1, { projectId: "not-a-uuid", since: null }),
      (error) =>
        code("load.invalid_args")(error) && /invalid UUID/.test(error.message),
    );
    assert.equal((await client.listLoads()).length, before);
    await assert.rejects(
      client.listLoads({ limit: 0 }),
      code("load.invalid_options"),
    );
    await assert.rejects(
      client.listLoads({ limit: 101 }),
      code("load.invalid_options"),
    );
    await assert.rejects(
      client.transaction(() => client.startLoad("Entries", 1, args)),
      code("transaction_active"),
    );
  });
});

test("failure, retry, cancel and forget keep once ownership with the job that holds it", async () => {
  await harness(async ({ client, server, connect }) => {
    server.answer = (intent) => failed(intent, "handler.failed");
    const doomed = await client.startLoad("Entries", 1, args, { once: true });
    const connection = await connect();
    await assert.rejects(doomed.wait(), (error) => {
      assert.ok(error instanceof LoadError);
      assert.equal(error.code, "handler.failed");
      assert.equal(error.message, "refused");
      return true;
    });
    assert.equal(doomed.status.phase, "failed");
    assert.deepEqual(doomed.status.error, {
      code: "handler.failed",
      message: "refused",
    });
    // A failed once job is returned as failed, with no hidden request.
    const requests = server.batches.length;
    const reused = await client.startLoad("Entries", 1, args, { once: true });
    assert.equal(reused.id, doomed.id);
    assert.equal(reused.status.phase, "failed");
    await assert.rejects(reused.wait(), code("handler.failed"));
    assert.equal(server.batches.length, requests);
    // Explicit retry reads again under a new call ID.
    server.answer = (intent) => page(intent, [["r", "R"]]);
    await reused.retry();
    await doomed.wait();
    assert.equal(doomed.status.phase, "complete");
    const calls = server.intents.filter((i) => i.loadId === doomed.id);
    assert.equal(calls.length, 2);
    assert.notEqual(calls[0].callId, calls[1].callId);
    await assert.rejects(doomed.retry(), code("load.not_retryable"));
    // Cancel settles the waiter; cancelling a complete job changes nothing.
    await connection.close();
    const pending = await client.startLoad("Entries", 1, args);
    const waiting = pending.wait();
    await pending.cancel();
    await assert.rejects(waiting, code("load.cancelled"));
    assert.equal(pending.status.phase, "cancelled");
    await pending.cancel();
    await doomed.cancel();
    assert.equal(doomed.status.phase, "complete");
    // Refresh moves the mapping; the old job's cleanup cannot remove it.
    const replacement = await client.startLoad("Entries", 1, args, {
      once: true,
      refresh: true,
    });
    assert.notEqual(replacement.id, doomed.id);
    await doomed.forget();
    assert.equal(
      (await client.startLoad("Entries", 1, args, { once: true })).id,
      replacement.id,
      "forgetting the old job kept the newer mapping",
    );
    assert.equal(await client.getLoad(doomed.id), null);
    await assert.rejects(doomed.wait(), code("load.not_found"));
    await assert.rejects(doomed.cancel(), code("load.not_found"));
    // Forget refuses active work; cancelling it removes its own mapping.
    await assert.rejects(replacement.forget(), code("load.not_terminal"));
    await replacement.cancel();
    const after = await client.startLoad("Entries", 1, args, { once: true });
    assert.notEqual(after.id, replacement.id);
  });
});

test("handles reattach by ID, list newest first, and dispose releases only one observer", async () => {
  await harness(async ({ client, server, connect }) => {
    const job = await client.startLoad("Entries", 1, args);
    const second = await client.startLoad("Recent", 1, {});
    const restored = await client.getLoad(job.id);
    assert.notStrictEqual(restored, job);
    assert.equal(restored.id, job.id);
    assert.deepEqual(restored.status, job.status);
    assert.equal(
      await client.getLoad("00000000-0000-4000-8000-000000000000"),
      null,
    );
    const listed = await client.listLoads();
    assert.deepEqual(
      listed.map((s) => s.id),
      [second.id, job.id],
      "newest first",
    );
    assert.equal((await client.listLoads({ limit: 1 })).length, 1);
    assert.ok(Object.isFrozen(listed[0]));
    const heard = { job: [], restored: [] };
    job.watch((s) => heard.job.push(s.phase));
    restored.watch((s) => heard.restored.push(s.phase));
    restored.dispose();
    restored.dispose();
    const connection = await connect();
    await job.wait();
    assert.equal(job.status.phase, "complete", "dispose cancelled nothing");
    assert.equal(heard.job.at(-1), "complete");
    assert.deepEqual(
      heard.restored,
      ["waiting"],
      "a disposed handle hears nothing",
    );
    assert.equal(
      restored.status.phase,
      "waiting",
      "its last status stays readable",
    );
    const late = [];
    restored.watch((s) => late.push(s.phase));
    assert.deepEqual(
      late,
      ["waiting"],
      "a disposed handle delivers its last status",
    );
    // Management through a disposed handle still names the job.
    await restored.wait();
    await second.wait();
    await connection.close();
    assert.ok(server.batches.length >= 1);
  });
});

test("close settles every waiter, keeps the work, and terminal status is readable after reopen", async () => {
  await harness(async ({ client, open, connect }) => {
    const job = await client.startLoad("Entries", 1, args);
    const seen = [];
    job.watch((s) => seen.push(s.phase));
    const waits = [
      job.wait(),
      job.wait(),
      (await client.getLoad(job.id)).wait(),
    ];
    const settled = waits.map((waited) =>
      waited.then(
        () => assert.fail("a waiter resolved"),
        (error) => assert.ok(code("client_closed")(error), String(error)),
      ),
    );
    await client.close();
    await Promise.all(settled);
    assert.equal(job.status.phase, "waiting", "the last status stays readable");
    await assert.rejects(job.wait(), code("client_closed"));
    const reopened = await open();
    const restored = await reopened.getLoad(job.id);
    assert.equal(restored.status.phase, "waiting", "the job survived close");
    const connection = await connect(reopened);
    await restored.wait();
    await connection.close();
    await reopened.close();
    const third = await open();
    const terminal = await third.getLoad(job.id);
    assert.equal(terminal.status.phase, "complete");
    assert.equal(terminal.status.pages, 1);
    await terminal.wait();
    assert.deepEqual(await third.read("Entry", { id: job.id.slice(0, 8) }), {
      id: job.id.slice(0, 8),
      text: "loaded",
      note: null,
    });
  });
});

test("a page refused for a record reports that record through onError", async () => {
  await harness(async ({ client, server, connect }) => {
    server.answer = (intent) => {
      const item = page(intent, [
        ["a", "A"],
        ["b", "B"],
      ]);
      item.records[1].state = { text: 5, note: null };
      return item;
    };
    const job = await client.startLoad("Entries", 1, args);
    const errors = [];
    const connection = await connect(client, {
      onError: (error) => errors.push(error),
    });
    await assert.rejects(job.wait(), code("load.store_failed"));
    const reports = errors.filter((error) => error instanceof AxtonReport);
    assert.equal(reports.length, 1, String(errors));
    assert.equal(reports[0].kind, "skipped");
    assert.equal(reports[0].model, "Entry");
    assert.deepEqual(reports[0].identity, { id: "b" });
    assert.equal(await client.read("Entry", { id: "a" }), null);
    await connection.close();
  });
});

test("a refused credential refresh fails the batch load.unauthorized", async () => {
  await harness(async ({ client, server, connect }) => {
    server.error = Object.assign(Error("expired"), { status: 401 });
    let refreshes = 0;
    const job = await client.startLoad("Entries", 1, args);
    const connection = await connect(client, {
      refreshAuth: async () => {
        refreshes++;
        throw Object.assign(Error("forbidden"), { status: 403 });
      },
    });
    await assert.rejects(job.wait(), code("load.unauthorized"));
    assert.equal(refreshes, 1);
    assert.equal(job.status.phase, "failed");
    assert.equal(job.status.error.code, "load.unauthorized");
    await connection.close();
  });
});

test("a rebuild reports the Load jobs it left behind", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-loads-rebuild-"));
  const path = join(directory, "db");
  const Client = createClient(native, Transaction, () =>
    backend().connection(),
  );
  try {
    const client = await Client.open({ path, schema });
    const job = await client.startLoad("Entries", 1, args);
    await client.close();
    // A required field without a default is an incompatible read contract.
    const changed = structuredClone(schema);
    const extra = {
      name: "extra",
      nullable: false,
      type: { kind: "scalar", name: "string" },
    };
    changed.models[0].fields = [...changed.models[0].fields, extra];
    changed.resultModels[0].fields = [...changed.resultModels[0].fields, extra];
    const rebuilt = await Client.open({
      path,
      schema: changed,
      discardPending: true,
    });
    try {
      const report = (await rebuilt.syncState()).schema.lastRebuild;
      assert.deepEqual(report.abandonedLoads, [job.id]);
      assert.equal(await rebuilt.getLoad(job.id), null);
    } finally {
      await rebuilt.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a rebuild ends live handles and parked waiters with load.schema_changed", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axton-loads-rebuild-"));
  const path = join(directory, "db");
  const Client = createClient(native, Transaction, () =>
    backend().connection(),
  );
  const changed = structuredClone(schema);
  const extra = {
    name: "extra",
    nullable: false,
    type: { kind: "scalar", name: "string" },
  };
  changed.models[0].fields = [...changed.models[0].fields, extra];
  changed.resultModels[0].fields = [...changed.resultModels[0].fields, extra];
  try {
    // An unsent Mutation keeps the old file open behind a pending rebuild.
    const old = await Client.open({ path, schema });
    const started = await old.startLoad("Entries", 1, args);
    await old.transaction((tx) =>
      tx.direct({
        model: "Entry",
        op: "create",
        identity: { id: "e" },
        values: { text: "A", note: null },
      }),
    );
    await old.mutate({
      name: "Edit",
      operations: [
        {
          model: "Entry",
          op: "update",
          identity: { id: "e" },
          values: { text: "B" },
        },
      ],
    });
    await old.close();
    const client = await Client.open({ path, schema: changed });
    try {
      assert.notEqual((await client.syncState()).schema.pending, null);
      const job = await client.getLoad(started.id);
      assert.equal(job.status.phase, "waiting");
      const seen = [];
      job.watch((status) => seen.push(status));
      let settled;
      const waiting = job.wait().then(
        () => (settled = "resolved"),
        (error) => {
          settled = error;
        },
      );
      await new Promise((resolve) => setTimeout(resolve, 10));
      assert.equal(settled, undefined, "the waiter is parked");
      const report = await client.rebuild({ discardPending: true });
      assert.deepEqual(report.abandonedLoads, [started.id]);
      await waiting;
      assert.ok(code("load.schema_changed")(settled), String(settled));
      // The handle's last status says why it ended, and nothing follows.
      assert.equal(job.status.phase, "failed");
      assert.equal(job.status.error.code, "load.schema_changed");
      assert.equal(seen.at(-1), job.status);
      for (const manage of ["wait", "cancel", "retry", "forget"])
        await assert.rejects(
          job[manage](),
          code("load.schema_changed"),
          manage,
        );
      assert.equal(await client.getLoad(started.id), null);
      assert.deepEqual(await client.listLoads(), []);
    } finally {
      await client.close();
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("a disposed handle is not retained by the client", async () => {
  const module = fileURLToPath(new URL("./loads-harness.mjs", import.meta.url));
  const source = `
    import { createClient } from ${JSON.stringify(fileURLToPath(new URL("../../../packages/client-js/runtime.mts", import.meta.url)))};
    import { Transaction } from ${JSON.stringify(fileURLToPath(new URL("../../../packages/client-js/transaction.mts", import.meta.url)))};
    import { schema, args, backend } from ${JSON.stringify(module)};
    import { createRequire } from "node:module";
    import { mkdtemp } from "node:fs/promises";
    import { tmpdir } from "node:os";
    import { join } from "node:path";
    const native = createRequire(${JSON.stringify(module)})("../../../bindings/node/axton-node.node");
    const Client = createClient(native, Transaction, () => backend().connection());
    const client = await Client.open({ path: join(await mkdtemp(join(tmpdir(), "axton-loads-gc-")), "db"), schema });
    let disposed = await client.startLoad("Entries", 1, args);
    disposed.watch(() => {});
    const collected = new WeakRef(disposed);
    disposed.dispose();
    disposed = undefined;
    let kept = await client.startLoad("Entries", 1, args);
    const retained = new WeakRef(kept);
    kept = undefined;
    for (let i = 0; i < 8; i++) { await new Promise(setImmediate); global.gc(); }
    process.stdout.write(JSON.stringify({ disposed: collected.deref() === undefined, observed: retained.deref() !== undefined }));
    await client.close();
  `;
  const child = spawn(
    process.execPath,
    ["--expose-gc", "--no-warnings", "--input-type=module", "-e", source],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => (stdout += chunk));
  child.stderr.on("data", (chunk) => (stderr += chunk));
  const exit = await new Promise((resolve) => child.on("close", resolve));
  assert.equal(exit, 0, stderr);
  // A handle still observing is routed until dispose or close; a disposed one
  // is released at once.
  assert.deepEqual(JSON.parse(stdout), { disposed: true, observed: true });
});
