// Current named Actions through generated SDK, native actor, HTTP/WS and PG.
import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { setImmediate } from "node:timers/promises";
import { GeneratedClient } from "./client.ts";
import { createFixture } from "./backend-fixture.ts";
import { createProxy, type Proxy } from "../e2e/proxy.mts";
const fixture = await createFixture();
let proxy: Proxy;
before(async () => {
  await fixture.initialize();
  proxy = await createProxy((await fixture.listen()).url);
});
after(async () => {
  await proxy.close();
  await fixture.close();
});
const connection = (viewer = "alice") => ({
  url: proxy.url,
  token: viewer,
});
async function wait(predicate: () => Promise<boolean>, label: string) {
  const end = Date.now() + 20000;
  while (Date.now() < end) {
    if (await predicate()) return;
    await setImmediate();
  }
  throw Error(`Timed out: ${label}`);
}
async function session(viewer = "alice") {
  const dir = await mkdtemp(join(tmpdir(), "axton-action-"));
  const path = join(dir, "db");
  const open = () =>
    GeneratedClient.open({
      path,
      stream: `User:${viewer}`,
      connection: connection(viewer),
    });
  let client = await open();
  return {
    get client() {
      return client;
    },
    dir,
    path,
    async reopen() {
      await client.close();
      client = await open();
    },
    async close() {
      await client.close();
      await rm(dir, { recursive: true, force: true });
    },
  };
}
const action = (name: string) => (x: { path: string; body: string }) =>
  x.path === "/sync/mutations" &&
  JSON.parse(x.body).mutations.some(
    (mutation: { name: string }) => mutation.name === name,
  );
const draft = (id: string) => ({
  id,
  title: `draft ${id}`,
  body: `body ${id}`,
});
const publishInput = (id: string, source: { title: string; body: string }) => ({
  entry: { id, title: source.title, body: source.body },
  media: [
    { id: `${id}-m1`, entryId: id, url: "one.jpg" },
    { id: `${id}-m2`, entryId: id, url: "two.jpg" },
  ],
  placement: { id: `${id}-p`, entryId: id, journal: "daily", position: 1 },
});

test("typed named Mutation optimism and no-output settlement survive offline reopen; scalar-only Calls have durable outcomes", async () => {
  const s = await session();
  try {
    await s.client.bootstrap();
    await s.client.connection!.pause();
    await s.client.mutations.addTodo({
      todo: { id: "main", title: " first " },
    });
    assert.equal(
      (await s.client.models.todo.get({ id: "main" }))?.title,
      " first ",
    );
    await s.reopen();
    await wait(
      async () => (await s.client.syncState()).pending === 0,
      "reopened create",
    );
    assert.equal(
      (await s.client.models.todo.get({ id: "main" }))?.title,
      "first",
    );
    const read = await s.client.queries.searchTodos({ query: "first" });
    assert.deepEqual(read.labels, ["main"]);
    assert.equal(read.first?.title, "first");
    const email = await s.client.mutations.sendEmail({
      to: "test@example.invalid",
      subject: "once",
      body: "message",
    });
    const outcome = await email.wait();
    assert.equal(outcome.error, null);
    assert.match(outcome.result!.messageId, /^\d+$/);
    assert.equal(
      (
        await fixture.pool.query(
          "SELECT count(*)::int AS n FROM action_e2e_outbox WHERE subject=$1",
          ["once"],
        )
      ).rows[0].n,
      1,
    );
  } finally {
    await s.close();
  }
});

test("lost accepted scalar result retries exact frozen intent after reopen without repeating side effect", async () => {
  const s = await session();
  try {
    await s.client.bootstrap();
    const hold = proxy.holdResponse(action("SendEmail"));
    await s.client.mutations.sendEmail({
      to: "test@example.invalid",
      subject: "lost",
      body: "accepted",
    });
    const exchange = await hold.arrived;
    const frozen = JSON.parse(exchange.body);
    proxy.down();
    hold.release();
    await s.client.close();
    proxy.up();
    await s.reopen();
    await wait(
      async () => (await s.client.syncState()).pending === 0,
      "saved result replay",
    );
    const requests = proxy
      .requests("/sync/mutations")
      .filter(
        (x) =>
          x.request.batchId === frozen.batchId &&
          x.request.storeId === frozen.storeId,
      );
    assert.ok(requests.length >= 2);
    assert.deepEqual(requests.at(-1)!.request, frozen);
    assert.equal(
      (
        await fixture.pool.query(
          "SELECT count(*)::int AS n FROM action_e2e_outbox WHERE subject='lost'",
        )
      ).rows[0].n,
      1,
    );
  } finally {
    proxy.releaseAll();
    proxy.up();
    await s.close();
  }
});

test("Query/Fetch store policies preserve invocation snapshot separately from current protected authority", async () => {
  const s = await session();
  try {
    await (
      await s.client.mutations.addTodo({
        todo: { id: "policy-a", title: "policy initial" },
      })
    ).wait();
    const hold = proxy.holdResponse((x) => x.path === "/sync/actions" && JSON.parse(x.body).invocation?.name === "SearchTodos");
    const reading = s.client.queries.searchTodos(
      { query: "policy" },
      { store: true },
    );
    reading.catch(() => {});
    await hold.arrived;
    const editing = await s.client.mutations.updateTodo({
      todo: { id: "policy-a", title: "policy newer" },
    });
    assert.equal((await editing.wait()).error, null);
    hold.release();
    assert.equal((await reading).todos[0]!.title, "policy initial");
    assert.equal(
      (await s.client.models.todo.get({ id: "policy-a" }))?.title,
      "policy newer",
    );
    await fixture.pool.query(
      "INSERT INTO action_e2e_todo VALUES('policy-unheld','policy unheld')",
    );
    assert.equal(
      (await s.client.fetch.todo({ id: "policy-unheld" }, { store: false }))
        ?.title,
      "policy unheld",
    );
    assert.equal(await s.client.models.todo.get({ id: "policy-unheld" }), null);
    await s.client.fetch.todo({ id: "policy-unheld" });
    assert.equal(
      (await s.client.models.todo.get({ id: "policy-unheld" }))?.title,
      "policy unheld",
    );
    assert.equal(await s.client.fetch.todo({ id: "absent" }), null);
  } finally {
    proxy.releaseAll();
    await s.close();
  }
});

test("A-returns-B receipt reconciles A but preserves the immutable B business snapshot during later local work", async () => {
  const s = await session();
  try {
    await (
      await s.client.mutations.addTodo({ todo: { id: "result-a", title: "A" } })
    ).wait();
    await (
      await s.client.mutations.addTodo({ todo: { id: "result-b", title: "B" } })
    ).wait();
    const hold = proxy.holdResponse(action("EditAndShow"));
    const call = await s.client.mutations.editAndShow({
      todo: { id: "result-a", title: " A1 " },
      shown: "result-b",
    });
    await hold.arrived;
    await s.client.transaction(async (tx) => {
      await tx.models.todo.update(
        { id: "result-b" },
        { title: "later direct B" },
      );
    });
    hold.release();
    const output = await call.wait();
    assert.equal(output.result?.todo.title, "B");
    assert.equal(
      (await s.client.models.todo.get({ id: "result-a" }))?.title,
      "A1",
    );
    assert.equal(
      (await s.client.models.todo.get({ id: "result-b" }))?.title,
      "later direct B",
    );
  } finally {
    proxy.releaseAll();
    await s.close();
  }
});

test("real PostgreSQL serialization exhaustion leaves a Call durable; resend eventually accepts", async () => {
  const s = await session();
  try {
    await (
      await s.client.mutations.addTodo({
        todo: { id: "conflict", title: "before" },
      })
    ).wait();
    fixture.conflictUpdates = 5;
    const call = await s.client.mutations.updateTodo({
      todo: { id: "conflict", title: " after " },
    });
    assert.equal((await call.wait()).error, null);
    assert.equal(fixture.conflictUpdates, 0);
    assert.equal(
      (await s.client.models.todo.get({ id: "conflict" }))?.title,
      "after",
    );
    assert.equal((await s.client.syncState()).pending, 0);
    const requests = proxy
      .requests("/sync/mutations")
      .filter((x) =>
        x.request.mutations.some(
          (mutation: {
            name: string;
            operations: { identity: { id?: string } }[];
          }) =>
            mutation.name === "UpdateTodo" &&
            mutation.operations.some(
              (operation) => operation.identity?.id === "conflict",
            ),
        ),
      );
    assert.ok(
      requests.length >= 2,
      "exhausted whole-transaction retry must resend frozen request",
    );
    assert.deepEqual(requests[0]!.request, requests.at(-1)!.request);
  } finally {
    fixture.conflictUpdates = 0;
    await s.close();
  }
});

test("explicit extra publication reaches another bound file without making it an input target or auto-enrolling it", async () => {
  const a = await session();
  const b = await session("bob");
  try {
    await b.client.bootstrap();
    await (
      await a.client.mutations.addTodo({
        todo: { id: "extra-a", title: "before" },
      })
    ).wait();
    const note = await a.client.mutations.addNote({ note: { body: "old" } });
    const saved = (await note.wait()).result!.saved;
    await fixture.backend.transaction(async (ctx) => {
      ctx.streams(["User:bob"]).track.note(saved.id);
    });
    await wait(
      async () => (await b.client.models.note.get({ id: saved.id })) !== null,
      "Bob note holding",
    );
    const call = await a.client.mutations.annotateTodo({
      todo: { id: "extra-a", title: " after " },
      note: saved.id,
      body: "extra publication",
    });
    assert.equal((await call.wait()).error, null);
    await wait(
      async () =>
        (await b.client.models.note.get({ id: saved.id }))?.body ===
        "extra publication",
      "explicit extra authority",
    );
    assert.equal(
      await b.client.models.todo.get({ id: "extra-a" }),
      null,
      "global invalidation cannot enroll an unrelated identity",
    );
    const request = proxy
      .requests("/sync/mutations")
      .filter((x) =>
        x.request.mutations.some(
          (mutation: { name: string }) => mutation.name === "AnnotateTodo",
        ),
      )
      .at(-1)!.request;
    assert.equal(
      request.mutations
        .find((mutation: { name: string }) => mutation.name === "AnnotateTodo")
        .operations.find(
          (operation: { inputPath: string }) => operation.inputPath === "note",
        ).value,
      saved.id,
    );
    assert.equal(
      (await a.client.models.todo.get({ id: "extra-a" }))?.title,
      "after",
    );
  } finally {
    await a.close();
    await b.close();
  }
});

test("creation defaults expand once before durable queueing and preserve enum/null/date handler codecs", async () => {
  const s = await session();
  try {
    await s.client.bootstrap();
    await s.client.connection!.pause();
    await s.client.mutations.addNote({ note: {} });
    const rows = await s.client.models.note.query();
    const local = rows.at(-1)!;
    assert.match(local.id, /^[0-9a-f-]{36}$/);
    assert.equal(local.mood, "calm");
    assert.equal(local.body, "");
    assert.equal(local.tag, "inbox");
    const date = local.createdAt.toISOString();
    await s.reopen();
    await wait(
      async () => (await s.client.syncState()).pending === 0,
      "defaulted create",
    );
    const args = fixture.notes.find((note) => note.id === local.id)!;
    assert.equal(args.createdAt.toISOString(), date);
    assert.equal(args.mood, "calm");
    assert.equal(
      (
        await s.client.models.note.get({ id: local.id })
      )?.createdAt.toISOString(),
      date,
    );
  } finally {
    await s.close();
  }
});

test("fresh Query retains complete scalar/list/date/model codecs across reopen and propagates refusal", async () => {
  const s = await session();
  try {
    await (
      await s.client.mutations.addTodo({
        todo: { id: "fresh-a", title: "freshq" },
      })
    ).wait();
    const first = await s.client.queries.todoPage(
      { query: "freshq" },
      { store: false },
    );
    assert.equal(first.count, 1);
    assert.equal(first.todos[0]!.id, "fresh-a");
    assert.ok(first.asOf instanceof Date);
    await s.reopen();
    await fixture.pool.query(
      "INSERT INTO action_e2e_todo VALUES('fresh-b','freshq')",
    );
    const second = await s.client.queries.todoPage(
      { query: "freshq" },
      { store: false },
    );
    assert.equal(second.count, 2);
    assert.notEqual(second.asOf.toISOString(), first.asOf.toISOString());
    fixture.failQueries = true;
    await assert.rejects(
      s.client.queries.todoPage({ query: "freshq" }, { store: false }),
    );
    fixture.failQueries = false;
    assert.equal(
      (await s.client.queries.todoPage({ query: "freshq" }, { store: false }))
        .count,
      2,
    );
    assert.ok((await s.client.queries.countTodos({})).count >= 2);
  } finally {
    fixture.failQueries = false;
    await s.close();
  }
});

test("atomic PublishEntry optimism and backend rows accept or roll back together; device-only Composition never reaches wire", async () => {
  const s = await session();
  let runs = 0;
  try {
    await s.client.bootstrap();
    await s.client.transaction(async (tx) => {
      await tx.models.composition.create(draft("comp-ok"));
      await tx.models.composition.create(draft("comp-no"));
    });
    fixture.rejectedEntries.add("entry-no");
    await s.client.connection!.pause();
    let okCall:
      Awaited<ReturnType<typeof s.client.mutations.publishEntry>> | undefined;
    await s.client.transaction(async (tx) => {
      okCall = await tx.mutations.publishEntry(async (local) => {
        runs++;
        const source = (await local.models.composition.get({ id: "comp-ok" }))!;
        await local.models.composition.delete({ id: "comp-ok" });
        return publishInput("entry-ok", source);
      });
      await assert.rejects(okCall.wait(), /transaction_uncommitted/);
      await tx.mutations.publishEntry(async (local) => {
        runs++;
        const source = (await local.models.composition.get({ id: "comp-no" }))!;
        await local.models.composition.delete({ id: "comp-no" });
        return publishInput("entry-no", source);
      });
    });
    assert.equal((await s.client.syncState()).pending, 2);
    assert.equal(
      await s.client.models.composition.get({ id: "comp-ok" }),
      null,
    );
    assert.equal(
      await s.client.models.composition.get({ id: "comp-no" }),
      null,
    );
    await s.reopen();
    await wait(
      async () => (await s.client.syncState()).pending === 0,
      "independent aggregate fates",
    );
    assert.equal(runs, 2);
    assert.equal(
      await s.client.models.composition.get({ id: "comp-ok" }),
      null,
    );
    assert.deepEqual(
      await s.client.models.composition.get({ id: "comp-no" }),
      draft("comp-no"),
    );
    assert.equal(
      (await s.client.models.entry.get({ id: "entry-ok" }))?.title,
      "draft comp-ok",
    );
    assert.equal(await s.client.models.entry.get({ id: "entry-no" }), null);
    assert.equal(
      (
        await fixture.pool.query(
          "SELECT count(*)::int AS n FROM action_e2e_media WHERE entry_id='entry-ok'",
        )
      ).rows[0].n,
      2,
    );
    assert.equal(
      (
        await fixture.pool.query(
          "SELECT count(*)::int AS n FROM action_e2e_media WHERE entry_id='entry-no'",
        )
      ).rows[0].n,
      0,
    );
    for (const request of proxy
      .requests("/sync/mutations")
      .filter((x) =>
        x.request.mutations.some(
          (mutation: { name: string }) => mutation.name === "PublishEntry",
        ),
      ))
      assert.deepEqual(
        [
          ...new Set(
            request.request.mutations
              .find(
                (mutation: { name: string }) =>
                  mutation.name === "PublishEntry",
              )
              .operations.map(
                (operation: { inputPath: string }) =>
                  operation.inputPath.split(/[.\[]/)[0],
              ),
          ),
        ].sort(),
        ["entry", "media", "placement"],
      );
    assert.equal(
      fixture.publishes.some((input) =>
        JSON.stringify(input).includes("Composition"),
      ),
      false,
    );
  } finally {
    fixture.rejectedEntries.delete("entry-no");
    await s.close();
  }
});

test("typed callbacks commit device-only companions atomically before accepted Call result and observers", async () => {
  const s = await session();
  try {
    await (
      await s.client.mutations.addTodo({
        todo: { id: "callback-a", title: "A" },
      })
    ).wait();
    await (
      await s.client.mutations.addTodo({
        todo: { id: "callback-b", title: "B" },
      })
    ).wait();
    let observed: string[][] = [];
    const stop = s.client.models.composition.watch({}, (rows) =>
      observed.push(rows.map((row) => row.id)),
    );
    const call = await s.client.mutations.editAndShow(async (tx) => {
      await tx.models.composition.create(draft("derived"));
      return { todo: { id: "callback-a", title: " A1 " }, shown: "callback-b" };
    });
    assert.ok(await s.client.models.composition.get({ id: "derived" }));
    const output = await call.wait();
    assert.equal(output.result?.todo.title, "B");
    assert.equal(
      (await s.client.models.todo.get({ id: "callback-a" }))?.title,
      "A1",
    );
    await wait(
      async () => observed.some((rows) => rows.includes("derived")),
      "committed companion observer",
    );
    stop();
  } finally {
    await s.close();
  }
});

test("scalar-input Mutation explicitly publishes affected rows and deletion installs canonical absence", async () => {
  const s = await session();
  try {
    for (const id of ["bulk-a", "bulk-b"]) {
      await (
        await s.client.mutations.addTodo({ todo: { id, title: "bulk-before" } })
      ).wait();
    }
    const call = await s.client.mutations.retitleTodos({
      query: "bulk-before",
      title: "bulk-after",
    });
    const result = (await call.wait()).result!;
    assert.deepEqual(
      result.todos.map((row) => row.id),
      ["bulk-a", "bulk-b"],
    );
    assert.equal(result.first?.title, "bulk-after");
    await wait(
      async () =>
        (await s.client.models.todo.get({ id: "bulk-a" }))?.title ===
          "bulk-after" &&
        (await s.client.models.todo.get({ id: "bulk-b" }))?.title ===
          "bulk-after",
      "explicit non-input publications",
    );
    const removed = await s.client.mutations.deleteTodo({
      todo: { id: "bulk-a" },
    });
    assert.equal(await s.client.models.todo.get({ id: "bulk-a" }), null);
    await removed.wait();
    await s.reopen();
    assert.equal(await s.client.models.todo.get({ id: "bulk-a" }), null);
    assert.equal(
      (await s.client.models.todo.get({ id: "bulk-b" }))?.title,
      "bulk-after",
    );
  } finally {
    await s.close();
  }
});

test("retired hook, enqueue-Query, anonymous write and multistream facades are absent from generated bound API", async () => {
  const s = await session();
  try {
    assert.equal("streams" in s.client, false);
    assert.equal("call" in s.client.mutations, false);
    assert.equal("enqueue" in s.client.queries, false);
    assert.equal("mutate" in s.client, false);
    assert.equal("StoreHooks" in (await import("./generated.ts")), false);
    assert.equal("onStore" in s.client, false);
  } finally {
    await s.close();
  }
});

test("Dart named callbacks/default/date codecs execute against the same actual host", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axton-action-dart-"));
  try {
    fixture.rejectedEntries.add("dart-entry-no");
    fixture.rejectedEntries.add("dart-entry-live-no");
    const library =
      process.env.AXTON_LIBRARY ?? process.env.AXTON_DART_LIBRARY!;
    for (const script of [
      "action_e2e_publish.dart",
      "action_e2e_datetime.dart",
      "action_e2e_hook.dart",
    ]) {
      const result = await promisify(execFile)(
        "dart",
        ["run", script, proxy.url, join(dir, script), library],
        {
          cwd: new URL("../action-runtime-dart/", import.meta.url).pathname,
          timeout: 40000,
          env: process.env,
        },
      );
      assert.match(result.stdout, /passed/);
      if (script === "action_e2e_datetime.dart") {
        const facts = JSON.parse(result.stdout.trim().split("\n").at(-1)!);
        const captured = fixture.restamps.slice(-4);
        assert.equal(captured.length, 4);
        assert.equal(captured[0]!.note, null);
        assert.equal(captured[1]!.note, null);
        for (const entry of captured)
          assert.equal(
            entry.at.toISOString(),
            new Date(facts.at).toISOString(),
          );
        assert.equal(
          captured[2]!.note?.createdAt?.toISOString(),
          new Date(facts.moved).toISOString(),
        );
        assert.equal(
          captured[3]!.note?.createdAt?.toISOString(),
          new Date(facts.created).toISOString(),
        );
      }
    }
  } finally {
    fixture.rejectedEntries.delete("dart-entry-no");
    fixture.rejectedEntries.delete("dart-entry-live-no");
    await rm(dir, { recursive: true, force: true });
  }
});

test("a suspended real Query handler leaves Stream sync and committed local transactions responsive", { timeout: 30000 }, async () => {
  const s = await session();
  let entered!: () => void, release!: () => void;
  const arrived = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  let pending: Promise<unknown> | undefined;
  try {
    await s.client.bootstrap();
    fixture.queryGate = async () => {
      entered();
      await held;
    };
    pending = s.client.queries.todoPage(
      { query: "responsive" },
      { store: false },
    );
    pending.catch(() => {});
    await arrived;
    await s.client.transaction(async (tx) => {
      await tx.models.composition.create(draft("responsive-local"));
    });
    assert.ok(
      await s.client.models.composition.get({ id: "responsive-local" }),
    );
    const mutation = await s.client.mutations.addTodo({
      todo: { id: "responsive-live", title: "responsive" },
    });
    let deadline!: ReturnType<typeof setTimeout>;
    const outcome = await Promise.race([
      mutation.wait(),
      new Promise<never>((_, reject) => {
        deadline = setTimeout(
          () => reject(Error("Mutation cannot settle while Query is suspended")),
          10000,
        );
      }),
    ]).finally(() => clearTimeout(deadline));
    assert.equal(outcome.error, null);
    assert.equal(
      (await s.client.models.todo.get({ id: "responsive-live" }))?.title,
      "responsive",
    );
    release();
    await pending;
  } finally {
    fixture.queryGate = undefined;
    release();
    await pending?.catch(() => {});
    await s.close();
  }
});
