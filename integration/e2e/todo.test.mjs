// Shared demo Stream, separate viewer-bound files, real native HTTP/WS/Prisma.
import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { createExample } from "../../examples/todo/server.mts";
import { GeneratedClient } from "../../examples/todo/generated/node/client.ts";
import { createProxy, wait, intent } from "./protocol-fixture.mjs";
const SCOPE = "todo:demo";
async function scenario(run) {
  let app = await createExample();
  const dir = await mkdtemp(join(tmpdir(), "axton-todo-"));
  const clients = new Set();
  let proxy;
  let server;
  try {
    await app.initialize();
    server = await app.listen(0);
    proxy = await createProxy(server.url);
    const open = async (name, viewer, bootstrap = true) => {
      const diagnostics = [];
      const client = await GeneratedClient.open({
        path: join(dir, name),
        stream: SCOPE,
        connection: {
          url: proxy.url,
          token: viewer,
          options: {
            onError: (error) =>
              diagnostics.push({
                name: error.name,
                message: error.message,
                code: error.code,
                details: error.details,
                status: error.status,
                kind: error.kind,
                identity: error.identity,
                ordinal: error.ordinal,
              }),
          },
        },
      });
      client.testDiagnostics = diagnostics;
      clients.add(client);
      if (bootstrap) await client.bootstrap();
      return client;
    };
    await run({
      get app() {
        return app;
      },
      dir,
      proxy,
      open,
      async close(client) {
        clients.delete(client);
        await client.close();
      },
      async restart() {
        const port = Number(new URL(server.url).port);
        await app.close();
        app = await createExample();
        await app.initialize();
        server = await app.listen(port);
      },
    });
  } finally {
    proxy?.releaseAll();
    for (const client of clients) await client.close();
    await proxy?.close();
    await app.close();
    await rm(dir, { recursive: true, force: true });
  }
}
const todo = (id, title = " task ") => ({
  id,
  title,
  done: false,
  createdById: "alice",
});
const target = (name) => (x) =>
  x.path === "/sync/mutations" &&
  intent(x).mutations.some((mutation) => mutation.name === name);

test("seeds materialize for both viewers and durable reopen keeps their edited state", async () =>
  scenario(async (ctx) => {
    const alice = await ctx.open("alice", "alice");
    const bob = await ctx.open("bob", "bob");
    assert.ok(await alice.models.user.get({ id: "alice" }));
    assert.ok((await alice.models.todo.query()).length > 0);
    assert.deepEqual(
      await alice.models.todo.query(),
      await bob.models.todo.query(),
    );
    const call = await alice.mutations.setTodoDone({
      todo: { id: "seed-1", done: true },
    });
    assert.equal((await call.wait()).error, null);
    await wait(
      async () => (await bob.models.todo.get({ id: "seed-1" })).done,
      "Bob receives completion",
    );
    const before = await alice.syncState();
    await ctx.close(alice);
    const reopened = await ctx.open("alice", "alice", false);
    assert.equal((await reopened.models.todo.get({ id: "seed-1" })).done, true);
    assert.deepEqual((await reopened.syncState()).cursors, before.cursors);
  }));

test("Alice adds and Bob completes one task; both files and PostgreSQL converge", async () =>
  scenario(async (ctx) => {
    const alice = await ctx.open("alice", "alice");
    const bob = await ctx.open("bob", "bob");
    const id = randomUUID();
    assert.equal(
      (await (await alice.mutations.addTodo({ todo: todo(id) })).wait()).error,
      null,
    );
    await wait(
      async () => !!(await bob.models.todo.get({ id })),
      "Bob receives create",
    );
    assert.equal(
      (
        await (
          await bob.mutations.setTodoDone({ todo: { id, done: true } })
        ).wait()
      ).error,
      null,
    );
    await wait(
      async () => (await alice.models.todo.get({ id })).done,
      "Alice receives Bob edit",
    );
    assert.deepEqual(
      await alice.models.todo.get({ id }),
      await bob.models.todo.get({ id }),
    );
    assert.equal(
      (await ctx.app.db.todo.findUnique({ where: { id } })).title,
      "task",
    );
  }));

test("unknown credential is refused without a handler or database write", async () =>
  scenario(async (ctx) => {
    const client = await ctx.open("outsider", "outsider", false);
    const id = randomUUID();
    const calls = ctx.app.handlerCalls;
    const response = await fetch(`${ctx.proxy.url}/sync/actions`, {
      method: "POST",
      headers: {
        authorization: "Bearer outsider",
        "content-type": "application/json",
      },
      body: JSON.stringify({}),
    });
    assert.equal(response.status, 401);
    assert.equal(ctx.app.handlerCalls, calls);
    assert.equal(await ctx.app.db.todo.findUnique({ where: { id } }), null);
    assert.equal((await client.syncState()).pending, 0);
  }));

test("missing target, duplicate create and invalid creator reject independently and restore local state", async () =>
  scenario(async (ctx) => {
    const alice = await ctx.open("alice", "alice");
    const id = randomUUID();
    await alice.transaction(async (tx) => {
      await tx.models.todo.create(todo(id));
    });
    const missing = await alice.mutations.setTodoDone({
      todo: { id, done: true },
    });
    assert.equal((await missing.wait()).error.code, "todo.missing");
    assert.equal((await alice.models.todo.get({ id })).done, false);
    // A separate create reaches the backend only when the device does not
    // already hold that identity. Release its local content before testing
    // the server's independent primary-key refusal.
    await alice.transaction(async (tx) => {
      await tx.models.todo.delete({ id: "seed-1" });
    });
    const duplicate = await alice.mutations.addTodo({
      todo: todo("seed-1", "replacement"),
    });
    assert.equal((await duplicate.wait()).error.code, "todo.id_conflict");
    assert.notEqual(
      (await ctx.app.db.todo.findUnique({ where: { id: "seed-1" } })).title,
      "replacement",
    );
    const bad = await alice.mutations.addTodo({
      todo: { ...todo(randomUUID()), createdById: "bob" },
    });
    assert.equal((await bad.wait()).error.code, "todo.creator_invalid");
  }));

test("lost accepted receipt retries exact intent after reopen and does not rerun handler", async () =>
  scenario(async (ctx) => {
    const alice = await ctx.open("alice", "alice");
    const id = randomUUID();
    const hold = ctx.proxy.holdResponse(target("AddTodo"));
    await alice.mutations.addTodo({ todo: todo(id) });
    await hold.arrived;
    const calls = ctx.app.handlerCalls;
    ctx.proxy.down();
    hold.release();
    await ctx.close(alice);
    ctx.proxy.up();
    const reopened = await ctx.open("alice", "alice", false);
    await wait(
      async () => (await reopened.syncState()).pending === 0,
      "saved receipt replay",
    );
    assert.equal(ctx.app.handlerCalls, calls);
    assert.equal((await reopened.models.todo.get({ id })).title, "task");
    const requests = ctx.proxy
      .requests("/sync/mutations")
      .filter((x) =>
        x.request.mutations.some((mutation) => mutation.name === "AddTodo"),
      );
    assert.ok(requests.length >= 2);
    assert.deepEqual(requests.at(-1).request, requests[0].request);
  }));

test("offline create then complete persists in order while another viewer keeps working", async () =>
  scenario(async (ctx) => {
    const alice = await ctx.open("alice", "alice");
    const bob = await ctx.open("bob", "bob");
    const id = randomUUID();
    await alice.connection.pause();
    await alice.mutations.addTodo({ todo: todo(id) });
    await alice.mutations.setTodoDone({ todo: { id, done: true } });
    assert.equal((await alice.syncState()).pending, 2);
    assert.equal(await ctx.app.db.todo.findUnique({ where: { id } }), null);
    assert.equal(
      (
        await (
          await bob.mutations.setTodoDone({
            todo: { id: "seed-1", done: false },
          })
        ).wait()
      ).error,
      null,
    );
    await ctx.close(alice);
    const reopened = await ctx.open("alice", "alice", false);
    try {
      await wait(
        async () => (await reopened.syncState()).pending === 0,
        "dependent durable calls",
      );
    } catch (error) {
      const inspect = async (sql) =>
        reopened.readSql(sql).catch((error) => ({ error: String(error) }));
      const evidence = {
        id,
        diagnostics: reopened.testDiagnostics.slice(-8),
        bobDiagnostics: bob.testDiagnostics.slice(-8),
        state: await reopened.syncState(),
        dependencies: await inspect("SELECT * FROM axton_mutation_dependency"),
        operations: await inspect("SELECT * FROM axton_mutation_queue_operation"),
        tasks: await reopened.pendingTasks(),
        refusals: await inspect("SELECT id,rejection_code,rejection_message FROM axton_mutation_queue WHERE rejection_code IS NOT NULL"),
        store: await inspect("SELECT * FROM axton_store"),
        queue: await inspect("SELECT * FROM axton_mutation_queue"),
        evidence: await inspect("SELECT * FROM axton_authority"),
        delivery: await inspect("SELECT * FROM axton_delivery_progress"),
        local: await reopened.models.todo.get({ id }),
        prisma: await ctx.app.db.todo.findUnique({ where: { id } }),
        savedCalls: await ctx.app.db
          .$queryRawUnsafe(
            "SELECT store_id,batch_id,mutation_id,result FROM axton_mutation_result WHERE result::text LIKE $1 LIMIT 4",
            `%${id}%`,
          )
          .catch((error) => ({ error: String(error) })),
        requests: ctx.proxy.requests("/sync/mutations").slice(-6),
        deliveryRequests: ctx.proxy.requests("/sync/pull").slice(-6),
      };
      console.error(
        "TODO_DEPENDENT_TIMEOUT",
        JSON.stringify(evidence, (_, value) =>
          typeof value === "bigint" ? String(value) : value,
        ),
      );
      throw error;
    }
    assert.equal(
      (await ctx.app.db.todo.findUnique({ where: { id } })).done,
      true,
    );
    await wait(
      async () => (await bob.models.todo.get({ id }))?.done === true,
      "Bob receives dependent result",
    );
  }));

test("backend restart preserves durable work; repeated true and empty patch have explicit no-op outcomes", async () =>
  scenario(async (ctx) => {
    const alice = await ctx.open("alice", "alice");
    await alice.connection.pause();
    await alice.mutations.setTodoDone({ todo: { id: "seed-1", done: true } });
    await ctx.restart();
    await alice.connection.resume();
    await wait(
      async () => (await alice.syncState()).pending === 0,
      "restart settlement",
    );
    const repeat = await alice.mutations.setTodoDone({
      todo: { id: "seed-1", done: true },
    });
    assert.equal((await repeat.wait()).result.todo.done, true);
    const noop = await alice.mutations.setTodoDone({ todo: { id: "seed-1" } });
    assert.equal((await noop.wait()).result.todo.done, true);
  }));

test("a held business result stays immutable while a later durable edit changes the Store", async () =>
  scenario(async (ctx) => {
    const alice = await ctx.open("alice", "alice");
    const hold = ctx.proxy.holdResponse(target("SetTodoDone"));
    const first = await alice.mutations.setTodoDone({
      todo: { id: "seed-1", done: true },
    });
    await hold.arrived;
    const second = await alice.mutations.setTodoDone({
      todo: { id: "seed-1", done: false },
    });
    hold.release();
    assert.equal((await first.wait()).result.todo.done, true);
    assert.equal((await second.wait()).result.todo.done, false);
    assert.equal((await alice.models.todo.get({ id: "seed-1" })).done, false);
  }));
