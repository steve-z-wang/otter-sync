import test from "node:test";
import assert from "node:assert/strict";
import { once } from "node:events";
import { WebSocketServer } from "ws";
import * as runtime from "../../../packages/client-js/index.mts";
import { createServerConnection } from "../../../packages/client-js/live.mts";

const timeout = (p) =>
  Promise.race([
    p,
    new Promise((_, reject) => {
      const t = setTimeout(() => reject(Error("timeout")), 3000);
      t.unref();
    }),
  ]);

const subscribe = JSON.stringify({
  protocol: 5,
  storeId: "00000000-0000-4000-8000-000000000001",
  stream: "User:viewer",
});
const ack = (sub, head = 0) => JSON.stringify(emptyHandshake({ ...sub, head }));
const handlers = (over = {}) => ({
  message: async () => {},
  overflow: async () => {},
  closed: () => {},
  ...over,
});

test("internal socket sends the subscribe frame, delivers frames in order, and cancellation ends the socket", async () => {
  assert.equal(typeof createServerConnection, "function");
  const server = new WebSocketServer({ port: 0 });
  await once(server, "listening");
  const abort = new AbortController();
  const frames = [];
  const connected = once(server, "connection");
  const live = createServerConnection({
    url: `http://127.0.0.1:${server.address().port}`,
    token: "secret",
  });
  live.open(
    subscribe,
    abort.signal,
    handlers({
      message: async (text) => {
        frames.push(JSON.parse(text));
      },
    }),
  );
  try {
    const [socket, request] = await timeout(connected);
    assert.equal(request.headers.authorization, "Bearer secret");
    const [message] = await timeout(once(socket, "message"));
    assert.deepEqual(JSON.parse(message), JSON.parse(subscribe));
    const closed = once(socket, "close");
    socket.send(ack(JSON.parse(message)));
    // This carrier test treats frames as opaque bytes, not authority pages.
    socket.send(JSON.stringify({ sequence: 13 }));
    await timeout(
      new Promise((resolve) => {
        const check = () =>
          frames.length === 2 ? resolve() : setImmediate(check);
        check();
      }),
    );
    assert.equal(
      frames[0].head,
      0,
      "the transport does not interpret frames",
    );
    assert.equal(frames[1].sequence, 13);
    abort.abort();
    await timeout(closed);
  } finally {
    abort.abort();
    for (const s of server.clients) s.terminate();
    await new Promise((r) => server.close(r));
  }
});

test("live transport cancellation does not wait for a stalled token", async () => {
  const abort = new AbortController();
  let closed = 0;
  const live = createServerConnection({
    url: "http://127.0.0.1:1",
    token: () => new Promise(() => {}),
  });
  live.open(
    subscribe,
    abort.signal,
    handlers({
      closed: () => {
        closed++;
      },
    }),
  );
  abort.abort();
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(closed, 0, "an aborted socket is not reported as closed");
});

test("invalid WebSocket credentials reject the session rather than leaking a rejected task", async () => {
  const live = createServerConnection({
    url: "http://127.0.0.1:1",
    token: "invalid\nheader",
  });
  const failure = new Promise((resolve) =>
    live.open(
      subscribe,
      new AbortController().signal,
      handlers({ closed: resolve }),
    ),
  );
  assert.match(String((await timeout(failure)).message), /header|character/i);
});

import { createServer } from "node:http";
import { mkdtemp, rm, readFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { setImmediate as tick } from "node:timers/promises";
import { openStore, emptyPull, emptyMutation, emptyHandshake, delivery05 } from "./store-fixture.mjs";
async function openClient() {
  const dir = await mkdtemp(join(tmpdir(), "axton-live05-"));
  const schema = JSON.parse(
    await readFile(
      new URL("../../v05-sdk/schema.json", import.meta.url),
      "utf8",
    ),
  );
  const edit = structuredClone(
    schema.actions.find((action) => action.name === "Publish"),
  );
  edit.name = "Edit";
  edit.inputs.find((input) => input.name === "entry").operation = "update";
  schema.actions.push(edit);
  const client = await openStore(runtime.Client, {
    path: join(dir, "db"),
    schema,
  });
  return {
    client,
    async close() {
      await client.close();
      await rm(dir, { recursive: true, force: true });
    },
  };
}
async function until(predicate) {
  const end = Date.now() + 10000;
  while (Date.now() < end) {
    if (await predicate()) return;
    await tick();
  }
  throw Error("condition timed out");
}
const page = (context, from, to, text, head = to) => delivery05(
  {...context, bootstrap:false, after:from, through:to},
  text === undefined ? [] : [{kind:"record",cursor:to,key:{model:"Entry",identity:{id:"live"}},state:text === null ? null : {text}}],head);
function receiptFor(body) {
  const receipt = JSON.parse(emptyMutation(body));
  const mutation = body.mutations[0];
  const operation = mutation.operations.find(op=>op.inputPath === "entry");
  const input = {...operation.identity,...operation.value};
  const row = { id: input.id, text: input.text.trim() };
  receipt.results[0].outcome.result = {entry:row};
  receipt.results[0].outcome.targets = [{kind:"private",record:{key:{model:"Entry",identity:{id:row.id}},cursor:null,state:{text:row.text}}}];
  return receipt;
}
async function syncFixture({ onPull, onAction, upgrade } = {}) {
  const requests = [],
    sockets = [],
    handshakes = [],
    errors = [];
  const state = { head: 0 };
  const contexts = new Map();
  const server = createServer(async (req, res) => {
    let text = "";
    for await (const c of req) text += c;
    const body = JSON.parse(text);
    requests.push({ url: req.url, body, headers: req.headers });
    try {
      if (req.url === "/sync/handshake") res.end(JSON.stringify({...emptyHandshake(body),head:state.head}));
      else if (req.url === "/sync/materialize") res.end(JSON.stringify({requestId:body.requestId,delivery:delivery05(body,[],state.head)}));
      else if (body.bootstrap) {
        const context = {protocol:body.protocol,storeId:body.storeId,stream:body.stream,materialization:body.materialization};
        contexts.set(body.storeId,context);
        for (const handshake of handshakes) if(handshake.storeId===body.storeId) handshake.context=context;
        res.end(JSON.stringify(delivery05(body,[],state.head)));
      }
      else if (body.mutations) {
        if (onAction) await onAction(body, res);
        else res.end(JSON.stringify(receiptFor(body)));
      } else if (onPull)
        await onPull(
          body,
          res,
          deltas({requests}).length,
        );
      else
        res.end(
          JSON.stringify(page(body, body.after, body.through, undefined, state.head)),
        );
    } catch (e) {
      errors.push(e);
      res.statusCode = 500;
      res.end(String(e));
    }
  });
  const ws = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    if (upgrade && !upgrade(req, socket)) return;
    ws.handleUpgrade(req, socket, head, (s) => {
      sockets.push(s);
      s.on("message", (m) => {
        const body = JSON.parse(m);
        const context = contexts.get(body.storeId);
        s.subscription = {...body,context,cursor:body.cursor};
        handshakes.push(s.subscription);
        s.send(JSON.stringify({...emptyHandshake(body),head:state.head}));
      });
    });
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return {
    requests,
    sockets,
    handshakes,
    state,
    errors,
    config: {
      url: `http://127.0.0.1:${server.address().port}`,
      token: "secret",
    },
    async close() {
      for (const s of ws.clients) s.terminate();
      await new Promise((r) => ws.close(r));
      await new Promise((r) => server.close(r));
      assert.deepEqual(errors, []);
    },
  };
}
const deltas = (n) => n.requests.filter((x) => x.body.after !== undefined && !x.body.bootstrap && !x.body.owner);
const publish = (c) =>
  c.submitMutation(
    "Publish",
    1,
    { entry: { id: "local", text: "  canonical  " }, call: "live" },
    (v) => v,
  );
async function connected(f, n, options = {}) {
  const c = await f.client.connect(n.config, options);
  await f.client.bootstrap();
  await until(() => n.handshakes.length > 0 && n.handshakes.every(h=>h.context));
  return c;
}

test("bound reconnect uses durable prefix; ACK cannot replace it; gaps recover through strict Delta", async () => {
  const f = await openClient(),
    n = await syncFixture({
      onPull: (b, r) =>
        r.end(
          JSON.stringify(page(b, b.after, n.state.head, "recovered")),
        ),
    });
  try {
    const c = await connected(f, n);
    n.sockets[0].send(
      JSON.stringify(page(n.handshakes[0].context, 0, 1, "first")),
    );
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 1,
    );
    await c.pause();
    n.state.head = 3;
    await c.resume();
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 3,
    );
    assert.equal(deltas(n)[0].body.after, 1);
    assert.equal(
      (await f.client.read("Entry", { id: "live" })).text,
      "recovered",
    );
    n.state.head = 7;
    n.sockets
      .at(-1)
      .send(JSON.stringify(page(n.handshakes.at(-1).context, 6, 7, "gap")));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 7,
    );
    assert.equal(deltas(n).at(-1).body.after, 3);
    assert.ok(
      n.handshakes.every(
        (x) => x.stream === "User:viewer" && !("streams" in x),
      ),
    );
  } finally {
    await f.close();
    await n.close();
  }
});

test("duplicate units cannot undo authority; a complete overlapping unit applies atomically and a real gap repairs", async () => {
  const f = await openClient(),
    errors = [],
    n = await syncFixture({
      onPull: (b, r) =>
        r.end(
          JSON.stringify(
            page(
              b,
              b.after,
              n.state.head,
              n.state.head === 2 ? "second" : "fourth",
            ),
          ),
        ),
    });
  try {
    await connected(f, n, { onError: (e) => errors.push(e) });
    const ctx = n.handshakes[0].context;
    const send = (p) => n.sockets.at(-1).send(JSON.stringify(p));
    send(page(ctx, 0, 1, "first"));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 1,
    );
    const snapshots = [];
    const stop = f.client.watchSql("SELECT id,text FROM Entry ORDER BY id",[],rows=>snapshots.push(rows));
    await until(()=>snapshots.length===1);
    send(page(ctx, 0, 1, "first"));
    n.state.head = 2;
    send(delivery05({...ctx,bootstrap:false,after:0,through:2},[
      {kind:"record",cursor:1,key:{model:"Entry",identity:{id:"live"}},state:{text:"stale replay"}},
      {kind:"record",cursor:2,key:{model:"Entry",identity:{id:"next"}},state:{text:"second"}},
    ]));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 2,
    );
    await until(()=>snapshots.length===2);
    assert.deepEqual(snapshots,[[{id:"live",text:"first"}],[{id:"live",text:"first"},{id:"next",text:"second"}]],"one whole-unit commit retains admitted history and applies the unseen key");
    stop();
    assert.equal((await f.client.read("Entry", { id: "live" })).text, "first");
    assert.equal((await f.client.read("Entry", { id: "next" })).text, "second");
    assert.equal(deltas(n).length,0,"complete overlapping unit needs no range repair");
    assert.deepEqual(errors, []);
    n.state.head = 4;
    send(page(ctx, 3, 4, "fourth"));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 4,
    );
    assert.equal(deltas(n).at(-1).body.after, 2);
    assert.equal((await f.client.read("Entry", { id: "live" })).text, "fourth");
    assert.deepEqual(errors, []);
    assert.equal(
      n.handshakes.length,
      1,
      "legal carrier handoff needs no reconnect",
    );
  } finally {
    await f.close();
    await n.close();
  }
});

test("HTTP recovery failure reports and retries from committed prefix without assuming an empty success", async () => {
  const f = await openClient(),
    errors = [];
  const n = await syncFixture({
    onPull: (b, r, count) => {
      if (count === 1) {
        r.statusCode = 503;
        r.end("unavailable");
      } else r.end(JSON.stringify(page(b, b.after, 1, "retried")));
    },
  });
  try {
    const c = await connected(f, n, { onError: (e) => errors.push(e) });
    await c.pause();
    n.state.head = 1;
    await c.resume();
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 1,
    );
    assert.equal(deltas(n)[0].body.after, 0);
    assert.equal(deltas(n)[1].body.after, 0);
    assert.ok(errors.some((e) => e.status === 503));
  } finally {
    await f.close();
    await n.close();
  }
});

test("pause cancels held catch-up; its late answer cannot overwrite newer resumed authority", async () => {
  const f = await openClient(),
    entered = Promise.withResolvers(),
    gate = Promise.withResolvers(),
    lateFinished = Promise.withResolvers();
  let held;
  const n = await syncFixture({
    onPull: async (b, r, count) => {
      if (count === 1) {
        held = b;
        entered.resolve();
        await gate.promise;
      }
      r.end(
        JSON.stringify(
          page(
            b,
            b.after,
            count === 1 ? 1 : 2,
            count === 1 ? "obsolete" : "fresh",
          ),
        ),
      );
      if (count === 1) lateFinished.resolve();
    },
  });
  try {
    const c = await connected(f, n);
    await c.pause();
    n.state.head = 1;
    await c.resume();
    await timeout(entered.promise);
    await c.pause();
    n.state.head = 2;
    await c.resume();
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 2,
    );
    gate.resolve();
    await timeout(lateFinished.promise);
    assert.equal((await f.client.read("Entry", { id: "live" })).text, "fresh");
    assert.equal(held.after, 0);
  } finally {
    gate.resolve();
    await f.close();
    await n.close();
  }
});

test("pause prevents a late HTTP credential from starting its canceled recovery request", async () => {
  const f = await openClient(),
    n = await syncFixture(),
    called = Promise.withResolvers(),
    token = Promise.withResolvers();
  let block = false;
  try {
    const c = await f.client.connect({
      ...n.config,
      token: () => (block ? (called.resolve(), token.promise) : "secret"),
    });
    await f.client.bootstrap();
    await until(() => n.handshakes.length === 1 && n.handshakes[0].context);
    await c.pause();
    await c.resume();
    await until(() => n.handshakes.length === 2);
    block = true;
    n.sockets
      .at(-1)
      .send(JSON.stringify(page(n.handshakes.at(-1).context, 2, 3, "gap")));
    await timeout(called.promise);
    assert.equal(deltas(n).length, 0);
    const before = n.requests.length;
    await c.pause();
    token.resolve("late");
    await tick();
    assert.equal(n.requests.length, before);
    assert.equal((await f.client.syncState()).cursors["User:viewer"] ?? 0, 0);
  } finally {
    token.resolve("late");
    await f.close();
    await n.close();
  }
});

test("upgrade authentication shares refresh and survives a failed refresh", async () => {
  const f = await openClient(),
    errors = [];
  let token = "expired",
    refreshes = 0;
  const n = await syncFixture({
    upgrade: (req, s) => {
      if (req.headers.authorization === "Bearer valid") return true;
      s.end("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n");
      return false;
    },
  });
  try {
    await f.client.connect(
      { ...n.config, token: () => token },
      {
        onError: (e) => errors.push(e),
        refreshAuth: async () => {
          if (++refreshes === 1) throw Error("refresh failed");
          token = "valid";
        },
      },
    );
    await until(() => n.handshakes.length === 1);
    assert.equal(refreshes, 2);
    assert.ok(errors.some((e) => e.message === "refresh failed"));
  } finally {
    await f.close();
    await n.close();
  }
});

test("close cancels an opening handshake and abandons a stalled credential", async () => {
  const f = await openClient(),
    entered = Promise.withResolvers();
  let pending;
  const server = createServer(async (req, res) => {
    let text = "";
    for await (const c of req) text += c;
    res.end(emptyPull(text));
  });
  server.on("upgrade", (_r, s) => {
    pending = s;
    s.resume();
    s.on("end", () => s.destroy());
    entered.resolve();
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  try {
    const c = await f.client.connect({
      url: `http://127.0.0.1:${server.address().port}`,
      token: "secret",
    });
    await timeout(entered.promise);
    const closed = once(pending, "close");
    await timeout(c.close());
    pending.end();
    await timeout(closed);
  } finally {
    await f.close();
    pending?.destroy();
    await new Promise((r) => server.close(r));
  }
  const b = await openClient(),
    called = Promise.withResolvers(),
    token = Promise.withResolvers();
  let requests = 0;
  const ws = new WebSocketServer({ port: 0 });
  await once(ws, "listening");
  ws.on("connection", () => requests++);
  try {
    await b.client.connect({
      url: `http://127.0.0.1:${ws.address().port}`,
      token: () => {
        called.resolve();
        return token.promise;
      },
    });
    await timeout(called.promise);
    await timeout(b.client.close());
    token.resolve("late");
    await tick();
    assert.equal(requests, 0);
  } finally {
    token.resolve("late");
    await b.close();
    for (const s of ws.clients) s.terminate();
    await new Promise((r) => ws.close(r));
  }
});

test("pause queued behind SQLite transaction leaves socket active until commit; resume continues the bound Store", async () => {
  const f = await openClient(),
    n = await syncFixture(),
    entered = Promise.withResolvers(),
    gate = Promise.withResolvers();
  try {
    const c = await connected(f, n);
    const old = n.sockets[0];
    const tx = f.client.transaction(async () => {
      entered.resolve();
      await gate.promise;
    });
    await entered.promise;
    const paused = c.pause();
    assert.equal(old.readyState, old.OPEN, "queued control has not committed");
    gate.resolve();
    await tx;
    await paused;
    await until(() => old.readyState === old.CLOSED);
    assert.equal((await f.client.syncState()).cursors["User:viewer"] ?? 0, 0);
    assert.equal(await f.client.read("Entry", { id: "live" }), null);
    await c.resume();
    await until(() => n.handshakes.length === 2);
    n.sockets[1].send(
      JSON.stringify(page(n.handshakes[1].context, 0, 1, "fresh")),
    );
    await until(
      async () =>
        (await f.client.read("Entry", { id: "live" }))?.text === "fresh",
    );
  } finally {
    gate.resolve();
    await f.close();
    await n.close();
  }
});

test("same Stream in independent files isolates connection cancellation and progress", async () => {
  const a = await openClient(),
    b = await openClient(),
    n = await syncFixture();
  try {
    const ca = await a.client.connect(n.config);
    await until(() => n.handshakes.length === 1);
    const cb = await b.client.connect(n.config);
    await a.client.bootstrap();
    await b.client.bootstrap();
    await until(() => n.handshakes.length === 2 && n.handshakes.every(h=>h.context));
    await ca.close();
    await until(() => n.sockets[0].readyState === n.sockets[0].CLOSED);
    const socket = n.sockets.find((s) => s.readyState === s.OPEN);
    socket.send(
      JSON.stringify(page(socket.subscription.context, 0, 1, "only b")),
    );
    await until(
      async () =>
        (await b.client.read("Entry", { id: "live" }))?.text === "only b",
    );
    assert.equal(await a.client.read("Entry", { id: "live" }), null);
    assert.equal((await a.client.syncState()).cursors["User:viewer"] ?? 0, 0);
    await cb.close();
  } finally {
    await a.close();
    await b.close();
    await n.close();
  }
});

test("Mutation receipt completes independently while upgrade is unavailable", async () => {
  const f = await openClient(),
    errors = [];
  let allow = false;
  const n = await syncFixture({
    upgrade: (_r, s) => {
      if (allow) return true;
      s.end("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
      return false;
    },
  });
  try {
    const call = await publish(f.client);
    await f.client.connect(n.config, { onError: (e) => errors.push(e) });
    assert.equal((await timeout(call.wait())).result.entry.text, "canonical");
    assert.equal((await f.client.syncState()).pending, 0);
    assert.equal(n.handshakes.length, 0);
    assert.equal(deltas(n).length, 0);
    await until(() => errors.some((e) => /503/.test(e.message)));
    allow = true;
    await until(() => n.handshakes.length === 1);
  } finally {
    await f.close();
    await n.close();
  }
});

test("simultaneous HTTP and socket 401 joins one refresh without dropping durable Mutation", async () => {
  const f = await openClient(),
    gate = Promise.withResolvers(),
    errors = [];
  let token = "expired",
    refreshed = 0,
    unauthorized = 0;
  const n = await syncFixture({
    upgrade: (req, s) => {
      if (req.headers.authorization === "Bearer valid") return true;
      unauthorized++;
      s.end("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n");
      return false;
    },
    onAction: (b, r) => {
      if (token !== "valid") {
        unauthorized++;
        r.statusCode = 401;
        r.end();
      } else r.end(JSON.stringify(receiptFor(b)));
    },
  });
  try {
    const call = await publish(f.client);
    await f.client.connect(
      { ...n.config, token: () => token },
      {
        onError: (e) => errors.push(e),
        refreshAuth: async () => {
          refreshed++;
          await gate.promise;
          token = "valid";
        },
      },
    );
    await until(
      () =>
        unauthorized >= 2 && errors.filter((e) => e.status === 401).length >= 2,
    );
    assert.equal(refreshed, 1);
    gate.resolve();
    assert.equal((await timeout(call.wait())).result.entry.text, "canonical");
    await until(() => n.handshakes.length === 1);
    assert.equal(refreshed, 1);
  } finally {
    gate.resolve();
    await f.close();
    await n.close();
  }
});

test("server socket close reconnects with retained context/cursor after backoff", async () => {
  const f = await openClient(),
    errors = [],
    n = await syncFixture();
  try {
    await connected(f, n, { onError: (e) => errors.push(e) });
    const first = n.handshakes[0];
    n.sockets[0].send(JSON.stringify(page(first.context, 0, 1, "first")));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 1,
    );
    n.state.head = 1;
    const when = Date.now();
    n.sockets[0].close(1001, "closing");
    await until(() => n.handshakes.length === 2);
    assert.ok(Date.now() - when >= 180);
    assert.deepEqual(n.handshakes[1].context, first.context);
    assert.equal((await f.client.syncState()).cursors["User:viewer"],1);
    assert.ok(errors.some((e) => /1001/.test(e.message)));
    n.sockets[1].send(JSON.stringify(page(first.context, 1, 2, "resumed")));
    await until(
      async () =>
        (await f.client.read("Entry", { id: "live" }))?.text === "resumed",
    );
  } finally {
    await f.close();
    await n.close();
  }
});

test("owner refusal preserves exact frozen intent for retry", async () => {
  const f = await openClient(),
    errors = [];
  let refuse = true;
  const n = await syncFixture({
    onAction: (b, r) => {
      if (refuse) {
        r.statusCode = 403;
        r.end(JSON.stringify({ code: "client.owner_mismatch" }));
      } else r.end(JSON.stringify(receiptFor(b)));
    },
  });
  try {
    const call = await publish(f.client);
    await f.client.connect(n.config, { onError: (e) => errors.push(e) });
    await until(
      () =>
        n.requests.filter((x) => x.body.mutations?.some(m=>["Publish","Edit"].includes(m.name)))
          .length >= 2,
    );
    const sent = n.requests.filter((x) =>
      x.body.mutations?.some(m=>["Publish","Edit"].includes(m.name)),
    );
    assert.deepEqual(sent[0].body, sent[1].body);
    assert.equal((await f.client.syncState()).pending, 1);
    assert.ok(errors.some((e) => /owner_mismatch/.test(e.message)));
    refuse = false;
    assert.equal((await timeout(call.wait())).result.entry.text, "canonical");
  } finally {
    await f.close();
    await n.close();
  }
});

test("malformed authority is observable and never skips failed coverage; valid retry recovers", async () => {
  const f = await openClient(),
    errors = [],
    n = await syncFixture();
  try {
    await connected(f, n, { onError: (e) => errors.push(e) });
    const ctx = n.handshakes[0].context;
    n.sockets[0].send(JSON.stringify(page(ctx, 0, 1, "first")));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 1,
    );
    const bad = page(ctx, 1, 2, "bad");
    bad.parts[0].changes[0].state = { text: 5 };
    n.sockets[0].send(JSON.stringify(bad));
    await until(() => errors.length > 0);
    assert.equal((await f.client.syncState()).cursors["User:viewer"], 1);
    assert.equal((await f.client.read("Entry", { id: "live" })).text, "first");
    n.state.head = 2;
    await until(() => n.handshakes.length >= 2);
    n.sockets.at(-1).send(JSON.stringify(page(ctx, 1, 2, "fixed")));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 2,
    );
  } finally {
    await f.close();
    await n.close();
  }
});

test("malformed accepted private settlement remains observable without retiring the durable queue", async () => {
  const f = await openClient(),
    errors = [];
  let valid = false;
  const n = await syncFixture({
    onAction: (b, r) => {
      const receipt = receiptFor(b);
      if (!valid) receipt.results[0].outcome.targets[0].record.state.text = 5;
      r.end(JSON.stringify(receipt));
    },
  });
  try {
    const call = await publish(f.client);
    await f.client.connect(n.config, { onError: (e) => errors.push(e) });
    await until(() => errors.length > 0);
    assert.equal((await f.client.syncState()).pending, 1);
    assert.equal(
      (await f.client.read("Entry", { id: "local" })).text,
      "  canonical  ",
    );
    valid = true;
    await f.client.connection.close();
    assert.equal(call.status, "pending");
    assert.equal((await f.client.syncState()).pending, 1);
  } finally {
    await f.close();
    await n.close();
  }
});

test("later true Stream absence prevents an older accepted private target from reviving content", async () => {
  const f = await openClient(),
    entered = Promise.withResolvers(),
    gate = Promise.withResolvers();
  const n = await syncFixture({
    onAction: async (b, r) => {
      entered.resolve();
      await gate.promise;
      r.end(JSON.stringify(receiptFor(b)));
    },
  });
  try {
    await connected(f, n);
    const call = await publish(f.client);
    await timeout(entered.promise);
    const nullPage = delivery05({...n.handshakes[0].context,bootstrap:false,after:0,through:1},[{kind:"record",cursor:1,key:{model:"Entry",identity:{id:"local"}},state:null}]);
    n.sockets[0].send(JSON.stringify(nullPage));
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 1,
    );
    gate.resolve();
    assert.equal((await timeout(call.wait())).result.entry.text, "canonical");
    assert.equal(await f.client.read("Entry", { id: "local" }), null);
    assert.equal((await f.client.syncState()).pending, 0);
  } finally {
    gate.resolve();
    await f.close();
    await n.close();
  }
});

test("queued Update divergence remains durable and later absence cannot be undone by its old receipt", async () => {
  const f = await openClient(),
    errors = [],
    entered = Promise.withResolvers(),
    gate = Promise.withResolvers();
  const n = await syncFixture({
    onAction: async (b, r) => {
      entered.resolve();
      await gate.promise;
      r.end(JSON.stringify(receiptFor(b)));
    },
  });
  try {
    await connected(f, n, { onError: (e) => errors.push(e) });
    const ctx = n.handshakes[0].context;
    n.sockets[0].send(JSON.stringify(page(ctx, 0, 1, "held")));
    await until(
      async () =>
        (await f.client.read("Entry", { id: "live" }))?.text === "held",
    );
    const call = await f.client.submitMutation(
      "Edit",
      1,
      { entry: { id: "live", text: "edited" }, call: "edit" },
      (v) => v,
    );
    await timeout(entered.promise);
    n.sockets[0].send(JSON.stringify(page(ctx, 1, 2, null)));
    await until(() =>
      errors.some(
        (e) => e instanceof runtime.AxtonReport && e.kind === "diverged",
      ),
    );
    assert.equal(await f.client.read("Entry", { id: "live" }), null);
    assert.equal((await f.client.syncState()).pending, 1);
    const report = errors.find(
      (e) => e instanceof runtime.AxtonReport && e.kind === "diverged",
    );
    assert.deepEqual(report.identity, { id: "live" });
    gate.resolve();
    assert.equal((await timeout(call.wait())).result.entry.text, "edited");
    assert.equal(await f.client.read("Entry", { id: "live" }), null);
    assert.equal((await f.client.syncState()).pending, 0);
  } finally {
    gate.resolve();
    await f.close();
    await n.close();
  }
});

test("bounded live receive recovery preserves held HTTP progress and catches the latest real head", async () => {
  const f = await openClient(),
    entered = Promise.withResolvers(),
    gate = Promise.withResolvers();
  const n = await syncFixture({
    onPull: async (b, r, count) => {
      const response = page(
        b,
        b.after,
        n.state.head,
        `head ${n.state.head}`,
      );
      if (count === 1) {
        entered.resolve();
        await gate.promise;
      }
      r.end(JSON.stringify(response));
    },
  });
  try {
    const c = await connected(f, n);
    await c.pause();
    n.state.head = 1;
    await c.resume();
    await timeout(entered.promise);
    await until(()=>n.handshakes.length===2 && n.sockets.at(-1).subscription);
    const socket = n.sockets.at(-1),
      ctx = socket.subscription.context,
      opened = n.sockets.length;
    socket._socket.cork();
    for (let from = 1; from <= 200; from++)
      socket.send(
        JSON.stringify(page(ctx, from, from + 1, `live ${from + 1}`)),
      );
    socket._socket.uncork();
    n.state.head = 201;
    gate.resolve();
    await until(
      async () => (await f.client.syncState()).cursors["User:viewer"] === 201,
    );
    assert.ok(
      n.sockets.length <= opened + 1,
      "one overflow permits bounded reconnect, never starvation",
    );
    assert.ok(deltas(n).length <= 5);
    assert.equal(deltas(n)[0].body.after, 0);
    assert.ok(
      deltas(n).every((x) => x.body.stream === "User:viewer"),
    );
  } finally {
    gate.resolve();
    await f.close();
    await n.close();
  }
});

test("bounded transport overflow reports once and preserves ordered resumed delivery", async () => {
  const server = new WebSocketServer({ port: 0 });
  await once(server, "listening");
  const gate = Promise.withResolvers(),
    entered = Promise.withResolvers(),
    overflow = Promise.withResolvers();
  const frames = [];
  let overflowCount = 0;
  const abort = new AbortController();
  const live = createServerConnection({
    url: `http://127.0.0.1:${server.address().port}`,
    token: "secret",
  });
  live.open(
    subscribe,
    abort.signal,
    handlers({
      message: async (text) => {
        frames.push(text);
        if (frames.length === 1) {
          entered.resolve();
          await gate.promise;
        }
      },
      overflow: async () => {
        overflowCount++;
        overflow.resolve();
      },
    }),
  );
  try {
    const [socket] = await once(server, "connection");
    await once(socket, "message");
    socket._socket.cork();
    for (let i = 0; i < 200; i++) socket.send(JSON.stringify({ i }));
    socket._socket.uncork();
    await timeout(entered.promise);
    gate.resolve();
    await timeout(overflow.promise);
    assert.equal(overflowCount, 1);
    socket.send(JSON.stringify({ i: "after" }));
    await until(() => frames.some((x) => JSON.parse(x).i === "after"));
  } finally {
    gate.resolve();
    abort.abort();
    for (const s of server.clients) s.terminate();
    await new Promise((r) => server.close(r));
  }
});

test("retired registration/raw authority boundaries refuse rather than fabricate alternate progress", async () => {
  assert.equal(runtime.websocketTransport, undefined);
  assert.equal(runtime.httpTransport, undefined);
  assert.equal(runtime.Client.prototype.subscribe, undefined);
  assert.equal(runtime.Client.prototype.unsubscribe, undefined);
  assert.equal(runtime.Client.prototype.mutate, undefined);
  const f = await openClient();
  try {
    assert.equal(f.client.freeze, undefined);
    assert.equal(f.client.applyPull, undefined);
    await assert.rejects(
      f.client.connect(async () => ""),
      /requires server/,
    );
  } finally {
    await f.close();
  }
});

// Every runtime route posts to its own path; a route the transport does not
// know is refused rather than posted to another one (#173).
test("the HTTP transport maps every runtime route and refuses an unknown one", async () => {
  const { createServer } = await import("node:http");
  const paths = [];
  const server = createServer((req, res) => {
    paths.push(req.url);
    req.resume();
    req.on("end", () => res.end("{}"));
  });
  server.listen(0);
  await once(server, "listening");
  try {
    const live = createServerConnection({
      url: `http://127.0.0.1:${server.address().port}`,
      token: "secret",
    });
    for (const route of ["handshake", "push", "pull", "action", "fetch", "materialize"])
      assert.equal(await timeout(live.push(route, "{}")), "{}");
    assert.deepEqual(paths, [
      "/sync/handshake",
      "/sync/mutations",
      "/sync/pull",
      "/sync/actions",
      "/sync/fetch",
      "/sync/materialize",
    ]);
    await assert.rejects(live.push("load", "{}"), /unknown route load/);
    await assert.rejects(live.push("nope", "{}"), /unknown route nope/);
    await assert.rejects(live.push("toString", "{}"), /unknown route toString/);
    assert.equal(paths.length, 6, "nothing was posted for an unknown route");
  } finally {
    await new Promise((r) => server.close(r));
  }
});
/** A fake backend admitting only `x-app-build` 7 or later, on every route and the upgrade (#181). */
async function admissionServer() {
  const seen = [];
  const state = { marked: true };
  const refusal = '{"minimumBuild":7}';
  const old = (req) => Number(req.headers["x-app-build"]) < 7;
  const server = createServer(async (req, res) => {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    seen.push([req.url, req.headers["x-app-build"], req.headers.authorization]);
    if (old(req)) {
      res.writeHead(426, {
        "content-type": "application/json",
        ...(state.marked ? { "axton-admission": "refused" } : {}),
      });
      res.end(refusal);
      return;
    }
    const body = JSON.parse(Buffer.concat(chunks));
    assert.equal(body.protocol, 5);
    res.end(
      JSON.stringify(
        body.mutations ? receiptFor(body)
          : req.url === "/sync/materialize" ? {requestId:body.requestId,delivery:delivery05(body)}
          : JSON.parse(emptyPull(body)),
      ),
    );
  });
  const ws = new WebSocketServer({ noServer: true });
  server.on("upgrade", (req, socket, head) => {
    seen.push([req.url, req.headers["x-app-build"], req.headers.authorization]);
    if (old(req)) {
      socket.end(
        `HTTP/1.1 426 Upgrade Required\r\nContent-Type: application/json\r\n${state.marked ? "axton-admission: refused\r\n" : ""}Content-Length: ${refusal.length}\r\n\r\n${refusal}`,
      );
      return;
    }
    ws.handleUpgrade(req, socket, head, (s) =>
      s.on("message", (m) => { const body=JSON.parse(m); s.send(JSON.stringify(emptyHandshake(body))); }),
    );
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  return {
    seen,
    state,
    url: `http://127.0.0.1:${server.address().port}`,
    async close() {
      for (const s of ws.clients) s.terminate();
      await new Promise((r) => ws.close(r));
      await new Promise((r) => server.close(r));
    },
  };
}
test("client headers reach every HTTP route and the upgrade; AXTON's own headers cannot be replaced", async () => {
  const server = await admissionServer();
  try {
    const live = createServerConnection({
      url: server.url,
      token: "secret",
      headers: { "x-app-build": "7" },
    });
    for (const route of ["handshake", "push", "pull", "action", "fetch", "materialize"])
      await timeout(live.push(route, subscribe));
    const opened = Promise.withResolvers();
    const abort = new AbortController();
    live.open(
      subscribe,
      abort.signal,
      handlers({ message: async () => opened.resolve() }),
    );
    await timeout(opened.promise);
    abort.abort();
    assert.deepEqual(server.seen, [
      ["/sync/handshake", "7", "Bearer secret"],
      ["/sync/mutations", "7", "Bearer secret"],
      ["/sync/pull", "7", "Bearer secret"],
      ["/sync/actions", "7", "Bearer secret"],
      ["/sync/fetch", "7", "Bearer secret"],
      ["/sync/materialize", "7", "Bearer secret"],
      ["/sync/live", "7", "Bearer secret"],
    ]);
    for (const name of [
      "Authorization",
      "content-type",
      "Sec-WebSocket-Key",
      "upgrade",
    ])
      assert.throws(
        () =>
          createServerConnection({
            url: server.url,
            token: "t",
            headers: { [name]: "x" },
          }),
        /reserved header/,
      );
    assert.throws(
      () =>
        createServerConnection({
          url: server.url,
          token: "t",
          headers: { "x-build": 7 },
        }),
      /header x-build must be a string/,
    );
  } finally {
    await server.close();
  }
});
test("a marked refusal carries its status and body on HTTP and on the upgrade; an unmarked 426 is an ordinary failure", async () => {
  const server = await admissionServer();
  try {
    const live = createServerConnection({
      url: server.url,
      token: "secret",
      headers: { "x-app-build": "6" },
    });
    const refused = async () => {
      const http = await timeout(
        live.push("pull", "{}").then(
          () => assert.fail("admitted"),
          (e) => e,
        ),
      );
      const closed = Promise.withResolvers();
      live.open(
        subscribe,
        new AbortController().signal,
        handlers({ closed: closed.resolve }),
      );
      const upgrade = await timeout(closed.promise);
      return [http, upgrade].map((e) => [e.status, e.refusal]);
    };
    assert.deepEqual(await refused(), [
      [426, '{"minimumBuild":7}'],
      [426, '{"minimumBuild":7}'],
    ]);
    server.state.marked = false;
    assert.deepEqual(await refused(), [
      [426, undefined],
      [426, undefined],
    ]);
  } finally {
    await server.close();
  }
});

test("marked admission refusal reports once, stops transport and reconnect preserves pending named work", async () => {
  const f = await openClient(),
    errors = [],
    server = await admissionServer();
  try {
    const call = await publish(f.client);
    await f.client.connect(
      { url: server.url, token: "secret", headers: { "x-app-build": "6" } },
      {
        onError: (e) => errors.push(e),
        refreshAuth: async () => assert.fail("admission is not auth"),
      },
    );
    await until(() =>
      errors.some((e) => e instanceof runtime.AdmissionRefused),
    );
    const seen = server.seen.length;
    await new Promise((r) => setTimeout(r, 600));
    assert.equal(errors.length, 1);
    assert.equal(server.seen.length, seen);
    assert.equal(errors[0].status, 426);
    assert.deepEqual(errors[0].body, { minimumBuild: 7 });
    assert.equal((await f.client.syncState()).pending, 1);
    await f.client.connect(
      { url: server.url, token: "secret", headers: { "x-app-build": "7" } },
      { onError: (e) => errors.push(e) },
    );
    assert.equal((await timeout(call.wait())).result.entry.text, "canonical");
    assert.equal(errors.length, 1);
    assert.ok(
      server.seen
        .slice(seen)
        .every(([, b, t]) => b === "7" && t === "Bearer secret"),
    );
  } finally {
    await f.close();
    await server.close();
  }
});
