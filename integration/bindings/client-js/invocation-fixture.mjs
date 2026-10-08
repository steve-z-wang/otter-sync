import test from "node:test";
import assert from "node:assert/strict";
import { createClient } from "../../../packages/client-js/runtime.mts";

export function invocationTests(Transaction) {
  function fixture(connected = false) {
    let wake;
    const events = [],
      requests = [];
    let open;
    const carrier = {
      runtimeOpen(text, notify) {
        open = JSON.parse(text);
        wake = notify;
        events.push({
          type: "taskCompleted",
          requestId: open.requestId,
          ok: true,
          value: { clientId: "native-store", schema: {} },
        });
        queueMicrotask(wake);
        return "1";
      },
      runtimeSubmit(id, text) {
        const input = JSON.parse(text);
        requests.push(input);
        events.push(
          input.type === "close"
            ? { type: "runtimeClosed" }
            : {
                type: "taskCompleted",
                requestId: input.requestId,
                ok: true,
                value: {
                  outcome: { status: "succeeded", result: requests.length },
                },
              },
        );
        queueMicrotask(wake);
      },
      runtimeDrain() {
        return JSON.stringify(events.splice(0));
      },
      runtimeDetach() {},
    };
    return {
      Client: createClient(carrier, Transaction, () => {
        if (!connected) throw Error("offline open connected");
        return {
          open() {},
          async push() {
            throw Error("no network effects expected");
          },
        };
      }),
      requests,
      get open() {
        return open;
      },
    };
  }
  test("offline open carries protocol 5 path/schema/stream without identity", async () => {
    const f = fixture();
    const c = await f.Client.open({
      path: "test.sqlite",
      schema: {},
      stream: "User:u",
    });
    assert.equal(f.open.protocol, 5);
    assert.equal(f.open.stream, "User:u");
    assert.equal(f.open.binding, undefined);
    assert.equal(f.open.projectionGeneration, "1");
    assert.equal(c.connection, undefined);
    await c.close();
  });
  test("connection projection generation is forwarded to the internal open", async () => {
    const f = fixture(true);
    const c = await f.Client.open({
      path: "test.sqlite",
      schema: {},
      stream: "User:u",
      connection: { url: "unused", token: "x", projectionGeneration: "3" },
    });
    assert.equal(f.open.projectionGeneration, "3");
    await c.close();
  });
  test("each Query invocation submits a fresh task with store policy and business arguments", async () => {
    const f = fixture();
    const c = await f.Client.open({
      path: "test.sqlite",
      schema: {},
      stream: "User:u",
    });
    const args = { once: true, refresh: false };
    for (const options of [undefined, { store: true }, { store: false }])
      await c.invokeQuery("Read", 1, args, (x) => x, options);
    assert.equal(f.requests.length, 3);
    assert.deepEqual(
      f.requests.map((x) => x.command.store),
      [undefined, true, false],
    );
    assert.ok(
      f.requests.every(
        (x) => x.command.args.once === true && !("once" in x.command),
      ),
    );
    assert.equal(c.invalidateQuery, undefined);
    await c.close();
  });
  test("removed Query controls fail before submission even when false", async () => {
    const f = fixture();
    const c = await f.Client.open({
      path: "test.sqlite",
      schema: {},
      stream: "User:u",
    });
    for (const options of [
      { once: false },
      { refresh: false },
      { once: undefined },
      { store: "yes" },
    ])
      await assert.rejects(
        c.invokeQuery("Read", 1, {}, (x) => x, options),
        (e) => e.code === "action.invalid_options",
      );
    assert.equal(f.requests.length, 0);
    await c.close();
  });

  test("removed connection identity is refused before opening", async () => {
    const f = fixture();
    await assert.rejects(
      f.Client.open({
        path: "test.sqlite",
        schema: {},
        stream: "User:u",
        connection: {
          url: "unused",
          token: "x",
          identity: { backend: "b", viewer: "u", contract: "c" },
        },
      }),
      /identity.*no longer supported/,
    );
    assert.equal(f.open, undefined);
  });
}
