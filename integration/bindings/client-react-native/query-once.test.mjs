import {openStore} from '../client-js/store-fixture.mjs';
// The React Native host shares the TypeScript runtime; this checks Query
// once through its own transaction adapter and the real native runtime.
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createClient } from "../../../packages/client-js/runtime.mts";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { schema } from "../client-js/query-once.test.mjs";

test("mobile once callers share one request, hit offline and refuse transactions", async () => {
  const native = createRequire(import.meta.url)(
    "../../../bindings/node/axton-node.node",
  );
  const directory = await mkdtemp(join(tmpdir(), "axton-rn-once-"));
  let requests = 0;
  const Client = createClient(native, Transaction, () => ({
    open() {},
    push: async (kind, text) => {
      assert.equal(kind, "action");
      const body = JSON.parse(text);
      requests++;
      await new Promise((resolve) => setTimeout(resolve, 10));
      return JSON.stringify({
        context: body.context,
        completion: {
          callId: body.callId,
          outcome: {
            status: "succeeded",
            result: {
              todos: [{ id: "a", title: "A" }],
              tags: ["t"],
              asOf: "2026-01-02T03:04:05.000Z",
            },
          },
        },
        records: [
          {
            model: "Todo",
            identity: { id: "a" },
            cursor: null,
            state: { title: "A" },
          },
        ],
      });
    },
  }));
  const client = await openStore(Client,{ path: join(directory, "db"), schema });
  const once = () =>
    client.invokeQuery("GetTodos", 1, { project: "p" }, (value) => value, {
      once: true,
    });
  try {
    const connection = await client.connect({
      url: "http://unused",
      token: "token",
    });
    const [first, second] = await Promise.all([once(), once()]);
    assert.equal(requests, 1);
    assert.deepEqual(first, second);
    assert.notStrictEqual(first, second);
    await connection.close();
    assert.deepEqual((await once()).todos, [{ id: "a", title: "A" }]);
    await client.transaction(async () => {
      await assert.rejects(
        once(),
        (error) => error.code === "transaction_active",
      );
    });
    assert.equal(requests, 1);
  } finally {
    await client.close();
    await rm(directory, { recursive: true, force: true });
  }
});
