// Real native local scope tests shared by the Node and React Native adapters.
// Backend settlement is covered by integration/v05-sdk/run-host.sh.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { readFile, mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { createClient } from "../../../packages/frontend/client-js/api/runtime.mts";
const native = createRequire(import.meta.url)(
  "../../../bindings/node/axton-node.node",
);
const schema = JSON.parse(
  await readFile(new URL("../../v05-sdk/schema.json", import.meta.url), "utf8"),
);
const connection = { url: "http://127.0.0.1:1", token: "offline" };
const input = (id) => ({ entry: { id, text: "published" }, call: "legal" });
const create = (id, text) => ({
  model: "Draft",
  op: "create",
  identity: { id },
  values: { text },
});
const remove = (id) => ({ model: "Draft", op: "delete", identity: { id } });
const code = (expected) => (error) => {
  assert.equal(error.code, expected);
  return true;
};
export function mutationTests(test, Transaction, { savepoints, exactGuard }) {
  const Client = createClient(native, Transaction, () => ({
    open() {},
    push: async () => {
      throw Error("offline");
    },
  }));
  async function run(body) {
    const directory = await mkdtemp(join(tmpdir(), "axton-scopes05-"));
    const path = join(directory, "db");
    let client = await Client.open({
      path,
      schema,
      stream: "User:alice",
      connection,
    });
    try {
      await body({
        get client() {
          return client;
        },
        queued: () =>
          Number(
            execFileSync(
              process.execPath,
              [
                "-e",
                "const {DatabaseSync}=require('node:sqlite');const db=new DatabaseSync(process.argv[1],{readOnly:true});try{process.stdout.write(String(db.prepare('SELECT count(*) AS n FROM axton_mutation_queue WHERE reconciled=0 AND rejection_code IS NULL').get().n));}finally{db.close();}",
                path,
              ],
              { encoding: "utf8" },
            ),
          ),
        reopen: async () => {
          await client.close();
          return (client = await Client.open({
            path,
            schema,
            stream: "User:alice",
            connection,
          }));
        },
      });
    } finally {
      await client.close();
      await rm(directory, { recursive: true, force: true });
    }
  }
  test("committed pending Call waits reject inside the owning transaction without settling the Call", () =>
    run(async ({ client }) => {
      const call = await client.submitMutation(
        "Publish",
        1,
        input("waiting"),
        (x) => x,
      );
      const outside = call.wait();
      await client.transaction(async () => {
        let timer;
        try {
          await assert.rejects(
            Promise.race([
              call.wait(),
              new Promise(
                (_, reject) =>
                  (timer = setTimeout(
                    () => reject(Error("wait admission timed out")),
                    200,
                  )),
              ),
            ]),
            /transaction_active/,
          );
        } finally {
          clearTimeout(timer);
        }
        assert.equal(call.status, "pending");
      });
      await client.resetStore({ discardPending: true });
      assert.equal((await outside).error.code, "abandoned");
      await client.transaction(async () =>
        assert.equal((await call.wait()).error.code, "abandoned"),
      );
    }));
  test("Mutation callback precedes optimism and commits companions and durable input together", () =>
    run(async ({ client, queued }) => {
      await client.direct(create("d", "draft"));
      const result = await client.transaction(async (tx) => {
        const call = await tx.submitMutation(
          "Publish",
          1,
          async (local) => {
            assert.equal(await local.read("Entry", { id: "p" }), null);
            assert.equal(
              (await local.read("Draft", { id: "d" })).text,
              "draft",
            );
            await local.direct(remove("d"));
            return input("p");
          },
          (value) => value,
        );
        assert.equal(queued(), 0);
        await assert.rejects(call.wait(), code("transaction_uncommitted"));
        assert.equal((await tx.read("Entry", { id: "p" })).text, "published");
        return { call, value: 7 };
      });
      assert.equal(result.value, 7);
      assert.equal(queued(), 1);
      assert.equal(result.call.status, "pending");
      assert.equal(await client.read("Draft", { id: "d" }), null);
    }));
  test("several named Calls commit together and retain independent durable identities", () =>
    run(async ({ client, queued, reopen }) => {
      const calls = await client.transaction(async (tx) => [
        await tx.submitMutation("Publish", 1, input("a"), (x) => x),
        await tx.submitMutation("Publish", 1, input("b"), (x) => x),
      ]);
      assert.equal(queued(), 2);
      assert.equal(calls.length, 2);
      client = await reopen();
      assert.equal((await client.read("Entry", { id: "a" })).text, "published");
      assert.equal((await client.read("Entry", { id: "b" })).text, "published");
    }));
  test("a leaked Call from a failed transaction is rolled back and never queued", () =>
    run(async ({ client, queued }) => {
      let call;
      await assert.rejects(
        client.transaction(async (tx) => {
          call = await tx.submitMutation("Publish", 1, input("p"), (x) => x);
          throw Error("rollback");
        }),
        /rollback/,
      );
      assert.equal(queued(), 0);
      await assert.rejects(call.wait(), code("transaction_rolled_back"));
      assert.equal(await client.read("Entry", { id: "p" }), null);
    }));
  test("a failed or invalid-input callback rolls back its companions", () =>
    run(async ({ client, queued }) => {
      await assert.rejects(
        client.submitMutation(
          "Publish",
          1,
          async (local) => {
            await local.direct(create("bad", "rollback"));
            throw Error("body failed");
          },
          (x) => x,
        ),
        /body failed/,
      );
      await assert.rejects(
        client.submitMutation(
          "Publish",
          1,
          async (local) => {
            await local.direct(create("invalid", "rollback"));
            return { entry: { id: "p" }, call: "bad" };
          },
          (x) => x,
        ),
      );
      assert.equal(await client.read("Draft", { id: "bad" }), null);
      assert.equal(await client.read("Draft", { id: "invalid" }), null);
      assert.equal(queued(), 0);
    }));
  test("companion capability exposes only local operations and expires after its callback", () =>
    run(async ({ client }) => {
      let escaped;
      await client.submitMutation(
        "Publish",
        1,
        async (local) => {
          escaped = local;
          for (const name of [
            "submitMutation",
            "streams",
            "fetch",
            "transaction",
            "savepoint",
          ])
            assert.equal(name in local, false);
          return input("p");
        },
        (x) => x,
      );
      await assert.rejects(
        escaped.read("Entry", { id: "p" }),
        /transaction_closed|closed/,
      );
    }));
  test("captured parent and outer-client remote reads fail during the owned callback", () =>
    run(async ({ client, queued }) => {
      await assert.rejects(
        client.transaction(async (tx) => {
          await tx.submitMutation(
            "Publish",
            1,
            async (local) => {
              await assert.rejects(
                tx.read("Entry", { id: "p" }),
                /invalid transaction capability/,
              );
              await assert.rejects(
                client.fetchModel("Entry", 1, { id: "p" }, (x) => x),
                (error) =>
                  error.code === "transaction_active" ||
                  /transaction_active/.test(String(error)),
              );
              return input("p");
            },
            (x) => x,
          );
        }),
        /invalid transaction capability/,
      );
      assert.equal(queued(), 0);
    }));
  test("unawaited companion operations prevent commit", () =>
    run(async ({ client, queued }) => {
      await assert.rejects(
        client.submitMutation(
          "Publish",
          1,
          async (local) => {
            void local.direct(create("bad", "rollback"));
            return input("p");
          },
          (x) => x,
        ),
        /unawaited/,
      );
      assert.equal(queued(), 0);
      assert.equal(await client.read("Draft", { id: "bad" }), null);
    }));
  test("close during a callback releases the Store and commits no pending scope", () =>
    run(async ({ client, queued }) => {
      let release, entered;
      const gate = new Promise((r) => (release = r)),
        started = new Promise((r) => (entered = r));
      const pending = client.submitMutation(
        "Publish",
        1,
        async (local) => {
          entered();
          await gate;
          return input("p");
        },
        (x) => x,
      );
      const rejected = assert.rejects(pending);
      await started;
      await client.close();
      release();
      await rejected;
      assert.equal(queued(), 0);
    }));
  if (savepoints)
    test("savepoint rollback discards only its named Call scopes", () =>
      run(async ({ client, queued }) => {
        let abandoned;
        const kept = await client.transaction(async (tx) => {
          const call = await tx.submitMutation(
            "Publish",
            1,
            input("kept"),
            (x) => x,
          );
          await assert.rejects(
            tx.savepoint(async () => {
              abandoned = await tx.submitMutation(
                "Publish",
                1,
                input("lost"),
                (x) => x,
              );
              throw Error("scope failed");
            }),
            /scope failed/,
          );
          return call;
        });
        assert.equal(queued(), 1);
        assert.equal(kept.status, "pending");
        await assert.rejects(abandoned.wait(), code("transaction_rolled_back"));
        assert.equal(await client.read("Entry", { id: "lost" }), null);
      }));
}
