import * as FileSystem from "expo-file-system/legacy";
import { requireNativeModule } from "expo-modules-core";
import { databasePath } from "../../../packages/client-react-native/index";
import { GeneratedClient } from "./generated/client";

type Config = {
  user: string;
  phase: string;
  url: string;
  expectedClientId?: string;
};
const documents = FileSystem.documentDirectory!;
async function writeResult(result: object) {
  const temporary = documents + "result.tmp";
  await FileSystem.writeAsStringAsync(temporary, JSON.stringify(result));
  await FileSystem.moveAsync({
    from: temporary,
    to: documents + "result.json",
  });
}
function check(condition: unknown, message: string): asserts condition {
  if (!condition) throw Error(message);
}
async function until(predicate: () => Promise<boolean>, message: string) {
  const deadline = Date.now() + 120000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw Error(`Timed out: ${message}`);
}

/** Integration assertions only; no product UI or synthetic sync state. */
export async function runSmoke(show: (message: string) => void) {
  let client: GeneratedClient | undefined;
  let config: Config | undefined;
  try {
    config = JSON.parse(
      await FileSystem.readAsStringAsync(documents + "config.json"),
    );
    check(config, "missing config");
    const { user, phase, url, expectedClientId } = config;
    show(`${user}: ${phase}`);
    // The raw carrier, deliberately below the package's Client: a malformed
    // open request and an envelope for a runtime that was never opened are
    // refused synchronously, before any runtime or task exists.
    const native = requireNativeModule<{
      runtimeOpen(request: string): string;
      runtimeSubmit(runtimeId: string, message: string): void;
    }>("AxtonNative");
    const refuses = (attempt: () => unknown) => {
      try {
        attempt();
        return false;
      } catch {
        return true;
      }
    };
    check(
      refuses(() => native.runtimeOpen("{")),
      "malformed native input must reject",
    );
    check(
      refuses(() => native.runtimeSubmit("0", '{"type":"close"}')),
      "an unopened runtime must refuse admission",
    );
    let pathRejected = false;
    try {
      await databasePath("../invalid.sqlite");
    } catch {
      pathRejected = true;
    }
    check(pathRejected, "database path traversal must reject");
    const path = await databasePath("smoke.sqlite");
    client = await GeneratedClient.open({
      path,
      server: { url, token: user },
      connection: {
        onError: (error) => console.log("transport:", String(error)),
      },
    });
    const current = client;
    if (expectedClientId)
      check(
        current.clientId === expectedClientId,
        "client identity changed across restart",
      );
    // The seeded row was published before this installation's subscription had
    // an origin, so subscribing does not deliver it
    // ([#150](https://github.com/zanminwang/axton/issues/150)). The explicit
    // load is what brings it ([#151](https://github.com/zanminwang/axton/issues/151));
    // it registers its work when the call is made and runs in the background.
    const subscription = await current.channels.subscribe("book:demo");
    subscription
      .bootstrap()
      .catch((error) => console.log("bootstrap:", String(error)));
    let watched = new Map<string, string>();
    current.models.entry.watch({}, (rows) => {
      watched = new Map(rows.map((row) => [row.id, row.text]));
    });
    const text = async (id = "entry-1") =>
      (await current.models.entry.get({ id }))?.text;
    const settled = () =>
      until(
        async () => (await current.syncState()).pending === 0,
        "pending queue drains",
      );
    if (phase === "online") {
      await until(async () => !!(await text()), "initial catch-up");
      let rolledBack = false;
      try {
        await current.transaction(async (tx) => {
          await tx.models.entry.create({
            id: "rollback",
            text: "discard",
            note: null,
          });
          throw Error("rollback test");
        });
      } catch {
        rolledBack = true;
      }
      check(
        rolledBack &&
          (await current.models.entry.get({ id: "rollback" })) === null,
        "transaction rollback failed",
      );
      if (user === "alice") {
        await current.mutate.edit({
          entry: {
            identity: { id: "entry-1" },
            values: { text: "from alice" },
          },
        });
        await until(
          async () => (await text()) === "from bob",
          "Bob live update",
        );
      } else {
        await until(
          async () => (await text()) === "from alice",
          "Alice live update",
        );
        await current.mutate.edit({
          entry: {
            identity: { id: "entry-1" },
            values: { text: "from bob" },
          },
        });
      }
      await settled();
    } else if (phase === "offline") {
      check((await text()) === "from bob", "cached record missing offline");
      await current.mutate.addEntry({
        entry: { id: "offline", text: "offline create", note: null },
      });
      await current.mutate.edit({
        entry: {
          identity: { id: "offline" },
          values: { text: "offline edited" },
        },
      });
      await until(
        async () => watched.get("offline") === "offline edited",
        "offline local watch",
      );
      check(
        (await current.syncState()).pending === 2,
        "offline queue must contain create and dependent edit",
      );
    } else if (phase === "restart") {
      check(
        (await text("offline")) === "offline edited",
        "offline work lost across process restart",
      );
      check(
        (await current.syncState()).pending === 2,
        "queued work lost across process restart",
      );
    } else if (phase === "remote") {
      await current.mutate.edit({
        entry: {
          identity: { id: "entry-1" },
          values: { text: "remote while offline" },
        },
      });
      await settled();
    } else if (phase === "settle" || phase === "observe") {
      await until(
        async () =>
          (await text("offline")) === "offline edited" &&
          (await text()) === "remote while offline",
        "authoritative convergence",
      );
      await settled();
    } else throw Error(`Unknown phase: ${phase}`);
    const status = await current.syncState();
    const rows = await current.models.entry.query();
    const result = {
      ok: true,
      user,
      phase,
      clientId: current.clientId,
      pending: status.pending,
      rows,
    };
    await writeResult(result);
    show(
      `PASS ${user}: ${phase}\nclient ${current.clientId}\npending ${status.pending}\n${rows.map((row) => `${row.id}: ${row.text}`).join("\n")}`,
    );
    // Keep the real connection alive until the runner terminates the process.
  } catch (error) {
    const result = { ok: false, phase: config?.phase, error: String(error) };
    await writeResult(result);
    show(`FAIL ${String(error)}`);
    await client?.close();
  }
}
