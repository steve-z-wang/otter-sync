// A client in its own process, so a test can kill it at an exact point and
// restart it on the same SQLite file. Every step is one JSON line on stdout,
// written synchronously so it is out before a kill.
//
//   start  URL PATH PROJECT [KILL_ITEM]  start ProjectItems(PROJECT); with
//                                        KILL_ITEM, SIGKILL itself inside the
//                                        onStore hook of the page storing it
//   resume URL PATH LOAD_ID              reattach and wait for completion
import { writeSync } from "node:fs";
import { GeneratedClient, type GeneratedTransaction, type StoreChange, type Item, type ItemIdentity } from "./client.ts";

/** The onStore bookkeeping every Load test client uses: one Seen row per stored Item, counting committed hook runs. */
export async function countSeen(tx: GeneratedTransaction, changes: readonly StoreChange<ItemIdentity, Item>[]) {
  for (const change of changes) {
    if (change.kind !== "upsert") continue;
    const seen = await tx.models.seen.get({ id: change.identity.id });
    if (seen) await tx.models.seen.update({ id: change.identity.id }, { hits: seen.hits + 1 });
    else await tx.models.seen.create({ id: change.identity.id, hits: 1 });
  }
}

const emit = (event: Record<string, unknown>) => { writeSync(1, `${JSON.stringify(event)}\n`); };

async function main([mode, url, path, subject, killItem]: string[]) {
  const client = await GeneratedClient.open({
    path,
    server: { url: url!, token: "alice" },
    onStore: {
      async item(tx, changes) {
        await countSeen(tx, changes);
        if (killItem !== undefined && changes.some((change) => change.identity.id === killItem)) {
          // The page's Seen writes are made but not committed.
          emit({ event: "applying", ids: changes.map((change) => change.identity.id) });
          process.kill(process.pid, "SIGKILL");
        }
      },
    },
  });
  try {
    const load = mode === "start" ? await client.loads.projectItems({ project: subject! }) : await client.loads.get(subject!);
    if (load === null) throw Error(`no Load ${subject}`);
    emit({ event: mode === "start" ? "started" : "resumed", loadId: load.id, status: load.status });
    await load.wait();
    // The runtime's own record of the job, not this handle's last snapshot.
    const status = (await client.loads.list({ limit: 100 })).find((candidate) => candidate.id === load.id);
    emit({ event: "complete", status });
  } finally {
    await client.close();
  }
}

if (import.meta.main) {
  main(process.argv.slice(2)).catch((error) => { emit({ event: "error", message: String(error?.stack ?? error) }); process.exitCode = 1; });
}
