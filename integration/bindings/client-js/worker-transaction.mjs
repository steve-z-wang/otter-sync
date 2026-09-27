// Worker body for `worker.test.mjs`: opens a client on `workerData.path`,
// writes inside a transaction whose callback never finishes, and says so.
import { parentPort, workerData } from "node:worker_threads";
import { Client } from "../../../packages/client-js/index.mts";

const client = await Client.open({ path: workerData.path, schema: workerData.schema });
void client.transaction(async (tx) => {
  await tx.direct({ model: "Entry", op: "create", identity: { id: "worker" }, values: { text: "held" } });
  parentPort.postMessage("holding");
  await new Promise(() => {});
});
