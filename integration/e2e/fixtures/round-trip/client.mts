import { createInterface } from "node:readline/promises";
import { resolve } from "node:path";
import { stdin, stdout } from "node:process";
import { GeneratedClient } from "./generated/client.ts";

// Open the local database and start syncing with the backend.
const client = await GeneratedClient.open({
  path: resolve(process.env.AXTON_DATABASE ?? "example-client.sqlite"),
  server: {
    url: process.env.AXTON_URL ?? "http://127.0.0.1:4242",
    token: "demo-user",
  },
  connection: { onError: (error) => console.error(`sync: ${String(error)}`) },
});
// A subscription is durable and starts at the first head the server
// acknowledges: it delivers what is published from then on, not the records the
// scope already held (#150; the explicit whole-Scope load is #151's
// `bootstrap()`). The status line shows when this client is live.
const subscription = await client.scopes.subscribe("book:demo");
subscription.watch((status) =>
  console.log(`book:demo ${status.initialization}/${status.connection}`),
);

// Print the entry whenever it changes: first the local edit, then the server's version.
client.models.entry.watch({}, (rows) => console.log(rows[0] ?? null));

const terminal = createInterface({ input: stdin, output: stdout });
console.log(
  "Commands: edit TEXT | offline | online | status | quit. Edits apply locally at once and sync in the background.",
);
try {
  for (;;) {
    const line = await terminal.question("> ");
    try {
      if (line === "quit") break;
      if (line.startsWith("edit ")) {
        await client.mutate.edit({
          entry: {
            identity: { id: "entry-1" },
            values: { text: line.slice(5) },
          },
        });
      } else if (line === "offline") {
        await client.connection!.pause();
        console.log("Sync paused. Local reads and writes remain available.");
      } else if (line === "online") {
        await client.connection!.resume();
        console.log("Sync resumed.");
      } else if (line === "status") console.log(await client.syncState());
    } catch (error) {
      console.error(String(error));
    }
  }
} finally {
  terminal.close();
  await client.close();
}
