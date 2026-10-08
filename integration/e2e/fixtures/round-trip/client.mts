import { createInterface } from "node:readline";
import { resolve } from "node:path";
import { GeneratedClient } from "./generated/client.ts";
const client = await GeneratedClient.open({
  path: resolve(process.env.AXTON_DATABASE ?? "example-client.sqlite"),
  stream: "User:demo-user",
  connection: {
    url: process.env.AXTON_URL ?? "http://127.0.0.1:4242",
    token: "demo-user",
  },
});
await client.bootstrap();
const stop = client.models.entry.watch({}, (rows) =>
  console.log(rows.find((row) => row.id === "entry-1") ?? null),
);
const terminal = createInterface({ input: process.stdin });
console.log("Commands: edit TEXT | offline | online | status | quit");
try {
  for await (const line of terminal) {
    if (line === "quit") break;
    if (line.startsWith("edit "))
      await client.mutations.editEntry({
        entry: { id: "entry-1", text: line.slice(5) },
      });
    else if (line === "offline") {
      await client.connection!.pause();
      console.log("Sync paused.");
    } else if (line === "online") {
      await client.connection!.resume();
      console.log("Sync resumed.");
    } else if (line === "status")
      console.log(JSON.stringify(await client.syncState()));
  }
} finally {
  stop();
  terminal.close();
  await client.close();
}
