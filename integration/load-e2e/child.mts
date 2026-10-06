import { GeneratedClient } from "./client.ts";
const [url, path, viewer] = process.argv.slice(2) as [string, string, string];
const client = await GeneratedClient.open({
  path,
  stream: `User:${viewer}`,
  connection: {
    url,
    token: viewer,
    identity: { backend: "read-e2e", viewer, contract: "read-v04" },
  },
});
await client.bootstrap();
const call = await client.mutations.renameItem({
  item: { id: `${viewer}-1`, title: "restarted" },
});
await call.wait();
await client.close();
