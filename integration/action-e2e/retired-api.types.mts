// These compile-time refusals fence the removed 0.3 write/read/hook facades.
import type { GeneratedClient } from "./client.ts";
import { GeneratedClient as Client } from "./client.ts";
function retired(client: GeneratedClient) {
  // @ts-expect-error A Store follows its bound Stream; no subscription registry.
  client.streams.subscribe("other");
  // @ts-expect-error Only named generated Mutation inputs reach the wire.
  client.mutations.call("Edit", {});
  // @ts-expect-error Queries are invocation reads, never durable Mutation jobs.
  client.queries.enqueue.searchTodos({});
  // @ts-expect-error No anonymous top-level write lane.
  client.mutate([]);
  Client.open({
    path: "unused",
    stream: "User:alice",
    connection: {
      url: "http://unused",
      token: "alice",
      identity: {
        backend: "action-e2e",
        viewer: "alice",
        contract: "action-v04",
      },
    },
    // @ts-expect-error Store hooks were replaced by typed owned companions.
    onStore: () => {},
  });
}
void retired;
