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
  // @ts-expect-error Query snapshots are fresh; once cache is retired.
  client.queries.searchTodos({}, { once: true });
  // @ts-expect-error There is no public Query invalidation facade.
  client.queries.invalidate.searchTodos({});
  Client.open({
    path: "unused",
    stream: "User:alice",
    connection: {
      url: "http://unused",
      token: "alice",
    },
    // @ts-expect-error Store hooks were replaced by typed owned companions.
    onStore: () => {},
  });
}
void retired;
