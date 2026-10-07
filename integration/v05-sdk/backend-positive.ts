import { type Options, createBackend } from "./backend.ts";
declare const database: Options<object>["database"];
const options: Options<object> = {
  database,
  authenticate: () => "alice",
  protocol5: {
    projectionGeneration: "1",
    authorizeStream: (principal, stream) => stream === `User:${principal}`,
  },
  mutations: {
    publish: async ({ args }) => ({ entry: { id: args.entry.id } }),
  },
  queries: {
    find: async () => ({ entry: null }),
    peek: async () => ({ entry: null }),
  },
  loaders: { entry: async ({ ids }) => ids.map(() => null) },
};
createBackend(options);
