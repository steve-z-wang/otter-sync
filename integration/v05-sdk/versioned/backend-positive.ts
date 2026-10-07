import { type Options, createBackend } from "./backend.ts";
declare const database: Options<object>["database"];
const options: Options<object> = {
  database,
  authenticate: () => "alice",
  protocol5: {
    authorizeStream: (principal, stream) => stream === `User:${principal}`,
  },
  mutations: {
    publish: {
      v1: async ({ args }) => ({ entry: { id: args.entry.id } }),
      v2: async ({ args }) => ({ entry: { id: args.entry.id } }),
    },
  },
  queries: {
    find: {
      v1: async () => ({ entry: null }),
      v2: async () => ({ entry: null }),
    },
    peek: async () => ({ entry: null }),
  },
  loaders: {
    entry: {
      v1: async ({ ids }) => ids.map(() => null),
      v2: async ({ ids }) =>
        ids.map((identity) => ({
          ...identity,
          text: "same row",
          note: "new schema",
        })),
    },
  },
};
createBackend(options);
