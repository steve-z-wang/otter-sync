import { Book, Entry, createBackend, devAuth, type Handlers, type Loaders, type Mutations } from "./backend.ts";
type Tx = { rows: Map<string, object> };
export const handlers: Handlers<Tx> = {
  // An ordinary write: the target record is stamped and read back without any Channel membership.
  async createEntry({ input, tx }) { tx.rows.set(input.entry.id, input.entry); },
  editEntry: {
    async v1({ input, channel }) { channel("c").entry.add(input.target.identity); },
    // A touch declares a changed record; membership is added or removed per Channel.
    async v2({ input, channel, touch }) { touch.entry(input.entry.identity); channel("c").entry.add(input.entry.identity); channel("audit").entry.remove(input.entry.identity); },
  },
  removeEntries: {
    async v1({ input, channel }) { channel("c").add(input.entries.map(({ identity }) => Entry(identity))); },
    async v2({ input, channel }) { channel("c").remove(input.entries.map(({ identity }) => Entry(identity))); },
  },
  async addBook({ input, channel, touch }) { touch.book({ id: input.book.id }); channel("c").add([]); channel("c").add([Book(input.book)]); },
  async addComment({ input, channel }) { channel("c").comment.add(input.comment); },
  // Handlers receive the client-expanded create: defaulted fields are present and required (#27).
  async addDraft({ input, tx }) { const { id, created, body }: { id: string; created: Date; body: string } = input.draft; tx.rows.set(id, { created, body }); },
};
// Transactional Mutation enqueue: the handler receives only the declared
// business args; a local companion never reaches the backend.
export const mutations: Mutations<Tx> = {
  async publishEntry({ ctx, args }) { ctx.tx.rows.set(args.entry.id, args.entry); return { published: { id: args.entry.id } }; },
  async rename({ ctx, args }) { ctx.tx.rows.set(args.id, { title: args.title }); },
};
export const loaders: Loaders<Tx> = {
  entry: {
    async v1({ ids }) { return ids.map((id) => ({ ...id, title: "old", note: null, at: new Date(0), status: "active" })); },
    async v2({ ids }) { return ids.map((id) => ({ ...id, title: "new", note: null, at: new Date(0), tags: [], status: "active" })); },
  },
  async book({ ids }) { return ids.map(() => null); },
  async comment({ ids }) { return ids.map(() => null); },
  async counter({ ids }) { return ids.map(() => null); },
  async composition({ ids }) { return ids.map(() => null); },
  async draft({ ids }) { return ids.map(() => null); },
  // A composite identity keeps every component, DateTime included.
  async placement({ ids }) { return ids.map((id) => ({ ...id, label: id.at.toISOString() })); },
};
export const backend = createBackend<Tx>({
  database: { transaction: async (body) => body({ rows: new Map() }), persistence: () => ({ call: async () => null }) },
  authenticate: devAuth(),
  handlers,
  mutations,
  loaders,
  native: { validateConfig() {}, processPush: async () => "", processAction: async () => "", processFetch: async () => "", processPull: async () => "", validateLoadBatch: () => [], encodeLoadBatch: () => "", processLoad: async () => "", settleExternal: async () => "", negotiateLive: async () => "", pullLive: async () => "", liveEvent: () => "[]", liveClose() {} },
});
// An external write declares through the same handles and answers its own value.
export const external: Promise<number> = backend.transaction(async ({ tx, channel, touch }) => { tx.rows.set("b", {}); touch.book({ id: "b" }); channel("c").book.add({ id: "b" }); return tx.rows.size; });
