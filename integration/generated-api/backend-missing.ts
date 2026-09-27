import type { Handlers } from "./backend.ts";
type Tx = { rows: Map<string, object> };
export const handlers: Handlers<Tx> = {
  async createEntry({ input, channel }) { channel("c").entry.add(input.entry); },
  editEntry: {
    async v1({ input, channel }) { channel("c").entry.add(input.target.identity); },
    async v2({ input, channel }) { channel("c").entry.add(input.entry.identity); },
  },
  removeEntries: {
    async v1({ input, touch }) { for (const { identity } of input.entries) touch.entry(identity); },
    async v2({ input, touch }) { for (const { identity } of input.entries) touch.entry(identity); },
  },
  async addBook({ input, channel }) { channel("c").book.add(input.book); },
};
