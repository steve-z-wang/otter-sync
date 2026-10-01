import type { Handlers } from "./backend.ts";
type Tx = { rows: Map<string, object> };
export const handlers: Handlers<Tx> = {
  async createEntry({ input, scope: scope }) { scope("c").add.entry(input.entry); },
  editEntry: {
    async v1({ input, scope: scope }) { scope("c").add.entry(input.target.identity); },
    async v2({ input, scope: scope }) { scope("c").add.entry(input.entry.identity); },
  },
  removeEntries: {
    async v1({ input, touch }) { for (const { identity } of input.entries) touch.entry(identity); },
    async v2({ input, touch }) { for (const { identity } of input.entries) touch.entry(identity); },
  },
  async addBook({ input, scope: scope }) { scope("c").add.book(input.book); },
};
