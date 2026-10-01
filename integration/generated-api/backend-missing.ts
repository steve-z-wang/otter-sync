import type { Handlers } from "./backend.ts";
type Tx = { rows: Map<string, object> };
export const handlers: Handlers<Tx> = {
  async createEntry({ input, stream: scope }) { scope("c").track.entry(input.entry); },
  editEntry: {
    async v1({ input, stream: scope }) { scope("c").track.entry(input.target.identity); },
    async v2({ input, stream: scope }) { scope("c").track.entry(input.entry.identity); },
  },
  removeEntries: {
    async v1({ input, invalidate: touch }) { for (const { identity } of input.entries) touch.entry(identity); },
    async v2({ input, invalidate: touch }) { for (const { identity } of input.entries) touch.entry(identity); },
  },
  async addBook({ input, stream: scope }) { scope("c").track.book(input.book); },
};
