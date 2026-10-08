import type { Mutations } from "./backend.ts";
type Tx = { rows: Map<string, object> };
// Missing Rename is a compile error: every declared named Mutation is required.
export const mutations: Mutations<Tx> = {
 async publishEntry({ctx,args}) {ctx.invalidate.entry(args.entry);return {published:{id:args.entry.id}};},
};
