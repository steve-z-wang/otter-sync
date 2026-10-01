import assert from "node:assert/strict";
import { drizzle, bindDrizzle } from "@axtonjs/postgres/drizzle";
assert.equal(typeof drizzle, "function");
assert.equal(typeof bindDrizzle("SELECT $1", [1]), "object");
console.log("import: postgres/drizzle with drizzle-orm installed");
