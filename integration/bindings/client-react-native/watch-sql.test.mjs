// Watched read-only SQL over several Models (#184) through the real native
// runtime (shared with Node): the mobile adapter has a coarse callback guard,
// so a call from inside a callback waits for the commit instead.
import test from "node:test";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { watchSqlTests } from "../client-js/watch-sql-harness.mjs";

watchSqlTests(test, Transaction, { exactGuard: false });
