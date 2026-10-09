// Watched read-only SQL over several Models (#184) through the real native
// runtime (shared with React Native).
import test from "node:test";
import { Transaction } from "../../../packages/frontend/client-js/api/transaction.mts";
import { watchSqlTests } from "./watch-sql-harness.mjs";

watchSqlTests(test, Transaction, { exactGuard: true });
