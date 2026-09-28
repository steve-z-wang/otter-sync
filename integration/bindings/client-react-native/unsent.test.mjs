// The React Native host shares the TypeScript unsent-work client (#186, #205,
// #204); this runs it through its own transaction adapter and server
// connection over the real native runtime and SQLite.
import test from "node:test";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { createServerConnection } from "../../../packages/client-react-native/live.mts";
import { unsentSuite } from "../client-js/unsent-harness.mjs";

unsentSuite(test, { Transaction, createServerConnection });
