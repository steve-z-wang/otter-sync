// The React Native host shares the TypeScript prerequisite runner (#185);
// this runs it through its own transaction adapter and server connection over
// the real native runtime and SQLite.
import test from "node:test";
import { Transaction } from "../../../packages/client-react-native/transaction.mts";
import { createServerConnection } from "../../../packages/client-react-native/live.mts";
import { prerequisiteSuite } from "../client-js/prerequisite-harness.mjs";

prerequisiteSuite(test, { Transaction, createServerConnection });
