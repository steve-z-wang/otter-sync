// Prerequisite handlers registered at open (#185) through the Node client:
// the native runtime and SQLite underneath.
import test from "node:test";
import { Transaction } from "../../../packages/client-js/transaction.mts";
import { createServerConnection } from "../../../packages/client-js/live.mts";
import { prerequisiteSuite } from "./prerequisite-harness.mjs";

prerequisiteSuite(test, { Transaction, createServerConnection });
