// The retired Load-job API is replaced by finite Bootstrap. Its offline
// registration, aggregate await and durable resume run through the RN scope.
import test from "node:test";
import { Transaction } from "../../../packages/frontend/client-react-native/api/transaction.mts";
import { bootstrapSuite } from "../client-js/bootstrap-harness.mjs";
bootstrapSuite(test, Transaction);
