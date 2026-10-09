import { carrier, native } from "../bindings/native.mts";
import { createClient } from "../../client-js/api/runtime.mts";
import { Transaction } from "./transaction.mts";
import { createServerConnection } from "../bindings/live.mts";
export {
  Transaction,
  LocalTransaction,
  type LocalCallback,
  type MutationInput,
} from "./transaction.mts";
export {
  CallError,
  type Call,
  type CallOptions,
  type QueryOptions,
  type CallOutcome,
  type CallStatus,
} from "../../client-js/api/actions.mts";
export type { QuerySpec, RecordValue } from "../../client-js/api/values.mts";
export type {
  FetchOptions,
  StoreConnection,
} from "../../client-js/api/runtime.mts";
export type {
  Connection,
  ConnectionOptions,
  HttpRoute,
  Transport,
} from "../../client-js/bindings/connection.mts";
export {
  AdmissionRefused,
  PrerequisiteRetry,
  type PrerequisiteHandler,
} from "../../client-js/bindings/connection.mts";
export type { ServerOptions } from "../../client-js/bindings/live.mts";

export type {
  BootstrapPhase,
  BootstrapStatus,
} from "../../client-js/api/runtime.mts";
export type {
  ClientSyncState,
  ModelSyncState,
  PendingMutation,
  Rejection,
} from "../../client-js/api/runtime.mts";
export type {
  ActOperation,
  ClientFailures,
  ClientOutbound,
  ClientRejections,
  FailedAct,
  FailedTask,
  RefusedAct,
  SubmittedAct,
  TransactionFailures,
  TransactionRejections,
} from "../../client-js/api/runtime.mts";

/** Resolves a basename inside persistent application storage. */
export function databasePath(name = "axton.sqlite"): Promise<string> {
  return native.databasePath(name);
}

export const Client = createClient(
  carrier,
  Transaction,
  createServerConnection,
);
export type Client = Awaited<ReturnType<typeof Client.open>>;
