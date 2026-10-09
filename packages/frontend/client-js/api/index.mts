import { native } from "../bindings/native.mts";
import { createClient } from "./runtime.mts";
import { Transaction } from "./transaction.mts";
import { createServerConnection } from "../bindings/live.mts";
export {
  Transaction,
  LocalTransaction,
  type LocalCallback,
  type MutationInput,
  type QuerySpec,
} from "./transaction.mts";
export {
  CallError,
  type Call,
  type CallOptions,
  type QueryOptions,
  type CallOutcome,
  type CallStatus,
} from "./actions.mts";
export type { RecordValue } from "./values.mts";
export type { FetchOptions, StoreConnection } from "./runtime.mts";
export type {
  Connection,
  ConnectionOptions,
  HttpRoute,
  Transport,
} from "../bindings/connection.mts";
export {
  AdmissionRefused,
  AxtonReport,
  PrerequisiteRetry,
  type PrerequisiteHandler,
  type ReportDetails,
  type ReportKind,
} from "../bindings/connection.mts";
export type { ServerOptions } from "../bindings/live.mts";

export type { BootstrapPhase, BootstrapStatus } from "./runtime.mts";
export type {
  ClientSyncState,
  ModelSyncState,
  PendingMutation,
  Rejection,
} from "./runtime.mts";
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
} from "./runtime.mts";
export const Client = createClient(native, Transaction, createServerConnection);
export type Client = Awaited<ReturnType<typeof Client.open>>;
