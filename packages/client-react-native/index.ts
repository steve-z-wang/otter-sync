import { requireNativeModule } from "expo-modules-core";
import { createClient, type NativeCarrier } from "../client-js/runtime.mts";
import { Transaction } from "./transaction.mts";
import { createServerConnection } from "./live.mts";
export { Transaction } from "./transaction.mts";
export {
  CallError,
  type Call,
  type CallOptions,
  type OnceOptions,
  type QueryOptions,
  type CallOutcome,
  type CallStatus,
} from "../client-js/actions.mts";
export type { QuerySpec, RecordValue } from "../client-js/values.mts";
export type {
  Connection,
  ConnectionOptions,
} from "../client-js/connection.mts";
export type { ServerOptions } from "../client-js/live.mts";
export type {
  BootstrapPhase,
  BootstrapStatus,
  Subscription,
  SubscriptionState,
  SubscriptionStatus,
} from "../client-js/runtime.mts";
export type {
  ClientSyncState,
  ModelSyncState,
  PendingMutation,
  RebuildReport,
  Rejection,
  SchemaState,
} from "../client-js/runtime.mts";

const native = requireNativeModule<{
  runtimeOpen(request: string): string;
  runtimeSubmit(runtimeId: string, message: string): void;
  runtimeDrain(runtimeId: string): string;
  runtimeDetach(runtimeId: string): void;
  addListener(
    event: "axtonWake",
    listener: (event: { runtimeId: string }) => void,
  ): { remove(): void };
  databasePath(name: string): Promise<string>;
}>("AxtonNative");

/** Resolves a basename inside persistent application storage. */
export function databasePath(name = "axton.sqlite"): Promise<string> {
  return native.databasePath(name);
}

/**
 * The shared JS Bridge's carrier over the Expo module. The native side posts
 * `axtonWake` on the main queue whenever a runtime has events; one listener,
 * registered on first open, routes it to that runtime's Bridge.
 */
const wakes = new Map<string, () => void>();
let listening = false;
const carrier: NativeCarrier = {
  runtimeOpen(request, wake) {
    if (!listening) {
      native.addListener("axtonWake", ({ runtimeId }) =>
        wakes.get(runtimeId)?.(),
      );
      listening = true;
    }
    const runtimeId = native.runtimeOpen(request);
    wakes.set(runtimeId, () => wake(runtimeId));
    return runtimeId;
  },
  runtimeSubmit: (runtimeId, message) =>
    native.runtimeSubmit(runtimeId, message),
  runtimeDrain: (runtimeId) => native.runtimeDrain(runtimeId),
  runtimeDetach(runtimeId) {
    wakes.delete(runtimeId);
    native.runtimeDetach(runtimeId);
  },
};
export const Client = createClient(
  carrier,
  Transaction,
  createServerConnection,
);
export type Client = Awaited<ReturnType<typeof Client.open>>;
