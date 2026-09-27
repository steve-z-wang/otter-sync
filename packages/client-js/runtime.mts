import {
  Effects,
  directTimeout,
  prerequisites,
  startConnection,
  type Connection,
  type ConnectionOptions,
} from "./connection.mts";
export type { Connection, ConnectionOptions } from "./connection.mts";
/** A rejection retained in the local inbox until dismissed. */
export type Rejection = {
  ordinal: number;
  code: string;
  [key: string]: unknown;
};
/** One queued mutation touching a record. `diverged` is set when its edit could not be replayed over newer authority. */
export type PendingMutation<Name extends string = string> = {
  ordinal: number;
  name: Name;
  phase: "queued" | "frozen";
  prerequisites: { key: string; state: "ready" | "pending" | "failed" }[];
  diverged?: boolean;
};
/** One record's sync state: what is still pending for it and what was rejected. */
export type ModelSyncState<Name extends string = string> = {
  pending: PendingMutation<Name>[];
  rejections: Rejection[];
};
/** What a rebuild left in the old database file. */
export type RebuildReport = {
  oldFile: string;
  newFile: string;
  reason: string;
  leftPending: number;
  leftDirect: number;
  abandonedCalls: { callId: string; frozen: boolean }[];
};
/** The open-time schema check: whether this open rebuilt, or is waiting to. */
export type SchemaState = {
  rebuilt: boolean;
  /** The incompatible file is still in use because it holds unsent work. */
  pending: {
    oldFile: string;
    reason: string;
    pending: number;
    direct: number;
  } | null;
  lastRebuild: RebuildReport | null;
};
/** The whole client's sync state: a local snapshot, not a network probe. */
export type ClientSyncState = {
  clientId: string;
  pending: number;
  beforeImages: number;
  cursors: Record<string, number>;
  channels: string[];
  rejections: Rejection[];
  schema: SchemaState;
};
import type { QuerySpec, RecordValue } from "./values.mts";
import {
  Bridge,
  reportCallbackError,
  type NativeCarrier,
  type TaskError,
  type TaskHooks,
  type RawStoreChange,
} from "./bridge.mts";
export type { NativeCarrier } from "./bridge.mts";
export type { RawStoreChange } from "./bridge.mts";
export type StoreHook<Tx = import("./transaction.mts").Transaction> = (
  tx: Tx,
  changes: readonly RawStoreChange[],
) => void | Promise<void>;

/** Clear the payload before a hook's Promise can become long lived. */
class StoreHookInvocation<Tx> {
  #hook: StoreHook<Tx> | undefined;
  #changes: readonly RawStoreChange[] | undefined;
  constructor(hook: StoreHook<Tx>, changes: readonly RawStoreChange[]) {
    this.#hook = hook;
    this.#changes = changes;
  }
  run = (tx: Tx): void | Promise<void> => {
    const hook = this.#hook!;
    const changes = this.#changes!;
    this.#hook = undefined;
    this.#changes = undefined;
    return hook(tx, changes);
  };
}
import type { ServerOptions, ServerConnection } from "./live.mts";
import { Subscriptions, type Subscription } from "./subscriptions.mts";
export type {
  BootstrapPhase,
  BootstrapStatus,
  Subscription,
  SubscriptionState,
  SubscriptionStatus,
} from "./subscriptions.mts";
import {
  ActionRegistry,
  CallError,
  actionError,
  assertCallOptions,
  onceControls,
  type Call,
  type CallOptions,
  type QueryOptions,
} from "./actions.mts";

/**
 * The native command field for an Action's store option, beside its args.
 * Once controls never reach this seam: Mutations and `enqueue` refuse them.
 */
function storeOption(options?: CallOptions): { store?: unknown } {
  assertCallOptions(options);
  return options?.store === undefined ? {} : { store: options.store };
}
type DirectOutcome = {
  status: string;
  result?: unknown;
  code?: string;
  execution?: string;
};
/** Decode one caller's result from a terminal direct outcome. */
function decodeOutcome<T>(
  outcome: DirectOutcome | undefined,
  decode: (value: unknown) => T,
): T {
  if (!outcome) throw new CallError("action.observation_failed");
  if (outcome.status === "failed")
    throw new CallError(
      outcome.code ?? "action.failed",
      outcome.execution === "rejected" ? "rejected" : "unknown",
    );
  try {
    return decode(outcome.result);
  } catch (cause) {
    throw new CallError("action.observation_failed", "unknown", cause);
  }
}
/**
 * Typed decoding: the codes an `invoke` task fails with, and the execution
 * each leaves, as the Call API names them. Rust decides the code.
 */
const INVOKE_CODES: Record<string, "unknown" | "rejected"> = {
  "action.unavailable": "unknown",
  "action.execution_unknown": "unknown",
  "action.observation_failed": "unknown",
  "action.invalid_options": "rejected",
};
/**
 * An `invoke` task's failure as the Call API names it: a code the runtime
 * decided keeps its execution, and a closed client leaves the call
 * unavailable as a missing connection does. Anything else stays the engine's
 * error for `actionError` to map.
 */
function invokeError(error: unknown): unknown {
  const details = (error as TaskError | null)?.details;
  if (details?.code === "store_hook_failed")
    return new CallError(
      "store_hook_failed",
      "unknown",
      (error as Error & { cause?: unknown }).cause ?? error,
    );
  const message = (error as { message?: unknown } | null)?.message;
  if (message === "client_closed")
    return new CallError("action.unavailable", "unknown", error);
  const execution =
    typeof message === "string" ? INVOKE_CODES[message] : undefined;
  return execution
    ? new CallError(message as string, execution, directCause(error))
    : error;
}
/**
 * What a direct call failed on, as the runtime's `details` report it: the
 * transport's message and the HTTP status it carried, or the deadline. A
 * failure without a message (`action.unavailable`) keeps the task's error.
 */
function directCause(error: unknown): unknown {
  const details = (error as TaskError | null)?.details;
  if (typeof details?.message !== "string") return error;
  return Object.assign(
    Error(details.message),
    typeof details.status === "number" ? { status: details.status } : {},
  );
}

/**
 * Model Fetch options ([#153](https://github.com/zanminwang/axton/issues/153)):
 * `store` defaults to `true`; `false` returns the snapshot without local
 * storage or onStore. There is no other option.
 */
export type FetchOptions = { store?: boolean };
/**
 * The command members of a Fetch's options. Rust validates `store` and
 * ignores members it does not know, so any other member is refused here.
 */
function fetchStore(options: unknown): { store?: unknown } {
  if (options === undefined) return {};
  if (
    options === null ||
    typeof options !== "object" ||
    Object.keys(options).some((key) => key !== "store")
  )
    throw new CallError(
      "fetch.invalid_options",
      "rejected",
      Error("Fetch accepts only a boolean store option"),
    );
  const store = (options as { store?: unknown }).store;
  return store === undefined ? {} : { store };
}
/** Fetch failures refused before any request was sent. */
const FETCH_REJECTED = new Set([
  "fetch.invalid_options",
  "fetch.schema_pending",
]);
/**
 * A `fetch` task's failure as a {@link CallError}: a `fetch.*` code the
 * runtime decided keeps its cause - the refusing onStore callback's value or
 * the transport failure with its status. A closed client's admission error
 * and any other engine error stay as they are.
 */
function fetchError(error: unknown): unknown {
  if (error instanceof CallError) return error;
  const code = (error as TaskError | null)?.details?.code;
  if (typeof code === "string" && code.startsWith("fetch."))
    return new CallError(
      code,
      FETCH_REJECTED.has(code) ? "rejected" : "unknown",
      (error as Error & { cause?: unknown }).cause ?? directCause(error),
    );
  if ((error as Error | null)?.message === "transaction_active")
    return actionError(error);
  return error;
}

/**
 * Hosts share the Rust-owned client runtime and supply only their carrier,
 * transaction scope and network. Every command is a task of that runtime
 * ([#134](https://github.com/zanminwang/axton/issues/134)): it queues,
 * schedules and completes them, runs the connection lanes and direct calls,
 * and asks for platform work as effects ([connection.mts](connection.mts));
 * this class adapts them to typed APIs.
 */
export function createClient<
  Tx extends {
    finish(): Promise<void>;
    cancel(): void;
    runCallback<T>(body: () => Promise<T>): Promise<T>;
    inCallback(): boolean;
    direct(operation: object): Promise<void>;
  },
>(
  native: NativeCarrier,
  Transaction: (new (
    send: (command: RecordValue, scope?: string) => Promise<any>,
  ) => Tx) & {
    /**
     * `inCallback()` identifies the callback's own async context. Without
     * it, `inCallback()` holds for every caller while a callback runs.
     */
    readonly exactCallbackGuard?: boolean;
  },
  createServerConnection: (options: ServerOptions) => ServerConnection,
) {
  return class Client {
    #tasks: Promise<void> | undefined;
    /** The live connection, and how to stop its effects without a task. */
    #connection: { handle: Connection; halt(): void } | undefined;
    /** Public transactions submitted and not yet settled. */
    #transactions = 0;
    #completionListeners = new Set<(completion: any) => void>();
    #actions = new ActionRegistry();
    /** Once callers still waiting: closing the client settles them at once. */
    #waitingOnce = new Set<(error: CallError) => void>();
    #connecting = false;
    #started: Promise<void> | undefined;
    #closing: Promise<void> | undefined;
    #bridge: Bridge;
    readonly #effects: Effects;
    #closed = false;
    #activePublicTx: Tx | undefined;
    /** Subscription handles by persistent identity; the runtime publishes their status. */
    readonly #subscriptions: Subscriptions;
    readonly clientId: string;
    private constructor(bridge: Bridge, id: string) {
      this.#bridge = bridge;
      this.clientId = id;
      this.#effects = new Effects(bridge);
      this.#subscriptions = new Subscriptions(bridge, reportCallbackError);
      // Every call outcome the runtime committed - receipts, discards, direct
      // calls, once flights and rebuild abandonments - after the commit that
      // decided it. This is the only path completions take.
      bridge.on("callCompleted", (event) =>
        this.#deliverCompletions([
          { callId: event.callId, outcome: event.outcome },
        ]),
      );
    }
    /**
     * Async-context guard: a task submitted from inside a transaction
     * callback would queue behind the transaction that awaits it. Only this
     * language knows which async context a call comes from.
     */
    #inCallback(): boolean {
      return this.#activePublicTx?.inCallback() ?? false;
    }
    /**
     * Refuse a call from inside a transaction callback with
     * `transaction_active`: any task it submitted would park behind the
     * transaction that awaits it. Where the guard knows the callback's async
     * context (Node) it covers every task; where it only knows that some
     * callback runs (React Native) it covers the Mutation and Query calls
     * (`writes`), so an unrelated caller's read or transaction still waits
     * its turn.
     */
    #guard(writes = false): void {
      if (
        (writes || Transaction.exactCallbackGuard === true) &&
        this.#inCallback()
      )
        throw Error("transaction_active");
    }
    /** Submit an ordinary task behind the guard. */
    #task(command: RecordValue, hooks?: TaskHooks): Promise<any> {
      try {
        this.#guard();
      } catch (error) {
        return Promise.reject(error);
      }
      return this.#bridge.task(command, hooks);
    }
    static async open(options: {
      path: string;
      schema: object;
      migration?: { defaults?: RecordValue; replayPull?: boolean };
      /** Rebuild at once when the schema is incompatible, leaving unsent work in the old file. */
      discardPending?: boolean;
      onStore?: Record<string, StoreHook<Tx>>;
    }) {
      const { onStore, ...wire } = options;
      let client!: Client;
      const handlers = Object.fromEntries(
        Object.entries(onStore ?? {}).map(([model, hook]) => [
          model,
          (
            transactionId: string,
            changes: readonly RawStoreChange[],
            cancellation: AbortSignal,
          ) =>
            client.#runStoreTransaction(
              transactionId,
              hook,
              changes,
              cancellation,
            ),
        ]),
      );
      const { bridge, opened } = await Bridge.open(native, {
        ...wire,
        onStore: handlers,
      });
      client = new Client(bridge, opened.clientId);
      return client;
    }
    /**
     * Run `body` as the callback of a local transaction the runtime owns. The
     * callback's commands carry its capability; its return value stays here
     * and is answered only after the runtime confirmed the commit.
     */
    async transaction<T>(body: (tx: Tx) => Promise<T>): Promise<T> {
      this.#guard();
      let result!: T;
      this.#transactions++;
      try {
        await this.#bridge.transaction(async (transactionId) => {
          result = await this.#runTransactionBody(transactionId, body);
        });
      } finally {
        this.#transactions--;
      }
      return result;
    }
    /** The same language-side transaction checks for public and store callbacks. */
    async #runTransactionBody<T>(
      transactionId: string,
      body: (tx: Tx) => T | Promise<T>,
      cancellation?: AbortSignal,
    ): Promise<T> {
      const tx = new Transaction((command, scope) =>
        this.#bridge.transactionCommand(transactionId, scope, command),
      );
      const cancel = () => tx.cancel();
      cancellation?.addEventListener("abort", cancel, { once: true });
      if (cancellation?.aborted) tx.cancel();
      this.#activePublicTx = tx;
      try {
        const result = await tx.runCallback(() => Promise.resolve(body(tx)));
        await tx.finish();
        return result;
      } catch (error) {
        await tx.finish().catch(() => {});
        throw error;
      } finally {
        cancellation?.removeEventListener("abort", cancel);
        if (this.#activePublicTx === tx) this.#activePublicTx = undefined;
      }
    }
    /** Store invocation and completion bookkeeping use separate frames. */
    #runStoreTransaction(
      transactionId: string,
      hook: StoreHook<Tx>,
      changes: readonly RawStoreChange[],
      cancellation: AbortSignal,
    ): Promise<void> {
      this.#transactions++;
      const invocation = new StoreHookInvocation(hook, changes);
      return this.#trackStoreTransaction(
        this.#runTransactionBody(transactionId, invocation.run, cancellation),
      );
    }
    #trackStoreTransaction(running: Promise<void>): Promise<void> {
      return running.finally(() => {
        this.#transactions--;
      });
    }
    read(model: string, identity: object): Promise<RecordValue | null> {
      return this.#task({ kind: "read", key: { model, identity } });
    }
    query(model: string, where: RecordValue = {}): Promise<RecordValue[]> {
      return this.#task({ kind: "query", model, filter: where });
    }
    readSql(sql: string, parameters: unknown[] = []): Promise<RecordValue[]> {
      return this.#task({ kind: "sql", sql, parameters });
    }
    querySpec(model: string, query: QuerySpec = {}): Promise<RecordValue[]> {
      return this.#task({ kind: "querySpec", model, query });
    }
    related(
      model: string,
      identity: object,
      relation: string,
    ): Promise<RecordValue | null> {
      return this.#task({
        kind: "related",
        key: { model, identity },
        relation,
      });
    }
    referencing(
      model: string,
      identity: object,
      source: string,
      relation: string,
    ): Promise<RecordValue[]> {
      return this.#task({
        kind: "referencing",
        key: { model, identity },
        source,
        relation,
      });
    }
    mutate(mutation: object): Promise<number> {
      try {
        this.#guard(true);
      } catch (error) {
        return Promise.reject(error);
      }
      return this.#bridge.task({ kind: "enqueue", mutation });
    }
    /** One standalone Model write in its own local transaction. */
    direct(operation: object): Promise<void> {
      try {
        this.#guard(true);
      } catch (error) {
        return Promise.reject(error);
      }
      return this.transaction(async (tx) => {
        await tx.direct(operation);
      });
    }
    /** Submit durable work and register its observer as soon as it committed. */
    async invokeAction<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
      options?: CallOptions,
    ): Promise<Call<T>> {
      this.#actions.assertSupported();
      let call: Call<T> | undefined;
      try {
        await this.submitAction(
          name,
          version,
          args,
          (callId) => {
            call = this.#actions.register(callId, decode);
          },
          options,
        );
      } catch (error) {
        throw actionError(error);
      }
      return call!;
    }
    /** Execute a direct Action and decode its committed result. */
    async invokeDirectAction<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
      options?: CallOptions,
    ): Promise<T> {
      let outcome: DirectOutcome | undefined;
      try {
        ({ outcome } = await this.callAction(name, version, args, options));
      } catch (error) {
        throw actionError(error);
      }
      return decodeOutcome(outcome, decode);
    }
    /**
     * Execute a direct Query. Without `once` it is exactly
     * [`invokeDirectAction`]: a fresh request that reads and writes no
     * snapshot. With `once`, Rust decides: a saved result is decoded without
     * any request or Model write, an active request is joined, or a new one
     * is executed and its successful result saved with its authority. Every
     * caller decodes its own copy of the outcome.
     */
    async invokeQuery<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
      options?: QueryOptions,
    ): Promise<T> {
      const { once, refresh } = onceControls(options);
      const call: CallOptions =
        options?.store === undefined ? {} : { store: options.store };
      if (!once)
        return this.invokeDirectAction(name, version, args, decode, call);
      let outcome: DirectOutcome | undefined;
      try {
        this.#guard(true);
        ({ outcome } = await this.#untilClosed(
          this.#bridge.task({
            kind: "invoke",
            name,
            version,
            args,
            once,
            refresh,
            ...storeOption(call),
          }),
        ));
      } catch (error) {
        throw actionError(invokeError(error));
      }
      return decodeOutcome(outcome, decode);
    }
    /**
     * Fetch one Model by identity through its existing Loader
     * ([#153](https://github.com/zanminwang/axton/issues/153)). Rust
     * validates the identity and options, joins an identical request in
     * flight or sends a new one, and by default stores the reply before
     * answering; this submits the task and decodes this caller's own copy of
     * the snapshot. `null` when the Loader has no readable record.
     */
    async fetchModel<T>(
      model: string,
      version: number,
      identity: object,
      decode: (row: RecordValue) => T,
      options?: FetchOptions,
    ): Promise<T | null> {
      let outcome: DirectOutcome | undefined;
      try {
        this.#guard(true);
        ({ outcome } = await this.#bridge.task({
          kind: "fetch",
          model,
          version,
          identity: identity as RecordValue,
          ...fetchStore(options),
        }));
      } catch (error) {
        throw fetchError(error);
      }
      return decodeOutcome(outcome, (result) =>
        result === null ? null : decode(result as RecordValue),
      );
    }
    /**
     * Discard the saved once results of one Query argument set, every store
     * variant, in a local transaction. Needs no network; an older request
     * still in flight cannot save its result afterwards.
     */
    async invalidateQuery(
      name: string,
      version: number,
      args: object,
    ): Promise<void> {
      try {
        this.#guard(true);
        await this.#bridge.task({
          kind: "invalidateQueryOnce",
          name,
          version,
          args,
        });
      } catch (error) {
        throw actionError(error);
      }
    }
    /**
     * Promise lifetime: a once caller settles with `client.closed` as soon as
     * the public `close()` is called, before the runtime's own `client_closed`.
     */
    #untilClosed<T>(task: Promise<T>): Promise<T> {
      return new Promise<T>((resolve, reject) => {
        this.#waitingOnce.add(reject);
        task
          .then(resolve, reject)
          .finally(() => this.#waitingOnce.delete(reject));
      });
    }
    /**
     * Internal Action seam. `onCommitted` runs while the submission's
     * completion is dispatched - after the local commit, before any later
     * event - so the call's `callCompleted` can never outrun it.
     */
    submitAction(
      name: string,
      version: number,
      args: object,
      onCommitted?: (callId: string, ordinal: number) => void,
      options?: CallOptions,
    ): Promise<{ callId: string; ordinal: number }> {
      let store: { store?: unknown };
      try {
        this.#guard(true);
        store = storeOption(options);
      } catch (error) {
        return Promise.reject(error);
      }
      return this.#bridge.task(
        { kind: "submitAction", name, version, args, ...store },
        {
          settled: ({ callId, ordinal }: { callId: string; ordinal: number }) =>
            onCommitted?.(callId, ordinal),
        },
      );
    }
    onActionCompletion(listener: (completion: any) => void): () => void {
      this.#completionListeners.add(listener);
      return () => this.#completionListeners.delete(listener);
    }
    #deliverCompletions(completions: any[]): void {
      // Finish every registered handle before an application diagnostic listener can throw.
      for (const completion of completions) this.#actions.complete(completion);
      for (const completion of completions)
        for (const listener of [...this.#completionListeners])
          try {
            listener(completion);
          } catch (error) {
            reportCallbackError(error);
          }
    }
    /**
     * One direct call as a runtime task: the runtime prepares it, sends it,
     * bounds it, refreshes credentials once on 401 and applies the response
     * in one local transaction; the value is `{outcome}` after that commit.
     */
    async callAction(
      name: string,
      version: number,
      args: object,
      options?: CallOptions,
    ): Promise<{ outcome: DirectOutcome }> {
      this.#guard(true);
      const store = storeOption(options);
      try {
        return await this.#bridge.task({
          kind: "invoke",
          name,
          version,
          args,
          ...store,
        });
      } catch (error) {
        throw invokeError(error);
      }
    }
    /**
     * Register durable intent to follow `scope` and answer with its handle. It
     * resolves when the local transaction commits: it awaits no
     * authentication, connection or acknowledgement, and the same Scope answers
     * with the same handle while its registration lives. The socket is never
     * cancelled here; the Downlink worker sees the committed change and
     * reconciles its own session.
     */
    async subscribeScope(scope: string): Promise<Subscription> {
      this.#guard();
      return this.#subscriptions.subscribe(scope);
    }
    /** The Scope surface the generated `scopes` facade delegates to, with no logic of its own. */
    get scopes(): { subscribe(scope: string): Promise<Subscription> } {
      return { subscribe: (scope) => this.subscribeScope(scope) };
    }
    subscribe(channel: string): Promise<Subscription> {
      return this.subscribeScope(channel);
    }
    /** Remove whatever registration this Scope name has; its handle stops. */
    async unsubscribe(channel: string): Promise<void> {
      this.#guard();
      return this.#subscriptions.unsubscribeScope(channel);
    }
    /**
     * Connect to `server`: install the effects the runtime will ask for, then
     * hand it the connection. The runtime runs both lanes and direct calls
     * until `close`.
     */
    async connect(
      server: ServerOptions,
      options: ConnectionOptions = {},
    ): Promise<Connection> {
      if (this.#closed || this.#closing) throw Error("client_closed");
      this.#guard();
      // Rust refuses a second connection too; this refuses it before a second
      // set of effect adapters could replace the first one's handlers.
      if (this.#connecting || this.#connection)
        throw Error("connection already active");
      this.#connecting = true;
      let finished!: () => void;
      this.#started = new Promise<void>((resolve) => {
        finished = resolve;
      });
      try {
        if (
          !server ||
          typeof server !== "object" ||
          typeof server.url !== "string" ||
          !(
            typeof server.token === "string" ||
            typeof server.token === "function"
          )
        )
          throw Error("connect requires server: {url, token}");
        const directTimeoutMs = directTimeout(options);
        const live = createServerConnection(server);
        const stop = startConnection(
          this.#bridge,
          this.#effects,
          live,
          options,
        );
        try {
          await this.#bridge.task({
            kind: "connect",
            directTimeoutMs,
            refreshAuth: Boolean(options.refreshAuth),
          });
        } catch (error) {
          stop();
          throw error;
        }
        let closed = false;
        const control = async (event: string) => {
          if (!closed) await this.#task({ kind: "connection", event });
        };
        // Stop this connection's effects here, without a task: the effect
        // handlers are uninstalled and its platform work aborted.
        const halt = () => {
          closed = true;
          stop();
          if (this.#connection?.handle === connection)
            this.#connection = undefined;
        };
        const connection: Connection = {
          pause: () => control("pause"),
          resume: () => control("resume"),
          wake: () => control("wake"),
          close: async () => {
            if (closed) return;
            this.#guard();
            halt();
            try {
              await this.#bridge.task({ kind: "connection", event: "stop" });
            } catch (error) {
              // A closed runtime already ended the connection.
              if (!this.#bridge.closed) throw error;
            }
          },
        };
        this.#connection = { handle: connection, halt };
        return connection;
      } finally {
        this.#connecting = false;
        finished();
      }
    }
    /**
     * Run the prerequisite tasks the runtime picks for these handler names
     * until none is left; it records each outcome. One run at a time.
     */
    runPrerequisites(
      handlers: Record<string, (arguments_: RecordValue) => Promise<void>>,
    ): Promise<void> {
      // The prerequisite adapter is one handler slot per client: a concurrent
      // call joins the running task instead of replacing its handlers.
      try {
        this.#guard();
      } catch (error) {
        return Promise.reject(error);
      }
      if (this.#tasks) return this.#tasks;
      const stop = prerequisites(this.#effects, handlers);
      this.#tasks = this.#bridge
        .task({ kind: "runPrerequisites", handlers: Object.keys(handlers) })
        .then(() => undefined)
        .finally(() => {
          stop();
          this.#tasks = undefined;
        });
      return this.#tasks;
    }
    /** Protocol seams for tests and tools; the connection never uses them. */
    freeze(): Promise<string | null> {
      return this.#task({ kind: "freeze" });
    }
    /** The completions in its value were already delivered as `callCompleted`. */
    acknowledge(sequence: number, receipt: object) {
      return this.#task({ kind: "ack", sequence, receipt });
    }
    applyPull(page: object) {
      return this.#task({ kind: "pull", page });
    }
    /** The client's sync state, or one record's when `model` and `identity` are given. */
    syncState(): Promise<ClientSyncState>;
    syncState(model: string, identity: object): Promise<ModelSyncState>;
    syncState(model?: string, identity?: object) {
      return model === undefined
        ? this.#task({ kind: "status" })
        : this.#task({ kind: "recordStatus", key: { model, identity } });
    }
    /**
     * Leave an incompatible database behind and open a fresh file for the
     * schema this client asked for. Refused while unsent mutations remain
     * unless `discardPending`; the report says what the old file keeps. The
     * runtime completes every abandoned call, ends every subscription handle
     * of the replica it left and re-runs every watch before the report
     * arrives.
     */
    rebuild(
      options: { discardPending?: boolean } = {},
    ): Promise<RebuildReport> {
      return this.#task({ kind: "rebuild", ...options });
    }
    pendingTasks(): Promise<RecordValue[]> {
      return this.#task({ kind: "tasks" });
    }
    setReadiness(key: string, state: "ready" | "pending" | "failed") {
      return this.#task({ kind: "readiness", key, state });
    }
    /** The dropped call completes through `callCompleted`, once. */
    drop(ordinal: number) {
      return this.#task({ kind: "drop", ordinal }).then(() => undefined);
    }
    dismissRejection(ordinal: number) {
      return this.#task({ kind: "dismiss", ordinal });
    }
    /**
     * Observe a local query. The runtime runs it on the committed state,
     * re-runs it after every commit and publishes only a result that differs
     * from the last one, starting with the current rows; `listener` receives
     * each. The returned function stops delivery at once and unregisters the
     * watch. `onError` receives what this call owns: the registration's
     * failure and the listener's exceptions. A re-run that fails is the
     * runtime's to report - through the connection's `onError` - and the
     * watch stays.
     */
    watch(
      model: string,
      where: RecordValue = {},
      listener: (rows: RecordValue[]) => void,
      onError: (error: unknown) => void = () => {},
    ) {
      const fail = (error: unknown) => {
        try {
          onError(error);
        } catch (thrown) {
          reportCallbackError(thrown);
        }
      };
      let stopped = false;
      let unwatch: (() => void) | undefined;
      this.#task(
        { kind: "watch", model, spec: { filter: where } },
        {
          // Routed while the completion is dispatched: the first rows are
          // published behind it in the same batch.
          settled: ({ observerId }: { observerId: string }) => {
            const detach = this.#bridge.observe(observerId, (snapshot) => {
              // A closed watch's last rows are the ones already delivered.
              if (stopped || snapshot.closed) return;
              try {
                listener(snapshot.rows);
              } catch (error) {
                fail(error);
              }
            });
            // The route stays until the runtime confirms nothing follows.
            unwatch = () =>
              void this.#bridge
                .task({ kind: "unwatch", observerId })
                .catch(() => {})
                .finally(detach);
            if (stopped) unwatch();
          },
        },
      ).catch((error) => {
        // The first query failed: the runtime registered nothing.
        if (!stopped) fail(error);
      });
      return () => {
        if (stopped) return;
        stopped = true;
        unwatch?.();
      };
    }
    close(): Promise<void> {
      this.#actions.close();
      for (const settle of [...this.#waitingOnce])
        settle(new CallError("client.closed"));
      this.#waitingOnce.clear();
      // The runtime's close ends every handle; they stop with this client.
      this.#subscriptions.close();
      return (this.#closing ??= this.#finishClose());
    }
    /**
     * Close is priority control: the runtime's `close` waits for no task, so
     * it is never parked behind a callback that holds the transaction. It
     * rolls that transaction back, fails every task and cancels every effect;
     * the connection's handlers are uninstalled here without a task.
     */
    async #finishClose(): Promise<void> {
      // A `connect` admitted behind an open transaction would wait for it:
      // then the runtime closes first and refuses it.
      const closing = this.#transactions > 0 ? this.#bridge.close() : undefined;
      await this.#started;
      this.#connection?.halt();
      try {
        await (closing ?? this.#bridge.close());
      } finally {
        this.#closed = true;
        this.#subscriptions.closed();
      }
    }
  };
}
