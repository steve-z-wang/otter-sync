import {
  Effects,
  directTimeout,
  prerequisites,
  startConnection,
  type Connection,
  type ConnectionOptions,
  type PrerequisiteHandler,
} from "./connection.mts";
export type { Connection, ConnectionOptions } from "./connection.mts";
/** Optional network connection for a Store. */
export type StoreConnection = ServerOptions & {
  projectionGeneration?: string;
  options?: ConnectionOptions;
};
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
  /** The Load jobs of the replica left behind; their handles and waiters ended with `load.schema_changed`. */
  abandonedLoads: string[];
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
  streams: string[];
  rejections: Rejection[];
  schema: SchemaState;
};
import type { QuerySpec, RecordValue } from "./values.mts";
import {
  Bridge,
  reportCallbackError,
  type NativeCarrier,
  type ObserverSnapshot,
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
  assertQueryOptions,
  type Call,
  type CallOptions,
  type QueryOptions,
} from "./actions.mts";
import type { MutationPort } from "./local.mts";
import {
  unsentClient,
  type ClientFailures,
  type ClientOutbound,
  type ClientRejections,
} from "./unsent.mts";
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
} from "./unsent.mts";

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
    submitMutation<T>(
      name: string,
      version: number,
      input:
        | object
        | ((
            port: import("./local.mts").LocalTransaction,
          ) => object | Promise<object>),
      decode: (value: unknown) => T,
    ): Promise<Call<T>>;
  },
>(
  native: NativeCarrier,
  Transaction: (new (
    send: (command: RecordValue, scope?: string) => Promise<any>,
    mutations: MutationPort,
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
    /** The live connection, and how to stop its effects without a task. */
    #connection: { handle: Connection; halt(): void } | undefined;
    /** Public transactions submitted and not yet settled. */
    #transactions = 0;
    #completionListeners = new Set<(completion: any) => void>();
    #actions = new ActionRegistry(
      undefined,
      (callId) => this.#bridge.task({ kind: "callCompletion", callId }),
      () => this.#guard(true),
    );
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
    /** The refusals retained until dismissed, each with the act as submitted. */
    readonly rejections: ClientRejections;
    /** The unsent acts blocked on a terminally failed prerequisite task. */
    readonly failures: ClientFailures;
    /** The queue of unsettled acts. */
    readonly outbound: ClientOutbound;
    private constructor(bridge: Bridge, id: string, effects: Effects) {
      this.#bridge = bridge;
      this.clientId = id;
      this.#effects = effects;
      this.#subscriptions = new Subscriptions(bridge, reportCallbackError);
      const unsent = unsentClient(
        (command) => this.#task(command),
        (command, pick, listener, onError) =>
          this.#observe(command, pick, listener, onError ?? (() => {})),
      );
      this.rejections = unsent.rejections;
      this.failures = unsent.failures;
      this.outbound = unsent.outbound;
      // Every call outcome the runtime committed - receipts, discards, direct
      // calls, once flights and rebuild abandonments - after the commit that
      // decided it. This is the only path completions take.
      bridge.on("callCompleted", (event) =>
        this.#deliverCompletions([
          { callId: event.callId, outcome: event.outcome },
        ]),
      );
      // A transaction's Calls become durable at its commit or end with its
      // rollback, whether or not anybody observes them.
      bridge.on("transactionCallState", (event) =>
        this.#actions.transition(event.callId, event.state),
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
      stream: string;
      connection?: StoreConnection;
      projectionGeneration?: string;
      prerequisites?: Record<string, PrerequisiteHandler>;
    }) {
      if (options.connection && "identity" in options.connection)
        throw Error("connection identity is no longer supported");
      const required = { ...options.prerequisites };
      let effects!: Effects;
      const { bridge, opened } = await Bridge.open(
        native,
        {
          path: options.path,
          schema: options.schema,
          stream: options.stream,
          projectionGeneration:
            options.projectionGeneration ??
            options.connection?.projectionGeneration ??
            "1",
          prerequisiteHandlers: Object.keys(required),
        },
        (bridge) => {
          effects = new Effects(bridge);
          prerequisites(effects, required);
        },
      );
      const client = new Client(bridge, opened.clientId, effects);
      client.#stream = options.stream;
      try {
        if (options.connection)
          await client.connect(
            options.connection,
            options.connection.options ?? {},
          );
      } catch (error) {
        await client.close().catch(() => {});
        throw error;
      }
      return client;
    }
    #stream!: string;
    get connection(): Connection | undefined {
      return this.#connection?.handle;
    }
    /** Wait for the native manifest coverage and ordinary delta handoff. */
    async bootstrap(): Promise<void> {
      this.#guard(true);
      return (await this.#subscriptions.subscribe(this.#stream)).bootstrap();
    }
    async resetStore(
      options: { discardPending?: boolean } = {},
    ): Promise<void> {
      await this.#task({ kind: "resetStore", ...options });
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
      const tx = new Transaction(
        (command, scope) =>
          this.#bridge.transactionCommand(transactionId, scope, command),
        {
          submit: (command, scope, decode, local) => {
            this.#actions.assertSupported();
            let call!: Call<any>;
            return this.#bridge
              .submitMutation(transactionId, scope, command, local, {
                // Routed while the answer is dispatched: the transaction's
                // rollback may follow it in the same batch.
                settled: ({ callId }: { callId: string }) => {
                  call = this.#actions.register(callId, decode, true);
                },
              })
              .then(() => call);
          },
        },
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
    /** Submit one named Mutation through the existing local transaction. */
    async submitMutation<T>(
      name: string,
      version: number,
      input: import("./local.mts").MutationInput,
      decode: (value: unknown) => T,
    ): Promise<Call<T>> {
      this.#guard(true);
      return this.transaction((tx) =>
        tx.submitMutation(name, version, input, decode),
      );
    }
    /** Execute a direct Action and decode its committed result. */
    async #invokeQueryAttempt<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
      options?: CallOptions,
    ): Promise<T> {
      let outcome: DirectOutcome | undefined;
      try {
        ({ outcome } = await this.#callAction(name, version, args, options));
      } catch (error) {
        throw actionError(error);
      }
      return decodeOutcome(outcome, decode);
    }
    /** Execute a fresh direct Query and decode its committed invocation snapshot. */
    async invokeQuery<T>(
      name: string,
      version: number,
      args: object,
      decode: (value: unknown) => T,
      options?: QueryOptions,
    ): Promise<T> {
      assertQueryOptions(options);
      return this.#invokeQueryAttempt(name, version, args, decode, options);
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
    async #callAction(
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
     * Register durable intent to follow `stream` and answer with its handle. It
     * resolves when the local transaction commits: it awaits no
     * authentication, connection or acknowledgement, and the same Stream answers
     * with the same handle while its registration lives. The socket is never
     * cancelled here; the Downlink worker sees the committed change and
     * reconciles its own session.
     */
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
        // A refused admission already stopped the runtime's connection.
        let ended = () => {};
        const stop = startConnection(
          this.#bridge,
          this.#effects,
          live,
          options,
          () => ended(),
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
        ended = halt;
        return connection;
      } finally {
        this.#connecting = false;
        finished();
      }
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
      return this.#observeRows(
        { kind: "watch", model, spec: { filter: where } },
        listener,
        onError,
      );
    }
    /**
     * Observe read-only SQL over several Models ([#184](https://github.com/zanminwang/axton/issues/184)).
     * The runtime asks SQLite which tables the statement reads, runs it on
     * the committed state and re-runs it only after a commit that writes one
     * of them, publishing a result only when it differs from the last one;
     * `listener` receives the current rows first. Only one read-only
     * `SELECT` (or `WITH … SELECT`) over Model tables is accepted: a write
     * or an engine table (`axton_*`) is refused through `onError`, like any
     * first failure. Errors and stopping are `watch`'s.
     */
    watchSql(
      sql: string,
      parameters: unknown[] = [],
      listener: (rows: RecordValue[]) => void,
      onError: (error: unknown) => void = () => {},
    ) {
      return this.#observeRows(
        { kind: "watchSql", sql, parameters },
        listener,
        onError,
      );
    }
    /** Register a row observer and deliver what the runtime publishes for it. */
    #observeRows(
      command: RecordValue,
      listener: (rows: RecordValue[]) => void,
      onError: (error: unknown) => void,
    ) {
      return this.#observe(
        command,
        (snapshot) => snapshot.rows,
        listener,
        onError,
      );
    }
    /**
     * Register an observer and deliver `pick` of each snapshot the runtime
     * publishes for it: rows for a watch, the items or count of an
     * unsent-work observer.
     */
    #observe<T>(
      command: RecordValue,
      pick: (snapshot: ObserverSnapshot) => T,
      listener: (value: T) => void,
      onError: (error: unknown) => void,
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
      this.#task(command, {
        // Routed while the completion is dispatched: the first rows are
        // published behind it in the same batch.
        settled: ({ observerId }: { observerId: string }) => {
          const detach = this.#bridge.observe(observerId, (snapshot) => {
            // A closed watch's last rows are the ones already delivered.
            if (stopped || snapshot.closed) return;
            try {
              listener(pick(snapshot));
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
      }).catch((error) => {
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
        this.#actions.ended();
      }
    }
  };
}
