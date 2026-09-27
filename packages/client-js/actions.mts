/** A terminal Mutation or Query error observed by the client. */
export class CallError extends Error {
  readonly code: string;
  readonly execution: "rejected" | "unknown";
  override readonly cause: unknown;
  constructor(
    code: string,
    execution: "rejected" | "unknown" = "unknown",
    cause?: unknown,
  ) {
    super(code);
    this.name = "CallError";
    this.code = code;
    this.execution = execution;
    this.cause = cause;
  }
}

export type CallStatus = "pending" | "succeeded" | "failed";
export type CallOutcome<T> =
  { result: T; error: null } | { result: undefined; error: CallError };
/**
 * Invocation options, kept apart from business args. `store` selects which
 * explicit Model outputs also update local Models: omitted or `true` stores
 * all, `false` none, and a map overrides named outputs (unnamed ones stay
 * true). Results are the same either way.
 */
export type CallOptions<K extends string = string> = {
  store?: boolean | Partial<Record<K, boolean>>;
};
/**
 * Direct Query controls, kept apart from business args and never sent to the
 * backend. `once` reuses the complete result saved by an earlier successful
 * `once` call with equal arguments and store policy, or saves this one; the
 * snapshot is persisted even with `store: false`. `refresh` (only with
 * `once`) always requests and replaces the snapshot on success.
 */
export type OnceOptions =
  { once?: false; refresh?: false } | { once: true; refresh?: boolean };
/** Invocation options of a direct Query: store policy plus once controls. */
export type QueryOptions<K extends string = string> = CallOptions<K> &
  OnceOptions;
const invalidOptions = (message: string) =>
  new CallError("action.invalid_options", "rejected", Error(message));
/** Mutations and `enqueue` accept no once controls, even from dynamic callers. */
export function assertNoOnce(options: unknown): void {
  const value = options as { once?: unknown; refresh?: unknown } | undefined;
  if (value?.once !== undefined || value?.refresh !== undefined)
    throw invalidOptions("once and refresh apply only to direct Queries");
}
/**
 * Only a Mutation submitted in a transaction (`tx.mutations`) runs a `local`
 * callback; every other route refuses one, even from dynamic callers, rather
 * than queue the call without it.
 */
function assertNoLocal(options: unknown): void {
  if ((options as { local?: unknown } | undefined)?.local !== undefined)
    throw invalidOptions(
      "local applies only to a Mutation submitted in a transaction",
    );
}
/** A standalone Mutation or direct call: no once controls, no `local`. */
export function assertCallOptions(options: unknown): void {
  assertNoOnce(options);
  assertNoLocal(options);
}
/** Validate a direct Query's once controls before any I/O. */
export function onceControls(options: unknown): {
  once: boolean;
  refresh: boolean;
} {
  assertNoLocal(options);
  const value = options as { once?: unknown; refresh?: unknown } | undefined;
  const once = value?.once ?? false;
  const refresh = value?.refresh ?? false;
  if (typeof once !== "boolean" || typeof refresh !== "boolean")
    throw invalidOptions("once and refresh must be booleans");
  if (refresh && !once) throw invalidOptions("refresh requires once: true");
  return { once, refresh };
}
export interface Call<T> {
  readonly status: CallStatus;
  wait(): Promise<CallOutcome<T>>;
}

type Completion = {
  callId: string;
  outcome:
    | { status: "succeeded"; result: unknown }
    | { status: "failed"; code: string; execution: "rejected" | "unknown" };
};

/**
 * Where a Call stands against the local transaction that submitted it. A
 * standalone submission is committed when its handle exists; one submitted in
 * a transaction is provisional until the runtime announces the commit or the
 * rollback that decides it.
 */
type Lifecycle = "provisional" | "committed" | "rolledBack";

class CallState<T> implements Call<T> {
  status: CallStatus = "pending";
  #lifecycle: Lifecycle;
  #outcome: CallOutcome<T> | undefined;
  #promise: Promise<CallOutcome<T>>;
  #resolve!: (value: CallOutcome<T>) => void;
  #activate: () => void;
  constructor(activate: () => void, lifecycle: Lifecycle) {
    this.#activate = activate;
    this.#lifecycle = lifecycle;
    this.#promise = new Promise((resolve) => {
      this.#resolve = resolve;
    });
  }
  get provisional(): boolean {
    return this.#lifecycle === "provisional";
  }
  /**
   * Local observation errors, never backend outcomes: before the commit a
   * wait is refused without settling or retaining the Call; after a rollback
   * every wait fails.
   */
  wait(): Promise<CallOutcome<T>> {
    if (this.#lifecycle === "rolledBack")
      return Promise.reject(
        new CallError("transaction_rolled_back", "rejected"),
      );
    if (this.#lifecycle === "provisional")
      return Promise.reject(new CallError("transaction_uncommitted"));
    if (!this.#outcome) this.#activate();
    return this.#promise;
  }
  commit(): void {
    if (this.#lifecycle === "provisional") this.#lifecycle = "committed";
  }
  /** The transaction or savepoint rolled back: the call never becomes sendable. */
  rollBack(): void {
    if (this.#lifecycle !== "provisional") return;
    this.#lifecycle = "rolledBack";
    this.status = "failed";
  }
  settle(outcome: CallOutcome<T>): void {
    if (this.#outcome) return;
    this.#outcome = outcome;
    this.status = outcome.error === null ? "succeeded" : "failed";
    this.#resolve(outcome);
  }
}

type WeakState = { deref(): CallState<unknown> | undefined };
type WeakFactory = (state: CallState<unknown>) => WeakState;

/** Routes transient completions without retaining abandoned handles. */
export class ActionRegistry {
  #routes = new Map<
    string,
    { ref: WeakState; decode: (value: unknown) => unknown }
  >();
  #active = new Map<string, CallState<unknown>>();
  #weak: WeakFactory | null;
  #usesRuntimeWeak: boolean;
  #closed = false;
  #ended = false;
  constructor(weak?: WeakFactory | null) {
    this.#usesRuntimeWeak = weak === undefined;
    this.#weak = weak === undefined ? (state) => new WeakRef(state) : weak;
  }
  get routingCount(): number {
    return this.#routes.size;
  }
  get activeCount(): number {
    return this.#active.size;
  }
  assertSupported(): void {
    if (!this.#weak) throw new CallError("action.unsupported_runtime");
    if (this.#usesRuntimeWeak) {
      try {
        if (
          typeof WeakRef !== "function" ||
          typeof new WeakRef({}).deref !== "function"
        )
          throw Error("WeakRef is unavailable");
      } catch (cause) {
        throw new CallError("action.unsupported_runtime", "unknown", cause);
      }
    }
  }
  /**
   * Route `callId`'s completion to a new handle. A `provisional` handle was
   * submitted in an open transaction: it waits for {@link transition}.
   */
  register<T>(
    callId: string,
    decode: (value: unknown) => T,
    provisional = false,
  ): Call<T> {
    this.assertSupported();
    this.#sweep();
    const state = new CallState<T>(
      () => {
        this.#active.set(callId, state as CallState<unknown>);
      },
      provisional ? "provisional" : "committed",
    );
    // Closed, a committed call can no longer be observed; a provisional one
    // still hears its transaction's fate, until the runtime ended.
    if (provisional && this.#ended) state.rollBack();
    else if (this.#closed && !provisional)
      state.settle({
        result: undefined,
        error: new CallError("client.closed"),
      });
    else
      this.#routes.set(callId, {
        ref: this.#weak!(state as CallState<unknown>),
        decode,
      });
    return state;
  }
  /**
   * The runtime's `transactionCallState`: the local commit made the call
   * durable, or a rollback discarded it. Nobody needs to be waiting.
   */
  transition(callId: string, state: "committed" | "rolledBack"): void {
    this.#sweep();
    const route = this.#routes.get(callId);
    const call = route?.ref.deref();
    if (!route || !call?.provisional) return;
    if (state === "committed") {
      call.commit();
      if (!this.#closed) return;
      call.settle({ result: undefined, error: new CallError("client.closed") });
    } else call.rollBack();
    this.#routes.delete(callId);
  }
  complete(completion: Completion): void {
    this.#sweep();
    const route = this.#routes.get(completion.callId);
    const state = this.#active.get(completion.callId) ?? route?.ref.deref();
    if (!state) return;
    state.commit();
    let outcome: CallOutcome<unknown>;
    if (completion.outcome.status === "failed") {
      outcome = {
        result: undefined,
        error: new CallError(
          completion.outcome.code,
          completion.outcome.execution,
        ),
      };
    } else {
      try {
        outcome = {
          result: route!.decode(completion.outcome.result),
          error: null,
        };
      } catch (cause) {
        outcome = {
          result: undefined,
          error: new CallError("action.observation_failed", "unknown", cause),
        };
      }
    }
    state.settle(outcome);
    this.#routes.delete(completion.callId);
    this.#active.delete(completion.callId);
  }
  /**
   * The client closes: every committed handle settles with `client.closed`.
   * A provisional one stays routed until the runtime announces its fate.
   */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const [id, route] of this.#routes) {
      const state = this.#active.get(id) ?? route.ref.deref();
      if (state?.provisional) continue;
      state?.settle({
        result: undefined,
        error: new CallError("client.closed"),
      });
      this.#routes.delete(id);
    }
    this.#active.clear();
  }
  /** The runtime ended: no transaction can commit any more. */
  ended(): void {
    this.close();
    this.#ended = true;
    for (const route of this.#routes.values()) route.ref.deref()?.rollBack();
    this.#routes.clear();
  }
  #sweep(): void {
    for (const [id, route] of this.#routes) {
      if (!route.ref.deref() && !this.#active.has(id)) this.#routes.delete(id);
    }
  }
}

export function actionError(error: unknown): CallError {
  if (error instanceof CallError) return error;
  const value = error as {
    code?: unknown;
    execution?: unknown;
    details?: { code?: unknown };
    cause?: unknown;
  } | null;
  const code =
    typeof value?.code === "string"
      ? value.code
      : value?.details?.code === "store_hook_failed"
        ? "store_hook_failed"
        : error instanceof Error && error.message === "transaction_active"
          ? "transaction_active"
          : "action.transport_failed";
  const execution =
    value?.execution === "rejected" || code === "transaction_active"
      ? "rejected"
      : "unknown";
  return new CallError(
    code,
    execution,
    code === "store_hook_failed" ? (value?.cause ?? error) : error,
  );
}
