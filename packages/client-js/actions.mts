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
export function assertCallOptions(options: unknown): void {
  const value = options as { once?: unknown; refresh?: unknown } | undefined;
  if (value?.once !== undefined || value?.refresh !== undefined)
    throw invalidOptions("once and refresh apply only to direct Queries");
}
/** Validate a direct Query's once controls before any I/O. */
export function onceControls(options: unknown): {
  once: boolean;
  refresh: boolean;
} {
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

class CallState<T> implements Call<T> {
  status: CallStatus = "pending";
  #outcome: CallOutcome<T> | undefined;
  #promise: Promise<CallOutcome<T>>;
  #resolve!: (value: CallOutcome<T>) => void;
  #activate: () => void;
  constructor(activate: () => void) {
    this.#activate = activate;
    this.#promise = new Promise((resolve) => {
      this.#resolve = resolve;
    });
  }
  wait(): Promise<CallOutcome<T>> {
    if (!this.#outcome) this.#activate();
    return this.#promise;
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
  register<T>(callId: string, decode: (value: unknown) => T): Call<T> {
    this.assertSupported();
    this.#sweep();
    const state = new CallState<T>(() => {
      this.#active.set(callId, state as CallState<unknown>);
    });
    if (this.#closed)
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
  complete(completion: Completion): void {
    this.#sweep();
    const route = this.#routes.get(completion.callId);
    const state = this.#active.get(completion.callId) ?? route?.ref.deref();
    if (!state) return;
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
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    for (const [id, route] of this.#routes) {
      const state = this.#active.get(id) ?? route.ref.deref();
      state?.settle({
        result: undefined,
        error: new CallError("client.closed"),
      });
    }
    this.#routes.clear();
    this.#active.clear();
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
