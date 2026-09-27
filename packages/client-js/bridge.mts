import { strictJson, type RecordValue } from "./values.mts";
import type { SchemaState } from "./runtime.mts";

/**
 * The carrier a platform supplies for its Rust-owned client runtimes
 * ([#134](https://github.com/zanminwang/axton/issues/134)). Every call is
 * synchronous and none waits for a task: `runtimeSubmit` only admits,
 * `runtimeDrain` hands over what is already published, and `wake` only says
 * that there is something to drain. `wake` runs on the JavaScript thread.
 */
export type NativeCarrier = {
  runtimeOpen(request: string, wake: (runtimeId: string) => void): string;
  /** Throws when the runtime cannot admit the message (`client_closed`). */
  runtimeSubmit(runtimeId: string, message: string): void;
  /** The published events as JSON array text. */
  runtimeDrain(runtimeId: string): string;
  runtimeDetach(runtimeId: string): void;
};

/** What a successful open answers. */
export type Opened = { clientId: string; schema: SchemaState };

/** One effect's answer, as the runtime reads it. */
export type EffectOutcome =
  | { ok: true; value?: unknown }
  | { ok: false; error: { message: string; status?: number } };

export type BridgeEventType =
  "callCompleted" | "observerChanged" | "report" | "cancelEffect";

/**
 * One observer's state as the runtime published it: a subscription status or
 * a watch's rows. `closed` marks the last one; nothing follows it.
 */
export type ObserverSnapshot = {
  kind: "subscription" | "watch";
  closed?: true;
  [field: string]: any;
};

type Route = {
  resolve(value: any): void;
  reject(error: unknown): void;
  /** Runs with a successful value while its completion is dispatched. */
  settled?: ((value: any) => void) | undefined;
};
/** What runs with a task's successful value, before any later event of its batch. */
export type TaskHooks = {
  /**
   * Claims what the value names - a Call handle, an observer route - while
   * the completion is dispatched, so an event published behind it in the same
   * batch (`callCompleted`, the observer's first snapshot) finds it. If it
   * throws, the task fails with what it threw.
   */
  settled?: (value: any) => void;
};
/**
 * A `transaction` task's callback: its request, the effect that asked for it
 * once published, whether the runtime cancelled that effect and, once it
 * failed, what it threw.
 */
type Callback = {
  requestId: string;
  start(effectId: string, transactionId: string): void;
  effectId?: string;
  cancelled?: true;
  thrown?: { value: unknown };
};
type Event = { type: string; [field: string]: any };
/** A task's failure: the runtime's message, and its machine-readable reason when it gave one. */
export type TaskError = Error & {
  details?: { code: string; [field: string]: unknown };
};

/** Report an application callback's failure without changing any outcome. */
export function reportCallbackError(error: unknown): void {
  if (typeof globalThis.reportError === "function") {
    globalThis.reportError(error);
  } else {
    setTimeout(() => {
      throw error;
    }, 0);
  }
}

/** The runtime's text for a thrown value; never throws itself. */
function describe(error: unknown): string {
  try {
    return String((error as { message?: unknown })?.message ?? error);
  } catch {
    return "transaction callback failed";
  }
}

/** The longest delay a timer accepts; the keep-alive timer never fires. */
const MAX_DELAY = 2 ** 31 - 1;

/**
 * The SDK side of one Rust-owned client runtime: request-to-Promise routes,
 * application callbacks and event dispatch. It holds maps and platform
 * resources only; which task runs, when it commits and what it answers are
 * the runtime's decisions.
 *
 * Every submitted task and transaction command has one route, registered
 * before admission and removed before its waiter settles, exactly once, from
 * the matching `taskCompleted`. Events are dispatched in the order the
 * runtime published them; a listener's exception is reported and never stops
 * the rest of the batch.
 */
export class Bridge {
  readonly #native: NativeCarrier;
  #runtimeId = "";
  /** The last request id issued; ids are decimal strings, never reused. */
  #issued = 0;
  #routes = new Map<string, Route>();
  #callbacks = new Map<string, Callback>();
  #listeners = new Map<BridgeEventType, Set<(event: any) => void>>();
  #effects = new Map<string, (effectId: string, operation: any) => void>();
  /**
   * Observer routes by `observerId`. A route is attached from the `settled`
   * hook of the task that names its observer, and the runtime publishes the
   * observer's first snapshot after that completion, so nothing has to be
   * buffered: a snapshot of an observer nobody routes (a watch stopped before
   * it was registered) is dropped.
   */
  #observers = new Map<string, (snapshot: ObserverSnapshot) => void>();
  #dispatching = false;
  #closing: Promise<void> | undefined;
  #settleClose: (() => void) | undefined;
  #closed = false;
  /**
   * The wake is weak: a pending Promise alone does not keep a Node process
   * alive. This timer does, while a route or the close is outstanding.
   */
  #keepAlive: ReturnType<typeof setTimeout> | undefined;

  private constructor(native: NativeCarrier) {
    this.#native = native;
  }

  /**
   * Open a runtime. Its routing is installed before the runtime can answer;
   * a failed open rejects with the engine message and detaches the runtime
   * once it announced its end.
   */
  static async open(
    native: NativeCarrier,
    request: {
      path: string;
      schema: object;
      discardPending?: boolean;
      migration?: unknown;
    },
  ): Promise<{ bridge: Bridge; opened: Opened }> {
    const bridge = new Bridge(native);
    const opened = bridge.#route<Opened>((requestId) => {
      bridge.#runtimeId = native.runtimeOpen(
        strictJson({ ...request, type: "open", requestId }),
        () => bridge.#drain(),
      );
    });
    return { bridge, opened: await opened };
  }

  get closed(): boolean {
    return this.#closed;
  }

  /**
   * Submit one `{kind, …}` command; settles with its value or engine error.
   * `hooks.settled` runs with a successful value while the completion is
   * dispatched, before the Promise's continuations and any later event.
   */
  task(command: RecordValue, hooks: TaskHooks = {}): Promise<any> {
    return this.#submitRouted(
      (requestId) => ({ type: "task", requestId, command }),
      hooks.settled,
    );
  }

  /**
   * Run `run` as the application callback of a local transaction. The
   * runtime opens the transaction and asks for the callback; `run` receives
   * the transaction's capability, and its settlement commits or rolls back.
   * The promise settles from the task's completion only: after the commit, or
   * with what `run` threw, or with the engine's refusal.
   */
  transaction(run: (transactionId: string) => Promise<void>): Promise<void> {
    // The callback starts from a continuation registered here, so it runs in
    // the caller's async context rather than inside the event dispatch.
    let start!: (effect: { effectId: string; transactionId: string }) => void;
    const started = new Promise<{ effectId: string; transactionId: string }>(
      (resolve) => (start = resolve),
    );
    const callback: Callback = {
      requestId: "",
      start: (effectId, transactionId) => start({ effectId, transactionId }),
    };
    const done = this.#submitRouted((requestId) => {
      callback.requestId = requestId;
      this.#callbacks.set(requestId, callback);
      return { type: "task", requestId, command: { kind: "transaction" } };
    });
    void started.then(async ({ effectId, transactionId }) => {
      // The batch that asked for the callback may also have cancelled it and
      // settled its task - a close admitted right behind it. The body of a
      // refused transaction never runs, and nobody wants its answer.
      if (callback.cancelled || !this.#routes.has(callback.requestId)) return;
      let result: RecordValue;
      try {
        await run(transactionId);
        result = { ok: true };
      } catch (error) {
        // Retained in memory: the transaction rejects with this very value.
        callback.thrown = { value: error };
        result = { ok: false, error: describe(error) };
      }
      this.#answer({
        type: "callbackResult",
        effectId,
        transactionId,
        ...result,
      });
    });
    return done;
  }

  /** Submit one command of the callback that owns `transactionId`. */
  transactionCommand(
    transactionId: string,
    scope: string | undefined,
    command: RecordValue,
  ): Promise<any> {
    return this.#submitRouted((requestId) => ({
      type: "transactionCommand",
      requestId,
      transactionId,
      ...(scope === undefined ? {} : { scope }),
      command,
    }));
  }

  /** Answer one effect. A late answer is fenced in the runtime. */
  effectResult(effectId: string, outcome: EffectOutcome): void {
    this.#answer({ type: "effectResult", effectId, outcome });
  }

  on(type: BridgeEventType, listener: (event: any) => void): () => void {
    let listeners = this.#listeners.get(type);
    if (!listeners) this.#listeners.set(type, (listeners = new Set()));
    listeners.add(listener);
    return () => void listeners.delete(listener);
  }

  /**
   * Route the snapshots of `observerId` to `listener`; attach it from the
   * `settled` hook of the task that answered the id. A terminal snapshot is
   * the last one delivered: its route ends with it. A listener's exception is
   * reported and changes nothing. Answers the detach.
   */
  observe(
    observerId: string,
    listener: (snapshot: ObserverSnapshot) => void,
  ): () => void {
    if (!this.#closed) this.#observers.set(observerId, listener);
    return () => {
      if (this.#observers.get(observerId) === listener)
        this.#observers.delete(observerId);
    };
  }

  /** Handle effects of one operation kind; unhandled kinds are refused. */
  onEffect(
    kind: string,
    handler: (effectId: string, operation: any) => void,
  ): () => void {
    this.#effects.set(kind, handler);
    return () => {
      if (this.#effects.get(kind) === handler) this.#effects.delete(kind);
    };
  }

  /**
   * Close the runtime: it settles every task, rolls back an open transaction
   * and announces its end, after which the carrier is detached. Idempotent.
   */
  close(): Promise<void> {
    return (this.#closing ??= new Promise<void>((resolve) => {
      if (this.#closed) return resolve();
      this.#settleClose = resolve;
      this.#hold();
      try {
        this.#native.runtimeSubmit(
          this.#runtimeId,
          strictJson({ type: "close" }),
        );
      } catch {
        // Already closed: its `runtimeClosed` is on its way or was handled.
        if (this.#closed) resolve();
      }
    }));
  }

  #submitRouted(
    envelope: (requestId: string) => RecordValue,
    settled?: (value: any) => void,
  ): Promise<any> {
    if (this.#closing || this.#closed)
      return Promise.reject(Error("client_closed"));
    return this.#route(
      (requestId) =>
        this.#native.runtimeSubmit(
          this.#runtimeId,
          strictJson(envelope(requestId)),
        ),
      settled,
    );
  }

  /**
   * Register a route under a fresh request id, then admit. An admission
   * failure removes the route and rejects with the carrier's error.
   */
  #route<T>(
    admit: (requestId: string) => void,
    hook?: (value: any) => void,
  ): Promise<T> {
    const requestId = String(++this.#issued);
    const settled = new Promise<T>((resolve, reject) =>
      this.#routes.set(requestId, { resolve, reject, settled: hook }),
    );
    this.#hold();
    try {
      admit(requestId);
    } catch (error) {
      this.#settle(requestId)?.reject(error);
      this.#callbacks.delete(requestId);
    }
    return settled;
  }

  /** Remove and answer the route of `requestId`, once. */
  #settle(requestId: string): Route | undefined {
    const route = this.#routes.get(requestId);
    this.#routes.delete(requestId);
    this.#hold();
    return route;
  }

  /** Keep the process alive exactly while a route or the close is outstanding. */
  #hold(): void {
    const outstanding =
      this.#routes.size > 0 || this.#settleClose !== undefined;
    if (outstanding && this.#keepAlive === undefined)
      this.#keepAlive = setTimeout(() => {}, MAX_DELAY);
    else if (!outstanding && this.#keepAlive !== undefined) {
      clearTimeout(this.#keepAlive);
      this.#keepAlive = undefined;
    }
  }

  /** Submit an answer; after close the runtime no longer wants it. */
  #answer(input: RecordValue): void {
    if (this.#closed) return;
    try {
      this.#native.runtimeSubmit(this.#runtimeId, strictJson(input));
    } catch {
      // The runtime is gone; `runtimeClosed` settles what it still routed.
    }
  }

  /**
   * Drain and dispatch until the outbox is empty. A wake that arrives while
   * this loop runs is covered by its recheck, so it never re-enters.
   */
  #drain(): void {
    if (this.#dispatching) return;
    this.#dispatching = true;
    try {
      while (!this.#closed) {
        const events = JSON.parse(
          this.#native.runtimeDrain(this.#runtimeId),
        ) as Event[];
        if (events.length === 0) break;
        for (const event of events) {
          try {
            this.#dispatch(event);
          } catch (error) {
            reportCallbackError(error);
          }
        }
      }
    } finally {
      this.#dispatching = false;
    }
  }

  #dispatch(event: Event): void {
    switch (event.type) {
      case "taskCompleted": {
        const route = this.#settle(event.requestId);
        const callback = this.#callbacks.get(event.requestId);
        this.#callbacks.delete(event.requestId);
        if (!route) return;
        if (event.ok) {
          try {
            route.settled?.(event.value);
          } catch (error) {
            return route.reject(error);
          }
          route.resolve(event.value);
        } else if (callback?.thrown) route.reject(callback.thrown.value);
        else
          route.reject(
            Object.assign(
              Error(event.error ?? "task failed"),
              event.details === undefined ? {} : { details: event.details },
            ),
          );
        return;
      }
      case "effect":
        return this.#effect(event.effectId, event.operation);
      case "runtimeClosed":
        return this.#terminate();
      case "observerChanged":
        this.#emit(event.type, event);
        return this.#snapshot(event.observerId, event.snapshot);
      case "cancelEffect":
        this.#cancelCallback(event.effectId);
        return this.#emit(event.type, event);
      case "callCompleted":
      case "report":
        return this.#emit(event.type, event);
    }
  }

  /** Deliver one snapshot to its route; nobody routes it, nobody hears it. */
  #snapshot(observerId: string, snapshot: ObserverSnapshot): void {
    const listener = this.#observers.get(observerId);
    if (!listener) return;
    if (snapshot.closed === true) this.#observers.delete(observerId);
    this.#observe(listener, snapshot);
  }

  #observe(
    listener: (snapshot: ObserverSnapshot) => void,
    snapshot: ObserverSnapshot,
  ): void {
    try {
      listener(snapshot);
    } catch (error) {
      reportCallbackError(error);
    }
  }

  #emit(type: BridgeEventType, event: Event): void {
    for (const listener of [...(this.#listeners.get(type) ?? [])])
      try {
        listener(event);
      } catch (error) {
        reportCallbackError(error);
      }
  }

  #effect(effectId: string, operation: { kind: string; [field: string]: any }) {
    if (operation.kind === "callback") {
      const callback = this.#callbacks.get(operation.requestId);
      if (!callback)
        return this.#answer({
          type: "callbackResult",
          effectId,
          transactionId: operation.transactionId,
          ok: false,
          error: "unknown transaction",
        });
      callback.effectId = effectId;
      return callback.start(effectId, operation.transactionId);
    }
    const handler = this.#effects.get(operation.kind);
    if (!handler)
      return this.effectResult(effectId, {
        ok: false,
        error: { message: "unsupported effect" },
      });
    try {
      handler(effectId, operation);
    } catch (error) {
      reportCallbackError(error);
    }
  }

  /** A cancelled callback effect: its callback must not start any more. */
  #cancelCallback(effectId: string): void {
    for (const callback of this.#callbacks.values())
      if (callback.effectId === effectId) callback.cancelled = true;
  }

  /** The runtime ended: settle every route, detach and finish `close()`. */
  #terminate(): void {
    if (this.#closed) return;
    this.#closed = true;
    const routes = [...this.#routes.values()];
    this.#routes.clear();
    this.#callbacks.clear();
    this.#observers.clear();
    for (const route of routes) route.reject(Error("client_closed"));
    this.#native.runtimeDetach(this.#runtimeId);
    const settle = this.#settleClose;
    this.#settleClose = undefined;
    this.#closing ??= Promise.resolve();
    this.#hold();
    settle?.();
  }
}
