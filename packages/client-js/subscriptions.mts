import type { ObserverSnapshot, TaskError, TaskHooks } from "./bridge.mts";
import type { RecordValue } from "./values.mts";

/**
 * Subscription handles: the identity a registration keeps, the status it
 * publishes and the observers watching it
 * ([#150](https://github.com/zanminwang/axton/issues/150)). A handle owns none
 * of the synchronization and none of the status: the Rust runtime projects
 * each registration's connection, initialization and Bootstrap phase, decides
 * every `bootstrap()` outcome and publishes the status as observer snapshots
 * ([#134](https://github.com/zanminwang/axton/issues/134)). This file keeps
 * the language objects: one handle per identity, its last snapshot and its
 * listeners.
 */

/** One stored subscription, as the native Stream commands answer it. A boundary that is not committed yet is `null`; zero is a delivery position. */
export type SubscriptionState = {
  stream: string;
  subscriptionId: number;
  startingCursor: number | null;
  cursor: number | null;
};
/**
 * What the durable load of this registration's published history is doing
 * ([#151](https://github.com/zanminwang/axton/issues/151)).
 * `waiting-for-initialization` is a requested run with no starting boundary to
 * bound its interval yet, `catching-up` a loaded interval whose completion
 * barrier ordinary delivery has not reached, and `complete` says the initial
 * publication coverage was processed - not that a snapshot was taken, nor that
 * the Stream is currently fresh.
 */
export type BootstrapPhase =
  | "not-requested"
  | "waiting-for-initialization"
  | "loading"
  | "catching-up"
  | "complete"
  | "failed";
export type BootstrapStatus = Readonly<{
  phase: BootstrapPhase;
  error: null | Readonly<{ code: string; message: string }>;
}>;
export type SubscriptionStatus = Readonly<{
  /** Whether this handle still names a live registration. */
  active: boolean;
  /** `ready` once a durable starting boundary exists; not a statement about the connection. */
  initialization: "pending" | "ready";
  /** `live` means the session delivers normally, not that all history is loaded. */
  connection: "offline" | "connecting" | "catching-up" | "live" | "stopped";
  /** The durable load of this Stream's published history, as it was last committed. */
  bootstrap: BootstrapStatus;
}>;
export interface Subscription {
  readonly stream: string;
  readonly status: SubscriptionStatus;
  /** Deliver the current snapshot at once, then every change, until the returned function is called. */
  watch(listener: (status: SubscriptionStatus) => void): () => void;
  /**
   * Prepare this Stream's published history. The registration is submitted when
   * the call is made, whether or not the returned Promise is awaited; the
   * Promise resolves only after the completion transaction commits. Calls
   * during one active run share it, a call after a valid completion resolves
   * locally - offline too - and a call after a terminal failure explicitly
   * retries the saved run.
   */
  bootstrap(): Promise<void>;
}
/** Work attempted through a handle that is closed after a Store reset or client shutdown. */
export const subscriptionClosed = () =>
  Object.assign(Error("subscription.closed"), {
    code: "subscription.closed" as const,
  });
/** This process stopped waiting because its client closed; the durable task is untouched. */
const clientClosed = () =>
  Object.assign(Error("client_closed"), { code: "client_closed" as const });
/** The stored failure of a run, as the waiters of that run are rejected with it. */
const bootstrapFailed = (error: { code: string; message: string }) =>
  Object.assign(Error(error.message), { code: error.code });
/**
 * A later run of the same registration was observed than the one this call is
 * attached to: its own outcome can no longer be observed, and a waiter never
 * resolves from another run's, so a rapid retry cannot turn an earlier failed
 * call into a success ([#151](https://github.com/zanminwang/axton/issues/151)).
 */
const bootstrapSuperseded = (stream: string) =>
  Object.assign(
    Error(`the bootstrap run of ${stream} this call waited for was superseded`),
    { code: "bootstrap.superseded" as const },
  );
/**
 * The public error of a failed `streamBootstrap` task, by the code the runtime
 * decided (`details.code`). A task still queued when the runtime closed has no
 * details and fails `client_closed`; any other failure is the caller's to see
 * unchanged.
 */
function bootstrapError(stream: string, error: TaskError): unknown {
  const details = error?.details;
  if (details === undefined)
    return error?.message === "client_closed" ? clientClosed() : error;
  switch (details.code) {
    case "subscription.closed":
      return subscriptionClosed();
    case "client_closed":
      return clientClosed();
    case "bootstrap.superseded":
      return bootstrapSuperseded(stream);
    default:
      return bootstrapFailed({
        code: details.code,
        message:
          typeof details.message === "string" ? details.message : error.message,
      });
  }
}

/** The part of the Bridge the handles use; tests supply a scripted runtime. */
export type SubscriptionBridge = {
  task(command: RecordValue, hooks?: TaskHooks): Promise<any>;
  observe(
    observerId: string,
    listener: (snapshot: ObserverSnapshot) => void,
  ): () => void;
};
/** A subscription observer's snapshot: the public status, verbatim. */
type StatusSnapshot = ObserverSnapshot & { status: SubscriptionStatus };

/** Freeze a status as the runtime published it; nothing is recomputed. */
function frozen(status: SubscriptionStatus): SubscriptionStatus {
  const error = status.bootstrap.error;
  return Object.freeze({
    ...status,
    bootstrap: Object.freeze({
      ...status.bootstrap,
      error: error === null ? null : Object.freeze({ ...error }),
    }),
  });
}

class Handle implements Subscription {
  readonly stream: string;
  readonly subscriptionId: number;
  #registry: Subscriptions;
  #bridge: SubscriptionBridge;
  #report: (error: unknown) => void;
  #snapshot: SubscriptionStatus;
  /**
   * Why the bound handle ended: its Store incarnation ended (`removed`),
   * or its client stopped (`stopped`).
   */
  #closed: "removed" | "stopped" | undefined;
  #listeners = new Set<(status: SubscriptionStatus) => void>();
  constructor(
    state: SubscriptionState,
    registry: Subscriptions,
    bridge: SubscriptionBridge,
    report: (error: unknown) => void,
  ) {
    this.stream = state.stream;
    this.subscriptionId = state.subscriptionId;
    this.#registry = registry;
    this.#bridge = bridge;
    this.#report = report;
    // Replaced by the runtime's first snapshot, which it publishes behind the
    // task that answered this identity, in the same batch.
    this.#snapshot = frozen({
      active: true,
      initialization: state.startingCursor === null ? "pending" : "ready",
      connection: "offline",
      bootstrap: { phase: "not-requested", error: null },
    });
  }
  get status(): SubscriptionStatus {
    return this.#snapshot;
  }
  get closed(): boolean {
    return this.#closed !== undefined;
  }
  /** Route this identity's observer to the handle, before its first snapshot. */
  attach(observerId: string): void {
    this.#bridge.observe(observerId, (snapshot) =>
      this.#receive(snapshot as StatusSnapshot),
    );
  }
  /** One snapshot the runtime published: the status, and whether it is the last. */
  #receive(snapshot: StatusSnapshot): void {
    if (this.closed) return;
    this.#publish(
      frozen(snapshot.status),
      snapshot.closed === true
        ? !this.#registry.closing
          ? "removed"
          : "stopped"
        : undefined,
    );
  }
  /**
   * The runtime ended without a terminal snapshot for this handle: it stops
   * the way the runtime's close would have stopped it.
   */
  stop(): void {
    if (this.closed) return;
    this.#publish(
      frozen({ ...this.#snapshot, active: false, connection: "stopped" }),
      "stopped",
    );
  }
  #publish(
    status: SubscriptionStatus,
    closed: "removed" | "stopped" | undefined,
  ): void {
    this.#snapshot = status;
    if (closed) {
      this.#closed = closed;
      this.#registry.forget(this);
    }
    for (const listener of [...this.#listeners]) this.#deliver(listener);
    // A closed handle has no changes left after that last snapshot.
    if (closed) this.#listeners.clear();
  }
  #deliver(listener: (status: SubscriptionStatus) => void): void {
    try {
      listener(this.#snapshot);
    } catch (error) {
      this.#report(error);
    }
  }
  watch(listener: (status: SubscriptionStatus) => void): () => void {
    // A closed handle has no changes left: it delivers its stopped snapshot and
    // is done.
    if (!this.closed) this.#listeners.add(listener);
    this.#deliver(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }
  bootstrap(): Promise<void> {
    if (this.closed) return Promise.reject(subscriptionClosed());
    // Eager: the registration is submitted when the call is made, not when the
    // returned Promise is awaited. The runtime parks the task on its run.
    return this.#bridge
      .task({
        kind: "streamBootstrap",
        stream: this.stream,
        subscriptionId: this.subscriptionId,
      })
      .then(
        () => undefined,
        (error: TaskError) => {
          throw bootstrapError(this.stream, error);
        },
      );
  }
}

/**
 * The bound Stream handle cache. Reset and close release handles only after
 * the runtime publishes their terminal snapshot.
 */
export class Subscriptions {
  #bridge: SubscriptionBridge;
  #report: (error: unknown) => void;
  #handles = new Map<number, Handle>();
  #closing = false;
  constructor(bridge: SubscriptionBridge, report: (error: unknown) => void) {
    this.#bridge = bridge;
    this.#report = report;
  }
  /** The client is closing: a terminal snapshot now means its handle stopped. */
  get closing(): boolean {
    return this.#closing;
  }
  /**
   * Register durable intent and answer with the handle of the identity that
   * commit belongs to. Concurrent calls run through the runtime's serialized
   * command path, read the same identity and share one cached handle.
   */
  async subscribe(stream: string): Promise<Subscription> {
    let handle!: Handle;
    await this.#bridge.task(
      { kind: "streamSubscribe", stream },
      {
        // While the completion is dispatched: the runtime publishes the
        // observer's first snapshot behind it, in the same batch.
        settled: ({
          state,
          observerId,
        }: {
          state: SubscriptionState;
          observerId: string;
        }) => {
          const existing = this.#handles.get(state.subscriptionId);
          if (existing) return void (handle = existing);
          handle = new Handle(state, this, this.#bridge, this.#report);
          this.#handles.set(state.subscriptionId, handle);
          handle.attach(observerId);
        },
      },
    );
    return handle;
  }
  /** A handle the runtime closed is no longer the identity's handle. */
  forget(handle: Handle): void {
    if (this.#handles.get(handle.subscriptionId) === handle)
      this.#handles.delete(handle.subscriptionId);
  }
  /** The client is closing: every handle the runtime ends from now on stopped with it. */
  close(): void {
    this.#closing = true;
  }
  /** The runtime is gone: a handle it did not end stops here. */
  closed(): void {
    this.#closing = true;
    for (const handle of [...this.#handles.values()]) handle.stop();
  }
}
