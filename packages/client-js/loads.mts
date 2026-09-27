import type { ObserverSnapshot, TaskError, TaskHooks } from "./bridge.mts";
import type { RecordValue } from "./values.mts";

/**
 * Native Load handles ([#173](https://github.com/zanminwang/axton/issues/173)).
 * A handle decides nothing: the Rust runtime persists every job, schedules and
 * batches its pages, projects its status and parks every `wait()` on its run.
 * This file keeps the language objects - a handle's last published status, its
 * listeners and its observer route - and maps the runtime's codes to
 * `LoadError`. There is no page loop, retry or once decision here.
 */

/**
 * Where a job is. `loading` is a page in flight or being applied; `waiting` is
 * offline, paused, backing off or behind a pending rebuild.
 */
export type LoadPhase =
  "pending" | "loading" | "waiting" | "complete" | "failed" | "cancelled";
/**
 * One immutable snapshot of a job, as the runtime projected it. `name` is the
 * schema operation name; `pages` counts committed pages, not rows or percent;
 * `error` is set only for a failed or cancelled job.
 */
export type LoadStatus<Name extends string = string> = Readonly<{
  id: string;
  name: Name;
  version: number;
  phase: LoadPhase;
  pages: number;
  error: null | Readonly<{ code: string; message: string }>;
}>;
/**
 * Call-site options, kept apart from business args and never sent to the
 * backend. `once` reuses the job an earlier `once` start of the same arguments
 * registered (active, complete or failed); `refresh` (only with `once`) starts
 * again from the first page unless that job is still active.
 */
export type LoadOptions = { once?: boolean; refresh?: boolean };
/** A Load operation or `wait()` the runtime refused or failed, with its code. */
export class LoadError extends Error {
  readonly code: string;
  override readonly cause: unknown;
  constructor(code: string, message: string = code, cause?: unknown) {
    super(message);
    this.name = "LoadError";
    this.code = code;
    this.cause = cause;
  }
}
/**
 * One process-local handle of a durable job. Several handles may name the same
 * job; each has its own observer. `dispose()` releases only that observer.
 */
export interface Load<Name extends string = string> {
  readonly id: string;
  /**
   * The runtime's last published status. It stays readable after dispose or
   * close. A schema rebuild ends the handle with a `failed` status whose error
   * is `load.schema_changed`.
   */
  readonly status: LoadStatus<Name>;
  /** Deliver the current status at once, then every distinct change, until the returned function is called. */
  watch(listener: (status: LoadStatus<Name>) => void): () => void;
  /** Resolves after the current run's final page committed; rejects with its recorded `LoadError`. */
  wait(): Promise<void>;
  /** Stop the job; committed pages stay. A no-op for a complete job. */
  cancel(): Promise<void>;
  /** Read a failed job again from its last committed continuation. */
  retry(): Promise<void>;
  /**
   * Delete a terminal job; later management calls fail `load.not_found`.
   * Every management call on a job a schema rebuild abandoned fails
   * `load.schema_changed`.
   */
  forget(): Promise<void>;
  /** Release this handle's observer. The job continues. */
  dispose(): void;
}

/** The part of the client the handles use. */
export type LoadBridge = {
  /** A task behind the client's callback guard; `writes` as the guard names it. */
  task(command: RecordValue, hooks?: TaskHooks, writes?: boolean): Promise<any>;
  /** A release that needs no guard and whose failure changes nothing. */
  release(command: RecordValue): Promise<unknown>;
  observe(
    observerId: string,
    listener: (snapshot: ObserverSnapshot) => void,
  ): () => void;
};
type Opened = { loadId: string; observerId: string; status: LoadStatus };

/**
 * A runtime failure as the Load API names it: the code the runtime decided
 * (`details.code`), a closed client, or a call from a transaction callback.
 * Anything else stays the engine's error.
 */
export function loadError(error: unknown): unknown {
  if (error instanceof LoadError) return error;
  const details = (error as TaskError | null)?.details;
  if (typeof details?.code === "string")
    return new LoadError(
      details.code,
      typeof details.message === "string" ? details.message : details.code,
      error,
    );
  const message = (error as { message?: unknown } | null)?.message;
  if (message === "client_closed" || message === "transaction_active")
    return new LoadError(message, message, error);
  return error;
}
/**
 * The command fields of `options`. The runtime refuses non-Boolean flags and
 * refresh without once before it creates any work; an options value that is
 * not an object, or names another option, is refused here.
 */
function optionFields(options: unknown): { once?: unknown; refresh?: unknown } {
  if (options === undefined) return {};
  if (options === null || typeof options !== "object" || Array.isArray(options))
    throw new LoadError(
      "load.invalid_options",
      "Load options must be an object",
    );
  const unknown = Object.keys(options).find(
    (key) => key !== "once" && key !== "refresh",
  );
  if (unknown !== undefined)
    throw new LoadError(
      "load.invalid_options",
      `${unknown} is not a Load option; only once and refresh are`,
    );
  const { once, refresh } = options as { once?: unknown; refresh?: unknown };
  return {
    ...(once === undefined ? {} : { once }),
    ...(refresh === undefined ? {} : { refresh }),
  };
}
function frozen<Name extends string>(status: LoadStatus): LoadStatus<Name> {
  return Object.freeze({
    ...status,
    error: status.error === null ? null : Object.freeze({ ...status.error }),
  }) as LoadStatus<Name>;
}

function sameStatus(a: LoadStatus, b: LoadStatus): boolean {
  return (
    a.id === b.id &&
    a.name === b.name &&
    a.version === b.version &&
    a.phase === b.phase &&
    a.pages === b.pages &&
    a.error?.code === b.error?.code &&
    a.error?.message === b.error?.message
  );
}

class LoadHandle<Name extends string> implements Load<Name> {
  readonly id: string;
  #bridge: LoadBridge;
  #report: (error: unknown) => void;
  #snapshot: LoadStatus<Name>;
  /** Unset once disposed or ended by the runtime: nothing more is delivered. */
  #observerId: string | undefined;
  #detach: (() => void) | undefined;
  #listeners = new Set<(status: LoadStatus<Name>) => void>();
  constructor(
    opened: Opened,
    bridge: LoadBridge,
    report: (error: unknown) => void,
  ) {
    this.id = opened.loadId;
    this.#bridge = bridge;
    this.#report = report;
    // Replaced by the runtime's first snapshot, published behind the answer.
    this.#snapshot = frozen(opened.status);
    this.#observerId = opened.observerId;
    this.#detach = bridge.observe(opened.observerId, (snapshot) =>
      this.#receive(snapshot),
    );
  }
  get status(): LoadStatus<Name> {
    return this.#snapshot;
  }
  #receive(snapshot: ObserverSnapshot): void {
    if (this.#observerId === undefined) return;
    const status = snapshot.status as LoadStatus;
    // The runtime publishes only changes; the status the answer carried can
    // equal its first snapshot, which is then not delivered again.
    if (!sameStatus(status, this.#snapshot)) {
      this.#snapshot = frozen(status);
      for (const listener of [...this.#listeners]) this.#deliver(listener);
    }
    // Close or rebuild ended the observer: its route ended with this snapshot.
    // The runtime decides what it carries: the last status on close, a failed
    // status with the rebuild's `load.schema_changed` error on rebuild.
    if (snapshot.closed === true) this.#end();
  }
  #deliver(listener: (status: LoadStatus<Name>) => void): void {
    try {
      listener(this.#snapshot);
    } catch (error) {
      this.#report(error);
    }
  }
  #end(): void {
    this.#observerId = undefined;
    this.#detach?.();
    this.#detach = undefined;
    this.#listeners.clear();
  }
  watch(listener: (status: LoadStatus<Name>) => void): () => void {
    if (this.#observerId !== undefined) this.#listeners.add(listener);
    this.#deliver(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }
  #manage(kind: string): Promise<void> {
    return this.#bridge.task({ kind, loadId: this.id }, undefined, true).then(
      () => undefined,
      (error) => {
        throw loadError(error);
      },
    );
  }
  wait(): Promise<void> {
    return this.#manage("loadWait");
  }
  cancel(): Promise<void> {
    return this.#manage("loadCancel");
  }
  retry(): Promise<void> {
    return this.#manage("loadRetry");
  }
  forget(): Promise<void> {
    return this.#manage("loadForget");
  }
  dispose(): void {
    const observerId = this.#observerId;
    if (observerId === undefined) return;
    this.#end();
    void this.#bridge
      .release({ kind: "loadDispose", observerId })
      .catch(() => {});
  }
}

/**
 * The `client.loads` surface: start, reattach, list and invalidate. Every
 * handle is a fresh language object with its own observer; none is cached
 * here, so a handle the application drops or disposes is not retained.
 */
export class Loads {
  #bridge: LoadBridge;
  #report: (error: unknown) => void;
  constructor(bridge: LoadBridge, report: (error: unknown) => void) {
    this.#bridge = bridge;
    this.#report = report;
  }
  /**
   * Accept a job durably and answer its handle: after the local commit, with
   * no connection needed and no data promised yet.
   */
  start<Name extends string>(
    name: Name,
    version: number,
    args: object,
    options?: LoadOptions,
  ): Promise<Load<Name>> {
    let fields: { once?: unknown; refresh?: unknown };
    try {
      fields = optionFields(options);
    } catch (error) {
      return Promise.reject(error);
    }
    return this.#open<Name>(
      { kind: "loadStart", name, version, args, ...fields },
      true,
    ) as Promise<Load<Name>>;
  }
  /** Reattach to a job of this replica by ID: `null` when there is none. */
  get(id: string): Promise<Load | null> {
    return this.#open({ kind: "loadGet", loadId: id }, false);
  }
  /** The most recently started jobs, newest first; `limit` is 1..100, 50 by default. */
  list(options: { limit?: number } = {}): Promise<LoadStatus[]> {
    const limit = options?.limit;
    return this.#bridge
      .task({ kind: "loadList", ...(limit === undefined ? {} : { limit }) })
      .then(
        (statuses: LoadStatus[]) => statuses.map((status) => frozen(status)),
        (error) => {
          throw loadError(error);
        },
      );
  }
  /**
   * Remove the once mappings of one operation and business arguments across
   * its retained versions, in a local commit. No job is cancelled and no Model
   * deleted; a later once start creates fresh work.
   */
  invalidate(name: string, args: object): Promise<void> {
    return this.#bridge
      .task({ kind: "loadInvalidate", name, args }, undefined, true)
      .then(
        () => undefined,
        (error) => {
          throw loadError(error);
        },
      );
  }
  #open<Name extends string>(
    command: RecordValue,
    writes: boolean,
  ): Promise<Load<Name> | null> {
    let handle: Load<Name> | null = null;
    return this.#bridge
      .task(
        command,
        {
          // While the answer is dispatched: the observer's first snapshot
          // follows it in the same batch.
          settled: (value: Opened | null) => {
            if (value !== null)
              handle = new LoadHandle<Name>(value, this.#bridge, this.#report);
          },
        },
        writes,
      )
      .then(
        () => handle,
        (error) => {
          throw loadError(error);
        },
      );
  }
}
