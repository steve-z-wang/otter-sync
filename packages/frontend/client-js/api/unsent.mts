/**
 * Unsent work, account-wide: the acts the server refused, with the act as
 * submitted, the queued acts blocked on a terminally failed prerequisite
 * task, and the pending count, each as a change stream, with their
 * resolutions on the client and inside `client.transaction`
 * ([#186](https://github.com/zanminwang/axton/issues/186),
 * [#205](https://github.com/zanminwang/axton/issues/205)).
 *
 * The runtime reads, re-reads after every commit and compares; the client
 * delivers what it publishes through the observer registration `watch` uses.
 */
import type { RecordValue } from "./values.mts";

/** One Model operation of an act, with its values. */
export type ActOperation = {
  model: string;
  op: "create" | "update" | "delete";
  identity: RecordValue;
  values?: RecordValue;
};
/**
 * A named act as submitted: normalized arguments with fresh-create defaults
 * already folded in, and its declared Model operations.
 * Local companions and cascade effects are not part of it.
 */
export type SubmittedAct = {
  args: RecordValue | null;
  operations: ActOperation[];
};
/** A retained refusal. `id` is the act's ordinal. */
export type RefusedAct = {
  id: number;
  name: string;
  version: number;
  code: string;
  act: SubmittedAct;
};
/**
 * A prerequisite task that failed terminally. `name` and `arguments` are
 * those of a schema-derived key, `null` for an opaque one.
 */
export type FailedTask = {
  key: string;
  name: string | null;
  arguments: RecordValue | null;
  error: string;
};
/** A queued act blocked on at least one failed task. */
export type FailedAct = {
  ordinal: number;
  name: string;
  version: number;
  act: SubmittedAct;
  tasks: FailedTask[];
};

/** `client.rejections`: the refusals retained until dismissed. */
export type ClientRejections = {
  /**
   * The retained refusals, oldest first: the current list, then every
   * different list after a commit. The returned function stops delivery.
   */
  watch(
    listener: (items: RefusedAct[]) => void,
    onError?: (error: unknown) => void,
  ): () => void;
  /** One retained refusal, or `null`. */
  get(id: number): Promise<RefusedAct | null>;
  /** Remove a retained refusal; it is not retried. */
  dismiss(id: number): Promise<void>;
};
/** `client.failures`: the acts blocked on a failed prerequisite task. */
export type ClientFailures = {
  /** The failed acts, oldest first: the current list, then every different list. */
  watch(
    listener: (items: FailedAct[]) => void,
    onError?: (error: unknown) => void,
  ): () => void;
  /**
   * Make the tasks pending again, for every act waiting on them; the
   * handlers registered at open run them.
   */
  retry(taskKeys: string[]): Promise<void>;
  /**
   * Remove an unsent act and its optimism, recording no refusal for it.
   * Acts that depended on its records are refused and appear in
   * `rejections`. Its Call completes as `dropped`.
   */
  drop(ordinal: number): Promise<void>;
};
/** `client.outbound`: the queue of unsettled acts. */
export type ClientOutbound = {
  /** The number of queued, unsettled acts: now, then every different count. */
  watchPending(
    listener: (count: number) => void,
    onError?: (error: unknown) => void,
  ): () => void;
};
/** `tx.rejections`: dismiss a refusal as part of the transaction. */
export type TransactionRejections = {
  dismiss(id: number): Promise<void>;
};
/**
 * `tx.failures`: resolve a failed act as part of the transaction. Each
 * resolution takes effect for the rest of the callback and commits or rolls
 * back with it; a dropped act's Call completes, and a retried task's handler
 * runs, only once the transaction commits.
 */
export type TransactionFailures = {
  retry(taskKeys: string[]): Promise<void>;
  drop(ordinal: number): Promise<void>;
};

/**
 * The client's own observer registration: it submits `command` behind the
 * client's guard and delivers `pick` of each snapshot to `listener`, with the
 * lifecycle of `watch`.
 */
export type Observe = <T>(
  command: RecordValue,
  pick: (snapshot: any) => T,
  listener: (value: T) => void,
  onError?: (error: unknown) => void,
) => () => void;

/**
 * The client's namespaces over `task`, which submits a task behind the
 * client's guard, and `observe`, which registers an observer the way `watch`
 * does.
 */
export function unsentClient(
  task: (command: RecordValue) => Promise<any>,
  observe: Observe,
): {
  rejections: ClientRejections;
  failures: ClientFailures;
  outbound: ClientOutbound;
} {
  return {
    rejections: {
      watch: (listener, onError) =>
        observe(
          { kind: "unsentWatch", view: "rejections" },
          (snapshot): RefusedAct[] => snapshot.items,
          listener,
          onError,
        ),
      get: (id) => task({ kind: "rejectionGet", id }),
      dismiss: (id) =>
        task({ kind: "dismiss", ordinal: id }).then(() => undefined),
    },
    failures: {
      watch: (listener, onError) =>
        observe(
          { kind: "unsentWatch", view: "failures" },
          (snapshot): FailedAct[] => snapshot.items,
          listener,
          onError,
        ),
      retry: (taskKeys) =>
        task({ kind: "retryTasks", keys: taskKeys }).then(() => undefined),
      drop: (ordinal) =>
        task({ kind: "discard", ordinal }).then(() => undefined),
    },
    outbound: {
      watchPending: (listener, onError) =>
        observe(
          { kind: "unsentWatch", view: "pending" },
          (snapshot): number => snapshot.count,
          listener,
          onError,
        ),
    },
  };
}

/** A transaction's namespaces over `call`, which submits one of its commands. */
export function unsentTransaction(
  call: (command: RecordValue) => Promise<any>,
): { rejections: TransactionRejections; failures: TransactionFailures } {
  return {
    rejections: {
      dismiss: (id) =>
        call({ kind: "dismiss", ordinal: id }).then(() => undefined),
    },
    failures: {
      retry: (taskKeys) =>
        call({ kind: "retryTasks", keys: taskKeys }).then(() => undefined),
      drop: (ordinal) =>
        call({ kind: "discard", ordinal }).then(() => undefined),
    },
  };
}
