/**
 * Unsent work, account-wide: the acts the server refused, with the act as
 * submitted, the queued acts blocked on a terminally failed prerequisite
 * task, and the pending count, each as a change stream, with their
 * resolutions on the client and inside `client.transaction`
 * ([#186](https://github.com/zanminwang/axton/issues/186),
 * [#205](https://github.com/zanminwang/axton/issues/205)).
 *
 * The runtime reads, re-reads after every commit and compares; this module
 * only delivers what it publishes, with the lifecycle of `watch`.
 */
import type { RecordValue } from "./values.mts";
import type { Bridge, TaskHooks } from "./bridge.mts";

/** One Model operation of an act, with its values. */
export type ActOperation = {
  model: string;
  op: "create" | "update" | "delete";
  identity: RecordValue;
  values?: RecordValue;
};
/**
 * An act as it was submitted: the call's arguments (after their one-time
 * default fill; `null` for a legacy mutation) and its Model operations.
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

type View = "rejections" | "failures" | "pending";

/**
 * The client's namespaces over `task`, which submits a task behind the
 * client's guard, and the bridge that routes observer snapshots.
 */
export function unsentClient(
  bridge: Bridge,
  task: (command: RecordValue, hooks?: TaskHooks) => Promise<any>,
  report: (error: unknown) => void,
): {
  rejections: ClientRejections;
  failures: ClientFailures;
  outbound: ClientOutbound;
} {
  const observe =
    <T,>(view: View, pick: (snapshot: any) => T) =>
    (listener: (value: T) => void, onError?: (error: unknown) => void) =>
      watchUnsent(bridge, task, report, view, pick, listener, onError);
  return {
    rejections: {
      watch: observe("rejections", (snapshot) => snapshot.items),
      get: (id) => task({ kind: "rejectionGet", id }),
      dismiss: (id) =>
        task({ kind: "dismiss", ordinal: id }).then(() => undefined),
    },
    failures: {
      watch: observe("failures", (snapshot) => snapshot.items),
      retry: (taskKeys) =>
        task({ kind: "retryTasks", keys: taskKeys }).then(() => undefined),
      drop: (ordinal) =>
        task({ kind: "discard", ordinal }).then(() => undefined),
    },
    outbound: {
      watchPending: observe("pending", (snapshot) => snapshot.count),
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

/**
 * One unsent-work observer, with the lifecycle of `watch`: the runtime
 * publishes the current result after the registration and every different
 * result after a commit; `listener` receives each. The returned function
 * stops delivery at once and unregisters the observer. `onError` receives
 * the registration's failure and the listener's exceptions; a re-run that
 * fails is the runtime's to report, and the observer stays.
 */
function watchUnsent<T>(
  bridge: Bridge,
  task: (command: RecordValue, hooks?: TaskHooks) => Promise<any>,
  report: (error: unknown) => void,
  view: View,
  pick: (snapshot: any) => T,
  listener: (value: T) => void,
  onError: (error: unknown) => void = () => {},
): () => void {
  const fail = (error: unknown) => {
    try {
      onError(error);
    } catch (thrown) {
      report(thrown);
    }
  };
  let stopped = false;
  let unwatch: (() => void) | undefined;
  task(
    { kind: "unsentWatch", view },
    {
      // Routed while the completion is dispatched: the first result is
      // published behind it in the same batch.
      settled: ({ observerId }: { observerId: string }) => {
        const detach = bridge.observe(observerId, (snapshot) => {
          // A closed observer's last result is the one already delivered.
          if (stopped || snapshot.closed) return;
          try {
            listener(pick(snapshot));
          } catch (error) {
            fail(error);
          }
        });
        unwatch = () =>
          void bridge
            .task({ kind: "unwatch", observerId })
            .catch(() => {})
            .finally(detach);
        if (stopped) unwatch();
      },
    },
  ).catch((error) => {
    if (!stopped) fail(error);
  });
  return () => {
    if (stopped) return;
    stopped = true;
    unwatch?.();
  };
}
