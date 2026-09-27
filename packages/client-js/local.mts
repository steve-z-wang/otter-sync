import type { QuerySpec, RecordValue } from "./values.mts";
import {
  CallError,
  assertCallOptions,
  type Call,
  type CallOptions,
} from "./actions.mts";

/**
 * A Mutation's `local` callback. It runs inside the submission, in the open
 * transaction, after the Mutation's optimism; its writes are that call's
 * local companions, never sent to the backend.
 */
export type LocalCallback = (local: LocalTransaction) => void | Promise<void>;
/** A Mutation submitted in a transaction: store policy plus the optional `local` callback. */
export type MutationOptions<K extends string = string> = CallOptions<K> & {
  local?: LocalCallback;
};
/** Runs a local callback over its capability's commands; rejects with its failure. */
export type LocalRun = (
  send: (command: RecordValue) => Promise<any>,
) => Promise<void>;
/**
 * The host's `submitMutation`: the runtime's answer registers a provisional
 * Call while it is dispatched, and `local` runs when the runtime asks for it.
 */
export type MutationPort = {
  submit<T>(
    command: RecordValue,
    scope: string | undefined,
    decode: (value: unknown) => T,
    local: LocalRun | undefined,
  ): Promise<Call<T>>;
};
/**
 * The runtime's refusal of an outer transaction command issued while a local
 * callback runs. A transaction adapter gives the same refusal to every
 * command issued while a submission with a callback is unfinished, so it
 * never depends on when the runtime received it.
 */
export const CAPABILITY = "invalid transaction capability";

/** Validate the options and build the runtime command, before any I/O. */
export function mutationCommand(
  name: string,
  version: number,
  args: object,
  options?: MutationOptions,
): { command: RecordValue; local: LocalCallback | undefined } {
  assertCallOptions(options);
  const local = options?.local;
  if (local !== undefined && typeof local !== "function")
    throw new CallError(
      "action.invalid_options",
      "rejected",
      Error("local must be a function"),
    );
  const store = options?.store;
  return {
    command: {
      kind: "submitMutation",
      name,
      version,
      args,
      ...(store === undefined ? {} : { store }),
      ...(local === undefined ? {} : { local: true }),
    },
    local,
  };
}

let open!: (send: (command: RecordValue) => Promise<any>) => LocalTransaction;
let finish!: (local: LocalTransaction) => Promise<void>;

/** Run `callback` as a local callback, then apply the transaction's checks to it. */
export function runLocal(callback: LocalCallback): LocalRun {
  return async (send) => {
    const local = open(send);
    try {
      await callback(local);
    } catch (error) {
      await finish(local).catch(() => {});
      throw error;
    }
    await finish(local);
  };
}

/**
 * The handle a `local` callback receives: local Model reads and direct
 * writes through the callback's own capability, nothing else - no Mutation,
 * Channel, watch or savepoint. It expires when the callback returns; the
 * outstanding-command and first-failure rules are the transaction's.
 */
export class LocalTransaction {
  #send: (command: RecordValue) => Promise<any>;
  #open = true;
  #tail: Promise<unknown> = Promise.resolve();
  #pending = 0;
  #failure: unknown;
  private constructor(send: (command: RecordValue) => Promise<any>) {
    this.#send = send;
  }
  static {
    open = (send) => new LocalTransaction(send);
    finish = (local) => local.#finish();
  }
  #call(command: RecordValue): Promise<any> {
    if (!this.#open) return Promise.reject(Error("transaction_closed"));
    this.#pending++;
    let work: Promise<any>;
    try {
      work = this.#send(command);
    } catch (error) {
      work = Promise.reject(error);
    }
    const settled = work.then(
      () => {
        this.#pending--;
      },
      (error) => {
        this.#pending--;
        this.#failure ??= error;
      },
    );
    this.#tail = Promise.all([this.#tail, settled]);
    return work;
  }
  async #finish(): Promise<void> {
    const outstanding = this.#pending > 0;
    this.#open = false;
    await this.#tail;
    if (outstanding) throw Error("unawaited transaction operation");
    if (this.#failure) throw this.#failure;
  }
  read(model: string, identity: object): Promise<RecordValue | null> {
    return this.#call({ kind: "read", key: { model, identity } });
  }
  query(model: string, where: RecordValue = {}): Promise<RecordValue[]> {
    return this.#call({ kind: "query", model, filter: where });
  }
  readSql(sql: string, parameters: unknown[] = []): Promise<RecordValue[]> {
    return this.#call({ kind: "sql", sql, parameters });
  }
  querySpec(model: string, query: QuerySpec = {}): Promise<RecordValue[]> {
    return this.#call({ kind: "querySpec", model, query });
  }
  related(
    model: string,
    identity: object,
    relation: string,
  ): Promise<RecordValue | null> {
    return this.#call({ kind: "related", key: { model, identity }, relation });
  }
  referencing(
    model: string,
    identity: object,
    source: string,
    relation: string,
  ): Promise<RecordValue[]> {
    return this.#call({
      kind: "referencing",
      key: { model, identity },
      source,
      relation,
    });
  }
  /** A Model write recorded as a local companion of the submitting call. */
  direct(operation: object): Promise<void> {
    return this.#call({ kind: "direct", operation });
  }
}
