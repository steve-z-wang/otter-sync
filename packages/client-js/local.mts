import type { QuerySpec, RecordValue } from "./values.mts";
import { CallError, type Call } from "./actions.mts";

/** Callback input is produced inside the Mutation's owned local scope. */
export type LocalCallback = (tx: LocalTransaction) => object | Promise<object>;
export type MutationInput = object | LocalCallback;
export type LocalRun = (
  send: (command: RecordValue) => Promise<any>,
) => Promise<object>;
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

/**
 * The refusal of a parent command while `running` submissions still run a
 * `local` callback: recorded through `poison` as structural, so the
 * transaction fails whatever the callback catches.
 */
export function callbackRefusal(
  running: number,
  poison: (error: Error) => void,
): Promise<never> | undefined {
  if (running === 0) return undefined;
  poison(Error(CAPABILITY));
  return Promise.reject(Error(CAPABILITY));
}

/**
 * The refusal of a command sent through an expired `local` handle. While the
 * transaction is open it is the runtime's answer to a stale capability and
 * poisons the transaction; afterwards the handle is simply closed.
 */
export function expiredRefusal(
  open: boolean,
  poison: (error: Error) => void,
): Error {
  if (!open) return Error("transaction_closed");
  poison(Error(CAPABILITY));
  return Error(CAPABILITY);
}

/** What a transaction adapter lends the shared Mutation submission. */
export type SubmissionHost = {
  /** The adapter's refusal of an outer command, if any. */
  admit(): Promise<never> | undefined;
  /** Track the submission as one of the transaction's commands. */
  track(submit: (scope: string | undefined) => Promise<any>): Promise<any>;
  /** A submission with a `local` callback started (+1) or settled (-1). */
  running(delta: 1 | -1): void;
  /** The refusal of a command sent through an expired `local` handle. */
  expired(): Error;
  mutations: MutationPort | undefined;
  localAdmit?(): Error | undefined;
};

/**
 * Queue a named Mutation in `host`'s transaction. It settles after the
 * Mutation's optimism and its `local` callback ran, with a Call that stays
 * provisional until the transaction commits; outer commands are refused
 * until it settled.
 */
export function submitMutation<T>(
  host: SubmissionHost,
  name: string,
  version: number,
  input: MutationInput,
  decode: (value: unknown) => T,
): Promise<Call<T>> {
  let submission: ReturnType<typeof mutationCommand>;
  try {
    submission = mutationCommand(name, version, input);
  } catch (error) {
    return Promise.reject(error);
  }
  const refused = host.admit();
  if (refused) return refused;
  const { command, local } = submission;
  const mutations = host.mutations;
  if (local) host.running(1);
  const work: Promise<Call<T>> = host.track((scope) => {
    if (!mutations) throw Error("transaction cannot submit a Mutation");
    return mutations.submit(
      command,
      scope,
      decode,
      local && runLocal(local, host.expired, host.localAdmit),
    );
  });
  if (local) {
    const settled = () => host.running(-1);
    void work.then(settled, settled);
  }
  return work;
}

/** Only named input or a callback can select the submission scope. */
function mutationCommand(
  name: string,
  version: number,
  input: MutationInput,
): { command: RecordValue; local: LocalCallback | undefined } {
  const local =
    typeof input === "function" ? (input as LocalCallback) : undefined;
  if (input === null || (typeof input !== "object" && !local))
    throw new CallError("action.invalid", "rejected");
  return {
    command: {
      kind: "submitMutation",
      name,
      version,
      ...(local ? { local: true } : { args: input }),
    },
    local,
  };
}

let open!: (
  send: (command: RecordValue) => Promise<any>,
  expired: () => Error,
  admit?: () => Error | undefined,
) => LocalTransaction;
let finish!: (local: LocalTransaction) => Promise<void>;

/**
 * Run `callback` as a local callback, then apply the transaction's checks to
 * it. `expired` answers a command sent through its handle after it returned.
 */
function runLocal(
  callback: LocalCallback,
  expired: () => Error,
  admit?: () => Error | undefined,
): LocalRun {
  return async (send) => {
    const local = open(send, expired, admit);
    try {
      const input = await callback(local);
      await finish(local);
      return input;
    } catch (error) {
      await finish(local).catch(() => {});
      throw error;
    }
  };
}

/**
 * The handle a `local` callback receives: local Model reads and direct
 * writes through the callback's own capability, nothing else - no Mutation,
 * Scope, watch or savepoint. It expires when the callback returns: a later
 * command is refused, and poisons the transaction while it is still open.
 * The outstanding-command and first-failure rules are the transaction's.
 */
export class LocalTransaction {
  #send: (command: RecordValue) => Promise<any>;
  #expired: () => Error;
  #admit: (() => Error | undefined) | undefined;
  #open = true;
  #tail: Promise<unknown> = Promise.resolve();
  #pending = 0;
  #failure: unknown;
  private constructor(
    send: (command: RecordValue) => Promise<any>,
    expired: () => Error,
    admit?: () => Error | undefined,
  ) {
    this.#admit = admit;
    this.#send = send;
    this.#expired = expired;
  }
  static {
    open = (send, expired, admit) => new LocalTransaction(send, expired, admit);
    finish = (local) => local.#finish();
  }
  #call(command: RecordValue): Promise<any> {
    if (!this.#open) return Promise.reject(this.#expired());
    const refused = this.#admit?.();
    if (refused) return Promise.reject(refused);
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
