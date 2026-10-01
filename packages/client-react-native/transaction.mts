import type { RecordValue, QuerySpec } from "../client-js/values.mts";
import type { Call } from "../client-js/actions.mts";
import {
  unsentTransaction,
  type TransactionFailures,
  type TransactionRejections,
} from "../client-js/unsent.mts";
import {
  callbackRefusal,
  expiredRefusal,
  submitMutation,
  type MutationOptions,
  type MutationPort,
  type SubmissionHost,
} from "../client-js/local.mts";
export {
  LocalTransaction,
  type LocalCallback,
  type MutationOptions,
} from "../client-js/local.mts";
/**
 * The commands of one application transaction callback. The runtime runs
 * them in submission order inside the transaction it owns, and refuses them
 * once the callback finished; this object tracks unawaited work, the first
 * failure and which submissions still run a `local` callback. It has no
 * savepoint API.
 */
export class Transaction {
  #send: (command: RecordValue, scope?: string) => Promise<any>;
  #host: SubmissionHost;
  #open = true;
  /** Submissions with a `local` callback that have not settled. */
  #locals = 0;
  /** An outer command refused while a `local` callback ran. */
  #structural: unknown;
  /** Settles once every command submitted so far has settled. */
  #tail: Promise<unknown> = Promise.resolve();
  #pending = 0;
  #failure: unknown;
  /**
   * Without AsyncLocalStorage the callback guard is coarse: while a callback
   * runs, every public call counts as inside it (see the README).
   */
  #activeCallback = false;
  constructor(
    send: (command: RecordValue, scope?: string) => Promise<any>,
    mutations?: MutationPort,
  ) {
    this.#send = send;
    this.#host = {
      admit: () => this.#admit(),
      track: (submit) => this.#track(() => submit(undefined)),
      running: (delta) => void (this.#locals += delta),
      expired: () => expiredRefusal(this.#open, this.#poison),
      mutations,
    };
  }
  /** Record a structural refusal: the transaction fails whatever is caught. */
  #poison = (error: Error): void => {
    this.#structural ??= error;
  };
  cancel(): void {
    this.#open = false;
  }
  get scopes(): {
    subscribe(scope: string): Promise<void>;
    unsubscribe(scope: string): Promise<void>;
  } {
    return {
      subscribe: (scope) =>
        this.#call({ kind: "scope", scope, subscribed: true }).then(() => {}),
      unsubscribe: (scope) =>
        this.#call({ kind: "scope", scope, subscribed: false }).then(() => {}),
    };
  }
  /** Dismiss a refusal as part of this transaction. */
  get rejections(): TransactionRejections {
    return unsentTransaction((command) => this.#call(command)).rejections;
  }
  /** Retry failed tasks or drop a failed act as part of this transaction. */
  get failures(): TransactionFailures {
    return unsentTransaction((command) => this.#call(command)).failures;
  }
  async runCallback<T>(body: () => Promise<T>): Promise<T> {
    this.#activeCallback = true;
    try {
      return await body();
    } finally {
      this.#activeCallback = false;
    }
  }
  inCallback(): boolean {
    return this.#activeCallback;
  }
  #queue(command: RecordValue): Promise<any> {
    return this.#track(() => this.#send(command));
  }
  #track(submit: () => Promise<any>): Promise<any> {
    this.#pending++;
    let work: Promise<any>;
    try {
      work = submit();
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
  #call(command: RecordValue): Promise<any> {
    return this.#admit() ?? this.#queue(command);
  }
  /** The refusal of an outer command, if any, before it is submitted. */
  #admit(): Promise<never> | undefined {
    // Object lifetime: an escaped transaction object refuses before admission.
    if (!this.#open) return Promise.reject(Error("transaction_closed"));
    // A `local` callback owns the transaction until its submission settles:
    // a captured or pipelined parent command is refused as the runtime would.
    return callbackRefusal(this.#locals, this.#poison);
  }
  /**
   * Queue a named Mutation in this transaction; see the Node transaction.
   * Its Call stays provisional until the transaction commits.
   */
  submitMutation<T>(
    name: string,
    version: number,
    args: object,
    decode: (value: unknown) => T,
    options?: MutationOptions,
  ): Promise<Call<T>> {
    return submitMutation(this.#host, name, version, args, decode, options);
  }
  /**
   * The callback returned. Promise lifetime decides "unawaited", and a
   * failure is rethrown as the very object the command rejected with; see
   * the Node transaction.
   */
  async finish(): Promise<void> {
    const outstanding = this.#pending > 0;
    this.#open = false;
    await this.#tail;
    if (this.#structural) throw this.#structural;
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
  direct(operation: object) {
    return this.#call({ kind: "direct", operation });
  }
}
