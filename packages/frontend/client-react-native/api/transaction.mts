import type { RecordValue, QuerySpec } from "../../client-js/api/values.mts";
import { CommandAccounting } from "../../client-js/api/command-accounting.mts";
import type { Call } from "../../client-js/api/actions.mts";
import {
  unsentTransaction,
  type TransactionFailures,
  type TransactionRejections,
} from "../../client-js/api/unsent.mts";
import {
  callbackRefusal,
  expiredRefusal,
  submitMutation,
  type MutationInput,
  type MutationPort,
  type SubmissionHost,
} from "../../client-js/api/local.mts";
export {
  LocalTransaction,
  type LocalCallback,
  type MutationInput,
} from "../../client-js/api/local.mts";
const scopedCallbacks = new Set<object>();
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
  #commands = new CommandAccounting();
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
      track: (submit) => this.#commands.track(() => submit(undefined)),
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
  /** Dismiss a refusal as part of this transaction. */
  get rejections(): TransactionRejections {
    return unsentTransaction((command) => this.#call(command)).rejections;
  }
  /** Retry failed tasks or drop a failed act as part of this transaction. */
  get failures(): TransactionFailures {
    return unsentTransaction((command) => this.#call(command)).failures;
  }
  async runCallback<T>(body: () => Promise<T>): Promise<T> {
    if (scopedCallbacks.size)
      throw Error(
        "overlapping transaction callbacks require async context support",
      );
    scopedCallbacks.add(this);
    this.#activeCallback = true;
    try {
      return await body();
    } finally {
      this.#activeCallback = false;
      scopedCallbacks.delete(this);
    }
  }
  inCallback(): boolean {
    return this.#activeCallback;
  }
  #queue(command: RecordValue): Promise<any> {
    return this.#commands.track(() => this.#send(command));
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
    input: MutationInput,
    decode: (value: unknown) => T,
  ): Promise<Call<T>> {
    return submitMutation(this.#host, name, version, input, decode);
  }
  /**
   * The callback returned. Promise lifetime decides "unawaited", and a
   * failure is rethrown as the very object the command rejected with; see
   * the Node transaction.
   */
  async finish(): Promise<void> {
    const outstanding = this.#commands.outstanding;
    this.#open = false;
    await this.#commands.drain();
    if (this.#structural) throw this.#structural;
    if (outstanding) throw Error("unawaited transaction operation");
    if (this.#commands.failure) throw this.#commands.failure;
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
