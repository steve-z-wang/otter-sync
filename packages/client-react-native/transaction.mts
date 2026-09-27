import type { RecordValue, QuerySpec } from "../client-js/values.mts";
import type { Call } from "../client-js/actions.mts";
import {
  CAPABILITY,
  mutationCommand,
  runLocal,
  type MutationOptions,
  type MutationPort,
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
  #mutations: MutationPort | undefined;
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
    this.#mutations = mutations;
  }
  cancel(): void {
    this.#open = false;
  }
  get channels(): {
    subscribe(channel: string): Promise<void>;
    unsubscribe(channel: string): Promise<void>;
  } {
    return {
      subscribe: (channel) =>
        this.#call({ kind: "channel", channel, subscribed: true }).then(
          () => {},
        ),
      unsubscribe: (channel) =>
        this.#call({ kind: "channel", channel, subscribed: false }).then(
          () => {},
        ),
    };
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
    if (this.#locals > 0) {
      this.#structural ??= Error(CAPABILITY);
      return Promise.reject(Error(CAPABILITY));
    }
    return undefined;
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
    let submission: ReturnType<typeof mutationCommand>;
    try {
      submission = mutationCommand(name, version, args, options);
    } catch (error) {
      return Promise.reject(error);
    }
    const refused = this.#admit();
    if (refused) return refused;
    const { command, local } = submission;
    const mutations = this.#mutations;
    if (local) this.#locals++;
    const work: Promise<Call<T>> = this.#track(() => {
      if (!mutations) throw Error("transaction cannot submit a Mutation");
      return mutations.submit(
        command,
        undefined,
        decode,
        local && runLocal(local),
      );
    });
    if (local) {
      const settled = () => void this.#locals--;
      void work.then(settled, settled);
    }
    return work;
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
