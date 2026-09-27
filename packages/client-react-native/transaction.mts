import type { RecordValue, QuerySpec } from "../client-js/values.mts";
/**
 * The commands of one application transaction callback. The runtime runs
 * them in submission order inside the transaction it owns, and refuses them
 * once the callback finished; this object tracks unawaited work and the first
 * failure. It has no savepoint API.
 */
export class Transaction {
  #send: (command: RecordValue, scope?: string) => Promise<any>;
  #open = true;
  /** Settles once every command submitted so far has settled. */
  #tail: Promise<unknown> = Promise.resolve();
  #pending = 0;
  #failure: unknown;
  /**
   * Without AsyncLocalStorage the callback guard is coarse: while a callback
   * runs, every public call counts as inside it (see the README).
   */
  #activeCallback = false;
  constructor(send: (command: RecordValue, scope?: string) => Promise<any>) {
    this.#send = send;
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
  #call(command: RecordValue): Promise<any> {
    // Object lifetime: an escaped transaction object refuses before admission.
    if (!this.#open) return Promise.reject(Error("transaction_closed"));
    return this.#queue(command);
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
