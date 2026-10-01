import { AsyncLocalStorage } from "node:async_hooks";
import { type RecordValue, type QuerySpec } from "./values.mts";
import type { Call } from "./actions.mts";
import {
  unsentTransaction,
  type TransactionFailures,
  type TransactionRejections,
} from "./unsent.mts";
import {
  callbackRefusal,
  expiredRefusal,
  submitMutation,
  type MutationOptions,
  type MutationPort,
  type SubmissionHost,
} from "./local.mts";
export { strictJson, type RecordValue, type QuerySpec } from "./values.mts";
export {
  LocalTransaction,
  type LocalCallback,
  type MutationOptions,
  type MutationPort,
} from "./local.mts";
/** One open savepoint: the scope token the runtime issued for it, once known. */
type Frame = { scope?: string };
/**
 * The commands of one application transaction callback. The runtime runs
 * them in submission order inside the transaction it owns, and refuses them
 * once the callback finished; this object keeps the language-side evidence:
 * async-context ownership of savepoints, unawaited work, first failure and
 * which submissions still run a `local` callback.
 */
export class Transaction {
  /** `inCallback()` knows the callback's own async context. */
  static readonly exactCallbackGuard = true;
  #send: (command: RecordValue, scope?: string) => Promise<any>;
  #open = true;
  /** Submissions with a `local` callback that have not settled. */
  #locals = 0;
  /** Settles once every command submitted so far has settled. */
  #tail: Promise<unknown> = Promise.resolve();
  #pending = 0;
  #failure: unknown;
  #structural: unknown;
  #context = new AsyncLocalStorage<Frame>();
  #publicContext = new AsyncLocalStorage<symbol>();
  #publicToken = Symbol();
  #active: Frame | undefined;
  #scopes = new Set<Promise<unknown>>();
  #host: SubmissionHost;
  constructor(
    send: (command: RecordValue, scope?: string) => Promise<any>,
    mutations?: MutationPort,
  ) {
    this.#send = send;
    this.#host = {
      admit: () => this.#admit(),
      track: (submit) => this.#track(submit),
      running: (delta) => void (this.#locals += delta),
      expired: () => expiredRefusal(this.#open, this.#poison),
      mutations,
    };
  }
  /** Record a structural refusal: the transaction fails whatever is caught. */
  #poison = (error: Error): void => {
    this.#structural ??= error;
  };
  /** Runtime cancellation fences an escaped handle before user code settles. */
  cancel(): void {
    this.#open = false;
  }
  /** Local Scope intent inside this transaction; no Subscription handle. */
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
  /**
   * Retry failed tasks or drop a failed act as part of this transaction:
   * later commands see the effect, and it commits or rolls back with it.
   */
  get failures(): TransactionFailures {
    return unsentTransaction((command) => this.#call(command)).failures;
  }
  async runCallback<T>(body: () => Promise<T>): Promise<T> {
    try {
      return await this.#publicContext.run(this.#publicToken, body);
    } finally {
      this.#publicContext.disable();
    }
  }
  inCallback(): boolean {
    return this.#publicContext.getStore() === this.#publicToken;
  }
  /** Submit one command in the innermost open savepoint's scope. */
  #queue(command: RecordValue): Promise<any> {
    return this.#track((scope) => this.#send(command, scope));
  }
  #track(submit: (scope: string | undefined) => Promise<any>): Promise<any> {
    this.#pending++;
    let work: Promise<any>;
    try {
      work = submit(this.#active?.scope);
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
    const refused = callbackRefusal(this.#locals, this.#poison);
    if (refused) return refused;
    // Async-context guard: work from outside the innermost savepoint's context
    // would carry the wrong scope token.
    if (this.#active && this.#context.getStore() !== this.#active) {
      this.#structural = Error("overlapping savepoint work");
      return Promise.reject(this.#structural);
    }
    return undefined;
  }
  /**
   * Queue a named Mutation in this transaction. It settles after the
   * Mutation's optimism and its `local` callback ran, with a Call that stays
   * provisional until the transaction commits. The callback runs in this
   * async context; outer commands are refused until the submission settled.
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
   * The callback returned. Promise lifetime decides "unawaited": a command
   * the runtime already ran may not have settled here yet, which its lane
   * cannot see. A failure is rethrown as the very object the command
   * rejected with; the runtime refuses the commit for it as well.
   */
  async finish(): Promise<void> {
    const outstanding = this.#pending > 0 || this.#scopes.size > 0;
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
  savepoint<T>(body: () => Promise<T>): Promise<T> {
    if (!this.#open) return Promise.reject(Error("transaction_closed"));
    const refused = callbackRefusal(this.#locals, this.#poison);
    if (refused) return refused;
    // Async-context guard, as in `#call`.
    if (this.#active && this.#context.getStore() !== this.#active) {
      this.#structural = Error("overlapping savepoints");
      return Promise.reject(this.#structural);
    }
    const parent = this.#active;
    const token: Frame = {};
    // Opened in the parent's scope; the runtime answers the new one.
    const opened = this.#queue({ kind: "savepoint" });
    this.#active = token;
    const failure = this.#failure;
    const run = this.#context.run(token, async () => {
      const scope = (await opened)?.scope;
      if (typeof scope === "string") token.scope = scope;
      try {
        if (!this.#open) throw Error("transaction_closed");
        const value = await body();
        await this.#tail;
        if (!this.#open) throw Error("transaction_closed");
        if (this.#active !== token) {
          this.#structural = Error("unawaited nested savepoint");
          throw this.#structural;
        }
        // A command of this savepoint failed and the body swallowed it: the
        // savepoint rejects with that error object and rolls back, the same
        // outcome the runtime's accounting gives a failure in a savepoint.
        if (this.#failure !== failure) throw this.#failure;
        if (this.#structural) throw this.#structural;
        await this.#queue({ kind: "release", ...scopeOf(token) });
        return value;
      } catch (error) {
        await this.#tail;
        if (this.#open && !this.#structural) {
          if (this.#active !== token) {
            this.#structural = Error("unawaited nested savepoint");
            throw this.#structural;
          }
          await this.#queue({ kind: "rollbackSavepoint", ...scopeOf(token) });
          this.#failure = failure;
        }
        throw error;
      } finally {
        if (this.#active === token) this.#active = parent;
      }
    });
    this.#scopes.add(run);
    void run.then(
      () => this.#scopes.delete(run),
      () => this.#scopes.delete(run),
    );
    return run;
  }
}

/** The scope field of a savepoint's own `release` / `rollbackSavepoint`. */
function scopeOf(frame: Frame): { scope?: string } {
  return frame.scope === undefined ? {} : { scope: frame.scope };
}
