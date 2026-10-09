import { AsyncLocalStorage } from "node:async_hooks";
import { CommandAccounting } from "./command-accounting.mts";
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
  type MutationInput,
  type MutationPort,
  type SubmissionHost,
} from "./local.mts";
export { strictJson, type RecordValue, type QuerySpec } from "./values.mts";
export {
  LocalTransaction,
  type LocalCallback,
  type MutationInput,
  type MutationPort,
} from "./local.mts";
/** One open savepoint: the scope token the runtime issued for it, once known. */
const callbackOwner = new AsyncLocalStorage<object>();
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
  #commands = new CommandAccounting();
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
      localAdmit: () => this.#foreign(),
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
      return await callbackOwner.run(this, () =>
        this.#publicContext.run(this.#publicToken, body),
      );
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
    return this.#commands.track(() => submit(this.#active?.scope));
  }
  #call(command: RecordValue): Promise<any> {
    return this.#admit() ?? this.#queue(command);
  }
  /** The refusal of an outer command, if any, before it is submitted. */
  #foreign(): Error | undefined {
    const owner = callbackOwner.getStore();
    if (owner !== undefined && owner !== this) {
      const error = Error("foreign transaction scope");
      this.#poison(error);
      return error;
    }
    return undefined;
  }
  #admit(): Promise<never> | undefined {
    const foreign = this.#foreign();
    if (foreign) return Promise.reject(foreign);
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
    input: MutationInput,
    decode: (value: unknown) => T,
  ): Promise<Call<T>> {
    return submitMutation(this.#host, name, version, input, decode);
  }
  /**
   * The callback returned. Promise lifetime decides "unawaited": a command
   * the runtime already ran may not have settled here yet, which its lane
   * cannot see. A failure is rethrown as the very object the command
   * rejected with; the runtime refuses the commit for it as well.
   */
  async finish(): Promise<void> {
    const outstanding = this.#commands.outstanding || this.#scopes.size > 0;
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
  savepoint<T>(body: () => Promise<T>): Promise<T> {
    const foreign = this.#foreign();
    if (foreign) return Promise.reject(foreign);
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
    const failure = this.#commands.failure;
    const run = this.#context.run(token, async () => {
      const scope = (await opened)?.scope;
      if (typeof scope === "string") token.scope = scope;
      try {
        if (!this.#open) throw Error("transaction_closed");
        const value = await body();
        await this.#commands.drain();
        if (!this.#open) throw Error("transaction_closed");
        if (this.#active !== token) {
          this.#structural = Error("unawaited nested savepoint");
          throw this.#structural;
        }
        // A command of this savepoint failed and the body swallowed it: the
        // savepoint rejects with that error object and rolls back, the same
        // outcome the runtime's accounting gives a failure in a savepoint.
        if (this.#commands.failure !== failure) throw this.#commands.failure;
        if (this.#structural) throw this.#structural;
        await this.#queue({ kind: "release", ...scopeOf(token) });
        return value;
      } catch (error) {
        await this.#commands.drain();
        if (this.#open && !this.#structural) {
          if (this.#active !== token) {
            this.#structural = Error("unawaited nested savepoint");
            throw this.#structural;
          }
          await this.#queue({ kind: "rollbackSavepoint", ...scopeOf(token) });
          this.#commands.restoreFailure(failure);
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
