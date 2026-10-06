/** Promise lifetime and first failure for one transaction command scope. */
export class CommandAccounting {
  #pending = 0;
  #tail: Promise<unknown> = Promise.resolve();
  #failure: unknown;

  get outstanding(): boolean {
    return this.#pending > 0;
  }

  get failure(): unknown {
    return this.#failure;
  }

  /** A rolled-back savepoint restores the enclosing scope's failure. */
  restoreFailure(failure: unknown): void {
    this.#failure = failure;
  }

  track<T>(submit: () => Promise<T>): Promise<T> {
    this.#pending++;
    let work: Promise<T>;
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

  /** Drain submitted work without hiding its recorded failure. */
  drain(): Promise<unknown> {
    return this.#tail;
  }
}
