import { reportCallbackError, type EffectOutcome } from "./bridge.mts";
import type { RecordValue } from "./values.mts";

/** One HTTP POST: `kind` is the route (`push`, `pull` or `action`). Errors carry `status` when the server answered. */
export type Transport = (
  kind: string,
  body: string,
  signal?: AbortSignal,
) => Promise<string>;
/** How one socket reports to its effect. Frames arrive one at a time, in order. */
export type SocketEvents = {
  message(text: string): Promise<void>;
  /** The frame buffer overflowed; frames were dropped. */
  overflow(): Promise<void>;
  /** The socket ended on its own; not called for a socket the signal aborted. */
  closed(error: unknown): void;
};
/** The platform network one connection executes its effects on. */
export type EffectNetwork = {
  push: Transport;
  open(subscribe: string, signal: AbortSignal, on: SocketEvents): void;
};
export type ConnectionOptions = {
  onError?: (error: unknown) => void;
  refreshAuth?: () => Promise<void>;
  /** Maximum duration of one direct Action attempt, including token acquisition and authentication refresh. Integer 1..2147483647 ms; default 30000 ms. */
  directTimeoutMs?: number;
};
export type Connection = {
  pause(): Promise<void>;
  resume(): Promise<void>;
  wake(): Promise<void>;
  close(): Promise<void>;
};

/** Why one record or one queued mutation could not be applied as delivered. */
export type ReportKind = "readFailed" | "skipped" | "conflict" | "diverged";
export type ReportDetails = {
  kind: ReportKind;
  model: string;
  identity: Record<string, unknown>;
  stamp: number;
  /** `readFailed`: the server's code (`loader.failed`, or the refusal code). */
  code?: string;
  /** `diverged`: the queued mutation whose replay failed; it is still sent. */
  ordinal?: number;
  detail?: unknown;
};
/**
 * A delivery the client could not apply, handed to `onError`. The client stays
 * consistent: a `readFailed` or `skipped` record keeps its local content and
 * stamp, a `conflict` keeps the local content, a `diverged` mutation shows the
 * server's row and is still sent.
 */
export class AxtonReport extends Error {
  readonly kind: ReportKind;
  readonly model: string;
  readonly identity: Record<string, unknown>;
  readonly stamp: number;
  readonly code: string | undefined;
  readonly ordinal: number | undefined;
  readonly detail: unknown;
  constructor(report: ReportDetails) {
    super(
      `${report.kind}: ${report.model} ${JSON.stringify(report.identity)} at stamp ${report.stamp}` +
        (report.code ? ` (${report.code})` : "") +
        (report.ordinal !== undefined ? ` (mutation ${report.ordinal})` : ""),
    );
    this.name = "AxtonReport";
    this.kind = report.kind;
    this.model = report.model;
    this.identity = report.identity;
    this.stamp = report.stamp;
    this.code = report.code;
    this.ordinal = report.ordinal;
    this.detail = report.detail;
  }
}

/** What a runtime `report` event carries (`Diagnostic` in [protocol.rs](../../crates/client/src/runtime/protocol.rs)). */
export type Diagnostic =
  | { kind: "records"; reports: ReportDetails[] }
  | { kind: "error"; message: string; status?: number }
  | { kind: "protocol"; message: string };

/** The part of the Bridge an effect executor uses; tests supply a fake. */
export type EffectBridge = {
  effectResult(effectId: string, outcome: EffectOutcome): void;
  onEffect(
    kind: string,
    handler: (effectId: string, operation: any) => void,
  ): () => void;
  on(
    type: "cancelEffect" | "report",
    listener: (event: any) => void,
  ): () => void;
};

/** The longest delay a platform timer accepts. */
const MAX_DELAY = 2_147_483_647;

/**
 * Typed encoding of the direct-call deadline `connect` hands the runtime: a
 * value its `u64` field cannot decode would fail with a serde message, so the
 * SDK refuses it with the runtime's own wording before installing adapters.
 */
export function directTimeout(options: ConnectionOptions): number {
  const millis = options.directTimeoutMs ?? 30_000;
  if (!Number.isSafeInteger(millis) || millis <= 0 || millis > MAX_DELAY)
    throw Error("directTimeoutMs must be an integer from 1 to 2147483647");
  return millis;
}

/** The text a failure keeps: the error's message, or the thrown value. Never throws. */
export function reason(thrown: unknown): string {
  try {
    if (thrown instanceof Error && thrown.message) return thrown.message;
    return String(thrown);
  } catch {
    return "failed";
  }
}
/** A failed effect: its message, and the HTTP status it carried if any. */
function failure(error: unknown): EffectOutcome {
  const status = (error as { status?: unknown } | null)?.status;
  return {
    ok: false,
    error: {
      message: reason(error),
      ...(typeof status === "number" ? { status } : {}),
    },
  };
}

/**
 * The SDK's effect executor ([#134](https://github.com/zanminwang/axton/issues/134)).
 * The Rust runtime owns the connection lanes, direct calls, prerequisites,
 * credential refresh, deadlines and retries; it asks for platform work as
 * effects and this class only runs them. Each running effect keeps how to
 * abort it until its final answer; `cancelEffect` aborts it and silences any
 * later answer. No branch here decides what an outcome means.
 */
export class Effects {
  readonly #bridge: EffectBridge;
  /** Running effects by id: how to abort each, and who started it. */
  #running = new Map<string, { abort: () => void; owner: object }>();

  constructor(bridge: EffectBridge) {
    this.#bridge = bridge;
    bridge.on("cancelEffect", (event: { effectId: string }) =>
      this.#cancel(event.effectId),
    );
    // Deadlines and backoff: the runtime chose the delay, this only waits.
    this.handle(this, "timer", (effectId, { millis }: { millis: number }) => {
      const timer = setTimeout(
        () => this.answer(effectId, { ok: true }),
        Math.min(millis, MAX_DELAY),
      );
      return () => clearTimeout(timer);
    });
  }

  /**
   * Run effects of `kind` with `run`, which starts the platform work and
   * returns how to abort it. The effect is running before `run` starts, so an
   * answer given synchronously is delivered; a throw is its failure.
   */
  handle(
    owner: object,
    kind: string,
    run: (effectId: string, operation: any) => (() => void) | void,
  ): () => void {
    return this.#bridge.onEffect(kind, (effectId, operation) => {
      const entry = { abort: () => {}, owner };
      this.#running.set(effectId, entry);
      try {
        const abort = run(effectId, operation);
        if (abort) entry.abort = abort;
      } catch (error) {
        this.answer(effectId, failure(error));
      }
    });
  }

  /**
   * Answer a running effect; unless `final` is false (a socket frame), the
   * answer retires it. A cancelled or retired effect is not answered. True
   * when the answer went out.
   */
  answer(effectId: string, outcome: EffectOutcome, final = true): boolean {
    if (!this.#running.has(effectId)) return false;
    if (final) this.#running.delete(effectId);
    this.#bridge.effectResult(effectId, outcome);
    return true;
  }

  /** Abort every running effect `owner` started. */
  abort(owner: object): void {
    for (const [effectId, entry] of [...this.#running])
      if (entry.owner === owner) this.#cancel(effectId);
  }

  #cancel(effectId: string): void {
    const entry = this.#running.get(effectId);
    if (!entry) return;
    this.#running.delete(effectId);
    try {
      entry.abort();
    } catch (error) {
      reportCallbackError(error);
    }
  }
}

/**
 * Install one connection's effects - HTTP to its server, its socket and the
 * application's `refreshAuth` - and hand the runtime's reports to `onError`.
 * Answers the uninstall, which also aborts every platform resource the
 * connection still holds.
 */
export function startConnection(
  bridge: EffectBridge,
  effects: Effects,
  network: EffectNetwork,
  options: ConnectionOptions,
): () => void {
  const owner = {};
  const fail = (effectId: string, error: unknown) =>
    void effects.answer(effectId, failure(error));
  const uninstall = [
    effects.handle(owner, "http", (effectId, operation) => {
      const { route, body } = operation as { route: string; body: string };
      const abort = new AbortController();
      Promise.resolve()
        .then(() => network.push(route, body, abort.signal))
        .then(
          (text) => effects.answer(effectId, { ok: true, value: text }),
          (error) => fail(effectId, error),
        );
      return () => abort.abort();
    }),
    effects.handle(owner, "socket", (effectId, operation) => {
      const abort = new AbortController();
      const frame = async (value: RecordValue) =>
        void effects.answer(effectId, { ok: true, value }, false);
      network.open(
        (operation as { subscribe: string }).subscribe,
        abort.signal,
        {
          message: (body) => frame({ event: "message", body }),
          overflow: () => frame({ event: "overflow" }),
          closed: (error) => fail(effectId, error),
        },
      );
      return () => abort.abort();
    }),
  ];
  const refreshAuth = options.refreshAuth;
  if (refreshAuth)
    uninstall.push(
      effects.handle(owner, "refreshAuth", (effectId) => {
        Promise.resolve()
          .then(() => refreshAuth())
          .then(
            () => effects.answer(effectId, { ok: true }),
            (error) => fail(effectId, error),
          );
      }),
    );
  const onError = options.onError;
  uninstall.push(
    bridge.on("report", ({ diagnostic }: { diagnostic: Diagnostic }) => {
      if (!onError) return;
      // A lane failure is the runtime's report of it: its message, and the
      // HTTP status it carried when it had one.
      const errors =
        diagnostic.kind === "records"
          ? diagnostic.reports.map((report) => new AxtonReport(report))
          : [
              Object.assign(
                Error(diagnostic.message),
                diagnostic.kind === "error" && diagnostic.status !== undefined
                  ? { status: diagnostic.status }
                  : {},
              ),
            ];
      for (const error of errors)
        try {
          onError(error);
        } catch (thrown) {
          reportCallbackError(thrown);
        }
    }),
  );
  return () => {
    for (const remove of uninstall) remove();
    effects.abort(owner);
  };
}

/**
 * Install the application's prerequisite handlers for one `runPrerequisites`
 * task: the runtime picks each task and records its outcome, this only runs
 * `handlers[name](arguments)`. Answers the uninstall.
 */
export function prerequisites(
  effects: Effects,
  handlers: Record<string, (arguments_: RecordValue) => unknown>,
): () => void {
  const owner = {};
  const remove = effects.handle(
    owner,
    "prerequisite",
    (effectId, operation) => {
      const { name, arguments: args } = operation as {
        name: string;
        arguments: RecordValue;
      };
      Promise.resolve()
        .then(() => handlers[name]!(args))
        .then(
          () => effects.answer(effectId, { ok: true }),
          (thrown) =>
            effects.answer(effectId, {
              ok: false,
              error: { message: reason(thrown) },
            }),
        );
    },
  );
  return () => {
    remove();
    effects.abort(owner);
  };
}
