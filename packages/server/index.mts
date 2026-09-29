import { createRequire } from "node:module";
import { createServer, STATUS_CODES } from "node:http";
import type { IncomingMessage, RequestListener, Server } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocketServer, WebSocket } from "ws";
import {
  effectsFor,
  lowerFirst,
  type RuntimeChannel,
  type RuntimeTouch,
} from "./effects.mts";
import type {
  HostRequest,
  LoadNext,
  SettlementEffects,
} from "./host-contract.mts";
import { isRetryableTransactionError } from "./retryable.mts";
export { WebSocket } from "ws";
export type {
  RecordRef,
  RuntimeChannel,
  RuntimeModelMembership,
  RuntimeTouch,
} from "./effects.mts";
export type { JsonValue, LoadNext } from "./host-contract.mts";
import type { JsonValue } from "./host-contract.mts";
const require = createRequire(import.meta.url);
/** What escaped one Load item's transaction, as the carrier observed it. */
export type LoadFault =
  | { kind: "engine"; code: string; message: string }
  | { kind: "conflict" }
  | { kind: "unavailable" };
/** One Load item as its transaction ended: its committed page or its fault. */
export type LoadItemAnswer = { page: string } | { fault: LoadFault };
export type Native = {
  validateConfig(config: string): void;
  processPush(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  processAction(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  /** Serves one Model Fetch (`POST /sync/fetch`) through its versioned Loader. */
  processFetch(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  processPull(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  /** Structurally validates a `{loads:[…]}` batch; answers each item's canonical JSON in order. */
  validateLoadBatch(request: string): string[];
  /**
   * The one bounded `{"loads":[…]}` response: the items `validateLoadBatch`
   * answered and, in the same order, each one's committed page or the fault
   * that escaped its transaction. The engine classifies every fault.
   */
  encodeLoadBatch(items: string[], answers: LoadItemAnswer[]): string;
  /** Executes or replays one validated Load page item in the callback's transaction. */
  processLoad(
    config: string,
    owner: string,
    item: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  /** Settles a business change made outside a handler: the same `{changes, memberships}` a handler answers with. */
  settleExternal(
    config: string,
    settlement: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  /** Negotiates and opens the socket's `Subscriptions`; answers `{handle, actions}` JSON. */
  negotiateLive(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  pullLive(
    config: string,
    owner: string,
    cursors: string,
    models: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  /** Applies one `LiveEvent` JSON to the session and answers its `LiveAction[]` JSON. */
  liveEvent(handle: number, event: string): string;
  /** Forgets the session; idempotent. */
  liveClose(handle: number): void;
};
/** One channel's progress in a page: after `from`, up to `to`, of a channel at `head`. */
export type CursorRange = { from: number; to: number; head: number };
/** What the executor reports to the Rust `Subscriptions` controller. */
export type LiveEvent =
  | { type: "committed"; scope: string }
  | { type: "pulled"; page: string }
  | { type: "closed" };
/** What the controller asks the executor to do, in order. */
export type LiveAction =
  | { type: "listen"; scope: string }
  | { type: "send"; frame: string }
  | {
      type: "pull";
      /** The cursor to pull after, per channel: one pull covers them all. */
      cursors: Record<string, number>;
      /** The read contracts the session declared: model name to version. */
      models: Record<string, number>;
    };
export interface Persistence {
  call(request: Record<string, any>): Promise<unknown>;
}
export interface Database<T> {
  /**
   * Must provide serializable isolation (PostgreSQL SERIALIZABLE: one
   * snapshot for the whole transaction, and a serialization failure instead
   * of any non-serial outcome), roll back rejected callbacks and retry
   * serialization failures by running the whole body again.
   */
  transaction: <R>(body: (tx: T) => Promise<R>) => Promise<R>;
  persistence: (tx: T) => Persistence;
}
export type Authenticate = (
  request: IncomingMessage,
) => Promise<string | null | undefined> | string | null | undefined;
/**
 * An admission refusal: the HTTP status and JSON body the listener answers,
 * marked `axton-admission: refused` so a client stops its connection instead
 * of retrying. `status` is 400..599.
 */
export type AdmissionRefusal = { status: number; body: JsonValue };
/**
 * Decides, before any route runs, whether a client may use the listener: its
 * request (the headers the client was configured with included) and the user
 * id `authenticate` resolved, or `null` without one. Answer `null` or
 * `undefined` to admit, or the refusal to answer instead.
 */
export type Admit = (
  request: IncomingMessage,
  userId: string | null,
) =>
  | Promise<AdmissionRefusal | null | undefined>
  | AdmissionRefusal
  | null
  | undefined;
/** Development only: the bearer token is used verbatim as the user id. Never use in production. */
export function devAuth(): Authenticate {
  return (request) => {
    const header = request.headers.authorization;
    if (typeof header !== "string" || !header.startsWith("Bearer "))
      return null;
    const id = header.slice("Bearer ".length).trim();
    return id === "" ? null : id;
  };
}
/**
 * A failure reported by the native engine. `code` is the stable machine name
 * transports and applications should branch on; `message` is for people and
 * may be reworded; `details` carries the fields a code promises (only
 * `mutation_version_unsupported` has any: `ordinal`, `name`, `version`).
 */
export class EngineError extends Error {
  readonly code: string;
  readonly details: Record<string, unknown> | undefined;
  constructor(
    code: string,
    message: string,
    details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = "EngineError";
    this.code = code;
    this.details = details;
  }
}
/** The native addon carries the engine error as JSON in the error message. */
function engineError(error: unknown): unknown {
  if (error instanceof EngineError) return error;
  const text =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "";
  if (!text.startsWith("{")) return error;
  try {
    const parsed = JSON.parse(text);
    if (
      parsed &&
      typeof parsed === "object" &&
      typeof parsed.code === "string" &&
      typeof parsed.message === "string"
    ) {
      const details =
        parsed.details && typeof parsed.details === "object"
          ? (parsed.details as Record<string, unknown>)
          : undefined;
      return new EngineError(parsed.code, parsed.message, details);
    }
  } catch {
    // Not an engine error; leave it as received.
  }
  return error;
}
/** Wrap every native function so its failures surface as `EngineError`. */
function typedNative(native: Native): Native {
  type Async =
    | "processPush"
    | "processAction"
    | "processFetch"
    | "processPull"
    | "processLoad"
    | "settleExternal"
    | "negotiateLive"
    | "pullLive";
  type Sync =
    | "validateConfig"
    | "validateLoadBatch"
    | "encodeLoadBatch"
    | "liveEvent"
    | "liveClose";
  const wrap =
    <K extends Async>(key: K) =>
    (...args: Parameters<Native[K]>): ReturnType<Native[K]> =>
      (native[key] as (...a: Parameters<Native[K]>) => ReturnType<Native[K]>)(
        ...args,
      ).catch((error: unknown) => {
        throw engineError(error);
      }) as ReturnType<Native[K]>;
  const wrapSync =
    <K extends Sync>(key: K) =>
    (...args: Parameters<Native[K]>): ReturnType<Native[K]> => {
      try {
        return (
          native[key] as (...a: Parameters<Native[K]>) => ReturnType<Native[K]>
        )(...args);
      } catch (error) {
        throw engineError(error);
      }
    };
  return {
    validateConfig: wrapSync("validateConfig"),
    processPush: wrap("processPush"),
    processAction: wrap("processAction"),
    processFetch: wrap("processFetch"),
    processPull: wrap("processPull"),
    validateLoadBatch: wrapSync("validateLoadBatch"),
    encodeLoadBatch: wrapSync("encodeLoadBatch"),
    processLoad: wrap("processLoad"),
    settleExternal: wrap("settleExternal"),
    negotiateLive: wrap("negotiateLive"),
    pullLive: wrap("pullLive"),
    liveEvent: wrapSync("liveEvent"),
    liveClose: wrapSync("liveClose"),
  };
}
/**
 * Engine codes with a client-visible HTTP status. Every other failure is a
 * server-side defect: reported to `onError` and answered `500 {code: "server"}`.
 */
const HTTP_STATUS_BY_CODE: Readonly<Record<string, number>> = {
  "request.invalid": 400,
  "client.owner_mismatch": 403,
  gap: 409,
  overlap: 409,
  mutation_version_unsupported: 409,
  model_version_unsupported: 409,
};
export class MutationRejected extends Error {
  readonly code: string;
  constructor(code: string) {
    if (!/^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$/.test(code))
      throw new Error("rejection code must be a stable machine code");
    super(code);
    this.code = code;
  }
}
/** The Mutation and Query spelling of the same business rejection contract. */
export { MutationRejected as CallRejected };
/**
 * What `backend.transaction` hands its body: the application transaction and
 * the same declaration handles a Mutation receives. `touch` declares a record
 * the body changed; `channel(name)` adds or removes Channel members. The
 * engine settles them after the body returns, inside the same transaction.
 * A generated backend narrows both to its schema's Models.
 */
export interface TransactionCall<Tx> {
  tx: Tx;
  channel(name: string): RuntimeChannel;
  touch: RuntimeTouch;
}
/** A legacy slot handler's call: its decoded input and the same declaration handles. */
export interface HandlerCall<Tx, Input> {
  input: Input;
  tx: Tx;
  userId: string;
  channel(name: string): RuntimeChannel;
  touch: RuntimeTouch;
}
/** Loads name no channel: the same identity, version and stamp describe the same content on every delivery path. */
export interface LoaderCall<Tx, Identity> {
  ids: readonly Identity[];
  tx: Tx;
  userId: string;
}
export type Handler<Tx, Input = any> = (
  call: HandlerCall<Tx, Input>,
) => Promise<void>;
export type Loader<Tx, Identity = any, Row = object> = (
  call: LoaderCall<Tx, Identity>,
) => Promise<readonly (Row | null)[]>;
/** Every retained version of one mutation, or a bare function as shorthand for a v1-only contract. */
export type HandlerRegistration<Tx> =
  Handler<Tx> | { [version: `v${number}`]: Handler<Tx> };
/**
 * Trusted framework context of a Mutation: it may change business state,
 * declare records it changed beyond its inputs (`touch`) and add or remove
 * Channel members (`channel(name)`). The handles close when the handler
 * settles.
 */
export interface MutationContext<Tx> {
  tx: Tx;
  userId: string;
  callId: string;
  channel(name: string): RuntimeChannel;
  touch: RuntimeTouch;
}
/**
 * Trusted framework context of a Query. It carries no `channel` or `touch`:
 * a Query reads without business side effects. `tx` is still the
 * application's own transaction; the framework cannot inspect arbitrary SQL,
 * so honoring the read-only contract is the handler's responsibility.
 */
export interface QueryContext<Tx> {
  tx: Tx;
  userId: string;
  callId: string;
}
/**
 * Trusted framework context of one Load page. Like a Query it carries no
 * `channel` or `touch`: a Load reads without business side effects, and the
 * framework cannot inspect arbitrary SQL on `tx`. `callId` is the page's
 * durable call ID and `loadId` its job.
 */
export interface LoadContext<Tx> {
  tx: Tx;
  userId: string;
  callId: string;
  loadId: string;
}
/**
 * One page of a Load: `continuation` is `null` on the first page and the
 * previous `next` wrapper afterwards. Answers the declared identity lists and
 * `next` (`null` completes the traversal). `next.state` must be portable
 * JSON: no undefined, BigInt, non-finite or unsafe-integer numbers, cycles
 * or class instances, at most 64 levels deep and 64 KiB encoded.
 */
export type LoadHandler<Tx, Args = any, Data = any> = (call: {
  ctx: LoadContext<Tx>;
  args: Args;
  continuation: LoadNext;
}) => Promise<{ data: Data; next: LoadNext }>;
export type MutationHandler<Tx, Args = any, Outputs = any> = (call: {
  ctx: MutationContext<Tx>;
  args: Args;
}) => Promise<Outputs | void>;
export type QueryHandler<Tx, Args = any, Outputs = any> = (call: {
  ctx: QueryContext<Tx>;
  args: Args;
}) => Promise<Outputs | void>;
/** Every retained version of one operation of this kind, or a bare function for a v1-only contract. */
export type MutationHandlerRegistration<Tx> =
  MutationHandler<Tx> | { [version: `v${number}`]: MutationHandler<Tx> };
export type QueryHandlerRegistration<Tx> =
  QueryHandler<Tx> | { [version: `v${number}`]: QueryHandler<Tx> };
export type LoadHandlerRegistration<Tx> =
  LoadHandler<Tx> | { [version: `v${number}`]: LoadHandler<Tx> };
/** Every retained version of one model's read contract, or a bare function as shorthand for a v1-only model. */
export type LoaderRegistration<Tx> =
  Loader<Tx> | { [version: `v${number}`]: Loader<Tx> };
/**
 * One registration holds every retained version under the mutation or model
 * name; a bare function is shorthand for a v1-only contract and never stands
 * for the latest version. Refused at startup, naming the key and version.
 */
function versioned<F>(
  kind: "handler" | "loader" | "mutation" | "query" | "load",
  name: string,
  key: string,
  versions: readonly number[],
  registration: unknown,
): Map<number, F> {
  const label = kind.charAt(0).toUpperCase() + kind.slice(1);
  const list = versions.map((version) => `v${version}`).join(", ");
  const table = new Map<number, F>();
  if (typeof registration === "function") {
    if (versions.length !== 1 || versions[0] !== 1)
      throw new Error(
        `${label} ${key} must register ${list} of ${name}; a function registers v1 only`,
      );
    table.set(1, registration as F);
    return table;
  }
  if (registration === null || typeof registration !== "object")
    throw new Error(`Missing ${kind} ${key} for ${name} ${list}`);
  for (const version of versions) {
    const found = (registration as Record<string, unknown>)[`v${version}`];
    if (found === undefined)
      throw new Error(
        `Missing ${kind} ${key}.v${version} for ${name} v${version}`,
      );
    if (typeof found !== "function")
      throw new Error(
        `${label} ${key}.v${version} for ${name} v${version} must be a function`,
      );
    table.set(version, found as F);
  }
  for (const found of Object.keys(registration))
    if (!/^v[1-9][0-9]*$/.test(found) || !table.has(Number(found.slice(1))))
      throw new Error(
        `Unknown ${kind} ${key}.${found} for ${name}: retained ${kind === "mutation" || kind === "query" || kind === "load" ? `${kind} ` : ""}versions are ${list}`,
      );
  return table;
}
/** Decode an operation value from its wire form into the API view (a DateTime becomes a Date). */
function decodeActionValue(type: any, value: unknown): unknown {
  if (value == null) return value;
  if (type?.kind === "list")
    return (value as unknown[]).map((item) =>
      decodeActionValue(type.element, item),
    );
  if (type?.name === "dateTime") return new Date(value as string);
  return value;
}
function decodeActionRecord(
  value: unknown,
  model: { fields?: { name: string; type: unknown }[] },
): unknown {
  if (value == null) return value;
  const record = value as Record<string, unknown>;
  for (const field of model.fields ?? [])
    if (Object.hasOwn(record, field.name))
      record[field.name] = decodeActionValue(field.type, record[field.name]);
  return record;
}
export interface BackendOptions<T> {
  config: object;
  database: Database<T>;
  authenticate: Authenticate;
  /**
   * Admission for every `listen` route and the live upgrade, after
   * `authenticate` and before anything else: a refusal is answered with its
   * own status and body instead of `401` or the route. Throwing, or answering
   * something that is not a refusal, is a server error.
   */
  admit?: Admit | undefined;
  /** Legacy slot mutations (`mutation Name { slots }`), by lower-camel name. */
  handlers?: Record<string, HandlerRegistration<T>> | undefined;
  /** Every retained Mutation version, by lower-camel name. */
  mutations?: Record<string, MutationHandlerRegistration<T>> | undefined;
  /** Every retained Query version, by lower-camel name. */
  queries?: Record<string, QueryHandlerRegistration<T>> | undefined;
  /** Every retained Load version, by lower-camel name. */
  loads?: Record<string, LoadHandlerRegistration<T>> | undefined;
  /**
   * Every retained version of each Model's read contract, by lower-camel
   * name. A Model left out (or `undefined`) is device-only: never published,
   * and no retained Mutation, Query or Load may name it on the wire.
   */
  loaders: Record<string, LoaderRegistration<T> | undefined>;
  loaderHooks?: Record<
    string,
    { prepareForViewer(call: LoaderCall<T, any>): Promise<void> }
  >;
  translateRejection?: (error: unknown) => string | null | undefined;
  native?: Native;
  /**
   * Called for every non-business error the host catches: a thrown handler
   * or loader error (answered as `handler.failed`/`loader.failed`, visible
   * to the client only as that mutation's rejection code), and every
   * server-side failure clients see only as `{ code: "server" }` -
   * authenticate throws, persistence faults, loader defects, live drain
   * failures. A handler or loader failure is reported from inside the
   * application transaction, before it commits: when that transaction is
   * then retried after a serialization failure, the same failure can be
   * reported again, once per attempt.
   */
  onError?: (error: unknown) => void;
}
/** JSON cannot represent nonfinite values or undefined array items. Never turn either into null. */
function callbackJson(value: unknown): string {
  return JSON.stringify(value, (_key, item) => {
    if (typeof item === "bigint") {
      const number = Number(item);
      if (!Number.isSafeInteger(number))
        throw new Error("bigint outside safe integer range");
      return number;
    }
    if (typeof item === "number" && !Number.isFinite(item))
      throw new Error("nonfinite callback value");
    if (item === undefined) throw new Error("undefined callback value");
    return item;
  });
}
/** Portable continuation bounds; the engine rechecks them independently. */
const LOAD_STATE_BYTES = 64 * 1024;
const LOAD_STATE_DEPTH = 64;
/**
 * Why a Load handler's `next` is not `null` or `{state}` with portable JSON
 * state, or `undefined` when it is. Checked before `callbackJson`, which
 * would silently turn a safe BigInt into a number or call `toJSON`. A value
 * that throws while it is inspected (a getter, a Proxy trap) is not portable.
 */
/**
 * A lone UTF-16 surrogate: `JSON.stringify` escapes it, but it is not Unicode
 * text, so Rust refuses the answer. Well-formed pairs match nothing here.
 */
const LONE_SURROGATE = /\p{Surrogate}/u;
function continuationProblem(next: unknown): string | undefined {
  try {
    return inspectContinuation(next);
  } catch (error) {
    return `next throws when read: ${error instanceof Error ? error.message : String(error)}`;
  }
}
function inspectContinuation(next: unknown): string | undefined {
  if (next === null) return undefined;
  if (
    typeof next !== "object" ||
    Array.isArray(next) ||
    Object.getPrototypeOf(next) !== Object.prototype ||
    Reflect.ownKeys(next).length !== 1 ||
    !Object.hasOwn(next, "state")
  )
    return "next must be null or exactly {state}";
  const open = new Set<object>();
  const visit = (value: unknown, depth: number): string | undefined => {
    switch (typeof value) {
      case "string":
        return LONE_SURROGATE.test(value)
          ? "a string with a lone UTF-16 surrogate"
          : undefined;
      case "boolean":
        return undefined;
      case "number":
        if (!Number.isFinite(value)) return "a non-finite number";
        if (Number.isInteger(value) && !Number.isSafeInteger(value))
          return "an integer outside the safe range";
        return undefined;
      case "object":
        break;
      default:
        return `a ${typeof value} value`;
    }
    if (value === null) return undefined;
    if (depth + 1 > LOAD_STATE_DEPTH)
      return `nesting deeper than ${LOAD_STATE_DEPTH}`;
    if (open.has(value)) return "a cycle";
    const prototype = Object.getPrototypeOf(value);
    if (Array.isArray(value)) {
      if (prototype !== Array.prototype) return "a class instance";
    } else if (prototype !== Object.prototype && prototype !== null)
      return "a class instance";
    if (Object.getOwnPropertySymbols(value).length) return "a symbol key";
    if (typeof (value as { toJSON?: unknown }).toJSON === "function")
      return "custom JSON serialization";
    open.add(value);
    try {
      if (Array.isArray(value)) {
        for (let index = 0; index < value.length; index++) {
          if (!Object.hasOwn(value, index)) return "an array hole";
          const problem = visit(value[index], depth + 1);
          if (problem) return problem;
        }
      } else
        for (const key of Object.keys(value)) {
          if (LONE_SURROGATE.test(key))
            return "a key with a lone UTF-16 surrogate";
          const problem = visit(
            (value as Record<string, unknown>)[key],
            depth + 1,
          );
          if (problem) return problem;
        }
    } finally {
      open.delete(value);
    }
    return undefined;
  };
  const state = (next as { state: unknown }).state;
  const problem = visit(state, 0);
  if (problem) return `state holds ${problem}`;
  if (Buffer.byteLength(JSON.stringify(state), "utf8") > LOAD_STATE_BYTES)
    return `state exceeds ${LOAD_STATE_BYTES} bytes`;
  return undefined;
}
/** At most this many Load items of one request hold a database transaction at once. */
const LOAD_ITEM_TRANSACTIONS = 4;
class WakeHub {
  private listeners = new Map<string, Set<() => void>>();
  subscribe(scope: string, wake: () => void): () => void {
    const listeners = this.listeners.get(scope) ?? new Set();
    listeners.add(wake);
    this.listeners.set(scope, listeners);
    return () => {
      listeners.delete(wake);
      if (!listeners.size) this.listeners.delete(scope);
    };
  }
  notify(scopes: Iterable<string>): void {
    for (const scope of new Set(scopes))
      for (const wake of [...(this.listeners.get(scope) ?? [])])
        queueMicrotask(wake);
  }
  clear(): void {
    this.listeners.clear();
  }
}
class Session {
  failed: unknown;
  closed = false;
  pending = new Set<Promise<unknown>>();
  touched = new Set<string>();
  savepoints = new Map<number, Set<string>>();
  track<R>(body: () => Promise<R>): Promise<R> {
    if (this.closed)
      return Promise.reject(new Error("transaction session closed"));
    const result = Promise.resolve()
      .then(body)
      .catch((error) => {
        this.failed ??= error;
        throw error;
      });
    this.pending.add(result);
    void result.then(
      () => this.pending.delete(result),
      () => this.pending.delete(result),
    );
    return result;
  }
  savepoint(ordinal: number): void {
    this.savepoints.set(ordinal, new Set(this.touched));
  }
  rollback(ordinal: number): void {
    this.touched = new Set(this.savepoints.get(ordinal) ?? []);
  }
  release(ordinal: number): void {
    this.savepoints.delete(ordinal);
  }
  async assertCommittable(): Promise<void> {
    const unawaited = this.pending.size > 0;
    while (this.pending.size) await Promise.allSettled([...this.pending]);
    if (this.failed !== undefined) throw this.failed;
    if (unawaited) throw new Error("unawaited transaction operations");
    if (this.closed) throw new Error("transaction session closed");
  }
}
/** The retained backend business kind of an operation; omitted is `mutation`. */
type CallKind = "mutation" | "query";
/** Where each operation kind registers its handlers. */
const REGISTRATION_GROUP = {
  mutation: "mutations",
  query: "queries",
  load: "loads",
} as const;
type MutationSlot = {
  name: string;
  operation: string;
  cardinality: string;
  model: string;
};
type MutationDescriptor = {
  name: string;
  version: number;
  slots?: MutationSlot[];
};
/**
 * `External` is what `backend.transaction` hands its body; a generated
 * backend passes its own `TransactionCall`, typed by its schema's Models.
 */
export function createBackend<T, External extends object = TransactionCall<T>>(
  options: BackendOptions<T>,
) {
  const native = typedNative(
    options.native ??
      (require("../../bindings/node/axton-node.node") as Native),
  );
  // Nothing is dropped silently: without a handler, failures go to the console.
  const onError: (error: unknown) => void =
    options.onError ?? ((error) => console.error(error));
  const descriptor = options.config as {
    schema?: {
      enums?: { name: string; values?: string[] }[];
      models?: {
        name: string;
        version?: number;
        identity?: string[];
        fields?: { name: string; type: unknown }[];
      }[];
      actions?: {
        name: string;
        version: number;
        kind?: CallKind;
        inputs?: {
          kind: string;
          name: string;
          model?: string;
          cardinality?: string;
          list?: boolean;
          type?: unknown;
        }[];
        outputs?: { source: unknown }[];
        input?: {
          models?: {
            name: string;
            fields?: { name: string; type: unknown }[];
          }[];
        };
      }[];
      loads?: {
        name: string;
        version: number;
        inputs?: {
          kind: string;
          name: string;
          list?: boolean;
          type?: unknown;
        }[];
      }[];
    };
    mutations?: MutationDescriptor[];
    models?: {
      name: string;
      version: number;
      fields?: { name: string; type: unknown }[];
    }[];
  };
  const retained = new Map<string, number[]>();
  for (const m of descriptor.mutations ?? [])
    retained.set(
      m.name,
      [...(retained.get(m.name) ?? []), m.version].sort((a, b) => a - b),
    );
  const schemaModels = descriptor.schema?.models ?? [];
  const modelNames = schemaModels.map((model) => model.name);
  // A Model whose Loader is omitted (undefined) is device-only (#187): the
  // engine is told only the registered Models, refuses at `validateConfig` any
  // retained descriptor that would put another on the wire, and never
  // publishes one. A key naming no Model is a typo, never an omission.
  for (const key of Object.keys(options.loaders))
    if (!modelNames.some((name) => lowerFirst(name) === key))
      throw new Error(`Unknown loader ${key}: no Model ${key}`);
  const loadedModels = modelNames.filter(
    (name) => options.loaders[lowerFirst(name)] !== undefined,
  );
  const config = JSON.stringify({
    ...options.config,
    loaders: loadedModels,
  });
  native.validateConfig(config);
  // Refuses Models whose accessors collide or take a Channel's add/remove,
  // and declarations naming a device-only Model.
  const createEffects = effectsFor(
    schemaModels,
    descriptor.schema?.enums,
    new Set(loadedModels),
  );
  // Every retained model read contract; a config without `models` retains each
  // model at the schema's own version, as the engine does.
  const retainedModels = new Map<string, number[]>();
  for (const m of descriptor.models?.length
    ? descriptor.models
    : schemaModels.map((model) => ({
        name: model.name,
        version: model.version ?? 1,
      })))
    retainedModels.set(
      m.name,
      [...(retainedModels.get(m.name) ?? []), m.version].sort((a, b) => a - b),
    );
  const loaderTable = new Map<string, Loader<T>>();
  for (const name of loadedModels) {
    const key = lowerFirst(name);
    const table = versioned<Loader<T>>(
      "loader",
      name,
      key,
      retainedModels.get(name) ?? [],
      options.loaders[key],
    );
    for (const [version, loader] of table)
      loaderTable.set(`${name}:${version}`, loader);
  }
  const registered = new Map<string, Map<number, Handler<T>>>();
  for (const [name, versions] of retained) {
    const key = lowerFirst(name);
    registered.set(
      name,
      versioned<Handler<T>>(
        "handler",
        name,
        key,
        versions,
        options.handlers?.[key],
      ),
    );
  }
  const operations = descriptor.schema?.actions ?? [];
  const loadDescriptors = descriptor.schema?.loads ?? [];
  /** Every retained operation version with its kind; Loads share the operation namespace. */
  const retainedOperations = [
    ...operations.map((action) => ({
      name: action.name,
      version: action.version,
      kind: (action.kind ?? "mutation") as CallKind | "load",
    })),
    ...loadDescriptors.map((load) => ({
      name: load.name,
      version: load.version,
      kind: "load" as const,
    })),
  ];
  const retainedVersions = (key: string) =>
    retainedOperations.filter(
      (operation) => lowerFirst(operation.name) === key,
    );
  /** Each retained version of an operation key with its kind, e.g. "Find v1 (mutation), v2 (query)". */
  const retainedKinds = (key: string): string | undefined => {
    const versions = retainedVersions(key);
    if (!versions.length) return undefined;
    return `${versions[0]!.name} ${versions
      .map((operation) => [operation.version, operation.kind] as const)
      .sort(([a], [b]) => a - b)
      .map(([version, kind]) => `v${version} (${kind})`)
      .join(", ")}`;
  };
  /** The registration groups the retained kinds of `key` belong in, except `except`. */
  const groupsFor = (key: string, except?: string): string =>
    [
      ...new Set(
        retainedVersions(key).map(
          (operation) => REGISTRATION_GROUP[operation.kind],
        ),
      ),
    ]
      .filter((group) => group !== except)
      .sort()
      .join(" or ");
  for (const key of Object.keys(options.handlers ?? {}))
    if (![...retained.keys()].some((name) => lowerFirst(name) === key)) {
      const kinds = retainedKinds(key);
      throw new Error(
        kinds
          ? `Handler ${key} names ${kinds}; register each version under ${retainedVersions(key).some((operation) => operation.kind === "load") ? "mutations, queries or loads" : "mutations or queries"} by its kind`
          : `Unknown handler ${key}: no retained mutation ${key}`,
      );
    }
  const handlerTable = new Map<
    string,
    { handler: Handler<T>; slots: MutationSlot[] }
  >();
  for (const m of descriptor.mutations ?? [])
    handlerTable.set(`${m.name}:${m.version}`, {
      handler: registered.get(m.name)!.get(m.version)!,
      slots: m.slots ?? [],
    });
  // Registration follows each retained version's own kind: one name may
  // retain a Mutation version and a Query version, each in its own map.
  const actionHandlers = new Map<
    string,
    MutationHandler<T> | QueryHandler<T>
  >();
  for (const kind of ["mutation", "query"] as const) {
    const map = kind === "mutation" ? options.mutations : options.queries;
    const versions = new Map<string, number[]>();
    for (const action of operations)
      if ((action.kind ?? "mutation") === kind)
        versions.set(
          action.name,
          [...(versions.get(action.name) ?? []), action.version].sort(
            (a, b) => a - b,
          ),
        );
    for (const [name, list] of versions) {
      const table = versioned<MutationHandler<T> | QueryHandler<T>>(
        kind,
        name,
        lowerFirst(name),
        list,
        map?.[lowerFirst(name)],
      );
      for (const [version, handler] of table)
        actionHandlers.set(`${name}:${version}`, handler);
    }
    const group = REGISTRATION_GROUP[kind];
    for (const key of Object.keys(map ?? {}))
      if (![...versions.keys()].some((name) => lowerFirst(name) === key)) {
        const kinds = retainedKinds(key);
        throw new Error(
          kinds
            ? `${group}.${key}: ${kinds} retains no ${kind} version; register it under ${groupsFor(key, group)}`
            : `Unknown ${kind} ${key}: no retained ${kind} ${key}`,
        );
      }
  }
  // Every retained Load version registers under `loads`, like an operation
  // under its kind's map.
  const loadHandlers = new Map<string, LoadHandler<T>>();
  const loadVersions = new Map<string, number[]>();
  for (const load of loadDescriptors)
    loadVersions.set(
      load.name,
      [...(loadVersions.get(load.name) ?? []), load.version].sort(
        (a, b) => a - b,
      ),
    );
  for (const [name, list] of loadVersions)
    for (const [version, handler] of versioned<LoadHandler<T>>(
      "load",
      name,
      lowerFirst(name),
      list,
      options.loads?.[lowerFirst(name)],
    ))
      loadHandlers.set(`${name}:${version}`, handler);
  for (const key of Object.keys(options.loads ?? {}))
    if (![...loadVersions.keys()].some((name) => lowerFirst(name) === key)) {
      const kinds = retainedKinds(key);
      throw new Error(
        kinds
          ? `loads.${key}: ${kinds} retains no load version; register it under ${groupsFor(key, "loads")}`
          : `Unknown load ${key}: no retained load ${key}`,
      );
    }
  const loadTable = new Map(
    loadDescriptors.map((load) => [`${load.name}:${load.version}`, load]),
  );
  const actionTable = new Map(
    (descriptor.schema?.actions ?? []).map((action) => [
      `${action.name}:${action.version}`,
      action,
    ]),
  );
  const sessions = new Map<T, Session>();
  const wakes = new WakeHub();
  /**
   * Rejection versus failure: a business error a handler or loader raises on
   * purpose (`MutationRejected`, or one `translateRejection` recognizes)
   * rejects only that mutation with its stable code. Any other thrown error
   * is a defect: reported to `onError` and answered as a failure, which also
   * rejects only that mutation (`handler.failed` or `loader.failed`), but
   * carries the thrown message as data instead of a machine code. Only a
   * persistence fault - outside these try blocks - still aborts the whole
   * delivery.
   */
  const refusal = (
    error: unknown,
  ): { rejection: string } | { error: string } => {
    const code =
      error instanceof MutationRejected
        ? error.code
        : options.translateRejection?.(error);
    if (code != null) return { rejection: new MutationRejected(code).code };
    onError(error);
    return { error: error instanceof Error ? error.message : String(error) };
  };
  const host = (
    tx: T,
    session: Session,
  ): ((request: string) => Promise<string>) => {
    const storage = options.database.persistence(tx);
    return (raw) =>
      session.track(async () => {
        const req = JSON.parse(raw) as HostRequest;
        let result: unknown;
        // `savepoint`, `rollback` and `release` are answered by the persistence
        // and also bookkept here, so each one does both.
        if (req.op === "savepoint") session.savepoint(req.ordinal);
        if (req.op === "rollback") session.rollback(req.ordinal);
        if (req.op === "release") session.release(req.ordinal);
        if (req.op === "handle") {
          const entry = handlerTable.get(`${req.name}:${req.version}`);
          if (!entry)
            throw new Error(`Missing handler ${req.name} v${req.version}`);
          const shape = (slot: MutationSlot, raw: any) => {
            if (raw === null || raw === undefined) return null;
            if (slot.operation === "create")
              return { ...raw.identity, ...raw.data };
            if (slot.operation === "update")
              return { identity: raw.identity, patch: raw.patch };
            return { identity: raw.identity };
          };
          const input: Record<string, unknown> = {};
          for (const slot of entry.slots) {
            const raw = req.arguments[slot.name] as any;
            input[slot.name] =
              slot.cardinality === "list"
                ? (raw as any[]).map((item) => shape(slot, item))
                : shape(slot, raw);
          }
          // The engine derives the records the operations target and adds
          // them to the change set itself; `changes` carries only the
          // handler's own `touch` declarations.
          const effects = createEffects();
          try {
            await entry.handler({
              input,
              tx,
              userId: req.owner,
              channel: effects.channel,
              touch: effects.touch,
            });
            result = effects.settlement();
          } catch (error) {
            if (isRetryableTransactionError(error)) throw error;
            result = refusal(error);
          } finally {
            effects.close();
          }
        } else if (req.op === "handleAction") {
          const action = actionTable.get(`${req.name}:${req.version}`);
          const handler = actionHandlers.get(`${req.name}:${req.version}`);
          if (!action || !handler)
            throw new Error(`Missing handler ${req.name} v${req.version}`);
          const args = { ...req.arguments };
          for (const input of action.inputs ?? []) {
            if (input.kind === "value") {
              const type = input.list
                ? { kind: "list", element: input.type }
                : input.type;
              args[input.name] = decodeActionValue(type, args[input.name]);
              continue;
            }
            if (!input.model) continue;
            const model =
              action.input?.models?.find(
                (candidate: any) => candidate.name === input.model,
              ) ??
              schemaModels.find((candidate) => candidate.name === input.model);
            if (!model) throw new Error(`Missing Action model ${input.model}`);
            // The engine infers each operand as an input target; the handler
            // only sees the decoded record.
            const shape = (value: unknown): unknown =>
              value === null || value === undefined
                ? null
                : decodeActionRecord(value, model);
            const value = args[input.name];
            args[input.name] =
              input.cardinality === "list"
                ? (value as unknown[]).map(shape)
                : shape(value);
          }
          // A Query context has no declaration handles at runtime either:
          // its settlement never carries changes or memberships.
          const query = (action.kind ?? "mutation") === "query";
          const effects = query ? undefined : createEffects();
          try {
            const outputs = await handler({
              ctx: effects
                ? {
                    tx,
                    userId: req.owner,
                    callId: req.callId,
                    channel: effects.channel,
                    touch: effects.touch,
                  }
                : { tx, userId: req.owner, callId: req.callId },
              args,
            } as Parameters<MutationHandler<T>>[0]);
            result = {
              outputs: outputs === undefined ? {} : outputs,
              ...(effects
                ? effects.settlement()
                : { changes: [], memberships: [] }),
            };
          } catch (error) {
            if (isRetryableTransactionError(error)) throw error;
            result = refusal(error);
          } finally {
            effects?.close();
          }
        } else if (req.op === "handleLoad") {
          const load = loadTable.get(`${req.name}:${req.version}`);
          const handler = loadHandlers.get(`${req.name}:${req.version}`);
          if (!load || !handler)
            throw new Error(`Missing Load handler ${req.name} v${req.version}`);
          const args = { ...req.arguments };
          for (const input of load.inputs ?? [])
            if (input.kind === "value")
              args[input.name] = decodeActionValue(
                input.list ? { kind: "list", element: input.type } : input.type,
                args[input.name],
              );
          const label = `${req.name} v${req.version}`;
          const invalid = (problem: string) => {
            const error = new Error(
              `invalid Load handler answer for ${label}: ${problem}`,
            );
            onError(error);
            return callbackJson({ error: error.message });
          };
          // The handler's answer is judged inside its error boundary, like
          // the call itself: reading it can throw (a getter, a Proxy), and
          // whatever it answered is this page's saved outcome, never a host
          // fault. A continuation that is not portable JSON is refused
          // before `callbackJson` could coerce it; any other unencodable
          // answer is a failure. A Load context has no declaration handles:
          // its answer carries identities and a continuation, never changes
          // or memberships.
          try {
            const page: unknown = await handler({
              ctx: {
                tx,
                userId: req.owner,
                callId: req.callId,
                loadId: req.loadId,
              },
              args,
              continuation: req.continuation,
            });
            if (
              page === null ||
              typeof page !== "object" ||
              Array.isArray(page)
            )
              return invalid("expected {data, next}");
            const { data, next } = page as { data: unknown; next: unknown };
            const problem = continuationProblem(next);
            if (problem !== undefined) {
              onError(
                new Error(`invalid Load continuation for ${label}: ${problem}`),
              );
              return callbackJson({ rejection: "load.invalid_continuation" });
            }
            let answer: string;
            try {
              answer = callbackJson({ data, next });
            } catch (error) {
              return invalid(
                error instanceof Error ? error.message : String(error),
              );
            }
            return answer;
          } catch (error) {
            if (isRetryableTransactionError(error)) throw error;
            return callbackJson(refusal(error));
          }
        } else if (req.op === "load") {
          // Dispatch is by model name and contract version; a version that
          // was not registered is a defect, never another version's loader.
          const loader = loaderTable.get(`${req.model}:${req.version}`);
          if (!loader)
            throw new Error(`Missing loader ${req.model} v${req.version}`);
          const loaderModel =
            (descriptor.models ?? []).find(
              (model: any) =>
                model.name === req.model && model.version === req.version,
            ) ?? schemaModels.find((model) => model.name === req.model);
          const call = {
            ids: (req.identities as any[]).map((identity) =>
              loaderModel
                ? decodeActionRecord(identity, loaderModel)
                : identity,
            ),
            tx,
            userId: req.owner,
          };
          // A read refusal (`MutationRejected` or a translated error) is
          // answered as data: the engine records it as the mutation's
          // rejection in a push and as that record's `error` change in a
          // pull. Any other thrown error is also answered as data - a
          // failure - which becomes `loader.failed` for that one mutation or
          // record.
          let refused: { rejection: string } | { error: string } | undefined;
          let rows: unknown;
          try {
            await options.loaderHooks?.[
              lowerFirst(req.model)
            ]?.prepareForViewer(call);
            rows = await loader(call);
          } catch (error) {
            if (isRetryableTransactionError(error)) throw error;
            refused = refusal(error);
          }
          if (refused) return callbackJson(refused);
          // An answer JSON cannot carry faithfully is a failed read, never a
          // null: the engine retries the records one by one, so only the
          // record whose row is broken fails.
          let reason: string | undefined;
          let answer = "";
          if (!Array.isArray(rows)) reason = "a non-array result";
          else if (rows.some((value) => value === undefined))
            reason = "an undefined entry";
          else
            try {
              answer = callbackJson(rows);
            } catch (error) {
              reason = error instanceof Error ? error.message : String(error);
            }
          if (reason === undefined) return answer;
          const invalid = new Error(
            `invalid loader answer for ${req.model} v${req.version}: ${reason}`,
          );
          onError(invalid);
          return callbackJson({ error: invalid.message });
        } else {
          // Everything the persistence owns, plus anything this build does not
          // know: an operation added to the contract without an arm here is a
          // compile error, not a silent forward.
          switch (req.op) {
            case "claim":
            case "saveReceipt":
            case "claimCall":
            case "saveCall":
            case "head":
            case "scan":
            case "savepoint":
            case "rollback":
            case "release":
            case "advanceStamp":
            case "ensureStamp":
            case "readStamps":
            case "publish":
            case "lockRecord":
            case "memberships":
            case "setMembership":
              break;
            default: {
              const unreachable: never = req;
              void unreachable;
            }
          }
          result = await storage.call(req);
          // Every publication that survives its savepoint wakes the channel's
          // subscribers after commit; `rollback` restores the set it snapshot.
          if (req.op === "publish") session.touched.add(req.channel);
        }
        return callbackJson(result);
      });
  };
  /**
   * Runs `operation` under one session bound to `tx`: every host callback is
   * tracked, and the operation completes only once none is unfinished or
   * failed. Answers its value and the Channels it published to, which the
   * caller wakes after `tx` commits. A transaction holds one session at a
   * time, so AXTON never settles into a transaction it is already serving.
   */
  const bound = async <R,>(
    tx: T,
    operation: (session: Session) => Promise<R>,
  ): Promise<{ result: R; published: string[] }> => {
    if (sessions.has(tx))
      throw new Error(
        "transaction already bound: AXTON is serving it; declare through its own handles",
      );
    const session = new Session();
    sessions.set(tx, session);
    try {
      const result = await operation(session);
      await session.assertCommittable();
      return { result, published: [...session.touched] };
    } catch (error) {
      // Preserve the original database error so the caller can retry serialization failures.
      while (session.pending.size)
        await Promise.allSettled([...session.pending]);
      throw session.failed ?? error;
    } finally {
      session.closed = true;
      sessions.delete(tx);
    }
  };
  const run = async <R,>(
    operation: (tx: T, session: Session) => Promise<R>,
  ) => {
    let committed: string[] = [];
    const result = await options.database.transaction(async (tx) => {
      const { result, published } = await bound(tx, (session) =>
        operation(tx, session),
      );
      committed = published;
      return result;
    });
    wakes.notify(committed);
    return result;
  };
  /**
   * Runs `body` with a Mutation's `channel` and `touch`, then settles what it
   * declared in `tx`: one new stamp per touched record, published at that
   * stamp to each Channel it is a member of, and each newly added member
   * published once. The handles close when the body settles, whether it
   * returns or throws.
   */
  const settle = async <R,>(
    tx: T,
    session: Session,
    body: (call: External) => R | Promise<R>,
  ): Promise<R> => {
    const effects = createEffects();
    let result: R;
    try {
      const call: TransactionCall<T> = {
        tx,
        channel: effects.channel,
        touch: effects.touch,
      };
      result = await body(call as unknown as External);
    } finally {
      effects.close();
    }
    await session.track(() =>
      native.settleExternal(
        config,
        JSON.stringify(effects.settlement()),
        host(tx, session),
      ),
    );
    return result;
  };
  /**
   * Runs `body` in one application transaction the framework opens, and
   * settles its declarations there. After the driver commits, the live
   * subscribers of every channel published to are woken; a failure rolls
   * back and wakes nobody. Answers the body's own value. Not for use inside
   * a handler, which already has a transaction.
   */
  const transaction = <R,>(body: (call: External) => Promise<R>): Promise<R> =>
    run((tx, session) => settle(tx, session, body));
  /**
   * Settles `body`'s declarations in `tx`, a transaction the application
   * opened and still owns, before this call resolves: the stamps,
   * memberships and positions are written through `tx`, so they commit or
   * roll back with it, a savepoint included. Answers the wake: call it once
   * `tx` has committed, and never after a rollback; until then no live
   * subscriber hears of the change. A thrown database error is the original
   * one, for the caller's retry loop. Refuses a transaction the framework
   * is serving (a handler's or `backend.transaction`'s): declare through
   * its own handles instead.
   */
  const publish = async (
    tx: T,
    body: (call: External) => void | Promise<void>,
  ): Promise<() => void> => {
    const { published } = await bound(tx, (session) =>
      settle(tx, session, body),
    );
    return () => wakes.notify(published);
  };
  const text = (request: Uint8Array | string) =>
    typeof request === "string"
      ? request
      : new TextDecoder("utf-8", { fatal: true }).decode(request);
  // A loader row the served contract does not accept is checked by the
  // engine, which fails only that record (`loader.invalid`). The developer
  // still hears about each one.
  const INVALID = '"loader.invalid"';
  const reportInvalidPage = (page: string): string => {
    if (!page.includes(INVALID)) return page;
    const { changes } = JSON.parse(page) as {
      changes: { model: string; identity: unknown; error?: string }[];
    };
    for (const change of changes)
      if (change.error === "loader.invalid")
        onError(
          new Error(
            `loader returned a row the served ${change.model} contract does not accept: ${JSON.stringify(change.identity)}`,
          ),
        );
    return page;
  };
  const reportInvalidReceipt = (receipt: string): string => {
    if (!receipt.includes(INVALID)) return receipt;
    const { rejections } = JSON.parse(receipt) as {
      rejections: { ordinal: number; code: string }[];
    };
    for (const rejection of rejections)
      if (rejection.code === "loader.invalid")
        onError(
          new Error(
            `loader returned a row the declared contract does not accept while reading back mutation ${rejection.ordinal}`,
          ),
        );
    return receipt;
  };
  /**
   * One Load page in its own application transaction: the engine's page JSON
   * once it committed, or what escaped the transaction boundary. The carrier
   * only reports what it observed; the engine classifies it in
   * `encodeLoadBatch` (a commit whose result is unknown, a pool or driver
   * failure and a conflict are `retryable`, so the client resends the same
   * call ID and the saved claim decides what committed; a deterministic
   * engine defect is an unsaved `failed` item, so the job stops).
   */
  const loadItem = async (
    owner: string,
    item: string,
  ): Promise<LoadItemAnswer> => {
    try {
      return {
        page: await run((tx, session) =>
          native.processLoad(config, owner, item, host(tx, session)),
        ),
      };
    } catch (error) {
      onError(error);
      return {
        fault:
          error instanceof EngineError
            ? { kind: "engine", code: error.code, message: error.message }
            : isRetryableTransactionError(error)
              ? { kind: "conflict" }
              : { kind: "unavailable" },
      };
    }
  };
  /**
   * One `POST /sync/loads` batch: validated whole by the engine, then each
   * item in its own transaction, at most `LOAD_ITEM_TRANSACTIONS` at once.
   * Transport grouping only: items share no transaction, and the engine
   * writes the one bounded response after every item has committed or
   * rolled back.
   */
  const loads = async (
    owner: string,
    request: Uint8Array | string,
  ): Promise<string> => {
    const items = native.validateLoadBatch(text(request));
    const answers: LoadItemAnswer[] = new Array(items.length);
    let next = 0;
    const worker = async () => {
      while (next < items.length) {
        const index = next++;
        answers[index] = await loadItem(owner, items[index]!);
      }
    };
    await Promise.all(
      Array.from(
        { length: Math.min(LOAD_ITEM_TRANSACTIONS, items.length) },
        worker,
      ),
    );
    return native.encodeLoadBatch(items, answers);
  };
  /** @internal Raw protocol seams used by the framework's own tests; not part of the supported surface. */
  const api = {
    push: (owner: string, request: Uint8Array | string) =>
      run((tx, session) =>
        native.processPush(config, owner, text(request), host(tx, session)),
      ).then(reportInvalidReceipt),
    action: (owner: string, request: Uint8Array | string) =>
      run((tx, session) =>
        native.processAction(config, owner, text(request), host(tx, session)),
      ),
    fetch: (owner: string, request: Uint8Array | string) =>
      run((tx, session) =>
        native.processFetch(config, owner, text(request), host(tx, session)),
      ),
    pull: (owner: string, request: Uint8Array | string) =>
      run((tx, session) =>
        native.processPull(config, owner, text(request), host(tx, session)),
      ).then(reportInvalidPage),
    loads,
    negotiateLive: (
      owner: string,
      request: Uint8Array | string,
    ): Promise<{ handle: number; actions: LiveAction[] }> =>
      run((tx, session) =>
        native.negotiateLive(config, owner, text(request), host(tx, session)),
      ).then(JSON.parse),
    pullLive: (
      owner: string,
      cursors: Record<string, number>,
      models: Record<string, number>,
    ): Promise<{ page: string; cursors: Record<string, CursorRange> }> =>
      run((tx, session) =>
        native.pullLive(
          config,
          owner,
          JSON.stringify(cursors),
          JSON.stringify(models),
          host(tx, session),
        ),
      ).then((result) => {
        const parsed = JSON.parse(result) as {
          page: string;
          cursors: Record<string, CursorRange>;
        };
        reportInvalidPage(parsed.page);
        return parsed;
      }),
    liveEvent: (handle: number, event: LiveEvent): LiveAction[] =>
      JSON.parse(native.liveEvent(handle, JSON.stringify(event))),
    liveClose: (handle: number): void => native.liveClose(handle),
    onCommitted: (scope: string, wake: () => void) =>
      wakes.subscribe(scope, wake),
    notifyCommitted: (scopes: readonly string[]) => wakes.notify(scopes),
    closeLive: () => wakes.clear(),
    transaction,
    publish,
  };
  const authenticate = async (request: IncomingMessage) => {
    const id = await options.authenticate(request);
    if (typeof id !== "string") return null;
    const trimmed = id.trim();
    return trimmed === "" ? null : trimmed;
  };
  const admit = async (
    request: IncomingMessage,
  ): Promise<{ owner: string | null; refusal: Refusal | null }> => {
    const owner = await authenticate(request);
    if (!options.admit) return { owner, refusal: null };
    return {
      owner,
      refusal: checkedRefusal(await options.admit(request, owner)),
    };
  };
  const listen = async ({
    port,
    host = "127.0.0.1",
  }: {
    port: number;
    host?: string;
  }) => {
    const server = createServer(
      createHttpHandler({
        backend: api,
        admit,
        onError,
      }),
    );
    const live = attachLive(server, {
      backend: api,
      admit,
      onError,
    });
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error) => reject(error);
      server.once("error", onError);
      server.listen(port, host, () => {
        server.off("error", onError);
        resolve();
      });
    });
    const address = server.address();
    const actual = typeof address === "object" && address ? address.port : port;
    const urlHost =
      host === "0.0.0.0"
        ? "127.0.0.1"
        : host === "::"
          ? "localhost"
          : host.includes(":")
            ? `[${host}]`
            : host;
    let closed = false;
    return {
      url: `http://${urlHost}:${actual}`,
      close: async () => {
        if (closed) return;
        closed = true;
        await live.close();
        server.closeIdleConnections();
        await new Promise<void>((resolve, reject) =>
          server.close((error) => (error ? reject(error) : resolve())),
        );
      },
    };
  };
  return { ...api, listen };
}
/** The response header that marks an admission refusal. */
const ADMISSION_HEADER = "axton-admission";
/** An admission refusal ready to answer: its status and JSON text. */
type Refusal = { status: number; body: string };
/**
 * What `admit` answered, checked: `null` admits, a refusal needs a status of
 * 400..599 and a body JSON can encode. Anything else is a defect of the hook.
 */
function checkedRefusal(answer: unknown): Refusal | null {
  if (answer === null || answer === undefined) return null;
  const { status, body } = answer as Partial<AdmissionRefusal>;
  const text = body === undefined ? undefined : JSON.stringify(body);
  if (
    !Number.isInteger(status) ||
    (status as number) < 400 ||
    (status as number) > 599 ||
    typeof text !== "string"
  )
    throw new TypeError(
      "admit must answer null or {status: 400..599, body: JSON}",
    );
  return { status: status as number, body: text };
}
interface HttpBackend {
  push(owner: string, request: Uint8Array | string): Promise<string>;
  pull(owner: string, request: Uint8Array | string): Promise<string>;
  action(owner: string, request: Uint8Array | string): Promise<string>;
  loads(owner: string, request: Uint8Array | string): Promise<string>;
  fetch(owner: string, request: Uint8Array | string): Promise<string>;
}
/** Authenticate a request, then ask the application's `admit` about it. */
type Admission = (
  request: IncomingMessage,
) => Promise<{ owner: string | null; refusal: Refusal | null }>;
function createHttpHandler(options: {
  backend: HttpBackend;
  admit: Admission;
  maxBodyBytes?: number;
  onError?: (error: unknown) => void;
}): RequestListener {
  return async (request, response) => {
    const send = (status: number, value: unknown) => {
      response.writeHead(status, {
        "content-type": "application/json; charset=utf-8",
        "cache-control": "no-store",
      });
      response.end(typeof value === "string" ? value : JSON.stringify(value));
    };
    const path = request.url?.split("?")[0];
    if (
      path !== "/sync/mutations" &&
      path !== "/sync/pull" &&
      path !== "/sync/actions" &&
      path !== "/sync/loads" &&
      path !== "/sync/fetch"
    ) {
      send(404, { code: "not_found" });
      return;
    }
    if (request.method !== "POST") {
      response.setHeader("allow", "POST");
      send(405, { code: "method_not_allowed" });
      return;
    }
    try {
      const { owner, refusal: refused } = await options.admit(request);
      if (refused) {
        response.writeHead(refused.status, {
          "content-type": "application/json; charset=utf-8",
          "cache-control": "no-store",
          [ADMISSION_HEADER]: "refused",
        });
        response.end(refused.body);
        return;
      }
      if (!owner?.trim()) {
        send(401, { code: "unauthenticated" });
        return;
      }
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of request) {
        const buffer = Buffer.from(chunk);
        size += buffer.length;
        if (size > (options.maxBodyBytes ?? 1_048_576)) {
          send(413, { code: "request_too_large" });
          return;
        }
        chunks.push(buffer);
      }
      const bytes = Buffer.concat(chunks);
      let body: unknown;
      try {
        body = JSON.parse(
          new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        );
      } catch {
        send(400, { code: "request.invalid" });
        return;
      }
      if (body === null || typeof body !== "object" || Array.isArray(body)) {
        send(400, { code: "request.invalid" });
        return;
      }
      const result = await (path === "/sync/mutations"
        ? options.backend.push(owner, bytes)
        : path === "/sync/actions"
          ? options.backend.action(owner, bytes)
          : path === "/sync/loads"
            ? options.backend.loads(owner, bytes)
            : path === "/sync/fetch"
              ? options.backend.fetch(owner, bytes)
              : options.backend.pull(owner, bytes));
      send(200, result);
    } catch (error) {
      const status =
        error instanceof EngineError
          ? HTTP_STATUS_BY_CODE[error.code]
          : undefined;
      if (error instanceof EngineError && status !== undefined) {
        send(status, { code: error.code, ...(error.details ?? {}) });
        return;
      }
      options.onError?.(error);
      send(500, { code: "server" });
    }
  };
}

/**
 * The live executor's seams: the Rust `Subscriptions` controller behind
 * `negotiateLive`, `liveEvent` and `liveClose`, plus the database pull and the
 * commit hub it asks the executor to use.
 */
interface LiveBackend {
  negotiateLive(
    owner: string,
    request: Uint8Array | string,
  ): Promise<{ handle: number; actions: LiveAction[] }>;
  pullLive(
    owner: string,
    cursors: Record<string, number>,
    models: Record<string, number>,
  ): Promise<{ page: string; cursors: Record<string, CursorRange> }>;
  liveEvent(handle: number, event: LiveEvent): LiveAction[];
  liveClose(handle: number): void;
  onCommitted(scope: string, wake: () => void): () => void;
}

function attachLive(
  server: Server,
  options: {
    backend: LiveBackend;
    admit: Admission;
    maxPayloadBytes?: number;
    onError?: (error: unknown) => void;
  },
) {
  const sockets = new WebSocketServer({
    noServer: true,
    maxPayload: options.maxPayloadBytes ?? 1_048_576,
  });
  let closing = false;
  const refuse = (socket: Duplex, status: number) => {
    socket.end(
      `HTTP/1.1 ${status} ${status === 401 ? "Unauthorized" : "Error"}\r\nConnection: close\r\n\r\n`,
    );
  };
  const refuseAdmission = (socket: Duplex, { status, body }: Refusal) => {
    socket.end(
      `HTTP/1.1 ${status} ${STATUS_CODES[status] ?? "Error"}\r\n` +
        `Content-Type: application/json; charset=utf-8\r\n` +
        `Content-Length: ${Buffer.byteLength(body)}\r\n` +
        `Cache-Control: no-store\r\n${ADMISSION_HEADER}: refused\r\n` +
        `Connection: close\r\n\r\n${body}`,
    );
  };
  const upgrade = (request: IncomingMessage, socket: Duplex, head: Buffer) => {
    void (async () => {
      if (request.url?.split("?")[0] !== "/sync/live") return;
      if (closing) {
        refuse(socket, 503);
        return;
      }
      let owner: string | null;
      try {
        const admission = await options.admit(request);
        if (closing || socket.destroyed) {
          refuse(socket, 503);
          return;
        }
        if (admission.refusal) {
          refuseAdmission(socket, admission.refusal);
          return;
        }
        owner = admission.owner;
      } catch (error) {
        options.onError?.(error);
        refuse(socket, 500);
        return;
      }
      if (!owner?.trim()) {
        refuse(socket, 401);
        return;
      }
      sockets.handleUpgrade(request, socket, head, (connection) => {
        void serveLive(connection, owner!, options.backend, options.onError);
      });
    })();
  };
  server.on("upgrade", upgrade);
  return {
    close: async () => {
      if (closing) return;
      closing = true;
      server.off("upgrade", upgrade);
      for (const socket of sockets.clients) socket.close(1001, "closing");
      await new Promise<void>((resolve) => sockets.close(() => resolve()));
    },
  };
}

/**
 * Executes the Rust controller's actions for one socket. Every sync decision
 * (what to pull, when, what to send) is the controller's; this only carries
 * events in and performs actions out. The controller keeps at most one pull
 * outstanding per session; it covers every scope with a pending commit.
 */
async function serveLive(
  connection: WebSocket,
  owner: string,
  backend: LiveBackend,
  onError?: (error: unknown) => void,
): Promise<void> {
  const cleanups: (() => void)[] = [];
  let settled = false;
  let handshakeReject: ((error: Error) => void) | undefined;
  const transportError = (error: Error) => {
    handshakeReject?.(error);
  };
  connection.on("error", transportError);
  cleanups.push(() => connection.off("error", transportError));
  let handle: number | undefined;
  let released = false;
  const open = () => connection.readyState === WebSocket.OPEN;
  const fail = (error: unknown) => {
    onError?.(error);
    if (open()) connection.close(1011, "server");
  };
  const dispatch = (event: LiveEvent) => {
    if (handle === undefined || released) return;
    let actions: LiveAction[];
    try {
      actions = backend.liveEvent(handle, event);
    } catch (error) {
      fail(error);
      return;
    }
    execute(actions);
  };
  const execute = (actions: LiveAction[]) => {
    for (const action of actions) {
      if (action.type === "listen") {
        const { scope } = action;
        cleanups.push(
          backend.onCommitted(scope, () =>
            dispatch({ type: "committed", scope }),
          ),
        );
      } else if (action.type === "send") {
        if (open()) connection.send(action.frame);
      } else {
        backend
          .pullLive(owner, action.cursors, action.models)
          .then(
            (progress) => dispatch({ type: "pulled", page: progress.page }),
            fail,
          );
      }
    }
  };
  const closed = () => dispatch({ type: "closed" });
  try {
    const first = await new Promise<Buffer>((resolve, reject) => {
      const message = (data: Buffer) => {
        if (settled) {
          connection.close(1002, "subscribe is the only client frame");
          return;
        }
        settled = true;
        resolve(Buffer.from(data));
      };
      const handshakeClosed = () => reject(new Error("live handshake closed"));
      handshakeReject = reject;
      connection.on("message", message);
      connection.once("close", handshakeClosed);
      cleanups.push(
        () => connection.off("message", message),
        () => connection.off("close", handshakeClosed),
      );
    });
    const opened = await backend.negotiateLive(owner, first);
    handshakeReject = undefined;
    handle = opened.handle;
    connection.once("close", closed);
    connection.once("error", closed);
    cleanups.push(
      () => connection.off("close", closed),
      () => connection.off("error", closed),
    );
    if (!open()) return;
    execute(opened.actions);
    await new Promise<void>((resolve) => {
      connection.once("close", () => resolve());
      connection.once("error", () => resolve());
    });
  } catch (error) {
    // A malformed subscribe or a refused read-contract declaration is the
    // client's fault: closed as a protocol violation, not reported as a failure.
    const refused =
      error instanceof EngineError &&
      (error.code === "request.invalid" ||
        error.code === "model_version_unsupported");
    if (open())
      connection.close(
        refused ? 1002 : 1011,
        refused ? (error as EngineError).code : "request.invalid",
      );
    if (!refused) onError?.(error);
  } finally {
    if (handle !== undefined) {
      closed();
      released = true;
      backend.liveClose(handle);
    }
    for (const cleanup of cleanups) cleanup();
  }
}
