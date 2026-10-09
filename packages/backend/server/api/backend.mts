import type { BackendDescriptor } from "./schema.mts";
import { Session } from "../bindings/session.mts";
import { createHost, type HostDependencies } from "../bindings/host.mts";
import type { RuntimeStream, RuntimeLoadStream } from "./stream.mts";
export type { RuntimeStream, RuntimeLoadStream } from "./stream.mts";
import { createServer, STATUS_CODES } from "node:http";
import type { IncomingMessage, ServerResponse, Server } from "node:http";
import type { Duplex } from "node:stream";
import { WebSocketServer, WebSocket } from "ws";
import {
  effectsFor,
  loadEffectsFor,
  bootstrapEffectsFor,
  lowerFirst,
  type RuntimeInvalidate,
} from "../bindings/effects.mts";
import type {
  HostRequest,
  SettlementEffects,
} from "../bindings/host-contract.mts";
import { isRetryableTransactionError } from "../bindings/retryable.mts";
export { WebSocket } from "ws";
export { isRetryableTransactionError } from "../bindings/retryable.mts";
export type { RecordRef, RuntimeInvalidate } from "../bindings/effects.mts";
export type {
  Acknowledged,
  Head,
  HostRequest,
  JsonValue,
  TrackingDelta,
  TrackingPair,
  MemberKey,
  MemberPosition,
  Protocol05Context,
  Protocol05Operation,
  Protocol05Request,
} from "../bindings/host-contract.mts";
import type { JsonValue } from "../bindings/host-contract.mts";
export type { Native } from "../bindings/native.mts";
import type { Native } from "../bindings/native.mts";
/** One stream's progress in a page: after `from`, up to `to`, of a stream at `head`. */
export type CursorRange = { from: number; to: number; head: number };
/** What the executor reports to the Rust `Subscriptions` controller. */
export type LiveEvent =
  | { type: "committed"; stream: string }
  | { type: "pulled"; page: string }
  | { type: "closed" };
/** What the controller asks the executor to do, in order. */
export type LiveAction =
  | { type: "pullV05"; request: string }
  | { type: "listen"; stream: string }
  | { type: "send"; frame: string };
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
export { EngineError } from "../bindings/native.mts";
import { EngineError, loadNative } from "../bindings/native.mts";
/**
 * Engine codes with a client-visible HTTP status. Every other failure is a
 * server-side defect: reported to `onError` and answered `500 {code: "server"}`.
 */
const HTTP_STATUS_BY_CODE: Readonly<Record<string, number>> = {
  "delivery.expired": 410,
  "delivery.capacity": 413,
  "request.invalid": 400,
  context_mismatch: 409,
  "stream.forbidden": 403,
  "store.binding": 403,
  "principal.invalid": 400,
  "batch.conflict": 409,
  "batch.sequence": 409,
  "batch.progress": 409,
  "page.capacity": 413,
  constraint_group_capacity: 413,
  "protocol.unsupported": 426,
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
 * the same declaration handles a Mutation receives. `invalidate` declares a record
 * the body changed; `stream(names)` tracks records or targets invalidation. The
 * engine settles them after the body returns, inside the same transaction.
 * A generated backend narrows both to its schema's Models.
 */
export interface TransactionCall<Tx> {
  tx: Tx;
  streams(names: readonly string[]): RuntimeStream;
  stream(names: string | readonly string[]): RuntimeStream;
  invalidate: RuntimeInvalidate;
}
/** A viewer Loader answers current state at one retained read version. */
export interface LoaderCall<Tx, Identity> {
  ids: readonly Identity[];
  tx: Tx;
  userId: string;
}
export type Loader<Tx, Identity = any, Row = object> = (
  call: LoaderCall<Tx, Identity>,
) => Promise<readonly (Row | null)[]>;
/**
 * Trusted framework context of a Mutation: it may change business state,
 * declare records it changed beyond its inputs (`invalidate`) and track records or target invalidation
 * through `stream(names)`. The handles close when the handler
 * settles.
 */
export interface MutationContext<Tx> {
  tx: Tx;
  userId: string;
  callId: string;
  readonly stream: RuntimeStream &
    ((names: string | readonly string[]) => RuntimeStream);
  streams(names: readonly string[]): RuntimeStream;
  invalidate: RuntimeInvalidate;
}
/**
 * Trusted framework context of a Query. It may track explicitly in its authenticated
 * Stream, and carries no `invalidate`. A Query reads without business side effects.
 * `tx` is still the application's own transaction; the framework cannot inspect arbitrary SQL,
 * so honoring the read-only contract is the handler's responsibility.
 */
export interface QueryContext<Tx> {
  tx: Tx;
  userId: string;
  callId: string;
  readonly stream: RuntimeLoadStream &
    ((names: string | readonly string[]) => RuntimeLoadStream);
  streams(names: readonly string[]): RuntimeLoadStream;
}
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
export type LoaderRegistration<Tx> =
  Loader<Tx> | { [version: `v${number}`]: Loader<Tx> };
/**
 * One registration holds every retained version under the mutation or model
 * name; a bare function is shorthand for a v1-only contract and never stands
 * for the latest version. Refused at startup, naming the key and version.
 */
function versioned<F>(
  kind: "loader" | "mutation" | "query",
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
        `Unknown ${kind} ${key}.${found} for ${name}: retained ${kind === "mutation" || kind === "query" ? `${kind} ` : ""}versions are ${list}`,
      );
  return table;
}
export interface BackendOptions<T> {
  protocol5?: {
    projectionGeneration?: string;
    materializations?: Record<
      string,
      { schema: object; projectionGeneration?: string }
    >;
    authorizeStream(
      principal: string,
      stream: string,
      tx: T,
    ): boolean | Promise<boolean>;
  };
  bootstrap?: (call: { ctx: QueryContext<T> }) => void | Promise<void>;
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
  /** Every retained Mutation version, by lower-camel name. */
  mutations?: Record<string, MutationHandlerRegistration<T>> | undefined;
  /** Every retained Query version, by lower-camel name. */
  queries?: Record<string, QueryHandlerRegistration<T>> | undefined;
  /**
   * Every retained version of each Model's read contract, by lower-camel
   * name. A Model left out (or `undefined`) is device-only: never published,
   * and no retained Mutation or Query may name it on the wire.
   */
  loaders: Record<string, LoaderRegistration<T> | undefined>;
  loaderHooks?: Record<
    string,
    {
      prepareForViewer(
        call: LoaderCall<T, any> &
          Pick<TransactionCall<T>, "streams" | "invalidate">,
      ): Promise<void>;
    }
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
class WakeHub {
  private listeners = new Map<string, Set<() => void>>();
  subscribe(stream: string, wake: () => void): () => void {
    const listeners = this.listeners.get(stream) ?? new Set();
    listeners.add(wake);
    this.listeners.set(stream, listeners);
    return () => {
      listeners.delete(wake);
      if (!listeners.size) this.listeners.delete(stream);
    };
  }
  notify(streams: Iterable<string>): void {
    for (const stream of new Set(streams))
      for (const wake of [...(this.listeners.get(stream) ?? [])])
        queueMicrotask(wake);
  }
  clear(): void {
    this.listeners.clear();
  }
}
/** The retained backend business kind of an operation; omitted is `mutation`. */
type CallKind = "mutation" | "query";
/** Where each operation kind registers its handlers. */
const REGISTRATION_GROUP = {
  mutation: "mutations",
  query: "queries",
} as const;
/**
 * `External` is what `backend.transaction` hands its body; a generated
 * backend passes its own `TransactionCall`, typed by its schema's Models.
 */
export function createBackend<T, External extends object = TransactionCall<T>>(
  options: BackendOptions<T>,
) {
  const native = loadNative(options.native);
  // Nothing is dropped silently: without a handler, failures go to the console.
  const onError: (error: unknown) => void =
    options.onError ?? ((error) => console.error(error));
  const descriptor = options.config as BackendDescriptor;
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
    ...(options.protocol5
      ? {
          protocol5: {
            projectionGeneration: options.protocol5.projectionGeneration ?? "1",
            materializations: options.protocol5.materializations ?? {},
          },
        }
      : {}),
  });
  const materializationId = options.protocol5
    ? native.serverMaterializationId05?.(
        config,
        options.protocol5.projectionGeneration ?? "1",
      )
    : undefined;
  native.validateConfig(config);
  if (options.protocol5 && !materializationId)
    throw new Error("protocol5 native materialization derivation unavailable");
  // Refuses Models whose generated accessors collide,
  // and declarations naming a device-only Model.
  const createEffects = effectsFor(
    schemaModels,
    descriptor.schema?.enums,
    new Set(loadedModels),
  );
  const createLoadEffects = loadEffectsFor(
    schemaModels,
    descriptor.schema?.enums,
    new Set(loadedModels),
  );
  const createBootstrapEffects = bootstrapEffectsFor(
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
  const operations = descriptor.schema?.actions ?? [];
  const retainedOperations = operations.map((action) => ({
    name: action.name,
    version: action.version,
    kind: (action.kind ?? "mutation") as CallKind,
  }));
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
  const hostDependencies: HostDependencies<T> = {
    options,
    native,
    config,
    descriptor,
    schemaModels,
    actionTable,
    actionHandlers,
    loaderTable,
    createEffects,
    createLoadEffects,
    createBootstrapEffects,
    refusal,
    onError,
  };
  const host = (tx: T, session: Session) =>
    createHost(hostDependencies, tx, session);
  /**
   * Runs `operation` under one session bound to `tx`: every host callback is
   * tracked, and the operation completes only once none is unfinished or
   * failed. Answers its value and the Streams it published to, which the
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
   * Runs `body` with a Mutation's `stream` and `invalidate`, then settles what it
   * declared in `tx`: current state for each invalidated record, delivered to its selected
   * tracking streams, and each newly tracked pair delivered once. The handles close when the body settles, whether it
   * returns or throws.
   */
  const settle = async <R,>(
    tx: T,
    session: Session,
    body: (call: External) => R | Promise<R>,
  ): Promise<R> => {
    await session.track(() =>
      options.database.persistence(tx).call({ op: "publicationFence" }),
    );
    const effects = createEffects();
    let result: R;
    try {
      const call: TransactionCall<T> = {
        tx,
        stream: effects.stream,
        streams: effects.stream,
        invalidate: effects.invalidate,
      };
      result = await body(call as unknown as External);
    } finally {
      effects.close();
    }
    await session.track(() =>
      native.settleExternal05!(
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
   * subscribers of every stream published to are woken; a failure rolls
   * back and wakes nobody. Answers the body's own value. Not for use inside
   * a handler, which already has a transaction.
   */
  const transaction = <R,>(body: (call: External) => Promise<R>): Promise<R> =>
    run((tx, session) => settle(tx, session, body));
  /**
   * Settles `body`'s declarations in `tx`, a transaction the application
   * opened and still owns, before this call resolves: the identities,
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
  // Expiry is a successful read outcome: commit staging cleanup, then surface
  // the explicit error outside the transaction. Capacity/faults still roll back.
  const deliveryRead = async (
    operation: (tx: T, session: Session) => Promise<string>,
  ): Promise<string> => {
    const outcome = await run(async (tx, session) => {
      try {
        return { response: await operation(tx, session) };
      } catch (error) {
        if (error instanceof EngineError && error.code === "delivery.expired")
          return { expired: error };
        throw error;
      }
    });
    if ("expired" in outcome) throw outcome.expired;
    return outcome.response;
  };
  /** @internal Raw protocol seams used by the framework's own tests; not part of the supported surface. */
  const api = {
    materializationId,
    push: async (owner: string, request: Uint8Array | string) => {
      const wire = text(request);
      {
        if (
          !native.validateMutationBatch ||
          !native.processBatchMember ||
          !native.encodeBatchAcknowledgement
        )
          throw new Error("protocol05 native Batch support unavailable");
        const frozen = native.validateMutationBatch(config, wire);
        const count = (JSON.parse(frozen).mutations as unknown[]).length;
        const results: string[] = [];
        for (let ordinal = 0; ordinal < count; ordinal++)
          results.push(
            await run((tx, session) =>
              native.processBatchMember!(
                config,
                owner,
                frozen,
                ordinal,
                host(tx, session),
              ),
            ),
          );
        return native.encodeBatchAcknowledgement(frozen, results);
      }
    },
    action: (owner: string, request: Uint8Array | string) =>
      run((tx, session) =>
        native.processRead05!(config, owner, text(request), host(tx, session)),
      ),
    fetch: (owner: string, request: Uint8Array | string) =>
      run((tx, session) =>
        native.processRead05!(config, owner, text(request), host(tx, session)),
      ),
    pull: (owner: string, request: Uint8Array | string) =>
      deliveryRead((tx, session) =>
        native.processDelivery05!(
          config,
          owner,
          text(request),
          host(tx, session),
        ),
      ),
    pull05Live: (owner: string, request: string) =>
      deliveryRead((tx, session) =>
        native.processLive05!(config, owner, request, host(tx, session)),
      ),
    handshake: (owner: string, request: Uint8Array | string) =>
      run((tx, session) =>
        native.handshake05!(config, owner, text(request), host(tx, session)),
      ),
    materialize: (owner: string, request: Uint8Array | string) =>
      deliveryRead((tx, session) =>
        native.processMaterialization05!(
          config,
          owner,
          text(request),
          host(tx, session),
        ),
      ),
    negotiateLive: (
      owner: string,
      request: Uint8Array | string,
    ): Promise<{ handle: number; actions: LiveAction[] }> =>
      run((tx, session) =>
        native.negotiateLive(config, owner, text(request), host(tx, session)),
      ).then(JSON.parse),
    liveEvent: (handle: number, event: LiveEvent): LiveAction[] =>
      JSON.parse(native.liveEvent(handle, JSON.stringify(event))),
    liveClose: (handle: number): void => native.liveClose(handle),
    onCommitted: (stream: string, wake: () => void) =>
      wakes.subscribe(stream, wake),
    notifyCommitted: (streams: readonly string[]) => wakes.notify(streams),
    closeLive: () => wakes.clear(),
    transaction,
    publish,
    /** Must be awaited BEFORE relevant business writes in a caller-owned tx. */
    acquirePublicationFence: (tx: T): Promise<void> =>
      options.database
        .persistence(tx)
        .call({ op: "publicationFence" })
        .then(() => {}),
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
    let stopping = false;
    const requests = new Set<Promise<void>>();
    const handler = createHttpHandler({ backend: api, admit, onError });
    const server = createServer((request, response) => {
      if (stopping) {
        response.writeHead(503, { connection: "close" });
        response.end();
        return;
      }
      const work = handler(request, response);
      requests.add(work);
      void work.then(
        () => requests.delete(work),
        () => requests.delete(work),
      );
    });
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
    let closing: Promise<void> | undefined;
    return {
      url: `http://${urlHost}:${actual}`,
      close: () => {
        stopping = true;
        return (closing ??= (async () => {
          const drained = await Promise.allSettled([live.close(), ...requests]);
          server.closeIdleConnections();
          const closed = await Promise.allSettled([
            new Promise<void>((resolve, reject) =>
              server.close((error) => (error ? reject(error) : resolve())),
            ),
          ]);
          const failure = [...drained, ...closed].find(
            (result) => result.status === "rejected",
          );
          if (failure?.status === "rejected") throw failure.reason;
        })());
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
  handshake?(owner: string, request: Uint8Array | string): Promise<string>;
  materialize?(owner: string, request: Uint8Array | string): Promise<string>;
  push(owner: string, request: Uint8Array | string): Promise<string>;
  pull(owner: string, request: Uint8Array | string): Promise<string>;
  action(owner: string, request: Uint8Array | string): Promise<string>;
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
}): (request: IncomingMessage, response: ServerResponse) => Promise<void> {
  return async (request, response) => {
    const send = (status: number, value: unknown) => {
      if (response.destroyed || response.writableEnded) return;
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
      path !== "/sync/fetch" &&
      path !== "/sync/materialize" &&
      path !== "/sync/handshake"
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
        if (response.destroyed || response.writableEnded) return;
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
      const result = await (path === "/sync/handshake"
        ? options.backend.handshake!(owner, bytes)
        : path === "/sync/materialize"
          ? options.backend.materialize!(owner, bytes)
          : path === "/sync/mutations"
            ? options.backend.push(owner, bytes)
            : path === "/sync/actions"
              ? options.backend.action(owner, bytes)
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
  pull05Live?(owner: string, request: string): Promise<string>;
  pull?(owner: string, request: string): Promise<string>;
  negotiateLive(
    owner: string,
    request: Uint8Array | string,
  ): Promise<{ handle: number; actions: LiveAction[] }>;
  liveEvent(handle: number, event: LiveEvent): LiveAction[];
  liveClose(handle: number): void;
  onCommitted(stream: string, wake: () => void): () => void;
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
  const sessions = new Set<Promise<void>>();
  const upgrades = new Set<Promise<void>>();
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
    const pending = (async () => {
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
        const session = serveLive(
          connection,
          owner!,
          options.backend,
          options.onError,
        );
        sessions.add(session);
        void session.then(
          () => sessions.delete(session),
          () => sessions.delete(session),
        );
      });
    })();
    upgrades.add(pending);
    void pending.then(
      () => upgrades.delete(pending),
      () => upgrades.delete(pending),
    );
  };
  server.on("upgrade", upgrade);
  return {
    close: async () => {
      if (closing) return;
      closing = true;
      const owned = [...upgrades, ...sessions];
      server.off("upgrade", upgrade);
      for (const socket of sockets.clients) socket.close(1001, "closing");
      await new Promise<void>((resolve) => sockets.close(() => resolve()));
      const drained = await Promise.allSettled(owned);
      const failure = drained.find((result) => result.status === "rejected");
      if (failure?.status === "rejected") throw failure.reason;
    },
  };
}

/**
 * Executes the Rust controller's actions for one socket. Every sync decision
 * (what to pull, when, what to send) is the controller's; this only carries
 * events in and performs actions out. The controller keeps at most one pull
 * outstanding per session; it covers every stream with a pending commit.
 */
async function serveLive(
  connection: WebSocket,
  owner: string,
  backend: LiveBackend,
  onError?: (error: unknown) => void,
): Promise<void> {
  const cleanups: (() => void)[] = [];
  const pulls = new Set<Promise<void>>();
  const trackPull = (pull: Promise<void>) => {
    pulls.add(pull);
    void pull.then(
      () => pulls.delete(pull),
      () => pulls.delete(pull),
    );
  };
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
        const { stream } = action;
        cleanups.push(
          backend.onCommitted(stream, () =>
            dispatch({ type: "committed", stream }),
          ),
        );
      } else if (action.type === "send") {
        if (open()) connection.send(action.frame);
      } else if (action.type === "pullV05") {
        if (!backend.pull05Live) {
          fail(new Error("v05 live carrier unavailable"));
          return;
        }
        trackPull(
          backend
            .pull05Live(owner, action.request)
            .then((page) => dispatch({ type: "pulled", page }), fail),
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
      (error.code === "protocol.unsupported" ||
        error.code === "request.invalid" ||
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
    // Socket closure stops new work; already admitted database pulls still own
    // their transaction and must finish before the listener releases its host.
    await Promise.all(pulls);
  }
}
