import { createRequire } from "node:module";
export type Native = {
  processLive05?(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  processDelivery05?(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  processMaterialization05?(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  processRead05?(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  handshake05?(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  serverMaterializationId05?(
    config: string,
    projectionGeneration: string,
  ): string;
  validateMutationBatch?(config: string, request: string): string;
  processBatchMember?(
    config: string,
    owner: string,
    request: string,
    ordinal: number,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  encodeBatchAcknowledgement?(request: string, results: string[]): string;
  settleExternal05?(
    config: string,
    settlement: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  validateConfig(config: string): void;
  /** Negotiates and opens the socket's `Subscriptions`; answers `{handle, actions}` JSON. */
  negotiateLive(
    config: string,
    owner: string,
    request: string,
    callback: (request: string) => Promise<string>,
  ): Promise<string>;
  /** Applies one `LiveEvent` JSON to the session and answers its `LiveAction[]` JSON. */
  liveEvent(handle: number, event: string): string;
  /** Forgets the session; idempotent. */
  liveClose(handle: number): void;
};
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
  type Async = "negotiateLive";
  type Sync = "validateConfig" | "liveEvent" | "liveClose";
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
    ...Object.fromEntries(
      [
        "processDelivery05",
        "processMaterialization05",
        "processRead05",
        "handshake05",
        "processLive05",
      ]
        .filter((key) => typeof (native as any)[key] === "function")
        .map((key) => [
          key,
          (...args: any[]) =>
            (native as any)[key](...args).catch((error: unknown) => {
              throw engineError(error);
            }),
        ]),
    ),
    ...(native.serverMaterializationId05
      ? {
          serverMaterializationId05:
            native.serverMaterializationId05.bind(native),
        }
      : {}),
    ...(native.validateMutationBatch
      ? {
          validateMutationBatch: (config: string, request: string) => {
            try {
              return native.validateMutationBatch!(config, request);
            } catch (error) {
              throw engineError(error);
            }
          },
        }
      : {}),
    ...(native.processBatchMember
      ? {
          processBatchMember: (
            ...args: Parameters<NonNullable<Native["processBatchMember"]>>
          ) =>
            native.processBatchMember!(...args).catch((error) => {
              throw engineError(error);
            }),
        }
      : {}),
    ...(native.encodeBatchAcknowledgement
      ? {
          encodeBatchAcknowledgement: (request: string, results: string[]) => {
            try {
              return native.encodeBatchAcknowledgement!(request, results);
            } catch (error) {
              throw engineError(error);
            }
          },
        }
      : {}),
    ...(native.settleExternal05
      ? {
          settleExternal05: (
            ...args: Parameters<NonNullable<Native["settleExternal05"]>>
          ) =>
            native.settleExternal05!(...args).catch((error) => {
              throw engineError(error);
            }),
        }
      : {}),
    validateConfig: wrapSync("validateConfig"),
    negotiateLive: wrap("negotiateLive"),
    liveEvent: wrapSync("liveEvent"),
    liveClose: wrapSync("liveClose"),
  };
}

export function loadNative(native?: Native): Native {
  return typedNative(
    native ?? (createRequire(import.meta.url)("@axtonjs/native") as Native),
  );
}
