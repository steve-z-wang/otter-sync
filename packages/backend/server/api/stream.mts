/** Owned synchronous Stream declarations; the engine settles their meaning. */
import type {
  StreamIntent,
  HostRecordRef,
} from "../bindings/host-contract.mts";
type Namespace<R> = ((records: unknown) => R) & {
  readonly [model: string]: (ids: unknown) => R;
};
export interface RuntimeStream {
  readonly track: Namespace<void>;
  readonly invalidate: Namespace<void>;
}
export interface RuntimeLoadStream {
  readonly track: Namespace<void>;
}
export type RuntimeInvalidate = Namespace<void>;
export interface StreamEntry {
  name: string;
  key: string;
  scalar?: string;
  snapshot(value: unknown, caller: string): Readonly<Record<string, unknown>>;
}
export interface StreamCollector {
  entries: readonly StreamEntry[];
  guard<T>(body: () => T): T;
  check(caller: string): void;
  name(name: unknown): void;
  publishable(model: string, caller: string): void;
  list(
    records: unknown,
    caller: string,
  ): { entry: StreamEntry; identity: Readonly<Record<string, unknown>> }[];
  record(intents: readonly StreamIntent[], caller: string): void;
}
const define = (object: object, key: string, value: unknown) =>
  Object.defineProperty(object, key, {
    value,
    enumerable: true,
    configurable: true,
  });
function operands(
  c: StreamCollector,
  value: unknown,
  caller: string,
  entry?: StreamEntry,
): readonly HostRecordRef[] {
  c.check(caller);
  if (!entry)
    return c
      .list(Array.isArray(value) ? value : [value], caller)
      .map(
        ({ entry, identity }) =>
          Object.freeze({ model: entry.name, identity }) as HostRecordRef,
      );
  c.publishable(entry.name, caller);
  const values = Array.isArray(value) ? value : [value];
  return Array.from(
    values,
    (v) =>
      Object.freeze({
        model: entry.name,
        identity: entry.snapshot(
          entry.scalar &&
            (v === null || typeof v !== "object" || v instanceof Date)
            ? { [entry.scalar]: v }
            : v,
          caller,
        ),
      }) as HostRecordRef,
  );
}
function namespace<R>(
  c: StreamCollector,
  caller: string,
  body: (records: readonly HostRecordRef[]) => R,
): Namespace<R> {
  const fn = (value: unknown) =>
    c.guard(() => body(operands(c, value, caller)));
  Object.setPrototypeOf(fn, null);
  Reflect.deleteProperty(fn, "name");
  Reflect.deleteProperty(fn, "length");
  for (const entry of c.entries)
    define(fn, entry.key, (value: unknown) =>
      c.guard(() => body(operands(c, value, `${caller}.${entry.key}`, entry))),
    );
  return Object.freeze(fn) as Namespace<R>;
}
export function streamInvalidate(
  c: StreamCollector,
  streams: readonly string[] | null = null,
): RuntimeInvalidate {
  return namespace(c, "invalidate", (records) =>
    c.record(
      streams?.length === 0
        ? []
        : records.map(
            (record) =>
              Object.freeze({
                kind: "invalidate",
                streams,
                record,
              }) as StreamIntent,
          ),
      "invalidate",
    ),
  );
}
export function streamHandle(
  c: StreamCollector,
  value: string | readonly string[],
  load = false,
): RuntimeStream | RuntimeLoadStream {
  const names = c.guard(() => {
    c.check("stream");
    const values = typeof value === "string" ? [value] : value;
    if (!Array.isArray(values))
      throw new Error("stream: names must be a string or array");
    const captured = Array.from(values, (name) => {
      c.name(name);
      return name as string;
    });
    return Object.freeze([...new Set(captured)]);
  });
  const track = namespace(c, "stream.track", (records) =>
    c.record(
      names.flatMap((stream) =>
        records.map(
          (record) =>
            Object.freeze({ kind: "track", stream, record }) as StreamIntent,
        ),
      ),
      "stream.track",
    ),
  );
  return Object.freeze(
    load ? { track } : { track, invalidate: streamInvalidate(c, names) },
  );
}
