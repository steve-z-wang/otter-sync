import {
  scopeHandle,
  scopeTouch,
  type RuntimeScope,
  type RuntimeLoadScope,
  type RuntimeScopeTouch,
} from "./scope.mts";
/**
 * The declarations one handler, legacy handler or `backend.transaction` body
 * makes while it runs: changed records (`touch`) and ordered Scope intents through membership, label and selection handles.
 *
 * Declarations are synchronous and owned: each call validates its identity
 * against the Model's identity fields, and its tags against the tag rules,
 * and copies both at once, so a later change to the caller's object, Date or
 * array cannot retarget it. A collector closes when its callback settles;
 * every later declaration, through any escaped handle, is refused. The Rust
 * engine owns what the declarations mean: it infers input targets, reduces
 * the intents in order to their final state and settles them.
 *
 * A Load page declares through a narrower collector: add-only Scope
 * handles, with a repeated pair stored once (its tags unioned) and the page's
 * enrollment bounded like the engine bounds it.
 */
import type {
  ChannelIntent,
  HostRecordRef,
  SettlementEffects,
} from "./host-contract.mts";

/**
 * A record named by Model and identity. Generated backends narrow it to a
 * union of each Model with its own identity type and generate a constructor
 * per Model, e.g. `Todo({ id })`.
 */
export interface RecordRef {
  readonly model: string;
  readonly identity: object;
}
/** One change declaration per Model under its lower-first accessor: `touch.todo(identity)`. */
export type RuntimeTouch = RuntimeScopeTouch;
export interface EffectCollector {
  readonly touch: RuntimeTouch;
  scope(name: string): RuntimeScope;
  /** Owned copies of the declarations, readable after `close`. */
  settlement(): SettlementEffects;
  /** Refuses every later declaration, through any handle. Idempotent. */
  close(): void;
}
/**
 * Why a Load's declarations cannot settle: its enrollment passed a bound
 * (`overflow`), or a declaration was refused (`invalid`). Kept even when the
 * handler caught the error, so a page never enrolls part of what it meant.
 */
export type LoadEffectFailure = {
  kind: "overflow" | "invalid";
  /** What the declaration threw: an `Error`, or whatever a caller's getter threw. */
  error: unknown;
};
export interface LoadEffectCollector {
  scope(name: string): RuntimeLoadScope;
  /** Owned copies of the distinct additions, in first-declaration order. */
  memberships(): readonly ChannelIntent[];
  /** The first overflow, else the first refused declaration; `undefined` when neither happened. */
  failure(): LoadEffectFailure | undefined;
  /** Refuses every later declaration, through any handle. Idempotent. */
  close(): void;
}
/**
 * One Load page's enrollment bounds, counted over distinct Channel/record
 * pairs: the engine's `LOAD_ENROLLMENT_PAIRS` and `LOAD_ENROLLMENT_BYTES`
 * (`axton_core::limits`), shared through
 * `fixtures/protocol/load-enrollment-limits.json`.
 */
export const LOAD_ENROLLMENT_PAIRS = 1000;
export const LOAD_ENROLLMENT_BYTES = 1024 * 1024;
/** A configured Model descriptor, as the compiled schema's `models` holds it. */
export type EffectModel = {
  name: string;
  identity?: readonly string[];
  fields?: readonly { name: string; type?: unknown }[];
};

/** A configured enum descriptor, as the compiled schema's `enums` holds it. */
export type EffectEnum = { name: string; values?: readonly string[] };

export function lowerFirst(name: string): string {
  return name.charAt(0).toLowerCase() + name.slice(1);
}

/** A copied, encoded identity: plain own properties in identity order, frozen. */
type Identity = Readonly<Record<string, unknown>>;
type Entry = {
  name: string;
  key: string;
  scalar?: string;
  snapshot(value: unknown, caller: string): Identity;
  /** A snapshot as the engine canonicalizes it (`Schema::record_key`). */
  canonical(identity: Identity): Identity;
};
/** How one encoded component is spelled canonically; most are already. */
type Canonical = (value: unknown) => unknown;
const same: Canonical = (value) => value;
const INVALID = Symbol("invalid");

/**
 * The engine's UUID rule (`crates/core/src/schema.rs`): the 36-character
 * hyphenated form, RFC 4122 variant, version 1 to 8; either case.
 */
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
/**
 * RFC 3339 with 'T' at index 10 and a zone, as the engine parses it. Leap
 * seconds and fractions beyond nanoseconds are refused: a declaration may be
 * stricter than the engine, never more lenient.
 */
const ZONED =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:[Zz]|([+-])(\d{2}):(\d{2}))$/;
/** Whether `text` is a date-time the engine accepts: a zoned RFC 3339 date-time with real calendar fields. */
function zoned(text: string): boolean {
  const parts = ZONED.exec(text);
  if (!parts) return false;
  const [year, month, day, hour, minute, second] = parts
    .slice(1, 7)
    .map(Number) as [number, number, number, number, number, number];
  const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const days =
    month === 2 ? (leap ? 29 : 28) : [4, 6, 9, 11].includes(month) ? 30 : 31;
  const offset =
    parts[8] === undefined || (Number(parts[9]) < 24 && Number(parts[10]) < 60);
  return (
    month >= 1 &&
    month <= 12 &&
    day >= 1 &&
    day <= days &&
    hour < 24 &&
    minute < 60 &&
    second < 60 &&
    offset
  );
}

/**
 * A date-time `zoned` accepted, as the engine canonicalizes it: the UTC
 * instant with milliseconds, a fraction beyond them truncated.
 */
function instant(text: string): string {
  const parts = ZONED.exec(text)!;
  const [year, month, day, hour, minute, second] = parts
    .slice(1, 7)
    .map(Number) as [number, number, number, number, number, number];
  const millis = Number((parts[7] ?? "").padEnd(3, "0").slice(0, 3));
  const at = new Date(0);
  at.setUTCFullYear(year, month - 1, day);
  at.setUTCHours(hour, minute, second, millis);
  const offset =
    parts[8] === undefined
      ? 0
      : (parts[8] === "-" ? -1 : 1) *
        (Number(parts[9]) * 60 + Number(parts[10]));
  return new Date(at.getTime() - offset * 60_000).toISOString();
}

/**
 * Encodes one identity component as the engine receives it, or answers
 * INVALID. Every rule matches the engine's, so a declaration the collector
 * accepts is never refused when the engine resolves it.
 */
function component(
  model: string,
  field: string,
  type: unknown,
  enums: ReadonlyMap<string, readonly string[]>,
): [
  expected: string,
  encode: (value: unknown) => unknown,
  canonical: Canonical,
] {
  const { kind, name } = (type ?? {}) as { kind?: unknown; name?: unknown };
  if (kind === "enum") {
    const values = typeof name === "string" ? enums.get(name) : undefined;
    if (!values)
      throw new Error(
        `${model} identity field ${field} names an enum the configuration does not declare`,
      );
    return [
      `one of ${values.join(", ")}`,
      (v) => (typeof v === "string" && values.includes(v) ? v : INVALID),
      same,
    ];
  }
  if (kind === "scalar")
    switch (name) {
      case "string":
        return ["a string", (v) => (typeof v === "string" ? v : INVALID), same];
      case "uuid":
        return [
          "a UUID (36 characters, RFC 4122 variant, version 1 to 8)",
          (v) => (typeof v === "string" && UUID.test(v) ? v : INVALID),
          (v) => (v as string).toLowerCase(),
        ];
      case "boolean":
        return [
          "a boolean",
          (v) => (typeof v === "boolean" ? v : INVALID),
          same,
        ];
      case "int":
        return [
          "a safe integer",
          (v) => (Number.isSafeInteger(v) ? v : INVALID),
          same,
        ];
      case "float":
        return [
          "a finite number",
          (v) => (typeof v === "number" && Number.isFinite(v) ? v : INVALID),
          same,
        ];
      case "dateTime":
        // A decoded Date is encoded now; a wire string (a legacy slot
        // identity) passes through for the engine to canonicalize.
        return [
          "a valid Date or a zoned RFC 3339 date-time string",
          (v) => {
            const text =
              v instanceof Date
                ? Number.isNaN(v.getTime())
                  ? undefined
                  : v.toISOString()
                : v;
            return typeof text === "string" && zoned(text) ? text : INVALID;
          },
          (v) => instant(v as string),
        ];
    }
  throw new Error(
    `${model} identity field ${field} has an unsupported type ${JSON.stringify(type)}`,
  );
}

function define(target: object, key: string, value: unknown): void {
  Object.defineProperty(target, key, { value, enumerable: true });
}

/**
 * Validates the configured Models once: every Model needs identity fields
 * with supported types, and its lower-first accessor must be unique.
 */
function entriesOf(
  models: readonly EffectModel[],
  enums: readonly EffectEnum[],
): Entry[] {
  const owners = new Map<string, string>();
  const values = new Map(enums.map((en) => [en.name, en.values ?? []]));
  return models.map((model) => {
    const name = model?.name;
    if (typeof name !== "string" || name === "")
      throw new Error("every Model descriptor needs a name");
    const key = lowerFirst(name);
    const other = owners.get(key);
    if (other !== undefined)
      throw new Error(
        `Models ${other} and ${name} both generate the accessor ${key}; rename one`,
      );
    owners.set(key, name);
    if (!Array.isArray(model.identity) || model.identity.length === 0)
      throw new Error(`Model ${name} declares no identity fields`);
    const components = model.identity.map((field: string) => {
      const declared = model.fields?.find(
        (candidate) => candidate.name === field,
      );
      if (!declared)
        throw new Error(
          `${name} identity field ${field} is not one of its fields`,
        );
      return [field, ...component(name, field, declared.type, values)] as const;
    });
    const canonical = (identity: Identity): Identity => {
      const copy = {};
      for (const [field, , , spell] of components)
        define(copy, field, spell(identity[field]));
      return Object.freeze(copy);
    };
    const snapshot = (value: unknown, caller: string): Identity => {
      if (value === null || typeof value !== "object")
        throw new Error(`${caller}: ${name} identity must be an object`);
      const copy = {};
      for (const [field, expected, encode] of components) {
        const raw = (value as Record<string, unknown>)[field];
        if (raw === undefined || raw === null)
          throw new Error(
            `${caller}: ${name} identity field ${field} is missing`,
          );
        const encoded = encode(raw);
        if (encoded === INVALID)
          throw new Error(
            `${caller}: ${name} identity field ${field} must be ${expected}`,
          );
        define(copy, field, encoded);
      }
      return Object.freeze(copy);
    };
    return {
      name,
      key,
      ...(model.identity.length === 1 ? { scalar: model.identity[0] } : {}),
      snapshot,
      canonical,
    };
  });
}

/** A resolved declaration: the Model's entry and the owned identity. */
type Declared = { entry: Entry; identity: Identity };

const LONE_SURROGATE = /\p{Surrogate}/u;
type AddIntent = Extract<ChannelIntent, { kind: "add" }>;
/** An owned add intent: the record and tags frozen with it. */
function addIntent(
  channel: string,
  model: string,
  identity: Identity,
  tags: readonly string[],
): AddIntent {
  return Object.freeze({
    kind: "add",
    channel,
    record: Object.freeze({ model, identity }) as HostRecordRef,
    tags,
  });
}
/**
 * What every collector over one configuration shares: the validated Models
 * and the per-call checks that refuse a device-only Model, a malformed
 * reference or a blank Channel name.
 */
type Declarations = {
  entries: readonly Entry[];
  publishable(model: string, caller: string): void;
  /** Resolves a mixed list whole, so a caught failure declares nothing. */
  list(records: unknown, caller: string): Declared[];
  channelName(name: unknown): void;
};
function declarationsOf(
  models: readonly EffectModel[],
  enums: readonly EffectEnum[],
  loaded: ReadonlySet<string> | undefined,
): Declarations {
  const entries = entriesOf(models, enums);
  const byName = new Map(entries.map((entry) => [entry.name, entry]));
  const publishable = (model: string, caller: string) => {
    if (loaded && !loaded.has(model))
      throw new Error(
        `${caller}: Model ${model} has no Loader, so it is device-only and cannot be published`,
      );
  };
  /** Resolves one explicit reference; a raw identity names no Model and fails. */
  const reference = (value: unknown, caller: string): Declared => {
    const model = (value as { model?: unknown } | null)?.model;
    if (
      value === null ||
      typeof value !== "object" ||
      typeof model !== "string"
    )
      throw new Error(
        `${caller}: each element must be a record reference such as Todo({ id }); a raw identity names no Model`,
      );
    const entry = byName.get(model);
    if (!entry) throw new Error(`${caller}: unknown Model ${model}`);
    publishable(model, caller);
    return {
      entry,
      identity: entry.snapshot((value as RecordRef).identity, caller),
    };
  };
  return {
    entries,
    publishable,
    list(records, caller) {
      if (!Array.isArray(records))
        throw new Error(`${caller}: expected an array of record references`);
      const resolved = [];
      for (let index = 0; index < records.length; index++)
        resolved.push(reference(records[index], caller));
      return resolved;
    },
    channelName(name) {
      // Non-empty after JS `trim()`. The engine applies its own check
      // (`check_channel`, Rust `trim()`) at settlement; the two trims differ
      // on a few code points such as U+FEFF and U+0085.
      if (typeof name !== "string" || name.trim() === "")
        throw new Error("scope: a Scope name must be a nonblank string");
    },
  };
}
const closed = (caller: string) =>
  new Error(
    `${caller}: the callback has settled and its declarations are closed`,
  );

/**
 * Validates `models` (and the `enums` their identities use) once and answers
 * a factory of callback-scoped collectors. A malformed configuration throws
 * here, at startup.
 *
 * `loaded` names the Models with a registered Loader. Any other Model is
 * device-only ([#187](https://github.com/zanminwang/axton/issues/187)): it is
 * never published, so every declaration naming it throws at the call.
 * Without `loaded`, every Model may be declared.
 */
export function effectsFor(
  models: readonly EffectModel[],
  enums: readonly EffectEnum[] = [],
  loaded?: ReadonlySet<string>,
): () => EffectCollector {
  const { entries, publishable, list, channelName } = declarationsOf(
    models,
    enums,
    loaded,
  );
  return () => {
    let open = true;
    const changes: HostRecordRef[] = [];
    const changed = new Set<string>();
    const memberships: ChannelIntent[] = [];
    const assertOpen = (caller: string) => {
      if (!open) throw closed(caller);
    };
    const change = (model: string, identity: Identity) => {
      const key = `${model}\u0000${JSON.stringify(Object.values(identity))}`;
      if (changed.has(key)) return;
      changed.add(key);
      changes.push(Object.freeze({ model, identity }) as HostRecordRef);
    };
    const canonical = {
      guard: <T,>(body: () => T) => body(),
      entries,
      publishable,
      list,
      name: channelName,
      check: assertOpen,
      record: (intents: readonly ChannelIntent[]) => {
        memberships.push(...intents);
      },
    };
    const touch = scopeTouch(canonical, (record) =>
      change(record.model, record.identity),
    );
    return Object.freeze({
      touch: touch as RuntimeTouch,
      scope: (name: string) => scopeHandle(canonical, name) as RuntimeScope,
      settlement: (): SettlementEffects => ({
        changes: [...changes],
        memberships: [...memberships],
      }),
      close() {
        open = false;
      },
    });
  };
}

/** One collector over `models`, validating them first. */
export function createEffects(
  models: readonly EffectModel[],
  enums: readonly EffectEnum[] = [],
  loaded?: ReadonlySet<string>,
): EffectCollector {
  return effectsFor(models, enums, loaded)();
}

/** JSON with every object's keys in `Array.prototype.sort` order, as `axton_core::canonical_json` spells it. */
function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  const members = value as Record<string, unknown>;
  return `{${Object.keys(members)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(members[key])}`)
    .join(",")}}`;
}

/**
 * What one distinct pair counts toward `LOAD_ENROLLMENT_BYTES`: the UTF-8
 * length of its add intent's canonical JSON, identity canonical and tags
 * included.
 */
export function enrollmentBytes(intent: ChannelIntent): number {
  return Buffer.byteLength(canonicalJson(intent), "utf8");
}
/** A declaration past a bound: the page fails `load.page_too_large`. */
class EnrollmentOverflow extends Error {}

/**
 * The add-only counterpart of `effectsFor`, for Load pages: over the same
 * validated configuration, it answers a factory of collectors whose Channel
 * handles only add. A repeated Channel/record pair, as the engine
 * canonicalizes it, is stored once, in its first place, with its tags
 * unioned; the declaration that would pass `LOAD_ENROLLMENT_PAIRS` or
 * `LOAD_ENROLLMENT_BYTES` throws and stores nothing. Every refused
 * declaration is kept as the collector's `failure`, so a handler that
 * catches one still fails its page.
 */
export function loadEffectsFor(
  models: readonly EffectModel[],
  enums: readonly EffectEnum[] = [],
  loaded?: ReadonlySet<string>,
): () => LoadEffectCollector {
  const { entries, publishable, list, channelName } = declarationsOf(
    models,
    enums,
    loaded,
  );
  return () => {
    let open = true;
    let failed: LoadEffectFailure | undefined;
    /** Distinct additions by their canonical pair, in first-declaration order, with their bytes. */
    const pairs = new Map<string, { intent: AddIntent; bytes: number }>();
    let bytes = 0;
    const declarations: ChannelIntent[] = [];
    /** Runs one declaration, keeping its refusal even if the handler catches it. */
    const declare = <R,>(body: () => R): R => {
      try {
        return body();
      } catch (error) {
        const kind =
          error instanceof EnrollmentOverflow ? "overflow" : "invalid";
        if (
          failed === undefined ||
          (kind === "overflow" && failed.kind !== kind)
        )
          failed = { kind, error };
        throw error;
      }
    };
    const assertOpen = (caller: string) => {
      if (!open) throw closed(caller);
    };
    /**
     * Adds every new pair of one declaration and unions its tags into every
     * repeated one, or changes nothing when an identity string is not Unicode
     * text or the result passes a bound.
     */
    const enroll = (
      channel: string,
      declared: Declared[],
      tags: readonly string[],
      caller: string,
      kind: "add" | "tagAdd" = "add",
    ) => {
      for (const { entry, identity } of declared)
        for (const [field, value] of Object.entries(identity))
          if (typeof value === "string" && LONE_SURROGATE.test(value))
            throw new Error(
              `${caller}: ${entry.name} identity field ${field} must be Unicode text, without a lone surrogate`,
            );
      const fresh = new Map<string, { intent: AddIntent; bytes: number }>();
      let added = 0;
      let more = 0;
      const additions: ChannelIntent[] = [];
      for (const { entry, identity } of declared) {
        const canonical = entry.canonical(identity);
        const key = canonicalJson([channel, entry.name, canonical]);
        const prior = fresh.get(key) ?? pairs.get(key);
        let merged = tags;
        let contribution = tags;
        if (prior) {
          const held = prior.intent.tags;
          const extra = tags.filter((tag) => !held.includes(tag));
          if (extra.length === 0) continue;
          contribution = extra;
          merged = Object.freeze([...held, ...extra]);
        } else added++;
        // A repeated pair keeps the record as first declared.
        const intent = prior
          ? Object.freeze({ ...prior.intent, tags: merged })
          : addIntent(channel, entry.name, identity, merged);
        const size = enrollmentBytes(
          addIntent(channel, entry.name, canonical, merged),
        );
        more += size - (prior?.bytes ?? 0);
        fresh.set(key, { intent, bytes: size });
        additions.push(
          Object.freeze({
            ...addIntent(channel, entry.name, identity, contribution),
            kind,
          }),
        );
        if (pairs.size + added > LOAD_ENROLLMENT_PAIRS)
          throw new EnrollmentOverflow(
            `${caller}: the Load page enrolls more than ${LOAD_ENROLLMENT_PAIRS} Channel/record pairs`,
          );
        if (bytes + more > LOAD_ENROLLMENT_BYTES)
          throw new EnrollmentOverflow(
            `${caller}: the Load page's enrollment encodes to more than ${LOAD_ENROLLMENT_BYTES} bytes`,
          );
      }
      for (const [key, pair] of fresh) pairs.set(key, pair);
      bytes += more;
      // Keep each validated add boundary: the union may exceed 64 tags.
      declarations.push(...additions);
    };
    const canonical = {
      guard: declare,
      entries,
      publishable,
      list,
      name: (name: unknown) => {
        channelName(name);
        if (LONE_SURROGATE.test(name as string))
          throw new Error(
            "scope: a Scope name must be Unicode text, without a lone surrogate",
          );
      },
      check: (caller: string) => declare(() => assertOpen(caller)),
      record: (intents: readonly ChannelIntent[], caller: string) =>
        declare(() => {
          if (intents.length === 0) return;
          const first = intents[0]!;
          if (first.kind !== "add" && first.kind !== "tagAdd")
            throw new Error("Load scopes only add membership and labels");
          const declared = intents.map((intent) => {
            if (
              intent.kind !== first.kind ||
              (intent.kind !== "add" && intent.kind !== "tagAdd")
            )
              throw new Error("Load scopes only add membership and labels");
            const entry = entries.find(
              (entry) => entry.name === intent.record.model,
            )!;
            return { entry, identity: intent.record.identity };
          });
          enroll(first.channel, declared, first.tags, caller, first.kind);
        }),
    };
    return Object.freeze({
      scope: (name: string) =>
        declare(() => scopeHandle(canonical, name, true) as RuntimeLoadScope),
      memberships: (): readonly ChannelIntent[] => [...declarations],
      failure: () => failed,
      close() {
        open = false;
      },
    });
  };
}

/** One Load collector over `models`, validating them first. */
export function createLoadEffects(
  models: readonly EffectModel[],
  enums: readonly EffectEnum[] = [],
  loaded?: ReadonlySet<string>,
): LoadEffectCollector {
  return loadEffectsFor(models, enums, loaded)();
}
