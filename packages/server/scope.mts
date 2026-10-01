/** Canonical Scope handles. Validation and capture happen before the shared collector records effects. */
import type {
  ScopeIntent,
  HostRecordRef,
  ScopePredicate,
  SelectionAction,
} from "./host-contract.mts";
export type { ScopePredicate } from "./host-contract.mts";
export interface AddDeclaration {
  tag(labels: string | readonly string[]): AddDeclaration;
}
type Operands = unknown;
type Namespace<R> = ((records: Operands) => R) & {
  readonly [model: string]: (ids: Operands) => R;
};
export interface RuntimeScope {
  readonly add: Namespace<AddDeclaration>;
  readonly remove: Namespace<void>;
  tag(labels: string | readonly string[]): {
    add: Namespace<void>;
    remove: Namespace<void> & (() => void);
  };
  readonly where: ((predicate: ScopePredicate) => Selection) & {
    readonly [model: string]: (predicate: ScopePredicate) => Selection;
  };
}
interface Selection {
  remove(): void;
  tag(labels: string | readonly string[]): { add(): void; remove(): void };
}
export interface RuntimeLoadScope {
  readonly add: Namespace<AddDeclaration>;
  tag(labels: string | readonly string[]): { add: Namespace<void> };
}
export type RuntimeScopeTouch = Namespace<void>;
export interface ScopeEntry {
  name: string;
  key: string;
  scalar?: string;
  snapshot(value: unknown, caller: string): Readonly<Record<string, unknown>>;
}
export interface ScopeCollector {
  entries: readonly ScopeEntry[];
  guard<T>(body: () => T): T;
  check(caller: string): void;
  name(name: unknown): void;
  publishable(model: string, caller: string): void;
  list(
    records: unknown,
    caller: string,
  ): { entry: ScopeEntry; identity: Readonly<Record<string, unknown>> }[];
  record(intents: readonly ScopeIntent[], caller: string): void;
}
const define = (object: object, key: string, value: unknown) =>
  Object.defineProperty(object, key, {
    value,
    enumerable: true,
    configurable: true,
  });
const plain = (v: unknown): v is Record<string, unknown> =>
  v !== null &&
  typeof v === "object" &&
  !Array.isArray(v) &&
  (Object.getPrototypeOf(v) === Object.prototype ||
    Object.getPrototypeOf(v) === null);
export function scopeLabels(
  value: unknown,
  allowEmpty = false,
): readonly string[] {
  const values = typeof value === "string" ? [value] : value;
  if (!Array.isArray(values))
    throw new Error("scope: labels must be a string or array");
  const labels = new Set<string>();
  for (const item of values) {
    if (
      typeof item !== "string" ||
      item.trim() === "" ||
      /\p{Surrogate}/u.test(item) ||
      Buffer.byteLength(item, "utf8") > 256
    )
      throw new Error("scope: invalid label");
    labels.add(item);
  }
  if ((!allowEmpty && labels.size === 0) || labels.size > 64)
    throw new Error("scope: expected 1 to 64 distinct labels");
  return Object.freeze([...labels]);
}
export function scopePredicate(value: unknown): ScopePredicate {
  let nodes = 0;
  const visit = (v: unknown, depth: number): ScopePredicate => {
    if (depth > 16 || ++nodes > 128 || !plain(v) || Object.keys(v).length === 0)
      throw new Error("scope: invalid predicate");
    const result: Record<string, unknown> = {};
    for (const key of Object.keys(v)) {
      const item = v[key];
      if (key === "tags") {
        if (!plain(item) || Object.keys(item).length === 0)
          throw new Error("scope: invalid tags predicate");
        const tags: Record<string, unknown> = {};
        for (const op of Object.keys(item)) {
          const raw = item[op];
          if (
            !["all", "any", "none", "only"].includes(op) ||
            !Array.isArray(raw)
          )
            throw new Error("scope: invalid tags operator");
          const captured = [...raw];
          scopeLabels(captured, op === "only");
          tags[op] = Object.freeze(captured);
        }
        result[key] = Object.freeze(tags);
      } else if (key === "and" || key === "or") {
        if (!Array.isArray(item) || item.length === 0)
          throw new Error("scope: invalid predicate group");
        result[key] = Object.freeze(
          Array.from(item, (child) => visit(child, depth + 1)),
        );
      } else if (key === "not") result[key] = visit(item, depth + 1);
      else throw new Error("scope: unknown predicate key");
    }
    return Object.freeze(result) as ScopePredicate;
  };
  const result = visit(value, 1);
  if (Buffer.byteLength(JSON.stringify(result), "utf8") > 65536)
    throw new Error("scope: predicate exceeds 65536 bytes");
  return result;
}
function operands(
  c: ScopeCollector,
  value: unknown,
  caller: string,
  entry?: ScopeEntry,
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
  c: ScopeCollector,
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
export function scopeTouch(
  c: ScopeCollector,
  change: (record: HostRecordRef) => void,
): RuntimeScopeTouch {
  return namespace(c, "touch", (records) => {
    for (const record of records) change(record);
  });
}
export function scopeHandle(
  c: ScopeCollector,
  name: string,
  load = false,
): RuntimeScope | RuntimeLoadScope {
  c.guard(() => {
    c.check("scope");
    c.name(name);
  });
  const emit = (intents: readonly ScopeIntent[]) =>
    c.guard(() => {
      c.check("scope");
      c.record(intents, "scope");
    });
  const labels = (value: unknown) =>
    c.guard(() => {
      c.check("scope.tag");
      return scopeLabels(value);
    });
  const attach = (
    kind: "tagAdd" | "tagRemove",
    records: readonly HostRecordRef[],
    tags: readonly string[],
  ) =>
    emit(
      records.map((record) =>
        Object.freeze({ kind, scope: name, record, tags }),
      ),
    );
  const add = namespace(c, "scope.add", (records) => {
    emit(
      records.map((record) =>
        Object.freeze({
          kind: "add",
          scope: name,
          record,
          tags: Object.freeze([]),
        }),
      ),
    );
    const declaration: AddDeclaration = Object.freeze({
      tag(value: string | readonly string[]) {
        attach("tagAdd", records, labels(value));
        return declaration;
      },
    });
    return declaration;
  });
  const tag = (value: string | readonly string[]) => {
    const tags = labels(value);
    const add = namespace(c, "scope.tag.add", (records) =>
      attach("tagAdd", records, tags),
    );
    if (load) return Object.freeze({ add });
    // Build the no-argument detach overload before freezing the callable namespace.
    const remove = (...args: unknown[]) =>
      c.guard(() => {
        if (args.length === 0) {
          emit([Object.freeze({ kind: "detachTags", scope: name, tags })]);
          return;
        }
        attach("tagRemove", operands(c, args[0], "scope.tag.remove"), tags);
      });
    for (const entry of c.entries)
      define(remove, entry.key, (ids: unknown) =>
        c.guard(() =>
          attach(
            "tagRemove",
            operands(c, ids, "scope.tag.remove", entry),
            tags,
          ),
        ),
      );
    return Object.freeze({ add, remove: Object.freeze(remove) });
  };
  if (load) return Object.freeze({ add, tag }) as RuntimeLoadScope;
  const remove = namespace(c, "scope.remove", (records) =>
    emit(
      records.map((record) =>
        Object.freeze({ kind: "remove", scope: name, record }),
      ),
    ),
  );
  const selection = (value: unknown, entry?: ScopeEntry): Selection =>
    c.guard(() => {
      c.check("scope.where");
      if (entry) c.publishable(entry.name, "scope.where");
      const predicate = c.guard(() => scopePredicate(value));
      const select = (action: SelectionAction) =>
        emit([
          Object.freeze({
            kind: "select",
            scope: name,
            ...(entry ? { model: entry.name } : {}),
            predicate,
            action: Object.freeze(action),
          }),
        ]);
      return Object.freeze({
        remove() {
          select({ kind: "remove" });
        },
        tag(value: string | readonly string[]) {
          const tags = labels(value);
          return Object.freeze({
            add() {
              select({ kind: "tagAdd", tags });
            },
            remove() {
              select({ kind: "tagRemove", tags });
            },
          });
        },
      });
    });
  const where = (predicate: ScopePredicate) => selection(predicate);
  for (const entry of c.entries)
    define(where, entry.key, (predicate: ScopePredicate) =>
      selection(predicate, entry),
    );
  return Object.freeze({
    add,
    remove,
    tag,
    where: Object.freeze(where),
  }) as RuntimeScope;
}
