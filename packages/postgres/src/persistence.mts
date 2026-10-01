import type {
  Acknowledged,
  Claimed,
  ClaimedCall,
  Head,
  HostRequest,
  Invalidation,
  Locked,
  MemberPosition,
  MemberState,
  Memberships,
  Stamped,
  Stamps,
  Database,
  Persistence,
} from "@axtonjs/server";
import type { PostgresDriver } from "./driver.mts";
import * as SQL from "./sql.mts";

/** The bound of every stamp, cursor and head: `Number.MAX_SAFE_INTEGER`. */
const MAX_COUNTER = 9007199254740991;

const safe = (n: unknown): number => {
  const number = Number(n);
  if (!Number.isSafeInteger(number) || number < 0)
    throw new Error("Stored counter outside safe range");
  return number;
};

/** A stored stamp: a safe positive integer. */
const storedStamp = (n: unknown): number => {
  const number = Number(n);
  if (!Number.isSafeInteger(number) || number < 1)
    throw new Error("Stored stamp outside safe positive range");
  return number;
};

/**
 * A Scope name is a string that is non-empty after JS `trim()`. The engine
 * applies its own check (`check_scope` in crates/core, Rust `trim()`) at
 * settlement; the two trims differ on a few code points such as U+FEFF and
 * U+0085, so this is a guard, not the same rule.
 */
const scopeName = (scope: unknown): string => {
  if (typeof scope !== "string" || scope.trim() === "")
    throw new Error(`Invalid membership scope ${JSON.stringify(scope)}`);
  return scope;
};

/**
 * The record operations name exactly these fields: a request carrying
 * another, or an empty or non-string model or identity key, is refused before
 * any SQL runs.
 */
const MEMBERSHIP_FIELDS = {
  lockRecord: ["op", "model", "identityKey"],
  memberships: ["op", "model", "identityKey"],
} as const;
const checkMembershipRequest = (
  r: { op: keyof typeof MEMBERSHIP_FIELDS } & Record<string, unknown>,
): void => {
  const allowed: readonly string[] = MEMBERSHIP_FIELDS[r.op];
  for (const field of Object.keys(r))
    if (!allowed.includes(field))
      throw new Error(`Unknown ${r.op} field ${field}`);
  if (typeof r.model !== "string" || r.model === "")
    throw new Error(`${r.op}: model must be a non-empty string`);
  if (typeof r.identityKey !== "string" || r.identityKey === "")
    throw new Error(`${r.op}: identityKey must be a non-empty string`);
};

type Query = (
  sql: string,
  ...params: unknown[]
) => Promise<Record<string, unknown>[]>;

/** `items` in groups of at most `SQL.SCOPE_BATCH`, in order. */
const batches = <T,>(items: readonly T[]): T[][] => {
  const groups: T[][] = [];
  for (let at = 0; at < items.length; at += SQL.SCOPE_BATCH)
    groups.push(items.slice(at, at + SQL.SCOPE_BATCH));
  return groups;
};

/** Canonical Scope order: UTF-8 byte order, which is code point order. */
const byteOrder = (a: string, b: string): number => {
  const x = [...a].map((c) => c.codePointAt(0)!);
  const y = [...b].map((c) => c.codePointAt(0)!);
  for (let i = 0; i < Math.min(x.length, y.length); i++)
    if (x[i] !== y[i]) return x[i]! - y[i]!;
  return x.length - y.length;
};

/** A request object naming exactly `fields`; another field is refused before any SQL runs. */
const fieldsOf = (
  value: unknown,
  fields: readonly string[],
  what: string,
): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new Error(`${what} must be an object`);
  for (const field of Object.keys(value))
    if (!fields.includes(field))
      throw new Error(`Unknown ${what} field ${field}`);
  return value as Record<string, unknown>;
};
const nonEmpty = (value: unknown, what: string): string => {
  if (typeof value !== "string" || value === "")
    throw new Error(`${what} must be a non-empty string`);
  return value;
};
const strings = (value: unknown, what: string): string[] => {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string"))
    throw new Error(`${what} must be an array of strings`);
  if (new Set(value).size !== value.length)
    throw new Error(`${what} repeats an entry`);
  return value as string[];
};
/** A JSON column: drivers hand back parsed values; a string is parsed. */
const json = (value: unknown): unknown =>
  typeof value === "string" ? JSON.parse(value) : value;

async function lockScopes(q: Query, r: unknown): Promise<Acknowledged> {
  const request = fieldsOf(r, ["op", "scopes"], "lockScopes");
  const scopes = strings(request.scopes, "lockScopes scopes");
  if (scopes.length === 0)
    throw new Error("lockScopes needs at least one Scope");
  scopes.forEach(scopeName);
  await q(SQL.LOCK_SCOPES, JSON.stringify(scopes));
  return null;
}

async function readScopeMembers(q: Query, r: unknown): Promise<MemberState[]> {
  const request = fieldsOf(
    r,
    ["op", "scope", "explicitKeys", "tags", "all"],
    "readScopeMembers",
  );
  const scope = scopeName(request.scope);
  if (!Array.isArray(request.explicitKeys))
    throw new Error("readScopeMembers explicitKeys must be an array");
  const keys = request.explicitKeys.map((value) => {
    const key = fieldsOf(value, ["model", "identityKey"], "member key");
    return {
      model: nonEmpty(key.model, "member key model"),
      identityKey: nonEmpty(key.identityKey, "member key identityKey"),
    };
  });
  const tags = strings(request.tags, "readScopeMembers tags");
  if (request.all !== undefined && typeof request.all !== "boolean")
    throw new Error("readScopeMembers all must be a boolean");
  const all = request.all ?? false;
  // The tags are selected once, with the first group of keys; a member
  // reached twice is answered once.
  const members = new Map<string, MemberState>();
  const groups = keys.length ? batches(keys) : tags.length || all ? [[]] : [];
  for (const [index, group] of groups.entries())
    for (const row of await q(
      SQL.READ_SCOPE_MEMBERS,
      scope,
      JSON.stringify(group),
      JSON.stringify(index === 0 ? tags : []),
      index === 0 && all,
    ))
      members.set(String(row.member_id), {
        model: String(row.model),
        identityKey: String(row.identity_key),
        tags: strings(json(row.tags), "stored member tags"),
      });
  return [...members.values()];
}

type Delta = {
  scope: string;
  model: string;
  identityKey: string;
  present: boolean;
  tags: string[];
  publish: boolean;
};
const DELTA_FIELDS = [
  "scope",
  "model",
  "identity",
  "identityKey",
  "present",
  "tags",
  "publish",
] as const;
const deltaOf = (value: unknown): Delta => {
  const delta = fieldsOf(value, DELTA_FIELDS, "member delta");
  if (typeof delta.present !== "boolean" || typeof delta.publish !== "boolean")
    throw new Error("member delta present and publish must be booleans");
  const tags = strings(delta.tags, "member delta tags");
  if (!delta.present && (tags.length > 0 || !delta.publish))
    throw new Error("an absent member delta has no tags and publishes");
  return {
    scope: scopeName(delta.scope),
    model: nonEmpty(delta.model, "member delta model"),
    identityKey: nonEmpty(delta.identityKey, "member delta identityKey"),
    present: delta.present,
    tags,
    publish: delta.publish,
  };
};

/**
 * Persist final member states, in statement groups over batches of
 * `SQL.SCOPE_BATCH` deltas: one head reservation per Scope, the log
 * (published positions written, kept ones read), live members and their
 * tags, deleted members, then unused tags. Every statement runs in the
 * caller's transaction; a failure anywhere leaves it to roll back whole.
 */
async function applyScopeMembers(
  q: Query,
  r: unknown,
): Promise<MemberPosition[]> {
  const request = fieldsOf(r, ["op", "deltas"], "applyScopeMembers");
  if (!Array.isArray(request.deltas))
    throw new Error("applyScopeMembers deltas must be an array");
  const deltas = request.deltas.map(deltaOf);
  const pairs = new Set(
    deltas.map((d) => JSON.stringify([d.scope, d.model, d.identityKey])),
  );
  if (pairs.size !== deltas.length)
    throw new Error("applyScopeMembers names a pair twice");

  // 1. One `head += N` per Scope, in canonical order.
  const counts = new Map<string, number>();
  for (const delta of deltas)
    if (delta.publish)
      counts.set(delta.scope, (counts.get(delta.scope) ?? 0) + 1);
  const next = new Map<string, number>();
  for (const group of batches(
    [...counts].sort(([a], [b]) => byteOrder(a, b)),
  )) {
    const rows = await q(
      SQL.RESERVE_HEADS,
      JSON.stringify(group.map(([scope, count]) => ({ scope, count }))),
    );
    const heads = new Map(rows.map((row) => [String(row.scope), row.head]));
    for (const [scope, count] of group) {
      if (!heads.has(scope))
        throw new Error(
          `Scope ${scope} cannot take ${count} more positions: its head would pass ${MAX_COUNTER} (counter overflow)`,
        );
      next.set(scope, safe(heads.get(scope)) - count + 1);
    }
  }

  // 2. The log: consecutive cursors per Scope in delta order.
  const positions: MemberPosition[] = [];
  const records: string[] = [];
  for (const group of batches(deltas)) {
    const payload = group.map((d) => {
      const cursor = d.publish ? next.get(d.scope)! : null;
      if (cursor !== null) next.set(d.scope, cursor + 1);
      const kind = d.present ? "upsert" : "remove";
      return {
        scope: d.scope,
        model: d.model,
        identityKey: d.identityKey,
        cursor,
        kind,
      };
    });
    const rows = await q(SQL.WRITE_SCOPE_LOG, JSON.stringify(payload));
    if (rows.length !== group.length)
      throw new Error("The Scope log answered a different number of positions");
    group.forEach((d, i) => {
      const row = rows[i]!;
      if (row.record_id === null || row.record_id === undefined)
        throw new Error(
          `Record metadata missing for ${d.model} ${d.identityKey}`,
        );
      if (!d.publish && row.kind !== "upsert")
        throw new Error(
          `${d.model} ${d.identityKey} keeps no upsert position in Scope ${d.scope}`,
        );
      records.push(String(row.record_id));
      positions.push({
        scope: d.scope,
        model: d.model,
        identityKey: d.identityKey,
        cursor: safe(row.cursor),
        kind: d.present ? "upsert" : "remove",
      });
    });
  }

  // 3. Live members with exactly their tags; 4. deleted members.
  const dropped = new Set<string>();
  const present = deltas.flatMap((d, i) =>
    d.present ? [{ scope: d.scope, recordId: records[i]!, tags: d.tags }] : [],
  );
  for (const group of batches(present)) {
    await q(
      SQL.INSERT_SCOPE_MEMBERS,
      JSON.stringify(group.map(({ scope, recordId }) => ({ scope, recordId }))),
    );
    for (const row of await q(SQL.SET_MEMBER_TAGS, JSON.stringify(group)))
      dropped.add(String(row.tag_id));
  }
  const absent = deltas.flatMap((d, i) =>
    d.present ? [] : [{ scope: d.scope, recordId: records[i]! }],
  );
  for (const group of batches(absent))
    for (const row of await q(SQL.DELETE_SCOPE_MEMBERS, JSON.stringify(group)))
      dropped.add(String(row.tag_id));

  // 5. Tags nobody carries any more; no log or record refers to a tag.
  for (const group of batches([...dropped]))
    await q(SQL.COLLECT_TAGS, JSON.stringify(group));
  return positions;
}

/**
 * The position of the last fresh claim made through each transaction object,
 * so `saveCall` can update that row without reading it (`SAVE_CLAIMED_CALL`).
 * A tool that reuses one object across transactions (a `pg` pool client) can
 * leave an entry behind; its row is not the next transaction's claim, so the
 * statement matches nothing and `SAVE_CALL` answers as before.
 */
const lastClaim = new WeakMap<
  object,
  { owner: string; callId: string; tid: string }
>();

/**
 * Answer the persistence half of the host contract through a driver, inside
 * the transaction the driver's runner opened. `handle` and `load` never reach
 * here; an operation added to the contract without an arm is a compile error.
 */
export async function answer<Tx>(
  driver: PostgresDriver<Tx>,
  tx: Tx,
  r: HostRequest,
): Promise<unknown> {
  const q = (sql: string, ...params: unknown[]) =>
    driver.query(tx, sql, params);
  switch (r.op) {
    case "claim": {
      await q(SQL.CLAIM_INSERT, r.clientId, r.owner);
      const rows = await q(SQL.CLAIM_LOCK, r.clientId);
      if (rows.length !== 1) throw new Error("Failed to lock client");
      const row = rows[0]!;
      const claimed: Claimed = {
        clientId: String(row.client_id),
        owner: String(row.owner_id),
        sequence: safe(row.sequence),
        receipt: row.receipt === null ? null : String(row.receipt),
      };
      return claimed;
    }
    case "saveReceipt": {
      const rows = await q(
        SQL.SAVE_RECEIPT,
        r.clientId,
        r.owner,
        BigInt(r.sequence),
        r.receipt,
      );
      if (rows.length !== 1) throw new Error("Receipt owner mismatch");
      const acknowledged: Acknowledged = null;
      return acknowledged;
    }
    case "claimCall": {
      const inserted = await q(
        SQL.CLAIM_CALL_INSERT,
        r.owner,
        r.callId,
        r.request,
      );
      // A fresh claim is the row this statement just inserted: its request is
      // `r.request` and it has no response. Reading it back would take a
      // predicate lock at Serializable that, on a near-empty table, covers
      // every other call's claim, so only a duplicate or a concurrent claim
      // (the insert returned nothing) reads and locks the stored row.
      if (inserted.length === 1) {
        if (typeof tx === "object" && tx !== null)
          lastClaim.set(tx, {
            owner: r.owner,
            callId: r.callId,
            tid: String(inserted[0]!.tid),
          });
        const claimed: ClaimedCall = {
          fresh: true,
          request: r.request,
          response: null,
        };
        return claimed;
      }
      const rows = await q(SQL.CLAIM_CALL_LOCK, r.owner, r.callId);
      if (rows.length !== 1) throw new Error("Failed to lock call");
      const row = rows[0]!;
      if (row.response === null)
        throw new Error("Call has incomplete stored response");
      const claimed: ClaimedCall = {
        fresh: false,
        request: String(row.request),
        response: String(row.response),
      };
      return claimed;
    }
    case "saveCall": {
      // An index read here would take the predicate lock the fresh claim
      // avoided; the claim's own position needs none.
      const claim =
        typeof tx === "object" && tx !== null ? lastClaim.get(tx) : undefined;
      let rows: Record<string, unknown>[] = [];
      if (claim && claim.owner === r.owner && claim.callId === r.callId) {
        lastClaim.delete(tx as object);
        rows = await q(
          SQL.SAVE_CLAIMED_CALL,
          r.owner,
          r.callId,
          r.response,
          claim.tid,
        );
      }
      if (rows.length === 0)
        rows = await q(SQL.SAVE_CALL, r.owner, r.callId, r.response);
      if (rows.length !== 1)
        throw new Error("Call not claimed or already completed");
      const acknowledged: Acknowledged = null;
      return acknowledged;
    }
    case "head": {
      const rows = await q(SQL.HEAD, r.scope);
      const head: Head = rows.length ? safe(rows[0]!.head) : 0;
      return head;
    }
    case "scan": {
      const rows = await q(SQL.SCAN, r.scope, BigInt(r.after), r.limit);
      const scanned: Invalidation[] = rows.map((row) => {
        if (row.kind !== "upsert" && row.kind !== "remove")
          throw new Error("Invalid scope log kind");
        if (
          row.model === null ||
          row.identity === null ||
          (row.kind === "upsert" &&
            (row.stamp === null || row.stamp === undefined))
        )
          throw new Error(
            `Record metadata missing for record ${row.record_id} on scope ${row.scope}`,
          );
        return {
          scope: String(row.scope),
          kind: row.kind,
          cursor: safe(row.cursor),
          model: String(row.model),
          identityKey: String(row.identity_key),
          identity: json(row.identity) as Record<string, unknown>,
          ...(row.kind === "upsert" ? { stamp: safe(row.stamp) } : {}),
        };
      });
      return scanned;
    }
    case "advanceStamp": {
      const rows = await q(SQL.ADVANCE_STAMP, r.model, r.identityKey);
      const stamped: Stamped = safe(rows[0]!.stamp);
      return stamped;
    }
    case "ensureStamp": {
      const rows = await q(SQL.ENSURE_STAMP, r.model, r.identityKey);
      const stamped: Stamped = safe(rows[0]!.stamp);
      return stamped;
    }
    case "readStamps": {
      // A deterministic inconsistency is answered, never thrown: a thrown
      // error reads as an unavailable database, which the client would
      // retry forever. The engine refuses a missing position or a `null`
      // stamp as `host.invalid`, an unsaved `failed` page.
      if (
        typeof r.model !== "string" ||
        r.model === "" ||
        !Array.isArray(r.identityKeys) ||
        r.identityKeys.some((key) => typeof key !== "string" || key === "") ||
        new Set(r.identityKeys).size !== r.identityKeys.length
      )
        return [];
      const rows = await q(
        SQL.READ_STAMPS,
        r.model,
        JSON.stringify(r.identityKeys),
      );
      // Each stamp is looked up by its key, so a missing or foreign row
      // answers `null` in its position rather than someone else's stamp.
      const byKey = new Map<unknown, unknown>(
        rows.map((row) => [row.identity_key, row.stamp]),
      );
      const stamps: (number | null)[] = r.identityKeys.map((key) => {
        const stamp = Number(byKey.get(key));
        return byKey.has(key) && Number.isSafeInteger(stamp) && stamp >= 1
          ? stamp
          : null;
      });
      return stamps as Stamps;
    }
    case "lockRecord": {
      checkMembershipRequest(r);
      const rows = await q(SQL.LOCK_RECORD, r.model, r.identityKey);
      if (rows.length > 1)
        throw new Error(
          `Locked more than one record row for ${r.model} ${r.identityKey}`,
        );
      const locked: Locked = rows.length ? storedStamp(rows[0]!.stamp) : null;
      return locked;
    }
    case "memberships": {
      checkMembershipRequest(r);
      const rows = await q(SQL.MEMBERSHIPS, r.model, r.identityKey);
      const memberships: Memberships = rows.map((row) => scopeName(row.scope));
      if (new Set(memberships).size !== memberships.length)
        throw new Error(
          `Duplicate membership scope for ${r.model} ${r.identityKey}`,
        );
      return memberships;
    }
    case "lockScopes":
      return lockScopes(q, r);
    case "readScopeMembers":
      return readScopeMembers(q, r);
    case "applyScopeMembers":
      return applyScopeMembers(q, r);
    case "savepoint":
    case "rollback":
    case "release": {
      if (!Number.isSafeInteger(r.ordinal) || r.ordinal < 1)
        throw new Error("Invalid savepoint ordinal");
      const name = SQL.savepointName(r.ordinal);
      const command =
        r.op === "savepoint"
          ? "SAVEPOINT"
          : r.op === "rollback"
            ? "ROLLBACK TO SAVEPOINT"
            : "RELEASE SAVEPOINT";
      await q(`${command} ${name}`);
      const acknowledged: Acknowledged = null;
      return acknowledged;
    }
    case "handle":
    case "handleAction":
    case "handleLoad":
    case "load":
      break;
    default: {
      const unreachable: never = r;
      void unreachable;
    }
  }
  throw new Error(
    `Unsupported persistence operation ${(r as { op: string }).op}`,
  );
}

/**
 * The `database` option of `createBackend`, built on any driver: the driver's
 * transaction runner plus a persistence bound to each transaction.
 */
export function persistence<Tx>(
  driver: PostgresDriver<Tx>,
): Database<Tx> & { driver: PostgresDriver<Tx> } {
  return {
    driver,
    transaction: (body) => driver.transaction(body),
    persistence: (tx: Tx): Persistence => ({
      call: (request) => answer(driver, tx, request as HostRequest),
    }),
  };
}
