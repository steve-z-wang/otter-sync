import type {
  Acknowledged,
  Claimed,
  ClaimedCall,
  Head,
  HostRequest,
  Invalidation,
  Locked,
  MemberPosition,
  TrackingPair,
  Stamped,
  Stamps,
  Database,
  Persistence,
} from "@axtonjs/server";
import type { PostgresDriver } from "./driver.mts";
import * as SQL from "./sql.mts";

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
 * A Stream name is a string that is non-empty after JS `trim()`. The engine
 * applies its own check (`check_stream` in crates/core, Rust `trim()`) at
 * settlement; the two trims differ on a few code points such as U+FEFF and
 * U+0085, so this is a guard, not the same rule.
 */
const streamName = (stream: unknown): string => {
  if (typeof stream !== "string" || stream.trim() === "")
    throw new Error(`Invalid tracking stream ${JSON.stringify(stream)}`);
  return stream;
};

type Query = (
  sql: string,
  ...params: unknown[]
) => Promise<Record<string, unknown>[]>;

/** `items` in groups of at most `SQL.STREAM_BATCH`, in order. */
const batches = <T,>(items: readonly T[]): T[][] => {
  const groups: T[][] = [];
  for (let at = 0; at < items.length; at += SQL.STREAM_BATCH)
    groups.push(items.slice(at, at + SQL.STREAM_BATCH));
  return groups;
};

/** Canonical Stream order: UTF-8 byte order, which is code point order. */
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

const keyOf = (
  value: unknown,
  extra: readonly string[] = [],
): Record<string, unknown> & { model: string; identityKey: string } => {
  const key = fieldsOf(value, ["model", "identityKey", ...extra], "record key");
  return {
    ...key,
    model: nonEmpty(key.model, "record model"),
    identityKey: nonEmpty(key.identityKey, "record identityKey"),
  };
};
const pairId = (p: { model: string; identityKey: string; stream?: string }) =>
  JSON.stringify([p.stream, p.model, p.identityKey]);
const arrayOf = (value: unknown, what: string): unknown[] => {
  if (!Array.isArray(value)) throw new Error(`${what} must be an array`);
  return value;
};
const unique = (
  values: { model: string; identityKey: string; stream?: string }[],
  what: string,
) => {
  if (new Set(values.map(pairId)).size !== values.length)
    throw new Error(`${what} repeats a key`);
};
async function lockStreams(q: Query, r: unknown): Promise<Acknowledged> {
  const request = fieldsOf(r, ["op", "streams"], "lockStreams");
  const streams = strings(request.streams, "lockStreams streams");
  if (streams.length === 0)
    throw new Error("lockStreams needs at least one Stream");
  streams.forEach(streamName);
  for (let i = 1; i < streams.length; i++)
    if (byteOrder(streams[i - 1]!, streams[i]!) >= 0)
      throw new Error("lockStreams needs canonical order");
  for (const group of batches(streams))
    await q(SQL.LOCK_STREAMS, JSON.stringify(group));
  return null;
}
async function readTracking(q: Query, r: unknown): Promise<TrackingPair[]> {
  const request = fieldsOf(r, ["op", "records", "pairs"], "readTracking");
  const records = arrayOf(request.records, "records").map((v) => keyOf(v));
  const pairs = arrayOf(request.pairs, "pairs").map((v) => {
    const p = keyOf(v, ["stream"]);
    return { ...p, stream: streamName(p.stream) };
  });
  unique(records, "records");
  unique(pairs, "pairs");
  const result = new Map<string, TrackingPair>();
  const rg = batches(records),
    pg = batches(pairs);
  for (let i = 0; i < Math.max(rg.length, pg.length); i++)
    for (const row of await q(
      SQL.READ_TRACKING,
      JSON.stringify(rg[i] ?? []),
      JSON.stringify(pg[i] ?? []),
    )) {
      const pair = {
        stream: streamName(row.stream),
        model: nonEmpty(row.model, "stored model"),
        identityKey: nonEmpty(row.identity_key, "stored key"),
      };
      result.set(pairId(pair), pair);
    }
  return [...result.values()];
}
async function guardRecords(q: Query, r: unknown): Promise<(number | null)[]> {
  const request = fieldsOf(r, ["op", "records"], "guardRecords");
  const records = arrayOf(request.records, "records").map((v) => {
    const p = keyOf(v, ["mode"]);
    if (
      typeof p.mode !== "string" ||
      !["advance", "ensure", "lock"].includes(p.mode)
    )
      throw new Error("invalid guard mode");
    return { ...p, mode: p.mode };
  });
  unique(records, "guardRecords");
  for (let i = 1; i < records.length; i++) {
    const a = records[i - 1]!,
      b = records[i]!;
    if (
      byteOrder(a.model, b.model) > 0 ||
      (a.model === b.model && byteOrder(a.identityKey, b.identityKey) >= 0)
    )
      throw new Error("guardRecords needs canonical order");
  }
  const stamps: (number | null)[] = [];
  for (const group of batches(
    records.map((record, index) => ({ ...record, ordinal: index + 1 })),
  )) {
    const rows = await q(SQL.GUARD_RECORDS, JSON.stringify(group));
    if (rows.length !== group.length)
      throw new Error("guardRecords returned wrong number of stamps");
    rows.forEach((row, i) => {
      if (Number(row.ord) !== group[i]!.ordinal)
        throw new Error("guardRecords returned wrong order");
      if (row.stamp === null && group[i]!.mode !== "lock")
        throw new Error("Only a lock guard may return null");
      stamps.push(row.stamp === null ? null : storedStamp(row.stamp));
    });
  }
  return stamps;
}
async function applyStreamMembers(
  q: Query,
  r: unknown,
): Promise<MemberPosition[]> {
  const request = fieldsOf(r, ["op", "deltas"], "applyStreamMembers");
  const deltas = arrayOf(request.deltas, "deltas").map((v) => {
    const p = keyOf(v, ["stream", "identity", "publish"]);
    if (
      typeof p.identity !== "object" ||
      p.identity === null ||
      Array.isArray(p.identity)
    )
      throw new Error("identity must be an object");
    if (typeof p.publish !== "boolean")
      throw new Error("delta publish must be boolean");
    return {
      stream: streamName(p.stream),
      model: p.model,
      identityKey: p.identityKey,
      publish: p.publish,
    };
  });
  unique(deltas, "applyStreamMembers");
  const counts = new Map<string, number>();
  for (const d of deltas)
    if (d.publish) counts.set(d.stream, (counts.get(d.stream) ?? 0) + 1);
  const next = new Map<string, number>();
  for (const group of batches(
    [...counts].sort(([a], [b]) => byteOrder(a, b)),
  )) {
    const rows = await q(
      SQL.RESERVE_HEADS,
      JSON.stringify(group.map(([stream, count]) => ({ stream, count }))),
    );
    const heads = new Map(rows.map((row) => [String(row.stream), row.head]));
    for (const [stream, count] of group) {
      if (!heads.has(stream))
        throw new Error(`Stream ${stream} head counter overflow`);
      next.set(stream, safe(heads.get(stream)) - count + 1);
    }
  }
  const positions: MemberPosition[] = [];
  for (const group of batches(deltas)) {
    const payload = group.map((d, index) => {
      const cursor = d.publish ? next.get(d.stream)! : null;
      if (cursor !== null) next.set(d.stream, cursor + 1);
      return {
        ...d,
        cursor,
        kind: "upsert",
        ordinal: positions.length + index + 1,
      };
    });
    const rows = await q(SQL.WRITE_STREAM_LOG, JSON.stringify(payload));
    if (rows.length !== group.length)
      throw new Error("Stream log returned wrong number of positions");
    rows.forEach((row, i) => {
      const d = group[i]!,
        expected = payload[i]!;
      if (
        Number(row.ord) !== expected.ordinal ||
        row.record_id == null ||
        row.kind !== "upsert" ||
        (d.publish && safe(row.cursor) !== expected.cursor)
      )
        throw new Error("Invalid Stream position or missing record metadata");
      const cursor = storedStamp(row.cursor);
      positions.push({
        stream: d.stream,
        model: d.model,
        identityKey: d.identityKey,
        cursor,
        kind: "upsert",
      });
    });
    await q(
      SQL.INSERT_STREAM_MEMBERS,
      JSON.stringify(
        rows.map((row, i) => ({
          stream: group[i]!.stream,
          recordId: String(row.record_id),
        })),
      ),
    );
  }
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
      const rows = await q(SQL.HEAD, r.stream);
      const head: Head = rows.length ? safe(rows[0]!.head) : 0;
      return head;
    }
    case "scan": {
      const rows = await q(SQL.SCAN, r.stream, BigInt(r.after), r.limit);
      const scanned: Invalidation[] = rows.map((row) => {
        if (row.kind !== "upsert" && row.kind !== "remove")
          throw new Error("Invalid stream log kind");
        if (
          row.model === null ||
          row.identity === null ||
          (row.kind === "upsert" &&
            (row.stamp === null || row.stamp === undefined))
        )
          throw new Error(
            `Record metadata missing for record ${row.record_id} on stream ${row.stream}`,
          );
        return {
          stream: String(row.stream),
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
      fieldsOf(r, ["op", "model", "identityKey"], "lockRecord");
      keyOf({ model: r.model, identityKey: r.identityKey });
      const rows = await q(SQL.LOCK_RECORD, r.model, r.identityKey);
      if (rows.length > 1)
        throw new Error(
          `Locked more than one record row for ${r.model} ${r.identityKey}`,
        );
      const locked: Locked = rows.length ? storedStamp(rows[0]!.stamp) : null;
      return locked;
    }
    case "readTracking":
      return readTracking(q, r);
    case "guardRecords":
      return guardRecords(q, r);
    case "lockStreams":
      return lockStreams(q, r);
    case "applyStreamMembers":
      return applyStreamMembers(q, r);
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
