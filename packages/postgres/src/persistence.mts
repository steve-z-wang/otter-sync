import { EngineError } from "@axtonjs/server";
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

/** One reservation context per outer transaction; discarded after rollback/retry. */
type Publication05 = Map<string, number> & { transactionId?: string };
async function answer05(
  q: Query,
  r: Record<string, unknown>,
  cursors: Publication05,
): Promise<unknown> {
  switch (r.op) {
    case "claimStore": {
      await q(SQL.V05_STORE_INSERT, r.storeId, r.principal, r.stream);
      const [row] = await q(SQL.V05_STORE_LOCK, r.storeId);
      if (!row) throw new Error("Store missing");
      return {
        principal: row.principal,
        stream: row.stream,
        lastProcessedBatchId: safe(row.last_processed_batch_id),
        progress: safe(row.progress),
        currentDigest: row.current_digest,
        currentCount:
          row.current_count === null ? null : safe(row.current_count),
        lastDigest: row.last_digest,
        lastCount: row.last_count === null ? null : safe(row.last_count),
      };
    }
    case "beginBatch": {
      const rows = await q(
        SQL.V05_BATCH_BEGIN,
        r.storeId,
        r.batchId,
        r.digest,
        r.count,
      );
      if (rows.length !== 1) throw new Error("Batch admission state changed");
      await q(SQL.V05_BATCH_PRUNE, r.storeId, r.batchId);
      return null;
    }
    case "readResult": {
      const [row] = await q(
        SQL.V05_RESULT_READ,
        r.storeId,
        r.batchId,
        r.ordinal,
      );
      return row ? json(row.result) : null;
    }
    case "readResults":
      return (await q(SQL.V05_RESULTS_READ, r.storeId, r.batchId)).map((row) =>
        json(row.result),
      );
    case "saveResult": {
      const result = r.result as { mutationId: number };
      await q(
        SQL.V05_RESULT_SAVE,
        r.storeId,
        r.batchId,
        result.mutationId,
        r.ordinal,
        JSON.stringify(result),
      );
      const rows = await q(
        SQL.V05_PROGRESS_SAVE,
        r.storeId,
        r.batchId,
        r.ordinal,
        r.count,
      );
      if (rows.length !== 1) throw new Error("Batch progress state changed");
      return null;
    }
    case "readTracking": {
      const rows = await q(
        SQL.READ_TRACKING.replaceAll(
          "axton_stream_member",
          "axton_stream_record",
        ).replaceAll(
          "ON m.record_id=r.id",
          "ON m.record_id=r.id AND m.kind='upsert'",
        ),
        JSON.stringify(r.records),
        JSON.stringify(r.pairs),
      );
      return rows.map((row) => ({
        stream: row.stream,
        model: row.model,
        identityKey: row.identity_key,
      }));
    }
    case "guardRecords": {
      const records = arrayOf(r.records, "guards").map((value) =>
        keyOf(value, ["mode"]),
      );
      const result = [];
      for (const record of records) {
        const rows = await q(
          record.mode === "lock" ? SQL.LOCK_RECORD : SQL.ENSURE_STAMP,
          record.model,
          record.identityKey,
        );
        result.push(rows.length ? safe(rows[0]!.stamp) : null);
      }
      return result;
    }
    case "targetPositions":
    case "readPositions": {
      const records = arrayOf(r.records, "positions").map((value) =>
        keyOf(value),
      );
      const positions = [];
      for (const record of records) {
        const [row] = await q(
          SQL.V05_POSITIONS_READ,
          r.stream,
          record.model,
          record.identityKey,
        );
        if (!row) {
          if (r.op === "readPositions")
            throw new Error("missing Stream position");
          positions.push(null);
        } else
          positions.push({
            stream: r.stream,
            model: record.model,
            identityKey: record.identityKey,
            cursor: storedStamp(row.cursor),
            kind: row.kind,
          });
      }
      return positions;
    }
    case "applyStreamMembers": {
      // Object handles (PoolClient/ORM wrappers) may survive COMMIT and be reused.
      // Reservations are owned by the actual transaction, never by that object.
      const [transaction] = await q(SQL.V05_TRANSACTION_ID);
      if (!transaction) throw new Error("publication transaction missing");
      const transactionId = String(transaction.transaction_id);
      if (cursors.transactionId !== transactionId) {
        cursors.clear();
        cursors.transactionId = transactionId;
      }
      const deltas = arrayOf(r.deltas, "deltas").map((value) =>
        keyOf(value, ["identity", "stream", "publish"]),
      );
      unique(
        deltas as { model: string; identityKey: string; stream?: string }[],
        "deltas",
      );
      const streams = [
        ...new Set(
          deltas.filter((d) => d.publish).map((d) => streamName(d.stream)),
        ),
      ].sort(byteOrder);
      for (const stream of streams) {
        if (cursors.has(stream)) continue;
        const [row] = await q(
          SQL.RESERVE_HEADS,
          JSON.stringify([{ stream, count: 1 }]),
        );
        if (!row) throw new Error(`Stream ${stream} head counter overflow`);
        cursors.set(stream, storedStamp(row.head));
      }
      const positions = [];
      for (const d of deltas) {
        const stream = streamName(d.stream);
        const [record] = await q(SQL.V05_RECORD_ID, d.model, d.identityKey);
        if (!record) throw new Error("missing canonical record metadata");
        const rows = d.publish
          ? await q(
              SQL.V05_POSITION_WRITE,
              stream,
              record.id,
              cursors.get(stream),
            )
          : await q(SQL.V05_POSITION_READ, stream, record.id);
        const row = rows[0];
        if (!row || row.kind !== "upsert")
          throw new Error("unpublished pair has no live position");
        positions.push({
          stream,
          model: d.model,
          identityKey: d.identityKey,
          cursor: storedStamp(row.cursor),
          kind: "upsert",
        });
      }
      return positions;
    }
    default:
      throw new Error(
        `unsupported protocol05 persistence operation ${String(r.op)}`,
      );
  }
}

/**
 * Answer the persistence half of the host contract through a driver, inside
 * the transaction the driver's runner opened. `handle` and `load` never reach
 * here; an operation added to the contract without an arm is a compile error.
 */
export async function answer<Tx>(
  driver: PostgresDriver<Tx>,
  tx: Tx,
  r: HostRequest,
  publication05: Publication05 = new Map(),
): Promise<unknown> {
  const q = (sql: string, ...params: unknown[]) =>
    driver.query(tx, sql, params);
  switch (r.op) {
    case "protocol05":
      return answer05(q, r.request, publication05);
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
      const rows = await q(SQL.CLAIM_CALL_LOCK, r.owner, r.callId);
      if (rows.length !== 1) throw new Error("Failed to lock call");
      const row = rows[0]!;
      if (inserted.length === 0 && row.response === null)
        throw new Error("Call has incomplete stored response");
      const claimed: ClaimedCall = {
        fresh: inserted.length === 1,
        request: String(row.request),
        response: row.response === null ? null : String(row.response),
      };
      return claimed;
    }
    case "saveCall": {
      const rows = await q(SQL.SAVE_CALL, r.owner, r.callId, r.response);
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
    case "readCall": {
      fieldsOf(r, ["op", "owner", "callId"], "readCall");
      const row = (
        await q(
          "SELECT response FROM axton_call WHERE owner_id=$1 AND call_id=$2",
          r.owner,
          r.callId,
        )
      )[0];
      return row?.response ?? null;
    }
    case "createManifest": {
      fieldsOf(
        r,
        [
          "op",
          "owner",
          "manifestId",
          "context",
          "start",
          "models",
          "selected",
          "held",
          "budget",
        ],
        "createManifest",
      );
      const selected = strings(r.selected, "manifest Model selection");
      const held = arrayOf(r.held, "held manifest keys").map((v) => keyOf(v));
      unique(held, "held manifest keys");
      if (held.length > Math.min(storedStamp(r.budget), 100000))
        throw new EngineError("manifest.capacity", "manifest.capacity");
      for (const group of batches(held)) {
        const known = await q(
          "SELECT r.model,r.identity_key FROM axton_record r JOIN axton_stream_log l ON l.record_id=r.id AND l.stream=$1 JOIN jsonb_array_elements($2::jsonb) v ON r.model=v->>'model' AND r.identity_key=v->>'identityKey'",
          r.context.binding.stream,
          JSON.stringify(group),
        );
        if (known.length !== group.length)
          throw new EngineError(
            "manifest.identity_untracked",
            "manifest.identity_untracked",
          );
      }
      const rows = await q(
        "SELECT model,identity_key FROM (SELECT r.model,r.identity_key FROM axton_stream_member m JOIN axton_record r ON r.id=m.record_id WHERE m.stream=$1 AND r.model=ANY($2::text[]) UNION SELECT v->>'model',v->>'identityKey' FROM jsonb_array_elements($3::jsonb) v) wanted ORDER BY model COLLATE \"C\",identity_key COLLATE \"C\" LIMIT $4",
        r.context.binding.stream,
        selected,
        JSON.stringify(held),
        Math.min(storedStamp(r.budget), 100000) + 1,
      );
      if (rows.length > Math.min(r.budget, 100000))
        throw new EngineError("manifest.capacity", "manifest.capacity");
      await q(
        "INSERT INTO axton_bootstrap_manifest(owner_id,manifest_id,context,start_cursor,total,models) VALUES($1,$2,$3::jsonb,$4,$5,$6::jsonb)",
        r.owner,
        r.manifestId,
        JSON.stringify(r.context),
        safe(r.start),
        rows.length,
        JSON.stringify(r.models),
      );
      for (const group of batches(
        rows.map((row, index) => ({
          ordinal: index,
          model: row.model,
          identityKey: row.identity_key,
        })),
      ))
        await q(
          "INSERT INTO axton_bootstrap_identity(owner_id,manifest_id,ordinal,model,identity_key) SELECT $1,$2,(v->>'ordinal')::bigint,v->>'model',v->>'identityKey' FROM jsonb_array_elements($3::jsonb) v",
          r.owner,
          r.manifestId,
          JSON.stringify(group),
        );
      return {
        start: r.start,
        total: rows.length,
        models: r.models,
        from: 0,
        to: 0,
        keys: [],
      };
    }
    case "readManifest": {
      fieldsOf(
        r,
        [
          "op",
          "owner",
          "manifestId",
          "context",
          "from",
          "limit",
          "uniqueModels",
        ],
        "readManifest",
      );
      const manifest = (
        await q(
          "SELECT start_cursor,total,models FROM axton_bootstrap_manifest WHERE owner_id=$1 AND manifest_id=$2 AND context=$3::jsonb",
          r.owner,
          r.manifestId,
          JSON.stringify(r.context),
        )
      )[0];
      if (!manifest)
        throw new EngineError("manifest.invalid", "manifest.invalid");
      const from = safe(r.from),
        total = safe(manifest.total);
      if (from > total)
        throw new EngineError(
          "manifest.ordinal_invalid",
          "manifest.ordinal_invalid",
        );
      const to = Math.min(total, from + storedStamp(r.limit));
      const rows = await q(
        "SELECT ordinal,model,identity_key FROM axton_bootstrap_identity WHERE owner_id=$1 AND manifest_id=$2 AND ordinal>=$3 AND ordinal<$4 ORDER BY ordinal",
        r.owner,
        r.manifestId,
        from,
        to,
      );
      if (rows.length !== to - from)
        throw new EngineError(
          "manifest.coverage_missing",
          "manifest.coverage_missing",
        );
      await q(
        "INSERT INTO axton_bootstrap_range(owner_id,manifest_id,from_ordinal,to_ordinal) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
        r.owner,
        r.manifestId,
        from,
        to,
      );
      const roots = rows.map((row) => ({
        model: String(row.model),
        identityKey: String(row.identity_key),
      }));
      const keys = new Map(roots.map((key) => [pairId(key), key]));
      const uniqueModels = strings(
        r.uniqueModels,
        "unique Model constraints",
      ).filter((model) => roots.some((key) => key.model === model));
      // A release and a later acquisition may have no transaction identity
      // overlap. Cached prior unique values therefore require bounded current
      // authority for this same Model, independently of manifest ordinal order.
      if (uniqueModels.length) {
        const current = await q(
          "SELECT r.model,r.identity_key FROM axton_stream_member m JOIN axton_record r ON r.id=m.record_id WHERE m.stream=$1 AND r.model=ANY($2::text[]) UNION SELECT model,identity_key FROM axton_bootstrap_identity WHERE owner_id=$3 AND manifest_id=$4 AND model=ANY($2::text[]) LIMIT 10001",
          r.context.binding.stream,
          uniqueModels,
          r.owner,
          r.manifestId,
        );
        for (const row of current) {
          const key = {
            model: String(row.model),
            identityKey: String(row.identity_key),
          };
          keys.set(pairId(key), key);
        }
        if (
          keys.size > 10000 ||
          Buffer.byteLength(JSON.stringify([...keys.values()])) > 1024 * 1024
        )
          throw new EngineError(
            "constraint_group_capacity",
            "constraint_group_capacity",
          );
      }
      while (true) {
        // Historical unrelated types do not become Bootstrap-selected merely
        // through coarse transaction grouping. Same-Model unique ownership
        // transfers and post-N normal Stream publications require companions.
        const related = await q(
          "SELECT DISTINCT k->>'model' model,k->>'identityKey' identity_key FROM axton_publication_group g CROSS JOIN LATERAL jsonb_array_elements(g.keys) k JOIN axton_record rec ON rec.model=k->>'model' AND rec.identity_key=k->>'identityKey' JOIN axton_stream_log l ON l.record_id=rec.id AND l.stream=g.stream WHERE g.stream=$1 AND EXISTS (SELECT 1 FROM jsonb_array_elements(g.keys) member JOIN jsonb_array_elements($2::jsonb) wanted ON member=wanted) AND ((k->>'model')=ANY($3::text[]) OR l.cursor>$4)",
          r.context.binding.stream,
          JSON.stringify([...keys.values()]),
          uniqueModels,
          safe(manifest.start_cursor),
        );
        const before = keys.size;
        for (const row of related) {
          const key = {
            model: String(row.model),
            identityKey: String(row.identity_key),
          };
          keys.set(pairId(key), key);
        }
        if (
          keys.size > 10000 ||
          Buffer.byteLength(JSON.stringify([...keys.values()])) > 1024 * 1024
        )
          throw new EngineError(
            "constraint_group_capacity",
            "constraint_group_capacity",
          );
        if (keys.size === before) break;
      }
      const rootIds = new Set(roots.map(pairId));
      return {
        start: safe(manifest.start_cursor),
        total,
        models: manifest.models,
        from,
        to,
        keys: roots,
        companions: [...keys.values()].filter(
          (key) => !rootIds.has(pairId(key)),
        ),
      };
    }
    case "captureTail": {
      fieldsOf(
        r,
        ["op", "owner", "manifestId", "context", "head"],
        "captureTail",
      );
      const manifest = (
        await q(
          "SELECT total,tail FROM axton_bootstrap_manifest WHERE owner_id=$1 AND manifest_id=$2 AND context=$3::jsonb FOR UPDATE",
          r.owner,
          r.manifestId,
          JSON.stringify(r.context),
        )
      )[0];
      if (!manifest)
        throw new EngineError("manifest.invalid", "manifest.invalid");
      if (manifest.tail != null) return safe(manifest.tail);
      const ranges = await q(
        "SELECT from_ordinal,to_ordinal FROM axton_bootstrap_range WHERE owner_id=$1 AND manifest_id=$2 ORDER BY from_ordinal,to_ordinal",
        r.owner,
        r.manifestId,
      );
      let through = 0;
      for (const range of ranges) {
        if (safe(range.from_ordinal) > through) break;
        through = Math.max(through, safe(range.to_ordinal));
      }
      if (through !== safe(manifest.total))
        throw new EngineError(
          "bootstrap.coverage_incomplete",
          "bootstrap.coverage_incomplete",
        );
      await q(
        "UPDATE axton_bootstrap_manifest SET tail=$3 WHERE owner_id=$1 AND manifest_id=$2",
        r.owner,
        r.manifestId,
        safe(r.head),
      );
      return r.head;
    }
    case "savePublicationGroups": {
      fieldsOf(r, ["op", "positions"], "savePublicationGroups");
      const positions = arrayOf(r.positions, "positions").map((v) => {
        const p = keyOf(v, ["stream", "cursor", "kind"]);
        return {
          ...p,
          stream: streamName(p.stream),
          cursor: storedStamp(p.cursor),
        };
      });
      for (const stream of [...new Set(positions.map((p) => p.stream))].sort(
        byteOrder,
      )) {
        const selected = positions.filter((p) => p.stream === stream);
        const old = await q(
          "SELECT from_cursor,through_cursor,keys FROM axton_publication_group WHERE stream=$1 AND transaction_id=pg_current_xact_id()",
          stream,
        );
        const keys = new Map<string, { model: string; identityKey: string }>();
        for (const k of (old[0]?.keys ?? []) as {
          model: string;
          identityKey: string;
        }[])
          keys.set(pairId(k), k);
        for (const p of selected)
          keys.set(pairId({ model: p.model, identityKey: p.identityKey }), {
            model: p.model,
            identityKey: p.identityKey,
          });
        if (
          keys.size > 10000 ||
          Buffer.byteLength(JSON.stringify([...keys.values()])) > 1024 * 1024
        )
          throw new EngineError(
            "constraint_group_capacity",
            "constraint_group_capacity",
          );
        const from = Math.min(
          ...selected.map((p) => p.cursor - 1),
          old[0] ? safe(old[0].from_cursor) : Number.MAX_SAFE_INTEGER,
        );
        const through = Math.max(
          ...selected.map((p) => p.cursor),
          old[0] ? safe(old[0].through_cursor) : 0,
        );
        await q(
          "INSERT INTO axton_publication_group(stream,from_cursor,through_cursor,keys) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(stream,transaction_id) DO UPDATE SET from_cursor=EXCLUDED.from_cursor,through_cursor=EXCLUDED.through_cursor,keys=EXCLUDED.keys",
          stream,
          from,
          through,
          JSON.stringify([...keys.values()]),
        );
      }
      return null;
    }
    case "readPublicationGroups": {
      fieldsOf(r, ["op", "stream", "after", "limit"], "readPublicationGroups");
      const rows = await q(
        "SELECT from_cursor,through_cursor,keys FROM axton_publication_group WHERE stream=$1 AND through_cursor>$2 ORDER BY through_cursor LIMIT $3",
        streamName(r.stream),
        safe(r.after),
        storedStamp(r.limit),
      );
      const result = [];
      for (const row of rows) {
        const keys = new Map<string, { model: string; identityKey: string }>();
        for (const value of arrayOf(json(row.keys), "publication group keys")) {
          const key = keyOf(value);
          keys.set(pairId(key), key);
        }
        while (true) {
          const related = await q(
            "SELECT DISTINCT member AS key FROM axton_publication_group g CROSS JOIN LATERAL jsonb_array_elements(g.keys) member WHERE g.stream=$1 AND g.through_cursor>$2 AND EXISTS (SELECT 1 FROM jsonb_array_elements(g.keys) k JOIN jsonb_array_elements($3::jsonb) w ON k=w) LIMIT 10001",
            r.stream,
            safe(row.from_cursor),
            JSON.stringify([...keys.values()]),
          );
          const before = keys.size;
          // Keep full keys for validation; projecting only their fields would
          // silently normalize malformed history. One extra distinct key makes
          // truncation a capacity failure, never an incomplete successful closure.
          for (const member of related) {
            const key = keyOf(json(member.key));
            keys.set(pairId(key), key);
          }
          if (
            keys.size > 10000 ||
            Buffer.byteLength(JSON.stringify([...keys.values()])) > 1024 * 1024
          )
            throw new EngineError(
              "constraint_group_capacity",
              "constraint_group_capacity",
            );
          if (keys.size === before) break;
        }
        result.push({
          from: safe(row.from_cursor),
          through: safe(row.through_cursor),
          keys: [...keys.values()],
        });
      }
      return result;
    }
    case "readPositions": {
      fieldsOf(r, ["op", "stream", "records"], "readPositions");
      const records = arrayOf(r.records, "records").map((v) => keyOf(v));
      unique(records, "records");
      const result = [];
      for (const group of batches(records)) {
        const rows = await q(
          "WITH wanted AS (SELECT v->>'model' model,v->>'identityKey' identity_key,ord FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY x(v,ord)) SELECT w.ord,w.model,w.identity_key,l.cursor,l.kind FROM wanted w LEFT JOIN axton_record r USING(model,identity_key) LEFT JOIN axton_stream_log l ON l.record_id=r.id AND l.stream=$1 ORDER BY w.ord",
          streamName(r.stream),
          JSON.stringify(group),
        );
        if (
          rows.length !== group.length ||
          rows.some((row) => row.cursor == null)
        )
          throw new Error("missing publication group position");
        for (const row of rows)
          result.push({
            stream: r.stream,
            model: row.model,
            identityKey: row.identity_key,
            cursor: storedStamp(row.cursor),
            kind: row.kind,
          });
      }
      return result;
    }
    case "publicationFence": {
      fieldsOf(r, ["op"], "publicationFence");
      const rows = await q(SQL.PUBLICATION_FENCE);
      if (rows.length !== 1) throw new Error("publication fence missing");
      return null;
    }
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
    case "handleBootstrap":
    case "admitContext":
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
  const contexts = new WeakMap<object, Publication05>();
  const context = (tx: Tx): Publication05 => {
    if ((typeof tx !== "object" && typeof tx !== "function") || tx === null)
      throw new Error("protocol05 requires an object transaction handle");
    let value = contexts.get(tx as object);
    if (!value) {
      value = new Map();
      contexts.set(tx as object, value);
    }
    return value;
  };
  return {
    driver,
    transaction: (body) =>
      driver.transaction(async (tx) => {
        try {
          return await body(tx);
        } finally {
          if (
            (typeof tx === "object" || typeof tx === "function") &&
            tx !== null
          )
            contexts.delete(tx as object);
        }
      }),
    persistence: (tx: Tx): Persistence => ({
      call: (request) => {
        const state =
          request.op === "protocol05"
            ? context(tx)
            : (((typeof tx === "object" || typeof tx === "function") &&
              tx !== null
                ? contexts.get(tx as object)
                : undefined) ?? new Map<string, number>());
        if (request.op === "rollback") state.clear();
        return answer(driver, tx, request as HostRequest, state);
      },
    }),
  };
}
