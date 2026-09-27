/** Every statement AXTON runs against PostgreSQL. Tables: `migration.sql`. */
export const CLAIM_INSERT =
  "INSERT INTO axton_client (client_id, owner_id) VALUES ($1,$2) ON CONFLICT (client_id) DO NOTHING";
export const CLAIM_LOCK =
  "SELECT client_id, owner_id, sequence, receipt FROM axton_client WHERE client_id=$1 FOR UPDATE";
export const SAVE_RECEIPT =
  "UPDATE axton_client SET sequence=$3, receipt=$4 WHERE client_id=$1 AND owner_id=$2 RETURNING client_id";
/** The inserted row is the only fresh claim. A concurrent duplicate waits for commit. */
export const CLAIM_CALL_INSERT =
  "INSERT INTO axton_call(owner_id,call_id,request) VALUES($1,$2,$3) ON CONFLICT(owner_id,call_id) DO NOTHING RETURNING call_id";
export const CLAIM_CALL_LOCK =
  "SELECT request,response FROM axton_call WHERE owner_id=$1 AND call_id=$2 FOR UPDATE";
export const SAVE_CALL =
  // The full creating transaction ID survives savepoints and prevents a later
  // transaction from completing an unexpectedly committed placeholder.
  "UPDATE axton_call SET response=$3 WHERE owner_id=$1 AND call_id=$2 AND response IS NULL AND claim_tx=pg_current_xact_id() RETURNING call_id";
export const HEAD = "SELECT head FROM axton_channel WHERE channel=$1";
/**
 * The Channel's retained positions whose record is still a member, in cursor
 * order. The membership join filters before ORDER BY and LIMIT, so a removed
 * record's retained row is a hole that never fills a page. The invalidation
 * keeps its own cursor (delivery progress); the stamp is the record's current
 * one, read in the same snapshot the loader reads. A record with no metadata
 * is a storage defect: that join is outer so it is reported, never dropped as
 * a missing row.
 */
export const SCAN =
  "SELECT i.channel,i.cursor,i.model,i.identity_key,i.identity,r.stamp " +
  "FROM axton_invalidation i " +
  "JOIN axton_membership m " +
  "ON m.channel=i.channel AND m.model=i.model AND m.identity_key=i.identity_key " +
  "LEFT JOIN axton_record r " +
  "ON r.model=i.model AND r.identity_key=i.identity_key " +
  "WHERE i.channel=$1 AND i.cursor>$2 " +
  "ORDER BY i.cursor LIMIT $3";
/** The upsert locks the record row, so concurrent changes never share a stamp. */
export const ADVANCE_STAMP =
  "INSERT INTO axton_record(model,identity_key,stamp) VALUES($1,$2,1) ON CONFLICT(model,identity_key) DO UPDATE SET stamp=axton_record.stamp+1 RETURNING stamp";
/**
 * Initialise at 1 only when the record has no stamp. The no-op update (rather
 * than DO NOTHING plus a SELECT) makes a row another transaction initialised
 * after our snapshot surface as a serialization failure the runner retries.
 */
export const ENSURE_STAMP =
  "INSERT INTO axton_record(model,identity_key,stamp) VALUES($1,$2,1) ON CONFLICT(model,identity_key) DO UPDATE SET stamp=axton_record.stamp RETURNING stamp";
/**
 * Write-lock an existing record row without changing its stamp. A no-op UPDATE
 * rather than `SELECT … FOR UPDATE`: it writes a new row version, so a
 * concurrent Repeatable Read writer of the row fails serialization and retries
 * instead of acting on a membership snapshot taken before this commit. Never
 * creates a row.
 */
export const LOCK_RECORD =
  "UPDATE axton_record SET stamp=stamp WHERE model=$1 AND identity_key=$2 RETURNING stamp";
export const MEMBERSHIPS =
  "SELECT channel FROM axton_membership WHERE model=$1 AND identity_key=$2 ORDER BY channel";
/** A first member creates its Channel at head zero; publication allocates the first real position. */
export const ENSURE_CHANNEL =
  "INSERT INTO axton_channel(channel,head) VALUES($1,0) ON CONFLICT(channel) DO NOTHING";
/** The record's metadata row must exist (foreign key). An existing member is left alone. */
export const INSERT_MEMBERSHIP =
  "INSERT INTO axton_membership(channel,model,identity_key) VALUES($1,$2,$3) ON CONFLICT DO NOTHING";
export const DELETE_MEMBERSHIP =
  "DELETE FROM axton_membership WHERE channel=$1 AND model=$2 AND identity_key=$3";
export const LOCK_STAMP =
  "SELECT stamp FROM axton_record WHERE model=$1 AND identity_key=$2 FOR UPDATE";
export const ADVANCE_HEAD =
  "INSERT INTO axton_channel(channel,head) VALUES($1,1) ON CONFLICT(channel) DO UPDATE SET head=axton_channel.head+1 RETURNING head";
export const UPSERT_INVALIDATION =
  "INSERT INTO axton_invalidation(channel,model,identity_key,identity,cursor,stamp) VALUES($1,$2,$3,$4::jsonb,$5,$6) ON CONFLICT(channel,model,identity_key) DO UPDATE SET identity=EXCLUDED.identity,cursor=EXCLUDED.cursor,stamp=EXCLUDED.stamp";
export const savepointName = (ordinal: number): string =>
  `axton_mutation_${ordinal}`;
