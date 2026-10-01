/** Every statement AXTON runs against PostgreSQL. Tables: `migration.sql`. */
export const CLAIM_INSERT =
  "INSERT INTO axton_client (client_id, owner_id) VALUES ($1,$2) ON CONFLICT (client_id) DO NOTHING";
export const CLAIM_LOCK =
  "SELECT client_id, owner_id, sequence, receipt FROM axton_client WHERE client_id=$1 FOR UPDATE";
export const SAVE_RECEIPT =
  "UPDATE axton_client SET sequence=$3, receipt=$4 WHERE client_id=$1 AND owner_id=$2 RETURNING client_id";
/**
 * The inserted row is the only fresh claim, answered without reading it back.
 * A concurrent duplicate waits here for the first transaction to commit.
 */
export const CLAIM_CALL_INSERT =
  "INSERT INTO axton_call(owner_id,call_id,request) VALUES($1,$2,$3) ON CONFLICT(owner_id,call_id) DO NOTHING RETURNING call_id, ctid::text AS tid";
/** Only when the insert returned nothing: read and lock the stored call. */
export const CLAIM_CALL_LOCK =
  "SELECT request,response FROM axton_call WHERE owner_id=$1 AND call_id=$2 FOR UPDATE";
export const SAVE_CALL =
  // The full creating transaction ID survives savepoints and prevents a later
  // transaction from completing an unexpectedly committed placeholder.
  "UPDATE axton_call SET response=$3 WHERE owner_id=$1 AND call_id=$2 AND response IS NULL AND claim_tx=pg_current_xact_id() RETURNING call_id";
/**
 * Save the fresh claim this transaction just inserted, found by its row
 * position (`$4`, the `ctid` the insert returned) instead of an index read: a
 * transaction takes no predicate lock on a row version it wrote itself. The
 * claim row normally keeps its position until the save. A moved row, or a
 * position that is no longer this transaction's unsaved claim (rolled back
 * with a savepoint, or left by an earlier transaction on a reused connection)
 * matches nothing, and `SAVE_CALL` decides.
 */
export const SAVE_CLAIMED_CALL =
  "UPDATE axton_call SET response=$3 WHERE ctid=$4::tid AND owner_id=$1 AND call_id=$2 AND response IS NULL AND claim_tx=pg_current_xact_id() RETURNING call_id";
export const HEAD = "SELECT head FROM axton_stream WHERE stream=$1";
/**
 * The Stream's retained positions after a cursor, including removals,
 * ordered before the limit. Identity comes from centralized record metadata;
 * upserts carry its current stamp from the Loader's snapshot. The outer join
 * exposes a missing record as a storage defect rather than dropping evidence.
 */
export const SCAN =
  "SELECT l.stream,l.cursor,l.kind,l.record_id::text AS record_id,r.model,r.identity_key,r.identity,CASE WHEN l.kind='upsert' THEN r.stamp END AS stamp " +
  "FROM axton_stream_log l LEFT JOIN axton_record r ON r.id=l.record_id " +
  "WHERE l.stream=$1 AND l.cursor>$2 " +
  "ORDER BY l.cursor LIMIT $3";
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
 * The current stamps of many records of one model, in request order (`$2` is
 * a JSON array of identity keys). Only a record without a stamp is inserted
 * at 1; an existing row is read, never rewritten or locked. The outer SELECT
 * reads the transaction snapshot, which cannot see this statement's own
 * inserts, hence COALESCE. The transaction keeps one snapshot (SERIALIZABLE,
 * like Repeatable Read before it), so a key whose row another transaction
 * inserted or re-stamped after the snapshot fails the INSERT with a
 * serialization error the runner retries, and a Load page over records under
 * heavy write churn can retry repeatedly before it succeeds.
 *
 * COALESCE evaluates the correlated lookup only for a key the INSERT did not
 * return, one unique-key probe per existing record, so a page of new records
 * reads nothing back. At Serializable every read leaves a predicate lock; a
 * join over the model would lock every row of it, and on a near-empty table
 * that conflicts with every other transaction's new stamp.
 */
export const READ_STAMPS =
  "WITH keys AS (SELECT k.identity_key, k.position FROM jsonb_array_elements_text($2::jsonb) WITH ORDINALITY AS k(identity_key, position)), " +
  "inserted AS (INSERT INTO axton_record(model,identity_key,stamp) SELECT $1, identity_key, 1 FROM keys ON CONFLICT(model,identity_key) DO NOTHING RETURNING identity_key, stamp) " +
  "SELECT keys.identity_key, COALESCE(inserted.stamp, (SELECT r.stamp FROM axton_record r WHERE r.model=$1 AND r.identity_key=keys.identity_key)) AS stamp FROM keys " +
  "LEFT JOIN inserted ON inserted.identity_key=keys.identity_key " +
  "ORDER BY keys.position";
/**
 * Write-lock an existing record row without changing its stamp. A no-op UPDATE
 * rather than `SELECT … FOR UPDATE`: it writes a new row version, so a
 * concurrent writer of the row fails serialization and retries instead of
 * acting on a membership snapshot taken before this commit. SERIALIZABLE
 * alone already rules out a non-serial outcome; the write conflict also
 * holds in a caller-owned transaction at Repeatable Read (`backend.publish`).
 * Never creates a row.
 */
export const LOCK_RECORD =
  "UPDATE axton_record SET stamp=stamp WHERE model=$1 AND identity_key=$2 RETURNING stamp";
/**
 * The most entries one statement carries in its JSON array parameter. Every
 * Stream statement binds at most three parameters, so PostgreSQL's 65,535
 * bind-parameter limit never applies; this bounds each statement's payload
 * and row count instead. A larger call runs several statements of the same
 * group inside the caller's transaction.
 */
export const STREAM_BATCH = 1000;
/**
 * Lock the existing rows of these Streams (`$1`, a JSON array) in exactly
 * that order, then give each a new row version with a no-op UPDATE. Creates no
 * row. The version makes any concurrent Stream writer whose snapshot predates
 * this commit fail serialization and retry, so after the lock a settlement
 * reads every committed change of its Streams, at Serializable or in a
 * caller-owned Repeatable Read transaction; Read Committed reads them anyway.
 */
export const LOCK_STREAMS =
  "UPDATE axton_stream c SET head=c.head FROM (" +
  "SELECT ch.stream FROM axton_stream ch " +
  "JOIN jsonb_array_elements_text($1::jsonb) WITH ORDINALITY AS w(stream, ord) ON w.stream=ch.stream " +
  "ORDER BY w.ord FOR NO KEY UPDATE OF ch) locked " +
  "WHERE c.stream=locked.stream RETURNING c.stream";
/** Set-based all-holder and explicit-pair lookup, deduplicated by UNION. */
export const READ_TRACKING = `
WITH records AS (SELECT v->>'model' model,v->>'identityKey' identity_key FROM jsonb_array_elements($1::jsonb) v),
pairs AS (SELECT v->>'stream' stream,v->>'model' model,v->>'identityKey' identity_key FROM jsonb_array_elements($2::jsonb) v),
selected AS (
 SELECT m.stream,r.model,r.identity_key FROM records w JOIN axton_record r USING(model,identity_key) JOIN axton_stream_member m ON m.record_id=r.id
 UNION
 SELECT m.stream,r.model,r.identity_key FROM pairs w JOIN axton_record r USING(model,identity_key) JOIN axton_stream_member m ON m.record_id=r.id AND m.stream=w.stream)
SELECT * FROM selected`;
/** Canonical ordered INSERT input acquires mixed-mode record guards in one pass. */
export const GUARD_RECORDS = `
WITH wanted AS (
 SELECT v->>'model' model,v->>'identityKey' identity_key,v->>'mode' mode,COALESCE((v->>'ordinal')::bigint,ord) ord
 FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v,ord)
), guarded AS (
 INSERT INTO axton_record(model,identity_key,stamp)
 SELECT w.model,w.identity_key,1 FROM wanted w
 WHERE w.mode <> 'lock' OR EXISTS (
  SELECT 1 FROM axton_record r WHERE r.model=w.model AND r.identity_key=w.identity_key)
 ORDER BY w.ord
 ON CONFLICT(model,identity_key) DO UPDATE SET stamp=CASE
  WHEN (SELECT w.mode FROM wanted w WHERE w.model=EXCLUDED.model AND w.identity_key=EXCLUDED.identity_key)='advance'
  THEN axton_record.stamp+1 ELSE axton_record.stamp END
 RETURNING model,identity_key,stamp
)
SELECT w.ord,g.stamp FROM wanted w LEFT JOIN guarded g USING(model,identity_key) ORDER BY w.ord`;
/**
 * One reservation per Stream: `$1` is a JSON array of `{stream, count}`
 * in canonical order. A missing Stream is inserted at `count`; an existing
 * one is locked by the upsert and advanced, so two first writers of one
 * Stream serialize on its primary key. A head that would pass the safe bound
 * is not updated and its row is not returned, which the caller refuses.
 * Answers each Stream's new head; its range ends there.
 */
export const RESERVE_HEADS =
  "INSERT INTO axton_stream(stream,head) " +
  "SELECT v->>'stream', (v->>'count')::bigint FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord) ORDER BY ord " +
  "ON CONFLICT(stream) DO UPDATE SET head=axton_stream.head+EXCLUDED.head " +
  "WHERE axton_stream.head <= 9007199254740991 - EXCLUDED.head " +
  "RETURNING stream, head";
/**
 * Write the published deltas' positions and answer every delta's position,
 * in the order of `$1`, a JSON array of `{stream, model, identityKey,
 * cursor, kind}` where an unpublished delta has no cursor. A published one
 * upserts the pair's single log row; an unpublished one answers the existing
 * row, read in one set-based pass. Answers the record ID too, `null` for a
 * record without metadata, which the caller refuses.
 */
export const WRITE_STREAM_LOG =
  "WITH d AS (SELECT v->>'stream' AS stream, v->>'model' AS model, v->>'identityKey' AS identity_key, " +
  "(v->>'cursor')::bigint AS cursor, v->>'kind' AS kind, COALESCE((v->>'ordinal')::bigint,ord) ord FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord)), " +
  "resolved AS (SELECT d.*, r.id AS record_id FROM d LEFT JOIN axton_record r ON r.model=d.model AND r.identity_key=d.identity_key), " +
  "written AS (INSERT INTO axton_stream_log(stream,record_id,cursor,kind) " +
  "SELECT stream, record_id, cursor, kind FROM resolved WHERE cursor IS NOT NULL AND record_id IS NOT NULL ORDER BY ord " +
  "ON CONFLICT(stream,record_id) DO UPDATE SET cursor=EXCLUDED.cursor, kind=EXCLUDED.kind RETURNING 1) " +
  "SELECT s.ord, s.record_id::text AS record_id, COALESCE(s.cursor, l.cursor) AS cursor, " +
  "CASE WHEN s.cursor IS NULL THEN l.kind ELSE s.kind END AS kind " +
  "FROM resolved s LEFT JOIN axton_stream_log l ON s.cursor IS NULL AND l.stream=s.stream AND l.record_id=s.record_id " +
  "ORDER BY s.ord";
/** Make each `{stream, recordId}` of `$1` a live member; an existing one is left alone. */
export const INSERT_STREAM_MEMBERS =
  "INSERT INTO axton_stream_member(stream,record_id) " +
  "SELECT v->>'stream', (v->>'recordId')::bigint FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord) ORDER BY ord " +
  "ON CONFLICT(stream,record_id) DO NOTHING";
export const savepointName = (ordinal: number): string =>
  `axton_mutation_${ordinal}`;
