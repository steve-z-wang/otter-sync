/** Statements for the fresh protocol-5 PostgreSQL namespace. */
export const HEAD = "SELECT head FROM axton_stream WHERE stream=$1";
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
 SELECT m.stream,r.model,r.identity_key FROM records w JOIN axton_record r USING(model,identity_key) JOIN axton_stream_record m ON m.record_id=r.id AND m.kind='upsert'
 UNION
 SELECT m.stream,r.model,r.identity_key FROM pairs w JOIN axton_record r USING(model,identity_key) JOIN axton_stream_record m ON m.record_id=r.id AND m.kind='upsert' AND m.stream=w.stream)
SELECT * FROM selected`;
export const savepointName = (ordinal: number): string =>
  `axton_mutation_${ordinal}`;

/** UPDATE creates the Serializable snapshot conflict fence; advisory locks do not. */
export const PUBLICATION_FENCE =
  "UPDATE axton_publication_fence SET id=id WHERE id=1 RETURNING id";

/** Protocol-5 Store, immutable outcome and coalesced publication persistence. */
export const V05_STORE_INSERT =
  "INSERT INTO axton_store(id,principal,stream) VALUES($1,$2,$3) ON CONFLICT(id) DO NOTHING";
export const V05_STORE_INSPECT =
  "SELECT principal,stream FROM axton_store WHERE id=$1";
export const V05_STORE_LOCK =
  "SELECT principal,stream,last_processed_batch_id,progress,current_digest,current_count,last_digest,last_count FROM axton_store WHERE id=$1 FOR UPDATE";
export const V05_BATCH_BEGIN =
  "UPDATE axton_store SET current_digest=$3,current_count=$4 WHERE id=$1 AND last_processed_batch_id=$2-1 AND current_digest IS NULL AND progress=0 RETURNING id";
export const V05_BATCH_PRUNE =
  "DELETE FROM axton_mutation_result WHERE store_id=$1 AND batch_id<$2";
export const V05_RESULT_READ =
  "SELECT result FROM axton_mutation_result WHERE store_id=$1 AND batch_id=$2 AND ordinal=$3";
export const V05_RESULTS_READ =
  "SELECT result FROM axton_mutation_result WHERE store_id=$1 AND batch_id=$2 ORDER BY ordinal";
export const V05_RESULT_SAVE =
  "INSERT INTO axton_mutation_result(store_id,batch_id,mutation_id,ordinal,result) VALUES($1,$2,$3,$4,$5::jsonb)";
export const V05_PROGRESS_SAVE =
  "UPDATE axton_store SET progress=CASE WHEN $3+1=$4 THEN 0 ELSE $3+1 END,last_processed_batch_id=CASE WHEN $3+1=$4 THEN $2 ELSE last_processed_batch_id END,last_digest=CASE WHEN $3+1=$4 THEN current_digest ELSE last_digest END,last_count=CASE WHEN $3+1=$4 THEN current_count ELSE last_count END,current_digest=CASE WHEN $3+1=$4 THEN NULL ELSE current_digest END,current_count=CASE WHEN $3+1=$4 THEN NULL ELSE current_count END WHERE id=$1 AND last_processed_batch_id=$2-1 AND progress=$3 AND current_count=$4 RETURNING id";
/** Relation identity separates namespaces; reservations follow SQL savepoints and COMMIT. */
export const V05_PUBLICATION_CURSORS =
  "CREATE TEMP TABLE IF NOT EXISTS pg_temp.axton_publication_cursor (relation oid NOT NULL,stream text NOT NULL,cursor bigint NOT NULL,PRIMARY KEY(relation,stream)) ON COMMIT DELETE ROWS";
/** Reserve only once per transaction; a rolled-back reservation is free again. */
export const V05_RESERVE_CURSOR =
  "WITH reserved AS (INSERT INTO axton_stream(stream,head) " +
  "SELECT $1::text,1 WHERE NOT EXISTS (SELECT 1 FROM pg_temp.axton_publication_cursor WHERE relation='axton_stream'::regclass::oid AND stream=$1) " +
  "ON CONFLICT(stream) DO UPDATE SET head=axton_stream.head+1 WHERE axton_stream.head<9007199254740991 RETURNING head), " +
  "saved AS (INSERT INTO pg_temp.axton_publication_cursor(relation,stream,cursor) SELECT 'axton_stream'::regclass::oid,$1,head FROM reserved RETURNING cursor) " +
  "SELECT cursor FROM saved UNION ALL SELECT cursor FROM pg_temp.axton_publication_cursor WHERE relation='axton_stream'::regclass::oid AND stream=$1";
/** Ensure and lock identities with a real MVCC write, without a content stamp. */
export const ENSURE_IDENTITY =
  "INSERT INTO axton_record(model,identity_key) VALUES($1,$2) ON CONFLICT(model,identity_key) DO UPDATE SET identity_key=axton_record.identity_key RETURNING id";
/** Absence stays absent; an existing identity receives a new row version. */
export const LOCK_IDENTITY =
  "UPDATE axton_record SET identity_key=identity_key WHERE model=$1 AND identity_key=$2 RETURNING id";
/** Homogeneous canonical guard runs: conflict updates follow input order. */
export const ENSURE_IDENTITIES = `
INSERT INTO axton_record(model,identity_key)
SELECT v->>'model',v->>'identityKey' FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY w(v,ord) ORDER BY ord
ON CONFLICT(model,identity_key) DO UPDATE SET identity_key=axton_record.identity_key
RETURNING model,identity_key`;
/** Lock existing identities in input order before giving each a new MVCC version. */
export const LOCK_IDENTITIES = `
UPDATE axton_record r SET identity_key=r.identity_key FROM (
 SELECT a.id FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY w(v,ord)
 JOIN axton_record a ON a.model=v->>'model' AND a.identity_key=v->>'identityKey'
 ORDER BY ord FOR NO KEY UPDATE OF a) locked
WHERE r.id=locked.id RETURNING r.model,r.identity_key`;
/** LEFT joins and ordinality retain absent identities, duplicates and caller order. */
export const V05_POSITIONS_READ_BATCH = `
SELECT w.ord,s.cursor,s.kind FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY w(v,ord)
LEFT JOIN axton_record r ON r.model=v->>'model' AND r.identity_key=v->>'identityKey'
LEFT JOIN axton_stream_record s ON s.record_id=r.id AND s.stream=$1 ORDER BY w.ord`;
/** Published rows use RETURNING; ordinary reads cannot see this statement's new rows. */
export const V05_APPLY_MEMBERS_BATCH = `
WITH wanted AS (
 SELECT v->>'stream' stream,v->>'model' model,v->>'identityKey' identity_key,
 (v->>'publish')::boolean publish,(v->>'cursor')::bigint cursor,ord
 FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY w(v,ord)),
records AS (SELECT w.*,r.id FROM wanted w LEFT JOIN axton_record r USING(model,identity_key)),
written AS (
 INSERT INTO axton_stream_record(stream,record_id,cursor,kind)
 SELECT stream,id,cursor,'upsert' FROM records WHERE publish AND id IS NOT NULL ORDER BY ord
 ON CONFLICT(stream,record_id) DO UPDATE SET cursor=EXCLUDED.cursor,kind='upsert'
 RETURNING stream,record_id,cursor,kind)
SELECT r.ord,r.id IS NOT NULL AS represented,
 CASE WHEN r.publish THEN p.cursor ELSE s.cursor END cursor,
 CASE WHEN r.publish THEN p.kind ELSE s.kind END kind
FROM records r LEFT JOIN written p ON p.stream=r.stream AND p.record_id=r.id
LEFT JOIN axton_stream_record s ON s.stream=r.stream AND s.record_id=r.id ORDER BY r.ord`;
export const CHECK_LAYOUT =
  "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_client','axton_call','axton_stream_member','axton_stream_log','axton_publication_group','axton_bootstrap_manifest','axton_channel','axton_scope','axton_bootstrap_identity','axton_bootstrap_range','axton_channel_member','axton_channel_log','axton_channel_tag','axton_scope_member','axton_scope_log','axton_scope_tag','axton_membership','axton_invalidation']) AND relkind='r') OR EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='axton_record' AND column_name='stamp') OR (table_name='axton_publication_fence' AND column_name='held'))) AS legacy";
