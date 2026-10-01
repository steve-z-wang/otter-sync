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
export const HEAD = "SELECT head FROM axton_scope WHERE scope=$1";
/**
 * The Scope's retained positions after a cursor, including removals,
 * ordered before the limit. Identity comes from centralized record metadata;
 * upserts carry its current stamp from the Loader's snapshot. The outer join
 * exposes a missing record as a storage defect rather than dropping evidence.
 */
export const SCAN =
  "SELECT l.scope,l.cursor,l.kind,l.record_id::text AS record_id,r.model,r.identity_key,r.identity,CASE WHEN l.kind='upsert' THEN r.stamp END AS stamp " +
  "FROM axton_scope_log l LEFT JOIN axton_record r ON r.id=l.record_id " +
  "WHERE l.scope=$1 AND l.cursor>$2 " +
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
/** The Scopes this record is a live member of: a touch's recipients. */
export const MEMBERSHIPS =
  "SELECT m.scope FROM axton_scope_member m JOIN axton_record r ON r.id=m.record_id " +
  "WHERE r.model=$1 AND r.identity_key=$2 ORDER BY m.scope";
/**
 * The most entries one statement carries in its JSON array parameter. Every
 * Scope statement binds at most three parameters, so PostgreSQL's 65,535
 * bind-parameter limit never applies; this bounds each statement's payload
 * and row count instead. A larger call runs several statements of the same
 * group inside the caller's transaction.
 */
export const SCOPE_BATCH = 1000;
/**
 * Lock the existing rows of these Scopes (`$1`, a JSON array) in exactly
 * that order, then give each a new row version with a no-op UPDATE. Creates no
 * row. The version makes any concurrent Scope writer whose snapshot predates
 * this commit fail serialization and retry, so after the lock a settlement
 * reads every committed change of its Scopes, at Serializable or in a
 * caller-owned Repeatable Read transaction; Read Committed reads them anyway.
 */
export const LOCK_SCOPES =
  "UPDATE axton_scope c SET head=c.head FROM (" +
  "SELECT ch.scope FROM axton_scope ch " +
  "JOIN jsonb_array_elements_text($1::jsonb) WITH ORDINALITY AS w(scope, ord) ON w.scope=ch.scope " +
  "ORDER BY w.ord FOR NO KEY UPDATE OF ch) locked " +
  "WHERE c.scope=locked.scope RETURNING c.scope";
/**
 * The live members of Scope `$1` that `$2` names (a JSON array of
 * `{model, identityKey}`, in any order, repeats allowed) or that carry a tag
 * in `$3` (a JSON array), or all present members when `$4` is true,
 * each once with its complete tags.
 */
export const READ_SCOPE_MEMBERS =
  "WITH keys AS (SELECT k->>'model' AS model, k->>'identityKey' AS identity_key FROM jsonb_array_elements($2::jsonb) k), " +
  "selected AS (" +
  "SELECT m.id FROM keys JOIN axton_record r ON r.model=keys.model AND r.identity_key=keys.identity_key " +
  "JOIN axton_scope_member m ON m.scope=$1::text AND m.record_id=r.id " +
  "UNION SELECT mt.member_id FROM axton_scope_tag t JOIN axton_scope_member_tag mt ON mt.tag_id=t.id " +
  "WHERE t.scope=$1::text AND t.name IN (SELECT jsonb_array_elements_text($3::jsonb)) " +
  "UNION SELECT m.id FROM axton_scope_member m WHERE m.scope=$1::text AND $4::boolean) " +
  "SELECT m.id::text AS member_id, r.model, r.identity_key, " +
  "COALESCE((SELECT jsonb_agg(t.name) FROM axton_scope_member_tag mt JOIN axton_scope_tag t ON t.id=mt.tag_id WHERE mt.member_id=m.id), '[]'::jsonb) AS tags " +
  "FROM selected s JOIN axton_scope_member m ON m.id=s.id JOIN axton_record r ON r.id=m.record_id";
/**
 * One reservation per Scope: `$1` is a JSON array of `{scope, count}`
 * in canonical order. A missing Scope is inserted at `count`; an existing
 * one is locked by the upsert and advanced, so two first writers of one
 * Scope serialize on its primary key. A head that would pass the safe bound
 * is not updated and its row is not returned, which the caller refuses.
 * Answers each Scope's new head; its range ends there.
 */
export const RESERVE_HEADS =
  "INSERT INTO axton_scope(scope,head) " +
  "SELECT v->>'scope', (v->>'count')::bigint FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord) ORDER BY ord " +
  "ON CONFLICT(scope) DO UPDATE SET head=axton_scope.head+EXCLUDED.head " +
  "WHERE axton_scope.head <= 9007199254740991 - EXCLUDED.head " +
  "RETURNING scope, head";
/**
 * Write the published deltas' positions and answer every delta's position,
 * in the order of `$1`, a JSON array of `{scope, model, identityKey,
 * cursor, kind}` where an unpublished delta has no cursor. A published one
 * upserts the pair's single log row; an unpublished one answers the existing
 * row, read in one set-based pass. Answers the record ID too, `null` for a
 * record without metadata, which the caller refuses.
 */
export const WRITE_SCOPE_LOG =
  "WITH d AS (SELECT v->>'scope' AS scope, v->>'model' AS model, v->>'identityKey' AS identity_key, " +
  "(v->>'cursor')::bigint AS cursor, v->>'kind' AS kind, ord FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord)), " +
  "resolved AS (SELECT d.*, r.id AS record_id FROM d LEFT JOIN axton_record r ON r.model=d.model AND r.identity_key=d.identity_key), " +
  "written AS (INSERT INTO axton_scope_log(scope,record_id,cursor,kind) " +
  "SELECT scope, record_id, cursor, kind FROM resolved WHERE cursor IS NOT NULL AND record_id IS NOT NULL ORDER BY ord " +
  "ON CONFLICT(scope,record_id) DO UPDATE SET cursor=EXCLUDED.cursor, kind=EXCLUDED.kind RETURNING 1) " +
  "SELECT s.ord, s.record_id::text AS record_id, COALESCE(s.cursor, l.cursor) AS cursor, " +
  "CASE WHEN s.cursor IS NULL THEN l.kind ELSE s.kind END AS kind " +
  "FROM resolved s LEFT JOIN axton_scope_log l ON s.cursor IS NULL AND l.scope=s.scope AND l.record_id=s.record_id " +
  "ORDER BY s.ord";
/** Make each `{scope, recordId}` of `$1` a live member; an existing one is left alone. */
export const INSERT_SCOPE_MEMBERS =
  "INSERT INTO axton_scope_member(scope,record_id) " +
  "SELECT v->>'scope', (v->>'recordId')::bigint FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord) ORDER BY ord " +
  "ON CONFLICT(scope,record_id) DO NOTHING";
/**
 * Give each live member `{scope, recordId, tags}` of `$1` exactly `tags`:
 * missing tag rows are created, associations outside the set dropped and
 * missing ones added, set-based. An unchanged member writes nothing. Answers
 * the IDs of tags that lost an association, for collection.
 */
export const SET_MEMBER_TAGS =
  "WITH d AS (SELECT v->>'scope' AS scope, (v->>'recordId')::bigint AS record_id, v->'tags' AS tags FROM jsonb_array_elements($1::jsonb) v), " +
  "m AS (SELECT d.scope, mem.id AS member_id, d.tags FROM d JOIN axton_scope_member mem ON mem.scope=d.scope AND mem.record_id=d.record_id), " +
  "wanted AS (SELECT m.scope, m.member_id, t.name FROM m CROSS JOIN LATERAL jsonb_array_elements_text(m.tags) AS t(name)), " +
  "names AS (SELECT DISTINCT scope, name FROM wanted), " +
  "created AS (INSERT INTO axton_scope_tag(scope,name) SELECT scope, name FROM names ORDER BY scope, name " +
  "ON CONFLICT(scope,name) DO NOTHING RETURNING id, scope, name), " +
  "tags AS (SELECT id, scope, name FROM created UNION ALL " +
  "SELECT t.id, t.scope, t.name FROM axton_scope_tag t JOIN names n ON n.scope=t.scope AND n.name=t.name), " +
  "dropped AS (DELETE FROM axton_scope_member_tag mt USING m, axton_scope_tag t " +
  "WHERE mt.member_id=m.member_id AND t.id=mt.tag_id " +
  "AND NOT EXISTS (SELECT 1 FROM wanted w WHERE w.member_id=m.member_id AND w.name=t.name) RETURNING mt.tag_id), " +
  "added AS (INSERT INTO axton_scope_member_tag(member_id,tag_id) " +
  "SELECT w.member_id, tags.id FROM wanted w JOIN tags ON tags.scope=w.scope AND tags.name=w.name " +
  "ON CONFLICT DO NOTHING RETURNING 1) " +
  "SELECT DISTINCT tag_id::text AS tag_id FROM dropped";
/**
 * Delete the live members `{scope, recordId}` of `$1` and their
 * associations. Their log rows, already written, keep the record. Answers
 * the IDs of tags that lost an association, for collection.
 */
export const DELETE_SCOPE_MEMBERS =
  "WITH d AS (SELECT v->>'scope' AS scope, (v->>'recordId')::bigint AS record_id FROM jsonb_array_elements($1::jsonb) v), " +
  "gone AS (DELETE FROM axton_scope_member m USING d WHERE m.scope=d.scope AND m.record_id=d.record_id RETURNING m.id), " +
  "dropped AS (DELETE FROM axton_scope_member_tag mt USING gone WHERE mt.member_id=gone.id RETURNING mt.tag_id) " +
  "SELECT DISTINCT tag_id::text AS tag_id FROM dropped";
/** Delete the tags of `$1` (a JSON array of IDs) that no member carries any more. */
export const COLLECT_TAGS =
  "DELETE FROM axton_scope_tag t WHERE t.id IN (SELECT jsonb_array_elements_text($1::jsonb)::bigint) " +
  "AND NOT EXISTS (SELECT 1 FROM axton_scope_member_tag mt WHERE mt.tag_id=t.id)";
export const savepointName = (ordinal: number): string =>
  `axton_mutation_${ordinal}`;
