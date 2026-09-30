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
 * The Channel's `upsert` positions after a cursor, in cursor order, each with
 * its record's identity and *current* stamp, read in the snapshot the loader
 * reads. The kind filters before ORDER BY and LIMIT, so a `remove` position
 * never fills a page (their delivery is not implemented yet). A record with no
 * metadata is a storage defect: that join is outer so it is reported, never
 * dropped as a missing row.
 */
export const SCAN =
  "SELECT l.channel,l.cursor,l.record_id::text AS record_id,r.model,r.identity_key,r.identity,r.stamp " +
  "FROM axton_channel_log l LEFT JOIN axton_record r ON r.id=l.record_id " +
  "WHERE l.channel=$1 AND l.cursor>$2 AND l.kind='upsert' " +
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
 */
export const READ_STAMPS =
  "WITH keys AS (SELECT k.identity_key, k.position FROM jsonb_array_elements_text($2::jsonb) WITH ORDINALITY AS k(identity_key, position)), " +
  "inserted AS (INSERT INTO axton_record(model,identity_key,stamp) SELECT $1, identity_key, 1 FROM keys ON CONFLICT(model,identity_key) DO NOTHING RETURNING identity_key, stamp) " +
  "SELECT keys.identity_key, COALESCE(inserted.stamp, r.stamp) AS stamp FROM keys " +
  "LEFT JOIN inserted ON inserted.identity_key=keys.identity_key " +
  "LEFT JOIN axton_record r ON r.model=$1 AND r.identity_key=keys.identity_key " +
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
/** The Channels this record is a live member of: a touch's recipients. */
export const MEMBERSHIPS =
  "SELECT m.channel FROM axton_channel_member m JOIN axton_record r ON r.id=m.record_id " +
  "WHERE r.model=$1 AND r.identity_key=$2 ORDER BY m.channel";
/**
 * The most entries one statement carries in its JSON array parameter. Every
 * Channel statement binds at most three parameters, so PostgreSQL's 65,535
 * bind-parameter limit never applies; this bounds each statement's payload
 * and row count instead. A larger call runs several statements of the same
 * group inside the caller's transaction.
 */
export const CHANNEL_BATCH = 1000;
/**
 * Lock the existing rows of these Channels (`$1`, a JSON array) in exactly
 * that order, then give each a new row version with a no-op UPDATE. Creates no
 * row. The version makes any concurrent Channel writer whose snapshot predates
 * this commit fail serialization and retry, so after the lock a settlement
 * reads every committed change of its Channels, at Serializable or in a
 * caller-owned Repeatable Read transaction; Read Committed reads them anyway.
 */
export const LOCK_CHANNELS =
  "UPDATE axton_channel c SET head=c.head FROM (" +
  "SELECT ch.channel FROM axton_channel ch " +
  "JOIN jsonb_array_elements_text($1::jsonb) WITH ORDINALITY AS w(channel, ord) ON w.channel=ch.channel " +
  "ORDER BY w.ord FOR NO KEY UPDATE OF ch) locked " +
  "WHERE c.channel=locked.channel RETURNING c.channel";
/**
 * The live members of Channel `$1` that `$2` names (a JSON array of
 * `{model, identityKey}`, in any order, repeats allowed) or that carry a tag
 * in `$3` (a JSON array), each once with its complete tags.
 */
export const READ_CHANNEL_MEMBERS =
  "WITH keys AS (SELECT k->>'model' AS model, k->>'identityKey' AS identity_key FROM jsonb_array_elements($2::jsonb) k), " +
  "selected AS (" +
  "SELECT m.id FROM keys JOIN axton_record r ON r.model=keys.model AND r.identity_key=keys.identity_key " +
  "JOIN axton_channel_member m ON m.channel=$1::text AND m.record_id=r.id " +
  "UNION SELECT mt.member_id FROM axton_channel_tag t JOIN axton_channel_member_tag mt ON mt.tag_id=t.id " +
  "WHERE t.channel=$1::text AND t.name IN (SELECT jsonb_array_elements_text($3::jsonb))) " +
  "SELECT m.id::text AS member_id, r.model, r.identity_key, " +
  "COALESCE((SELECT jsonb_agg(t.name) FROM axton_channel_member_tag mt JOIN axton_channel_tag t ON t.id=mt.tag_id WHERE mt.member_id=m.id), '[]'::jsonb) AS tags " +
  "FROM selected s JOIN axton_channel_member m ON m.id=s.id JOIN axton_record r ON r.id=m.record_id";
/**
 * One reservation per Channel: `$1` is a JSON array of `{channel, count}`
 * in canonical order. A missing Channel is inserted at `count`; an existing
 * one is locked by the upsert and advanced, so two first writers of one
 * Channel serialize on its primary key. A head that would pass the safe bound
 * is not updated and its row is not returned, which the caller refuses.
 * Answers each Channel's new head; its range ends there.
 */
export const RESERVE_HEADS =
  "INSERT INTO axton_channel(channel,head) " +
  "SELECT v->>'channel', (v->>'count')::bigint FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord) ORDER BY ord " +
  "ON CONFLICT(channel) DO UPDATE SET head=axton_channel.head+EXCLUDED.head " +
  "WHERE axton_channel.head <= 9007199254740991 - EXCLUDED.head " +
  "RETURNING channel, head";
/**
 * Write the published deltas' positions and answer every delta's position,
 * in the order of `$1`, a JSON array of `{channel, model, identityKey,
 * cursor, kind}` where an unpublished delta has no cursor. A published one
 * upserts the pair's single log row; an unpublished one answers the existing
 * row, read in one set-based pass. Answers the record ID too, `null` for a
 * record without metadata, which the caller refuses.
 */
export const WRITE_CHANNEL_LOG =
  "WITH d AS (SELECT v->>'channel' AS channel, v->>'model' AS model, v->>'identityKey' AS identity_key, " +
  "(v->>'cursor')::bigint AS cursor, v->>'kind' AS kind, ord FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord)), " +
  "resolved AS (SELECT d.*, r.id AS record_id FROM d LEFT JOIN axton_record r ON r.model=d.model AND r.identity_key=d.identity_key), " +
  "written AS (INSERT INTO axton_channel_log(channel,record_id,cursor,kind) " +
  "SELECT channel, record_id, cursor, kind FROM resolved WHERE cursor IS NOT NULL AND record_id IS NOT NULL ORDER BY ord " +
  "ON CONFLICT(channel,record_id) DO UPDATE SET cursor=EXCLUDED.cursor, kind=EXCLUDED.kind RETURNING 1) " +
  "SELECT s.ord, s.record_id::text AS record_id, COALESCE(s.cursor, l.cursor) AS cursor, " +
  "CASE WHEN s.cursor IS NULL THEN l.kind ELSE s.kind END AS kind " +
  "FROM resolved s LEFT JOIN axton_channel_log l ON s.cursor IS NULL AND l.channel=s.channel AND l.record_id=s.record_id " +
  "ORDER BY s.ord";
/** Make each `{channel, recordId}` of `$1` a live member; an existing one is left alone. */
export const INSERT_CHANNEL_MEMBERS =
  "INSERT INTO axton_channel_member(channel,record_id) " +
  "SELECT v->>'channel', (v->>'recordId')::bigint FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY AS x(v, ord) ORDER BY ord " +
  "ON CONFLICT(channel,record_id) DO NOTHING";
/**
 * Give each live member `{channel, recordId, tags}` of `$1` exactly `tags`:
 * missing tag rows are created, associations outside the set dropped and
 * missing ones added, set-based. An unchanged member writes nothing. Answers
 * the IDs of tags that lost an association, for collection.
 */
export const SET_MEMBER_TAGS =
  "WITH d AS (SELECT v->>'channel' AS channel, (v->>'recordId')::bigint AS record_id, v->'tags' AS tags FROM jsonb_array_elements($1::jsonb) v), " +
  "m AS (SELECT d.channel, mem.id AS member_id, d.tags FROM d JOIN axton_channel_member mem ON mem.channel=d.channel AND mem.record_id=d.record_id), " +
  "wanted AS (SELECT m.channel, m.member_id, t.name FROM m CROSS JOIN LATERAL jsonb_array_elements_text(m.tags) AS t(name)), " +
  "names AS (SELECT DISTINCT channel, name FROM wanted), " +
  "created AS (INSERT INTO axton_channel_tag(channel,name) SELECT channel, name FROM names ORDER BY channel, name " +
  "ON CONFLICT(channel,name) DO NOTHING RETURNING id, channel, name), " +
  "tags AS (SELECT id, channel, name FROM created UNION ALL " +
  "SELECT t.id, t.channel, t.name FROM axton_channel_tag t JOIN names n ON n.channel=t.channel AND n.name=t.name), " +
  "dropped AS (DELETE FROM axton_channel_member_tag mt USING m, axton_channel_tag t " +
  "WHERE mt.member_id=m.member_id AND t.id=mt.tag_id " +
  "AND NOT EXISTS (SELECT 1 FROM wanted w WHERE w.member_id=m.member_id AND w.name=t.name) RETURNING mt.tag_id), " +
  "added AS (INSERT INTO axton_channel_member_tag(member_id,tag_id) " +
  "SELECT w.member_id, tags.id FROM wanted w JOIN tags ON tags.channel=w.channel AND tags.name=w.name " +
  "ON CONFLICT DO NOTHING RETURNING 1) " +
  "SELECT DISTINCT tag_id::text AS tag_id FROM dropped";
/**
 * Delete the live members `{channel, recordId}` of `$1` and their
 * associations. Their log rows, already written, keep the record. Answers
 * the IDs of tags that lost an association, for collection.
 */
export const DELETE_CHANNEL_MEMBERS =
  "WITH d AS (SELECT v->>'channel' AS channel, (v->>'recordId')::bigint AS record_id FROM jsonb_array_elements($1::jsonb) v), " +
  "gone AS (DELETE FROM axton_channel_member m USING d WHERE m.channel=d.channel AND m.record_id=d.record_id RETURNING m.id), " +
  "dropped AS (DELETE FROM axton_channel_member_tag mt USING gone WHERE mt.member_id=gone.id RETURNING mt.tag_id) " +
  "SELECT DISTINCT tag_id::text AS tag_id FROM dropped";
/** Delete the tags of `$1` (a JSON array of IDs) that no member carries any more. */
export const COLLECT_TAGS =
  "DELETE FROM axton_channel_tag t WHERE t.id IN (SELECT jsonb_array_elements_text($1::jsonb)::bigint) " +
  "AND NOT EXISTS (SELECT 1 FROM axton_channel_member_tag mt WHERE mt.tag_id=t.id)";
export const savepointName = (ordinal: number): string =>
  `axton_mutation_${ordinal}`;
