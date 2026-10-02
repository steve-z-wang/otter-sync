-- Stop old writers and live sessions before running this forward repair.
-- Apply previous layout migrations first; keep authority-only traffic stopped
-- until this transaction commits. Reapplying finds no latest removals.
BEGIN;
DO $validate$
DECLARE tbl record; col record; relation regclass;
BEGIN
 FOR tbl IN SELECT * FROM jsonb_each('{
  "axton_client":{"client_id":"text","owner_id":"text","sequence":"bigint","receipt":"text"},
  "axton_call":{"owner_id":"text","call_id":"text","request":"text","response":"text","claim_tx":"xid8"},
  "axton_record":{"id":"bigint","model":"text","identity_key":"text","identity":"jsonb","stamp":"bigint"},
  "axton_stream":{"stream":"text","head":"bigint"},
  "axton_stream_member":{"id":"bigint","stream":"text","record_id":"bigint"},
  "axton_stream_log":{"stream":"text","record_id":"bigint","cursor":"bigint","kind":"text"}
 }'::jsonb) LOOP
  SELECT c.oid INTO relation FROM pg_class c WHERE c.relnamespace=current_schema()::regnamespace AND c.relname=tbl.key AND c.relkind='r';
  IF relation IS NULL THEN RAISE EXCEPTION 'incomplete Stream framework layout: %',tbl.key; END IF;
  FOR col IN SELECT * FROM jsonb_each_text(tbl.value) LOOP
   IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=relation AND attname=col.key AND NOT attisdropped AND atttypid=col.value::regtype AND (attnotnull OR col.key IN ('receipt','response') OR (tbl.key='axton_record' AND col.key='identity' AND attgenerated='s'))) THEN
    RAISE EXCEPTION 'unsupported or incomplete framework column %.%',tbl.key,col.key;
   END IF;
  END LOOP;
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_channel','axton_channel_member','axton_channel_tag','axton_channel_member_tag','axton_channel_log','axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log']) AND relkind='r') THEN
  RAISE EXCEPTION 'unsupported conflicting legacy framework layout';
 END IF;
END
$validate$;
LOCK TABLE axton_stream, axton_record, axton_stream_member, axton_stream_log IN EXCLUSIVE MODE;
-- Refuse malformed storage rather than losing orphaned pairs in the joins.
DO $integrity$
DECLARE tbl text; counter text; minimum text;
BEGIN
 FOR tbl,counter,minimum IN SELECT * FROM (VALUES('axton_record','stamp','>0'),('axton_stream','head','>=0'),('axton_stream_log','cursor','>0')) v LOOP
  IF NOT EXISTS(SELECT 1 FROM pg_constraint c JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=ANY(c.conkey)
   WHERE c.conrelid=tbl::regclass AND c.contype='c' AND c.convalidated AND a.attname=counter AND regexp_replace(pg_get_expr(c.conbin,c.conrelid),'[()[:space:]'']|::bigint','','g')=counter||minimum||'AND'||counter||'<=9007199254740991') THEN
   RAISE EXCEPTION 'unsupported framework counter constraints in %',tbl;
  END IF;
 END LOOP;
 IF EXISTS(SELECT 1 FROM axton_stream_member m LEFT JOIN axton_record r ON r.id=m.record_id LEFT JOIN axton_stream s USING(stream) WHERE r.id IS NULL OR s.stream IS NULL)
 OR EXISTS(SELECT 1 FROM axton_stream_log l LEFT JOIN axton_record r ON r.id=l.record_id LEFT JOIN axton_stream s USING(stream) WHERE r.id IS NULL OR s.stream IS NULL OR l.cursor>s.head OR l.cursor<1 OR l.kind NOT IN ('upsert','remove')) THEN
  RAISE EXCEPTION 'unsupported corrupt Stream tracking or positions';
 END IF;
END
$integrity$;
CREATE TEMP TABLE axton_authority_removed ON COMMIT DROP AS
 SELECT stream,record_id FROM axton_stream_log WHERE kind='remove';
CREATE TEMP TABLE axton_authority_records ON COMMIT DROP AS
 SELECT DISTINCT record_id FROM axton_authority_removed;
INSERT INTO axton_stream_member(stream,record_id)
 SELECT stream,record_id FROM axton_authority_removed ORDER BY stream COLLATE "C",record_id
 ON CONFLICT(stream,record_id) DO NOTHING;
UPDATE axton_record r SET stamp=r.stamp+1
 FROM axton_authority_records w WHERE r.id=w.record_id;
CREATE TEMP TABLE axton_authority_pairs ON COMMIT DROP AS
 SELECT m.stream,m.record_id,
 row_number() OVER(PARTITION BY m.stream ORDER BY r.model COLLATE "C",r.identity_key COLLATE "C") AS offset
 FROM axton_stream_member m JOIN axton_authority_records w USING(record_id)
 JOIN axton_record r ON r.id=m.record_id;
CREATE TEMP TABLE axton_authority_heads ON COMMIT DROP AS
 SELECT s.stream,s.head AS old_head,count(p.record_id)::bigint AS count
 FROM axton_stream s JOIN axton_authority_pairs p USING(stream)
 GROUP BY s.stream,s.head;
-- Existing CHECK constraints refuse stamp or head overflow atomically.
UPDATE axton_stream s SET head=s.head+w.count
 FROM axton_authority_heads w WHERE s.stream=w.stream;
INSERT INTO axton_stream_log(stream,record_id,cursor,kind)
 SELECT p.stream,p.record_id,h.old_head+p.offset,'upsert'
 FROM axton_authority_pairs p JOIN axton_authority_heads h USING(stream)
 ORDER BY p.stream COLLATE "C",p.offset
 ON CONFLICT(stream,record_id) DO UPDATE SET cursor=EXCLUDED.cursor,kind='upsert';
COMMIT;
