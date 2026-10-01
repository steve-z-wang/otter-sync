-- Forward Scope-to-Stream upgrade. Stop old writers first.
-- Channel installations first apply the existing channel-members and scopes upgrades.
BEGIN;
DO $cutover$
DECLARE old_count int; new_count int; obj record; tbl text;
BEGIN
 SELECT count(*) INTO old_count FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log']) AND relkind='r';
 SELECT count(*) INTO new_count FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_stream','axton_stream_member','axton_stream_log']) AND relkind='r';
 -- Recognize complete supported storage, rather than legitimizing a
 -- partial layout because its table names happen to be present.
 IF (old_count=5 AND new_count=0) OR (old_count=0 AND new_count=3) THEN
  FOR obj IN SELECT * FROM jsonb_each('{"axton_client":["client_id","owner_id","sequence","receipt"],"axton_call":["owner_id","call_id","request","response","claim_tx"],"axton_record":["model","identity_key","stamp","id","identity"],"axton_stream":["stream","head"],"axton_stream_member":["id","stream","record_id"],"axton_stream_tag":["id","stream","name"],"axton_stream_member_tag":["member_id","tag_id"],"axton_stream_log":["stream","record_id","cursor","kind"]}'::jsonb) LOOP
   IF old_count=0 AND obj.key IN ('axton_stream_tag','axton_stream_member_tag') THEN CONTINUE; END IF;
   tbl:=CASE WHEN old_count=5 THEN replace(obj.key,'axton_stream','axton_scope') ELSE obj.key END;
   IF to_regclass(tbl) IS NULL THEN RAISE EXCEPTION 'incomplete framework layout: %',tbl; END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(obj.value) expected(column_name) WHERE NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND NOT attisdropped AND attname=CASE WHEN old_count=5 AND expected.column_name='stream' THEN 'scope' ELSE expected.column_name END)) THEN
    RAISE EXCEPTION 'incomplete framework columns in %',tbl;
   END IF;
  END LOOP;
 END IF;
 IF old_count=0 AND new_count=3 THEN
  IF to_regclass('axton_stream_tag') IS NOT NULL OR to_regclass('axton_stream_member_tag') IS NOT NULL THEN RAISE EXCEPTION 'conflicting retired tag layout'; END IF;
  FOREACH tbl IN ARRAY ARRAY['axton_stream','axton_stream_member','axton_stream_tag','axton_stream_log','axton_membership','axton_invalidation'] LOOP
   IF to_regclass(tbl) IS NULL THEN CONTINUE; END IF;
   IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='stream' AND NOT attisdropped)
      OR EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='scope' AND NOT attisdropped) THEN
    RAISE EXCEPTION 'conflicting or incomplete Stream ownership columns in %',tbl;
   END IF;
  END LOOP;
  FOREACH tbl IN ARRAY ARRAY['axton_client','axton_call','axton_record'] LOOP
   IF to_regclass(tbl) IS NULL THEN RAISE EXCEPTION 'incomplete Stream layout: %',tbl; END IF;
  END LOOP;
  RETURN;
 END IF;
 IF old_count<>5 OR new_count<>0 THEN RAISE EXCEPTION 'incomplete or conflicting framework layouts; empty databases use migration.sql'; END IF;
 FOREACH tbl IN ARRAY ARRAY['axton_client','axton_call','axton_record'] LOOP
  IF to_regclass(tbl) IS NULL THEN RAISE EXCEPTION 'incomplete old layout: %',tbl; END IF;
 END LOOP;
 FOREACH tbl IN ARRAY ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_log','axton_membership','axton_invalidation'] LOOP
  IF to_regclass(tbl) IS NULL THEN CONTINUE; END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='scope' AND NOT attisdropped)
     OR EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='stream' AND NOT attisdropped) THEN
   RAISE EXCEPTION 'conflicting or incomplete ownership columns in %',tbl;
  END IF;
 END LOOP;
 -- Retired dictionaries must contain framework data only. External foreign
 -- keys/functions remain protected by PostgreSQL's default RESTRICT drops.
 FOREACH tbl IN ARRAY ARRAY['axton_scope_tag','axton_scope_member_tag'] LOOP
  IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attnum>0 AND NOT attisdropped AND attname<>ALL(CASE WHEN tbl='axton_scope_tag' THEN ARRAY['id','scope','name'] ELSE ARRAY['member_id','tag_id'] END)) THEN
   RAISE EXCEPTION 'retired framework tag table has application columns: %',tbl;
  END IF;
 END LOOP;
 LOCK TABLE axton_scope,axton_scope_member,axton_scope_tag,axton_scope_member_tag,axton_scope_log,axton_client,axton_call IN ACCESS EXCLUSIVE MODE;
 FOREACH tbl IN ARRAY ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log'] LOOP
  EXECUTE format('ALTER TABLE %I RENAME TO %I',tbl,replace(tbl,'scope','stream'));
 END LOOP;
 FOREACH tbl IN ARRAY ARRAY['axton_stream','axton_stream_member','axton_stream_tag','axton_stream_log','axton_membership','axton_invalidation'] LOOP
  IF to_regclass(tbl) IS NOT NULL THEN EXECUTE format('ALTER TABLE %I RENAME COLUMN scope TO stream',tbl); END IF;
 END LOOP;
 -- Constraint-backed indexes follow their constraint. Explicit indexes and
 -- identity sequences retain their ownership and counters under their new names.
 FOR obj IN SELECT conrelid::regclass AS tbl,conname FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid IN (SELECT oid FROM pg_class WHERE relname=ANY(ARRAY['axton_stream','axton_stream_member','axton_stream_tag','axton_stream_member_tag','axton_stream_log','axton_membership','axton_invalidation'])) AND conname LIKE '%scope%' LOOP
  EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',obj.tbl,obj.conname,replace(obj.conname,'scope','stream'));
 END LOOP;
 FOR obj IN SELECT relname,relkind FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind IN ('i','S') AND (relname LIKE 'axton_scope%' OR relname='axton_membership_scope') LOOP
  EXECUTE format('ALTER %s %I RENAME TO %I',CASE WHEN obj.relkind='S' THEN 'SEQUENCE' ELSE 'INDEX' END,obj.relname,replace(obj.relname,'scope','stream'));
 END LOOP;
 -- PostgreSQL keeps index attribute labels when their table column is
 -- renamed. Rebuild affected keys/indexes from their current definitions;
 -- detach and restore FK dependencies before rebuilding referenced keys.
 CREATE TEMP TABLE axton_stream_cutover_constraints ON COMMIT DROP AS
  SELECT conrelid::regclass::text AS tbl,conname,contype,pg_get_constraintdef(oid) AS def
  FROM pg_constraint WHERE conrelid IN (SELECT oid FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_stream','axton_stream_member','axton_stream_tag','axton_stream_member_tag','axton_stream_log','axton_membership','axton_invalidation']))
  AND (contype='f' OR (contype IN ('p','u') AND EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=conindid AND attname='scope')));
 FOR obj IN SELECT * FROM axton_stream_cutover_constraints ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END LOOP
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',obj.tbl,obj.conname);
 END LOOP;
 FOR obj IN SELECT * FROM axton_stream_cutover_constraints ORDER BY CASE WHEN contype='f' THEN 1 ELSE 0 END LOOP
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s',obj.tbl,obj.conname,obj.def);
 END LOOP;
 FOR obj IN SELECT c.relname,pg_get_indexdef(c.oid) AS def FROM pg_class c WHERE c.relnamespace=current_schema()::regnamespace AND c.relkind='i' AND c.relname IN ('axton_stream_member_record','axton_membership_stream') AND EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=c.oid AND attname='scope') LOOP
  EXECUTE format('DROP INDEX %I',obj.relname);
  EXECUTE obj.def;
 END LOOP;
 FOR obj IN SELECT tgrelid::regclass AS tbl,tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN ('axton_stream_member'::regclass,'axton_stream_tag'::regclass,'axton_stream_member_tag'::regclass) AND tgname IN ('axton_scope_member_tag_same_scope','axton_scope_member_fixed','axton_scope_tag_fixed') LOOP
  EXECUTE format('DROP TRIGGER %I ON %s',obj.tgname,obj.tbl);
 END LOOP;
 DROP FUNCTION axton_scope_member_tag_same_scope();
 DROP FUNCTION axton_scope_owner_fixed();
 DROP TABLE axton_stream_member_tag;
 DROP TABLE axton_stream_tag;
 EXECUTE $install$
-- A tracking row never moves between Streams or changes identity.
CREATE OR REPLACE FUNCTION axton_stream_owner_fixed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.stream IS DISTINCT FROM OLD.stream OR NEW.record_id IS DISTINCT FROM OLD.record_id THEN
  RAISE EXCEPTION 'tracking row cannot move' USING ERRCODE = 'check_violation';
 END IF;
 RETURN NEW;
END
$$;
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid='axton_stream_member'::regclass AND tgname='axton_stream_member_fixed') THEN
 CREATE TRIGGER axton_stream_member_fixed BEFORE UPDATE ON axton_stream_member FOR EACH ROW EXECUTE FUNCTION axton_stream_owner_fixed();
 END IF;
END $$;

$install$;
END
$cutover$;
-- Convert ONLY framework envelope claims. Opaque payloads are never traversed.
CREATE FUNCTION pg_temp.axton_stream_claims(raw text) RETURNS text LANGUAGE plpgsql AS $claims$
DECLARE envelope jsonb; claim jsonb; claims jsonb := '[]'; changed boolean := false;
BEGIN
 IF raw IS NULL THEN RETURN raw; END IF;
 envelope := raw::jsonb;
 IF jsonb_typeof(envelope) <> 'object' THEN RAISE EXCEPTION 'malformed saved framework envelope'; END IF;
 IF NOT envelope ? 'memberships' THEN RETURN raw; END IF;
 IF jsonb_typeof(envelope->'memberships') <> 'array' THEN RAISE EXCEPTION 'malformed saved memberships'; END IF;
 FOR claim IN SELECT value FROM jsonb_array_elements(envelope->'memberships') LOOP
  IF jsonb_typeof(claim)<>'object' OR (claim ? 'scope')=(claim ? 'stream')
   OR jsonb_typeof(COALESCE(claim->'scope',claim->'stream'))<>'string'
   OR jsonb_typeof(claim->'model') IS DISTINCT FROM 'string'
   OR jsonb_typeof(claim->'identity') IS DISTINCT FROM 'object'
   OR jsonb_typeof(claim->'cursor') IS DISTINCT FROM 'number' THEN
   RAISE EXCEPTION 'malformed or conflicting saved membership claim';
  END IF;
  IF (claim->>'cursor')::numeric<1 OR (claim->>'cursor')::numeric>9007199254740991 OR trunc((claim->>'cursor')::numeric)<>(claim->>'cursor')::numeric THEN RAISE EXCEPTION 'invalid saved claim cursor'; END IF;
  IF claim ? 'scope' THEN claim := (claim-'scope') || jsonb_build_object('stream',claim->'scope'); changed:=true; END IF;
  claims := claims || jsonb_build_array(claim);
 END LOOP;
 IF NOT changed THEN RETURN raw; END IF;
 RETURN jsonb_set(envelope,'{memberships}',claims)::text;
END
$claims$;
UPDATE axton_client SET receipt=pg_temp.axton_stream_claims(receipt) WHERE receipt IS DISTINCT FROM pg_temp.axton_stream_claims(receipt);
UPDATE axton_call SET response=pg_temp.axton_stream_claims(response) WHERE response IS DISTINCT FROM pg_temp.axton_stream_claims(response);
DROP FUNCTION pg_temp.axton_stream_claims(text);

COMMIT;
