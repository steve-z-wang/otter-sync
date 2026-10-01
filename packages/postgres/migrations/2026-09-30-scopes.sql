-- Forward upgrade from the released v0.2 layout. Stop old writers first.
-- v0.1 installations first apply 2026-09-30-channel-members.sql once.
BEGIN;
DO $cutover$
DECLARE old_count int; new_count int; obj record; tbl text;
BEGIN
 SELECT count(*) INTO old_count FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_channel','axton_channel_member','axton_channel_tag','axton_channel_member_tag','axton_channel_log']) AND relkind='r';
 SELECT count(*) INTO new_count FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log']) AND relkind='r';
 -- Recognize complete supported storage, rather than legitimizing a
 -- partial layout because its table names happen to be present.
 IF (old_count=5 AND new_count=0) OR (old_count=0 AND new_count=5) THEN
  FOR obj IN SELECT * FROM jsonb_each('{"axton_client":["client_id","owner_id","sequence","receipt"],"axton_call":["owner_id","call_id","request","response","claim_tx"],"axton_record":["model","identity_key","stamp","id","identity"],"axton_scope":["scope","head"],"axton_scope_member":["id","scope","record_id"],"axton_scope_tag":["id","scope","name"],"axton_scope_member_tag":["member_id","tag_id"],"axton_scope_log":["scope","record_id","cursor","kind"]}'::jsonb) LOOP
   tbl:=CASE WHEN old_count=5 THEN replace(obj.key,'axton_scope','axton_channel') ELSE obj.key END;
   IF to_regclass(tbl) IS NULL THEN RAISE EXCEPTION 'incomplete framework layout: %',tbl; END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(obj.value) expected(column_name) WHERE NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND NOT attisdropped AND attname=CASE WHEN old_count=5 AND expected.column_name='scope' THEN 'channel' ELSE expected.column_name END)) THEN
    RAISE EXCEPTION 'incomplete framework columns in %',tbl;
   END IF;
  END LOOP;
 END IF;
 IF old_count=0 AND new_count=5 THEN
  FOREACH tbl IN ARRAY ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_log','axton_membership','axton_invalidation'] LOOP
   IF to_regclass(tbl) IS NULL THEN CONTINUE; END IF;
   IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='scope' AND NOT attisdropped)
      OR EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='channel' AND NOT attisdropped) THEN
    RAISE EXCEPTION 'conflicting or incomplete Scope ownership columns in %',tbl;
   END IF;
  END LOOP;
  FOREACH tbl IN ARRAY ARRAY['axton_client','axton_call','axton_record'] LOOP
   IF to_regclass(tbl) IS NULL THEN RAISE EXCEPTION 'incomplete Scope layout: %',tbl; END IF;
  END LOOP;
  RETURN;
 END IF;
 IF old_count<>5 OR new_count<>0 THEN RAISE EXCEPTION 'incomplete or conflicting framework layouts; empty databases use migration.sql'; END IF;
 FOREACH tbl IN ARRAY ARRAY['axton_client','axton_call','axton_record'] LOOP
  IF to_regclass(tbl) IS NULL THEN RAISE EXCEPTION 'incomplete old layout: %',tbl; END IF;
 END LOOP;
 FOREACH tbl IN ARRAY ARRAY['axton_channel','axton_channel_member','axton_channel_tag','axton_channel_log','axton_membership','axton_invalidation'] LOOP
  IF to_regclass(tbl) IS NULL THEN CONTINUE; END IF;
  IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='channel' AND NOT attisdropped)
     OR EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=to_regclass(tbl) AND attname='scope' AND NOT attisdropped) THEN
   RAISE EXCEPTION 'conflicting or incomplete ownership columns in %',tbl;
  END IF;
 END LOOP;
 LOCK TABLE axton_channel,axton_channel_member,axton_channel_tag,axton_channel_member_tag,axton_channel_log,axton_client,axton_call IN ACCESS EXCLUSIVE MODE;
 FOREACH tbl IN ARRAY ARRAY['axton_channel','axton_channel_member','axton_channel_tag','axton_channel_member_tag','axton_channel_log'] LOOP
  EXECUTE format('ALTER TABLE %I RENAME TO %I',tbl,replace(tbl,'channel','scope'));
 END LOOP;
 FOREACH tbl IN ARRAY ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_log','axton_membership','axton_invalidation'] LOOP
  IF to_regclass(tbl) IS NOT NULL THEN EXECUTE format('ALTER TABLE %I RENAME COLUMN channel TO scope',tbl); END IF;
 END LOOP;
 -- Constraint-backed indexes follow their constraint. Explicit indexes and
 -- identity sequences retain their ownership and counters under their new names.
 FOR obj IN SELECT conrelid::regclass AS tbl,conname FROM pg_constraint WHERE connamespace=current_schema()::regnamespace AND conrelid IN (SELECT oid FROM pg_class WHERE relname=ANY(ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log','axton_membership','axton_invalidation'])) AND conname LIKE '%channel%' LOOP
  EXECUTE format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',obj.tbl,obj.conname,replace(obj.conname,'channel','scope'));
 END LOOP;
 FOR obj IN SELECT relname,relkind FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relkind IN ('i','S') AND (relname LIKE 'axton_channel%' OR relname='axton_membership_channel') LOOP
  EXECUTE format('ALTER %s %I RENAME TO %I',CASE WHEN obj.relkind='S' THEN 'SEQUENCE' ELSE 'INDEX' END,obj.relname,replace(obj.relname,'channel','scope'));
 END LOOP;
 -- PostgreSQL keeps index attribute labels when their table column is
 -- renamed. Rebuild affected keys/indexes from their current definitions;
 -- detach and restore FK dependencies before rebuilding referenced keys.
 CREATE TEMP TABLE axton_scope_cutover_constraints ON COMMIT DROP AS
  SELECT conrelid::regclass::text AS tbl,conname,contype,pg_get_constraintdef(oid) AS def
  FROM pg_constraint WHERE conrelid IN (SELECT oid FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log','axton_membership','axton_invalidation']))
  AND (contype='f' OR (contype IN ('p','u') AND EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=conindid AND attname='channel')));
 FOR obj IN SELECT * FROM axton_scope_cutover_constraints ORDER BY CASE WHEN contype='f' THEN 0 ELSE 1 END LOOP
  EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',obj.tbl,obj.conname);
 END LOOP;
 FOR obj IN SELECT * FROM axton_scope_cutover_constraints ORDER BY CASE WHEN contype='f' THEN 1 ELSE 0 END LOOP
  EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s',obj.tbl,obj.conname,obj.def);
 END LOOP;
 FOR obj IN SELECT c.relname,pg_get_indexdef(c.oid) AS def FROM pg_class c WHERE c.relnamespace=current_schema()::regnamespace AND c.relkind='i' AND c.relname IN ('axton_scope_member_record','axton_membership_scope') AND EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=c.oid AND attname='channel') LOOP
  EXECUTE format('DROP INDEX %I',obj.relname);
  EXECUTE obj.def;
 END LOOP;
 FOR obj IN SELECT tgrelid::regclass AS tbl,tgname FROM pg_trigger WHERE NOT tgisinternal AND tgrelid IN ('axton_scope_member'::regclass,'axton_scope_tag'::regclass,'axton_scope_member_tag'::regclass) AND tgname IN ('axton_channel_member_tag_same_channel','axton_channel_member_fixed','axton_channel_tag_fixed') LOOP
  EXECUTE format('DROP TRIGGER %I ON %s',obj.tgname,obj.tbl);
 END LOOP;
 DROP FUNCTION axton_channel_member_tag_same_channel();
 DROP FUNCTION axton_channel_owner_fixed();
 EXECUTE $install$
-- Both sides of an association belong to one Scope.
CREATE OR REPLACE FUNCTION axton_scope_member_tag_same_scope() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
 member_scope text;
 tag_scope text;
BEGIN
 SELECT scope INTO member_scope FROM axton_scope_member WHERE id = NEW.member_id;
 SELECT scope INTO tag_scope FROM axton_scope_tag WHERE id = NEW.tag_id;
 IF member_scope IS DISTINCT FROM tag_scope THEN
  RAISE EXCEPTION 'member % of Scope % cannot carry tag % of Scope %',
   NEW.member_id, member_scope, NEW.tag_id, tag_scope USING ERRCODE = 'check_violation';
 END IF;
 RETURN NULL;
END
$$;
-- A member or tag never changes the Scope (or ID) it belongs to.
CREATE OR REPLACE FUNCTION axton_scope_owner_fixed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.scope IS DISTINCT FROM OLD.scope THEN
  RAISE EXCEPTION '% row % belongs to Scope % and cannot move',
   TG_TABLE_NAME, OLD.id, OLD.scope USING ERRCODE = 'check_violation';
 END IF;
 RETURN NEW;
END
$$;
DO $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'axton_scope_member_tag'::regclass AND tgname = 'axton_scope_member_tag_same_scope') THEN
  CREATE CONSTRAINT TRIGGER axton_scope_member_tag_same_scope
   AFTER INSERT OR UPDATE ON axton_scope_member_tag
   FOR EACH ROW EXECUTE FUNCTION axton_scope_member_tag_same_scope();
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'axton_scope_member'::regclass AND tgname = 'axton_scope_member_fixed') THEN
  CREATE TRIGGER axton_scope_member_fixed BEFORE UPDATE ON axton_scope_member
   FOR EACH ROW EXECUTE FUNCTION axton_scope_owner_fixed();
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'axton_scope_tag'::regclass AND tgname = 'axton_scope_tag_fixed') THEN
  CREATE TRIGGER axton_scope_tag_fixed BEFORE UPDATE ON axton_scope_tag
   FOR EACH ROW EXECUTE FUNCTION axton_scope_owner_fixed();
 END IF;
END
$$;

$install$;
END
$cutover$;
-- Convert ONLY framework envelope claims. Opaque payloads are never traversed.
CREATE FUNCTION pg_temp.axton_scope_claims(raw text) RETURNS text LANGUAGE plpgsql AS $claims$
DECLARE envelope jsonb; claim jsonb; claims jsonb := '[]'; changed boolean := false;
BEGIN
 IF raw IS NULL THEN RETURN raw; END IF;
 envelope := raw::jsonb;
 IF jsonb_typeof(envelope) <> 'object' THEN RAISE EXCEPTION 'malformed saved framework envelope'; END IF;
 IF NOT envelope ? 'memberships' THEN RETURN raw; END IF;
 IF jsonb_typeof(envelope->'memberships') <> 'array' THEN RAISE EXCEPTION 'malformed saved memberships'; END IF;
 FOR claim IN SELECT value FROM jsonb_array_elements(envelope->'memberships') LOOP
  IF jsonb_typeof(claim)<>'object' OR (claim ? 'channel')=(claim ? 'scope')
   OR jsonb_typeof(COALESCE(claim->'channel',claim->'scope'))<>'string'
   OR jsonb_typeof(claim->'model') IS DISTINCT FROM 'string'
   OR jsonb_typeof(claim->'identity') IS DISTINCT FROM 'object'
   OR jsonb_typeof(claim->'cursor') IS DISTINCT FROM 'number' THEN
   RAISE EXCEPTION 'malformed or conflicting saved membership claim';
  END IF;
  IF (claim->>'cursor')::numeric<1 OR (claim->>'cursor')::numeric>9007199254740991 OR trunc((claim->>'cursor')::numeric)<>(claim->>'cursor')::numeric THEN RAISE EXCEPTION 'invalid saved claim cursor'; END IF;
  IF claim ? 'channel' THEN claim := (claim-'channel') || jsonb_build_object('scope',claim->'channel'); changed:=true; END IF;
  claims := claims || jsonb_build_array(claim);
 END LOOP;
 IF NOT changed THEN RETURN raw; END IF;
 RETURN jsonb_set(envelope,'{memberships}',claims)::text;
END
$claims$;
UPDATE axton_client SET receipt=pg_temp.axton_scope_claims(receipt) WHERE receipt IS DISTINCT FROM pg_temp.axton_scope_claims(receipt);
UPDATE axton_call SET response=pg_temp.axton_scope_claims(response) WHERE response IS DISTINCT FROM pg_temp.axton_scope_claims(response);
DROP FUNCTION pg_temp.axton_scope_claims(text);

COMMIT;
