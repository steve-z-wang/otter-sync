-- Forward upgrade of a 0.1.x database (six tables) to Channel members, tags
-- and the compacted log of 0.2.0. Stop every older AXTON writer first, then
-- apply the whole file at once (psql, or one simple-protocol query): it runs
-- in one transaction and either completes or changes nothing.
--
-- 1. Give axton_record a surrogate ID and the identity decoded from its
--    canonical identity_key; a key that is not a JSON object fails.
-- 2. Create the four Channel tables and their triggers, as migration.sql does.
-- 3. Copy memberships; copy retained invalidations into log rows (`upsert`
--    for a live member, `remove` otherwise), keeping cursors and heads, after
--    checking each retained identity against the decoded one; give every
--    live member without a retained position an `upsert` above its head.
-- 4. Verify the result. axton_membership and axton_invalidation are kept
--    untouched; dropping them is a separate, later cleanup.
--
-- The copy runs only while axton_record has no `id`: a repeated run finds it,
-- copies nothing and only verifies, so rows the new runtime has written since
-- are never overwritten from the old tables.
BEGIN;
DO $$
BEGIN
 IF to_regclass('axton_record') IS NULL THEN
  RAISE EXCEPTION 'axton_record is missing: install a new database from migration.sql';
 END IF;
 IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'axton_record'::regclass AND attname = 'id' AND NOT attisdropped) THEN
  PERFORM set_config('axton.upgrade_copy', 'off', true);
 ELSE
  IF to_regclass('axton_membership') IS NULL OR to_regclass('axton_invalidation') IS NULL THEN
   RAISE EXCEPTION 'axton_membership and axton_invalidation are missing: apply the 0.1.x migration.sql first';
  END IF;
  PERFORM set_config('axton.upgrade_copy', 'on', true);
 END IF;
END
$$;

-- 1. Record IDs and identities. Adding the identity decodes every key.
ALTER TABLE axton_record ADD COLUMN IF NOT EXISTS id bigint GENERATED ALWAYS AS IDENTITY;
ALTER TABLE axton_record ADD COLUMN IF NOT EXISTS identity jsonb GENERATED ALWAYS AS (identity_key::jsonb) STORED;
DO $$
DECLARE
 fk record;
 fks text[] := '{}';
 tables regclass[] := '{}';
 names text[] := '{}';
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'axton_record'::regclass AND conname = 'axton_record_identity_object') THEN
  ALTER TABLE axton_record ADD CONSTRAINT axton_record_identity_object CHECK (jsonb_typeof(identity) = 'object');
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'axton_record'::regclass AND conname = 'axton_record_model_identity_key_key') THEN
  ALTER TABLE axton_record ADD CONSTRAINT axton_record_model_identity_key_key UNIQUE (model, identity_key);
 END IF;
 -- The primary key moves from (model, identity_key) to id. Foreign keys on
 -- the old key (axton_membership's) are re-created on the unique constraint.
 IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'axton_record'::regclass AND contype = 'p'
            AND conkey <> ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid = 'axton_record'::regclass AND attname = 'id')]) THEN
  FOR fk IN SELECT conrelid::regclass AS tbl, conname, pg_get_constraintdef(oid) AS def
            FROM pg_constraint WHERE confrelid = 'axton_record'::regclass AND contype = 'f' LOOP
   tables := tables || fk.tbl; names := names || fk.conname::text; fks := fks || fk.def;
   EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk.tbl, fk.conname);
  END LOOP;
  ALTER TABLE axton_record DROP CONSTRAINT axton_record_pkey;
  ALTER TABLE axton_record ADD CONSTRAINT axton_record_pkey PRIMARY KEY (id);
  FOR i IN 1 .. coalesce(array_length(fks, 1), 0) LOOP
   EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s', tables[i], names[i], fks[i]);
  END LOOP;
 END IF;
END
$$;

-- 2. The Channel tables and triggers, exactly as migration.sql creates them.
-- Live membership only: an absent pair has no row here and a `remove` log row.
CREATE TABLE IF NOT EXISTS axton_channel_member (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 channel text NOT NULL REFERENCES axton_channel(channel),
 record_id bigint NOT NULL REFERENCES axton_record(id),
 CONSTRAINT axton_channel_member_channel_record_id_key UNIQUE(channel,record_id)
);
CREATE INDEX IF NOT EXISTS axton_channel_member_record
 ON axton_channel_member(record_id, channel);
CREATE TABLE IF NOT EXISTS axton_channel_tag (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 channel text NOT NULL REFERENCES axton_channel(channel),
 name text NOT NULL CHECK(octet_length(name) BETWEEN 1 AND 256),
 CONSTRAINT axton_channel_tag_channel_name_key UNIQUE(channel,name)
);
CREATE TABLE IF NOT EXISTS axton_channel_member_tag (
 member_id bigint NOT NULL REFERENCES axton_channel_member(id) ON DELETE CASCADE,
 tag_id bigint NOT NULL REFERENCES axton_channel_tag(id) ON DELETE CASCADE,
 PRIMARY KEY(member_id,tag_id)
);
CREATE INDEX IF NOT EXISTS axton_channel_member_tag_tag
 ON axton_channel_member_tag(tag_id, member_id);
-- The latest deliverable state of each pair; `(channel, cursor)` orders scans.
CREATE TABLE IF NOT EXISTS axton_channel_log (
 channel text NOT NULL REFERENCES axton_channel(channel),
 record_id bigint NOT NULL REFERENCES axton_record(id),
 cursor bigint NOT NULL CHECK(cursor > 0 AND cursor <= 9007199254740991),
 kind text NOT NULL CHECK(kind IN ('upsert','remove')),
 PRIMARY KEY(channel,record_id),
 CONSTRAINT axton_channel_log_channel_cursor_key UNIQUE(channel,cursor)
);
-- Both sides of an association belong to one Channel.
CREATE OR REPLACE FUNCTION axton_channel_member_tag_same_channel() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
 member_channel text;
 tag_channel text;
BEGIN
 SELECT channel INTO member_channel FROM axton_channel_member WHERE id = NEW.member_id;
 SELECT channel INTO tag_channel FROM axton_channel_tag WHERE id = NEW.tag_id;
 IF member_channel IS DISTINCT FROM tag_channel THEN
  RAISE EXCEPTION 'member % of Channel % cannot carry tag % of Channel %',
   NEW.member_id, member_channel, NEW.tag_id, tag_channel USING ERRCODE = 'check_violation';
 END IF;
 RETURN NULL;
END
$$;
-- A member or tag never changes the Channel (or ID) it belongs to.
CREATE OR REPLACE FUNCTION axton_channel_owner_fixed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.channel IS DISTINCT FROM OLD.channel THEN
  RAISE EXCEPTION '% row % belongs to Channel % and cannot move',
   TG_TABLE_NAME, OLD.id, OLD.channel USING ERRCODE = 'check_violation';
 END IF;
 RETURN NEW;
END
$$;
DO $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'axton_channel_member_tag'::regclass AND tgname = 'axton_channel_member_tag_same_channel') THEN
  CREATE CONSTRAINT TRIGGER axton_channel_member_tag_same_channel
   AFTER INSERT OR UPDATE ON axton_channel_member_tag
   FOR EACH ROW EXECUTE FUNCTION axton_channel_member_tag_same_channel();
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'axton_channel_member'::regclass AND tgname = 'axton_channel_member_fixed') THEN
  CREATE TRIGGER axton_channel_member_fixed BEFORE UPDATE ON axton_channel_member
   FOR EACH ROW EXECUTE FUNCTION axton_channel_owner_fixed();
 END IF;
 IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = 'axton_channel_tag'::regclass AND tgname = 'axton_channel_tag_fixed') THEN
  CREATE TRIGGER axton_channel_tag_fixed BEFORE UPDATE ON axton_channel_tag
   FOR EACH ROW EXECUTE FUNCTION axton_channel_owner_fixed();
 END IF;
END
$$;

-- 3. The copy, once.
DO $$
DECLARE
 found_rows bigint;
BEGIN
 IF current_setting('axton.upgrade_copy') <> 'on' THEN
  RETURN;
 END IF;
 SELECT count(*) INTO found_rows FROM axton_invalidation i
  LEFT JOIN axton_record r ON r.model = i.model AND r.identity_key = i.identity_key WHERE r.id IS NULL;
 IF found_rows > 0 THEN
  RAISE EXCEPTION '% retained positions name a record without metadata', found_rows;
 END IF;
 SELECT count(*) INTO found_rows FROM axton_invalidation i
  JOIN axton_record r ON r.model = i.model AND r.identity_key = i.identity_key WHERE i.identity IS DISTINCT FROM r.identity;
 IF found_rows > 0 THEN
  RAISE EXCEPTION '% retained identities disagree with their canonical identity_key', found_rows;
 END IF;
 INSERT INTO axton_channel_member(channel, record_id)
  SELECT m.channel, r.id FROM axton_membership m
  JOIN axton_record r ON r.model = m.model AND r.identity_key = m.identity_key
  ORDER BY m.channel COLLATE "C", r.model COLLATE "C", r.identity_key COLLATE "C";
 INSERT INTO axton_channel_log(channel, record_id, cursor, kind)
  SELECT i.channel, r.id, i.cursor,
   CASE WHEN EXISTS (SELECT 1 FROM axton_channel_member m WHERE m.channel = i.channel AND m.record_id = r.id)
    THEN 'upsert' ELSE 'remove' END
  FROM axton_invalidation i JOIN axton_record r ON r.model = i.model AND r.identity_key = i.identity_key;
 -- Live members never published get positions after their Channel's head, in record-key order.
 WITH unlogged AS (
  SELECT m.channel, m.record_id,
   row_number() OVER (PARTITION BY m.channel ORDER BY r.model COLLATE "C", r.identity_key COLLATE "C") AS n
  FROM axton_channel_member m JOIN axton_record r ON r.id = m.record_id
  WHERE NOT EXISTS (SELECT 1 FROM axton_channel_log l WHERE l.channel = m.channel AND l.record_id = m.record_id)),
 counts AS (SELECT channel, max(n) AS n FROM unlogged GROUP BY channel),
 heads AS (UPDATE axton_channel c SET head = c.head + counts.n FROM counts WHERE c.channel = counts.channel
  RETURNING c.channel, c.head - counts.n AS start)
 INSERT INTO axton_channel_log(channel, record_id, cursor, kind)
  SELECT u.channel, u.record_id, heads.start + u.n, 'upsert' FROM unlogged u JOIN heads ON heads.channel = u.channel;
END
$$;

-- 4. Verify before any runtime reads or writes these tables.
DO $$
DECLARE
 found_rows bigint;
BEGIN
 SELECT count(*) INTO found_rows FROM axton_channel_member m
  WHERE NOT EXISTS (SELECT 1 FROM axton_channel_log l WHERE l.channel = m.channel AND l.record_id = m.record_id AND l.kind = 'upsert');
 IF found_rows > 0 THEN
  RAISE EXCEPTION '% live members have no upsert position', found_rows;
 END IF;
 SELECT count(*) INTO found_rows FROM axton_channel_log l WHERE l.kind = 'upsert'
  AND NOT EXISTS (SELECT 1 FROM axton_channel_member m WHERE m.channel = l.channel AND m.record_id = l.record_id);
 IF found_rows > 0 THEN
  RAISE EXCEPTION '% upsert positions name no live member', found_rows;
 END IF;
 SELECT count(*) INTO found_rows FROM axton_channel_log l JOIN axton_channel c ON c.channel = l.channel WHERE l.cursor > c.head;
 IF found_rows > 0 THEN
  RAISE EXCEPTION '% positions lie above their Channel head', found_rows;
 END IF;
 SELECT count(*) INTO found_rows FROM axton_channel_member_tag mt
  JOIN axton_channel_member m ON m.id = mt.member_id JOIN axton_channel_tag t ON t.id = mt.tag_id WHERE m.channel <> t.channel;
 IF found_rows > 0 THEN
  RAISE EXCEPTION '% tag associations cross Channels', found_rows;
 END IF;
 IF to_regclass('axton_invalidation') IS NOT NULL THEN
  EXECUTE 'SELECT count(*) FROM axton_invalidation i JOIN axton_record r ON r.model = i.model AND r.identity_key = i.identity_key
   WHERE i.identity IS DISTINCT FROM r.identity' INTO found_rows;
  IF found_rows > 0 THEN
   RAISE EXCEPTION '% retained identities disagree with their canonical identity_key', found_rows;
  END IF;
 END IF;
END
$$;
COMMIT;
