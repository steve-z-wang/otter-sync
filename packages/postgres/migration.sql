-- AXTON's framework tables for a new database. Apply the whole file at once
-- (psql, or one simple-protocol query): the trigger functions are
-- dollar-quoted. Re-applying it changes nothing. A database installed from
-- the six-table schema of 0.1.x upgrades with
-- migrations/2026-09-30-channel-members.sql instead.
CREATE TABLE IF NOT EXISTS axton_client (
 client_id text PRIMARY KEY,
 owner_id text NOT NULL,
 sequence bigint NOT NULL DEFAULT 0 CHECK(sequence >= 0 AND sequence <= 9007199254740991),
 receipt text
);
CREATE TABLE IF NOT EXISTS axton_call (
 owner_id text NOT NULL,
 call_id text NOT NULL,
 request text NOT NULL,
 response text,
 claim_tx xid8 NOT NULL DEFAULT pg_current_xact_id(),
 PRIMARY KEY(owner_id,call_id)
);
CREATE TABLE IF NOT EXISTS axton_channel (
 channel text PRIMARY KEY,
 head bigint NOT NULL CHECK(head >= 0 AND head <= 9007199254740991)
);
-- One row per record ever stamped or represented in a Channel. `identity` is
-- the decoded canonical `identity_key`, so a removal needs no domain row.
CREATE TABLE IF NOT EXISTS axton_record (
 model text NOT NULL,
 identity_key text NOT NULL,
 stamp bigint NOT NULL CHECK(stamp > 0 AND stamp <= 9007199254740991),
 id bigint GENERATED ALWAYS AS IDENTITY,
 identity jsonb GENERATED ALWAYS AS (identity_key::jsonb) STORED,
 CONSTRAINT axton_record_pkey PRIMARY KEY(id),
 CONSTRAINT axton_record_model_identity_key_key UNIQUE(model,identity_key),
 CONSTRAINT axton_record_identity_object CHECK(jsonb_typeof(identity) = 'object')
);
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
