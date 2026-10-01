DO $$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_channel','axton_channel_member','axton_channel_tag','axton_channel_member_tag','axton_channel_log','axton_scope','axton_scope_member','axton_scope_tag','axton_scope_member_tag','axton_scope_log']) AND relkind='r') THEN
  RAISE EXCEPTION 'installed legacy framework layout: apply forward migrations before migration.sql';
 END IF;
END
$$;
-- AXTON's framework tables for a new database. Apply the whole file at once
-- (psql, or one simple-protocol query): the trigger functions are
-- dollar-quoted. Re-applying it changes nothing. A database installed from
-- an earlier layout requires its forward upgrade before this file.
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
CREATE TABLE IF NOT EXISTS axton_stream (
 stream text PRIMARY KEY,
 head bigint NOT NULL CHECK(head >= 0 AND head <= 9007199254740991)
);
-- One row per record ever stamped or represented in a Stream. `identity` is
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
CREATE TABLE IF NOT EXISTS axton_stream_member (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 stream text NOT NULL REFERENCES axton_stream(stream),
 record_id bigint NOT NULL REFERENCES axton_record(id),
 CONSTRAINT axton_stream_member_stream_record_id_key UNIQUE(stream,record_id)
);
CREATE INDEX IF NOT EXISTS axton_stream_member_record
 ON axton_stream_member(record_id, stream);
-- The latest deliverable state of each pair, whose `(stream, cursor)` orders scans.
CREATE TABLE IF NOT EXISTS axton_stream_log (
 stream text NOT NULL REFERENCES axton_stream(stream),
 record_id bigint NOT NULL REFERENCES axton_record(id),
 cursor bigint NOT NULL CHECK(cursor > 0 AND cursor <= 9007199254740991),
 kind text NOT NULL CHECK(kind IN ('upsert','remove')),
 PRIMARY KEY(stream,record_id),
 CONSTRAINT axton_stream_log_stream_cursor_key UNIQUE(stream,cursor)
);
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
