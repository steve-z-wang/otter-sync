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
-- Durable tracking, including when a viewer Loader currently answers null.
-- Historical withdrawals are repaired by 2026-10-01-local-authority.sql.
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

-- One namespace-wide publication serialization fence, retained across restarts.
CREATE TABLE IF NOT EXISTS axton_publication_fence (id integer PRIMARY KEY CHECK(id=1), held boolean NOT NULL DEFAULT true);
INSERT INTO axton_publication_fence(id) VALUES(1) ON CONFLICT(id) DO NOTHING;

-- Original publication transaction spans/keys are durable coverage evidence.
-- Never garbage-collect by age: compacted members may still need this group.
CREATE TABLE IF NOT EXISTS axton_publication_group (
 stream text NOT NULL REFERENCES axton_stream(stream),
 transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),
 from_cursor bigint NOT NULL CHECK(from_cursor>=0),
 through_cursor bigint NOT NULL CHECK(through_cursor>from_cursor),
 keys jsonb NOT NULL CHECK(jsonb_typeof(keys)='array'),
 PRIMARY KEY(stream,transaction_id), UNIQUE(stream,through_cursor)
);

CREATE TABLE IF NOT EXISTS axton_bootstrap_manifest (
 owner_id text NOT NULL, manifest_id text NOT NULL, context jsonb NOT NULL,
 start_cursor bigint NOT NULL CHECK(start_cursor>=0), total bigint NOT NULL CHECK(total>=0),
 models jsonb NOT NULL, tail bigint CHECK(tail>=start_cursor),
 PRIMARY KEY(owner_id,manifest_id)
);
CREATE TABLE IF NOT EXISTS axton_bootstrap_identity (
 owner_id text NOT NULL,manifest_id text NOT NULL,ordinal bigint NOT NULL CHECK(ordinal>=0),
 model text NOT NULL,identity_key text NOT NULL,
 PRIMARY KEY(owner_id,manifest_id,ordinal), UNIQUE(owner_id,manifest_id,model,identity_key),
 FOREIGN KEY(owner_id,manifest_id) REFERENCES axton_bootstrap_manifest(owner_id,manifest_id)
);
CREATE TABLE IF NOT EXISTS axton_bootstrap_range (
 owner_id text NOT NULL,manifest_id text NOT NULL,from_ordinal bigint NOT NULL,to_ordinal bigint NOT NULL CHECK(to_ordinal>=from_ordinal),
 PRIMARY KEY(owner_id,manifest_id,from_ordinal,to_ordinal),
 FOREIGN KEY(owner_id,manifest_id) REFERENCES axton_bootstrap_manifest(owner_id,manifest_id)
);

-- Protocol 5 is additive while protocol 4 remains operational.
CREATE TABLE IF NOT EXISTS axton_store (
 id text PRIMARY KEY,
 principal text NOT NULL,
 stream text NOT NULL,
 last_processed_batch_id bigint NOT NULL DEFAULT 0 CHECK(last_processed_batch_id BETWEEN 0 AND 9007199254740991),
 progress bigint NOT NULL DEFAULT 0 CHECK(progress BETWEEN 0 AND 9007199254740991),
 current_digest text,
 current_count bigint,
 last_digest text,
 last_count bigint,
 bootstrap_prepared boolean NOT NULL DEFAULT false,
 start_cursor bigint CHECK(start_cursor BETWEEN 0 AND 9007199254740991),
 CHECK((current_digest IS NULL AND current_count IS NULL AND progress=0) OR
       (current_digest IS NOT NULL AND current_count IS NOT NULL AND current_count BETWEEN 1 AND 9007199254740991 AND progress<current_count)),
 CHECK((last_processed_batch_id=0 AND last_digest IS NULL AND last_count IS NULL) OR
       (last_processed_batch_id>0 AND last_digest IS NOT NULL AND last_count IS NOT NULL AND last_count BETWEEN 1 AND 9007199254740991))
);
CREATE TABLE IF NOT EXISTS axton_mutation_result (
 store_id text NOT NULL REFERENCES axton_store(id),
 batch_id bigint NOT NULL CHECK(batch_id BETWEEN 1 AND 9007199254740991),
 mutation_id bigint NOT NULL CHECK(mutation_id BETWEEN 1 AND 9007199254740991),
 ordinal bigint NOT NULL CHECK(ordinal BETWEEN 0 AND 9007199254740991),
 result jsonb NOT NULL,
 PRIMARY KEY(store_id,batch_id,mutation_id),
 UNIQUE(store_id,batch_id,ordinal)
);
CREATE TABLE IF NOT EXISTS axton_stream_record (
 stream text NOT NULL REFERENCES axton_stream(stream),
 record_id bigint NOT NULL REFERENCES axton_record(id),
 cursor bigint NOT NULL CHECK(cursor BETWEEN 1 AND 9007199254740991),
 kind text NOT NULL CHECK(kind IN ('upsert','remove')),
 PRIMARY KEY(stream,record_id)
);
CREATE INDEX IF NOT EXISTS axton_stream_record_cursor ON axton_stream_record(stream,cursor,record_id);
CREATE INDEX IF NOT EXISTS axton_stream_record_holders ON axton_stream_record(record_id,stream);
CREATE OR REPLACE FUNCTION axton_store_binding_fixed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.principal IS DISTINCT FROM OLD.principal OR NEW.stream IS DISTINCT FROM OLD.stream THEN
  RAISE EXCEPTION 'Store binding is immutable';
 END IF;
 RETURN NEW;
END $$;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='axton_store'::regclass AND tgname='axton_store_binding_fixed') THEN
  CREATE TRIGGER axton_store_binding_fixed BEFORE UPDATE ON axton_store FOR EACH ROW EXECUTE FUNCTION axton_store_binding_fixed();
 END IF;
END $$;
