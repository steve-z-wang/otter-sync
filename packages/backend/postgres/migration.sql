-- Fresh protocol-5 namespaces only. Installed historical tables stay unchanged.
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_class WHERE relnamespace=current_schema()::regnamespace AND relname=ANY(ARRAY['axton_client','axton_call','axton_stream_member','axton_stream_log','axton_publication_group','axton_bootstrap_manifest','axton_channel','axton_scope','axton_bootstrap_identity','axton_bootstrap_range','axton_channel_member','axton_channel_log','axton_channel_tag','axton_scope_member','axton_scope_log','axton_scope_tag','axton_membership','axton_invalidation']) AND relkind='r') OR EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND ((table_name='axton_record' AND column_name='stamp') OR (table_name='axton_publication_fence' AND column_name='held'))) THEN
  RAISE EXCEPTION 'installed legacy framework layout: protocol 5 requires a fresh namespace; existing data is unchanged';
 END IF;
END $$;
-- AXTON's framework tables for a new database. Apply the whole file at once
-- (psql, or one simple-protocol query): the trigger functions are
-- dollar-quoted. Re-applying it changes nothing. A database installed from
-- an earlier layout must use a separately adopted fresh namespace.
CREATE TABLE IF NOT EXISTS axton_stream (
 stream text PRIMARY KEY,
 head bigint NOT NULL CHECK(head >= 0 AND head <= 9007199254740991)
);
-- One stable catalog row per represented record. `identity` is
-- the decoded canonical `identity_key`, so a removal needs no domain row.
CREATE TABLE IF NOT EXISTS axton_record (
 model text NOT NULL,
 identity_key text NOT NULL,
 id bigint GENERATED ALWAYS AS IDENTITY,
 identity jsonb GENERATED ALWAYS AS (identity_key::jsonb) STORED,
 CONSTRAINT axton_record_pkey PRIMARY KEY(id),
 CONSTRAINT axton_record_model_identity_key_key UNIQUE(model,identity_key),
 CONSTRAINT axton_record_identity_object CHECK(jsonb_typeof(identity) = 'object')
);
-- One namespace-wide publication serialization fence, retained across restarts.
CREATE TABLE IF NOT EXISTS axton_publication_fence (id integer PRIMARY KEY CHECK(id=1));
INSERT INTO axton_publication_fence(id) VALUES(1) ON CONFLICT(id) DO NOTHING;

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

-- Immutable finite protocol-5 authority plans; no publication history.
CREATE TABLE IF NOT EXISTS axton_delivery_plan (
 plan_id text PRIMARY KEY,
 principal text NOT NULL,
 store_id text NOT NULL REFERENCES axton_store(id),
 context jsonb NOT NULL,
 intent text NOT NULL,
 header jsonb NOT NULL,
 digest text NOT NULL,
 expires_at bigint NOT NULL CHECK(expires_at BETWEEN 0 AND 9007199254740991),
 staged_bytes bigint NOT NULL CHECK(staged_bytes BETWEEN 0 AND 268435456)
);
CREATE INDEX IF NOT EXISTS axton_delivery_plan_expiry ON axton_delivery_plan(expires_at);
CREATE TABLE IF NOT EXISTS axton_delivery_unit (
 plan_id text NOT NULL REFERENCES axton_delivery_plan(plan_id) ON DELETE CASCADE,
 unit_index bigint NOT NULL CHECK(unit_index>=0),
 part_index bigint NOT NULL CHECK(part_index>=0),
 payload jsonb NOT NULL,
 digest text NOT NULL,
 PRIMARY KEY(plan_id,unit_index,part_index)
);
