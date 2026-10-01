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
CREATE TABLE IF NOT EXISTS axton_scope (
 scope text PRIMARY KEY,
 head bigint NOT NULL CHECK(head >= 0 AND head <= 9007199254740991)
);
-- One row per record ever stamped or represented in a Scope. `identity` is
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
CREATE TABLE IF NOT EXISTS axton_scope_member (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 scope text NOT NULL REFERENCES axton_scope(scope),
 record_id bigint NOT NULL REFERENCES axton_record(id),
 CONSTRAINT axton_scope_member_scope_record_id_key UNIQUE(scope,record_id)
);
CREATE INDEX IF NOT EXISTS axton_scope_member_record
 ON axton_scope_member(record_id, scope);
CREATE TABLE IF NOT EXISTS axton_scope_tag (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 scope text NOT NULL REFERENCES axton_scope(scope),
 name text NOT NULL CHECK(octet_length(name) BETWEEN 1 AND 256),
 CONSTRAINT axton_scope_tag_scope_name_key UNIQUE(scope,name)
);
CREATE TABLE IF NOT EXISTS axton_scope_member_tag (
 member_id bigint NOT NULL REFERENCES axton_scope_member(id) ON DELETE CASCADE,
 tag_id bigint NOT NULL REFERENCES axton_scope_tag(id) ON DELETE CASCADE,
 PRIMARY KEY(member_id,tag_id)
);
CREATE INDEX IF NOT EXISTS axton_scope_member_tag_tag
 ON axton_scope_member_tag(tag_id, member_id);
-- The latest deliverable state of each pair, whose `(scope, cursor)` orders scans.
CREATE TABLE IF NOT EXISTS axton_scope_log (
 scope text NOT NULL REFERENCES axton_scope(scope),
 record_id bigint NOT NULL REFERENCES axton_record(id),
 cursor bigint NOT NULL CHECK(cursor > 0 AND cursor <= 9007199254740991),
 kind text NOT NULL CHECK(kind IN ('upsert','remove')),
 PRIMARY KEY(scope,record_id),
 CONSTRAINT axton_scope_log_scope_cursor_key UNIQUE(scope,cursor)
);
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
