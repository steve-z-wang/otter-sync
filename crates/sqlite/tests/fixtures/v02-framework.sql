
CREATE TABLE IF NOT EXISTS axton_schema (
  descriptor TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS axton_client (
  client_id    TEXT PRIMARY KEY,
  next_ordinal INTEGER NOT NULL,
  next_push    INTEGER NOT NULL,
  generation   INTEGER NOT NULL,
  last_completed_push INTEGER NOT NULL DEFAULT 0,
  push_models  TEXT,
  push_results TEXT,
  channel_membership_version INTEGER NOT NULL DEFAULT 1,
  store_epoch INTEGER NOT NULL DEFAULT 0,
  next_subscription INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS axton_record (
  model TEXT NOT NULL, identity TEXT NOT NULL, stamp INTEGER NOT NULL,
  base_state TEXT NOT NULL DEFAULT 'materialized',
  evicted_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (model, identity)
);
CREATE TABLE IF NOT EXISTS axton_channel_member (
  channel TEXT NOT NULL, model TEXT NOT NULL, identity TEXT NOT NULL,
  cursor INTEGER NOT NULL CHECK(cursor > 0), present INTEGER NOT NULL CHECK(present IN (0,1)),
  PRIMARY KEY(channel, model, identity)
);
CREATE INDEX IF NOT EXISTS axton_channel_member_record ON axton_channel_member(model, identity, present);
CREATE TABLE IF NOT EXISTS axton_local_replica_layer (
  model TEXT NOT NULL, identity TEXT NOT NULL, operations TEXT NOT NULL,
  PRIMARY KEY(model, identity)
);
CREATE TABLE IF NOT EXISTS axton_subscription (
  channel          TEXT PRIMARY KEY,
  subscription_id  INTEGER NOT NULL UNIQUE,
  starting_cursor  INTEGER,
  cursor           INTEGER,
  bootstrap_state   TEXT NOT NULL DEFAULT 'not_requested',
  bootstrap_run     INTEGER NOT NULL DEFAULT 0,
  bootstrap_cursor  INTEGER NOT NULL DEFAULT 0,
  bootstrap_barrier INTEGER,
  bootstrap_error   TEXT,
  reconcile_state TEXT NOT NULL DEFAULT 'not_requested',
  reconcile_run INTEGER NOT NULL DEFAULT 0,
  reconcile_cursor INTEGER NOT NULL DEFAULT 0,
  reconcile_bound INTEGER,
  reconcile_barrier INTEGER,
  reconcile_error TEXT,
  CHECK ((starting_cursor IS NULL AND cursor IS NULL) OR
         (starting_cursor IS NOT NULL AND cursor IS NOT NULL AND
          starting_cursor >= 0 AND cursor >= starting_cursor))
);
CREATE TABLE IF NOT EXISTS axton_mutation (
  ordinal INTEGER PRIMARY KEY, name TEXT NOT NULL, version INTEGER NOT NULL, push INTEGER,
  store_epoch INTEGER NOT NULL DEFAULT 0,
  diverged INTEGER NOT NULL DEFAULT 0, call_id TEXT UNIQUE, args TEXT, store TEXT,
  CHECK ((call_id IS NULL AND args IS NULL) OR (call_id IS NOT NULL AND args IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS axton_mutation_operation (
  ordinal INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('wire','companion','effect')),
  model TEXT NOT NULL, identity TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('create','update','delete')),
  "values" TEXT,
  PRIMARY KEY (ordinal, position)
);
CREATE INDEX IF NOT EXISTS axton_mutation_operation_record ON axton_mutation_operation (model, identity, ordinal, position);
CREATE TABLE IF NOT EXISTS axton_mutation_dependency (
  ordinal INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  depends_on INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('lifecycle','sequence')),
  PRIMARY KEY (ordinal, depends_on),
  CHECK (depends_on < ordinal)
);
CREATE TABLE IF NOT EXISTS axton_mutation_prerequisite (
  ordinal INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  key TEXT NOT NULL, error TEXT,
  PRIMARY KEY (ordinal, key)
);
CREATE TABLE IF NOT EXISTS axton_local_write (
  sequence INTEGER PRIMARY KEY,
  ordinal INTEGER NOT NULL,
  position INTEGER,
  disposition TEXT NOT NULL CHECK (disposition IN ('independent','accepted')),
  model TEXT NOT NULL, identity TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('create','update','delete')),
  "values" TEXT,
  CHECK ((disposition = 'independent') = (position IS NULL))
);
CREATE INDEX IF NOT EXISTS axton_local_write_record ON axton_local_write (model, identity, sequence);
CREATE TABLE IF NOT EXISTS axton_rejection (
  ordinal INTEGER PRIMARY KEY, name TEXT NOT NULL, code TEXT NOT NULL, detail TEXT
);
CREATE TABLE IF NOT EXISTS axton_query_cache (
  key TEXT PRIMARY KEY, contract TEXT NOT NULL, name TEXT NOT NULL,
  version INTEGER NOT NULL, args TEXT NOT NULL, store TEXT NOT NULL,
  generation TEXT NOT NULL, result TEXT
);
CREATE INDEX IF NOT EXISTS axton_query_cache_arguments ON axton_query_cache (contract, name, version, args);
CREATE TABLE IF NOT EXISTS axton_load (
  store_epoch INTEGER NOT NULL DEFAULT 0,
  load_id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, ready INTEGER NOT NULL,
  name TEXT NOT NULL, version INTEGER NOT NULL, args TEXT NOT NULL, models TEXT NOT NULL,
  continuation TEXT, run INTEGER NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('pending','complete','failed','cancelled')),
  pages INTEGER NOT NULL DEFAULT 0,
  call_id TEXT UNIQUE, intent TEXT,
  retry TEXT CHECK (retry IN ('transport','backend','local')),
  attempts INTEGER NOT NULL DEFAULT 0, error TEXT,
  CHECK ((call_id IS NULL AND intent IS NULL) OR (call_id IS NOT NULL AND intent IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS axton_load_ready ON axton_load (phase, ready);
CREATE TABLE IF NOT EXISTS axton_load_once (
  key TEXT PRIMARY KEY, name TEXT NOT NULL, version INTEGER NOT NULL,
  args TEXT NOT NULL, models TEXT NOT NULL, load_id TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS axton_load_once_arguments ON axton_load_once (name, version, args);
