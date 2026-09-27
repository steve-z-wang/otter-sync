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
CREATE TABLE IF NOT EXISTS axton_record (
 model text NOT NULL,
 identity_key text NOT NULL,
 stamp bigint NOT NULL CHECK(stamp > 0 AND stamp <= 9007199254740991),
 PRIMARY KEY(model,identity_key)
);
CREATE TABLE IF NOT EXISTS axton_invalidation (
 channel text NOT NULL REFERENCES axton_channel(channel),
 model text NOT NULL,
 identity_key text NOT NULL,
 identity jsonb NOT NULL,
 cursor bigint NOT NULL CHECK(cursor > 0 AND cursor <= 9007199254740991),
 stamp bigint NOT NULL CHECK(stamp > 0 AND stamp <= 9007199254740991),
 PRIMARY KEY(channel,model,identity_key),
 UNIQUE(channel,cursor)
);
CREATE TABLE IF NOT EXISTS axton_membership (
 channel text NOT NULL REFERENCES axton_channel(channel),
 model text NOT NULL,
 identity_key text NOT NULL,
 PRIMARY KEY(model, identity_key, channel),
 FOREIGN KEY(model, identity_key) REFERENCES axton_record(model, identity_key)
);
CREATE INDEX IF NOT EXISTS axton_membership_channel
 ON axton_membership(channel, model, identity_key);
