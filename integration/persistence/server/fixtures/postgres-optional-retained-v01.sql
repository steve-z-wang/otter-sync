-- Optional retained v0.1 tables AFTER postgres-state.sql; stale historical membership/claim does not reenroll.
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

INSERT INTO axton_membership(channel,model,identity_key) VALUES('Other','Todo','{"id":"live"}');
INSERT INTO axton_invalidation(channel,model,identity_key,identity,cursor,stamp) VALUES('Channel:business-scope','Todo','{"id":"live"}','{"id":"live"}',10,7);
