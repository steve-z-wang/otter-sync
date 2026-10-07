-- Forward additive upgrade for databases already running protocol-5 Store tables.
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
