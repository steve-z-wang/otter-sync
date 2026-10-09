# Publish

## 1. Introduction and Goals

Explicit track creates interest with a real discoverable position; repeat live tracking retains its position. Global invalidation reaches current holders and targeted invalidation reaches selected existing holders, without enrolling new ones. The persisted fence and canonical Stream locks keep content and position consistent; one transaction reserves at most one cursor per affected Stream. Wakes occur after commit.

## 5. Building Block View

[settlement.rs](../../../../../crates/server/src/settlement.rs) normalizes tracking/invalidation, locks Streams and applies final membership through the Host. The [SDK effects collector](../../../../../packages/backend/server/bindings/effects.mts) records declarations; [PostgreSQL persistence](../../../../../packages/backend/postgres/src/persistence.mts) answers the retained Host requests. [Protocol 5](../../protocols/sync.md) owns shared context, delivery and settlement rules.
