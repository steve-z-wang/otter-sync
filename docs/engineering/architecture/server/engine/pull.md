# Pull

## 1. Introduction and Goals

Server pull delivers the latest retained Channel/record state after a position. Upserts use current stamped Loader authority; removals use stored identities without a Loader. [Protocol Pull](../../protocol/pull.md) owns exact wire and pagination rules.

## 5. Building Block View

The eight-table [persistence](../persistence.md) separates record metadata, live membership, server tags and one compacted log row per Channel/record. Scans include both event kinds before the page limit, without joining live members. Removal therefore consumes a page slot and remains deliverable after the domain row disappears.

The server groups repeated upsert identities across Channels into one Loader read while keeping every pair's provenance. Stamped null is authority; Loader error is a diagnostic; neither is a membership release. Loaders receive the viewer and identity, never a Channel or tag.

## 6. Runtime View

One application transaction reads heads, scans ordered positions and resolves upsert authority at the requested retained Model versions. Each Channel emits at most 50 events. Probe for a later retained row: a nonterminal page ends at its last emitted cursor, while an exhausted scan advances to the observed head across compacted gaps. Bootstrap uses the same logic within `(after, until]`; a pair moved above the fixed origin belongs to ordinary delivery.

A removal cannot expose domain content. Removing and re-adding an unchanged record gives a new upsert position at the existing content stamp. A null upsert remains possible while membership persists. Infrastructure defects abort the transaction; a Loader failure reports one upsert diagnostic and other identities proceed.

## 9. Architecture Decisions

Compaction retains one latest membership state per Channel/record. It avoids append-only delivery history while preserving identity-only release for offline clients. Content stamps stay global and Loader authority remains viewer-specific.

## 10. Quality Requirements

See the channel/provenance, removal-only, Loader-distinction and compacted-pagination cases in [stamp.rs](../../../../../crates/server/tests/stamp.rs), [bootstrap.rs](../../../../../crates/server/tests/bootstrap.rs), [live.rs](../../../../../crates/server/tests/live.rs) and the real PostgreSQL [runtime suite](../../../../../integration/persistence/server/runtime.test.mjs). Task 5 verified focused server and persistence suites; final capability/runtime acceptance is separate.

## 11. Risks and Technical Debt

Retained removals make reconnect possible without domain rows, at the cost of one log row per represented pair. No pruning floor or snapshot replacement exists. Removing N members remains O(N) identity delivery and storage work.
