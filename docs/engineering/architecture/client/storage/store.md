# Store

## 1. Introduction and Goals

SQLite gives each physical file one exclusive owner across aliases and processes. Unsupported-format admission checks a temporary database/WAL copy before modifying the original. SQL transactions and savepoints commit Model rows, retained input, replay evidence and progress together. Closing releases ownership; normal reopen neither wipes nor abandons pending work.

## 5. Building Block View

[Implementation](../../../../../crates/sqlite/src/lib.rs) owns this component. [Protocol 5](../../protocols/sync.md) owns shared context, delivery and settlement rules.


Fresh admission installs the canonical layout. Reopen requires its supported layout and preserves pending replay and observer filtering; it does not create old-name compatibility views or upgrade a provisional layout in place.
