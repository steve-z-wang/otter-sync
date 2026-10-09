# Push lane

## 1. Introduction and Goals

Uplink selects ready work, freezes one immutable Batch and retries those exact bytes until execution acknowledgment commits. Server members have independent outcomes. Acknowledged acceptance is retained while authority obligations settle; sending another request cannot replace saved intent or manufacture completion.

## 5. Building Block View

[Implementation](../../../../../../crates/client/src/sync05/uplink.rs) owns this component. [Protocol 5](../../../protocols/sync.md) owns shared context, delivery and settlement rules.

