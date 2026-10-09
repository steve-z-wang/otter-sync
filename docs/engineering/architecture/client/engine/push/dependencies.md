# Dependencies

## 1. Introduction and Goals

Prerequisite readiness and declared lifecycle/sequence dependencies determine eligible work. A failed prerequisite remains visible until explicitly retried or dropped; unrelated ready work can proceed. Derived local cascades may extend rollback/replay effects without mutating assigned wire input. Rejection undoes only its owned work and actual dependents.

## 5. Building Block View

[Implementation](../../../../../../crates/client/src/runtime/prerequisites.rs) owns this component. [Protocol 5](../../../protocols/sync.md) owns shared context, delivery and settlement rules.
