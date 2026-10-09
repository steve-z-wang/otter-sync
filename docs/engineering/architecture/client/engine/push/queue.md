# Queue

## 1. Introduction and Goals

The Mutation queue retains lifecycle, argument/input paths, local operation order, frozen membership and outcome. Null input paths identify device companions or derived cascades. Reconstruction uses the retained schema artifact and never applies new defaults to old input. Call completion is a queue projection rather than a second durable completion owner.

## 5. Building Block View

[Implementation](../../../../../../crates/client/src/store05.rs) owns this component. [Protocol 5](../../../protocols/sync.md) owns shared context, delivery and settlement rules.
