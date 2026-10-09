# Runtime

## 1. Introduction and Goals

ClientRuntime owns task admission, transactions, prerequisites, connection lifecycles, direct reads and observers. It drives protocol-5 handshake, Uplink and finite delivery effects. Priority close cancels parked remote tasks and releases the actor without waiting for a network response; post-close results cannot install Models. SDKs submit tasks and carry effects rather than making retry or coverage decisions.

The Store's complete pending-owner snapshot bounds owned materialization work. Completed schema or settlement owners release their requests, authentication refresh waiters and staged transfers; late effect outcomes cannot revive them. Already dispatched Store work retains its authority admission and reports completion before the next apply is scheduled.

Every publication path refreshes retained pending, failure and refusal observers before consuming the commit mark for Model watches. Application tasks and protocol-5 Store worker commits use this same boundary; listeners receive changed committed snapshots without polling.

## 5. Building Block View

[Implementation](../../../../crates/client/src/runtime/mod.rs) owns this component. [Protocol 5](../protocols/sync.md) owns shared context, delivery and settlement rules.
