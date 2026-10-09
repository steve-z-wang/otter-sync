# Sync protocol

## 1. Introduction and Goals

Define the messages exchanged by Client and Server: bound Store context, immutable Mutation Batches and acknowledgements, cursor-null reads, handshakes and finite delivery/materialization plans. [Protocol 5](../protocol/0.5.md) owns the complete wire semantics and limits.

## 5. Building Block View

[Sync contracts](../../../../crates/protocols/src/sync.rs) and their [delivery](../../../../crates/protocols/src/sync/delivery.rs) / [mutation](../../../../crates/protocols/src/sync/mutation.rs) helpers encode, validate and calculate pure protocol results. They use shared schema normalization and counters from core. Client/Server engines own persistence, scheduling, authority application and business execution.

## 10. Quality Requirements

Moving these definitions changes neither discriminator 5 nor encoded data. [Contract tests](../../../../crates/protocols/tests/sync.rs) verify valid/invalid carriers and planning helpers; runtime and joined native tests establish their execution and commit guarantees separately.
