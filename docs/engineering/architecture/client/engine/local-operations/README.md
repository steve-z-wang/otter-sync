# Local operations

Local operations are the write and read side of the engine: every change the application makes is applied to the local tables at once, and every query reads those tables.

- [Writes](writes.md) — Apply named mutations and direct writes optimistically while keeping the last server-known row underneath.
- [Queries](queries.md) — Read records by identity, filter, order and relation, and run read-only SQL.

The visible projection combines authoritative base state with pending operations and ordered local writes. [Push](../push/README.md) sends durable named input; [Sync](../../../protocols/sync.md) installs authority beneath pending work; [Settlement](../settlement.md) reconciles each Call only when its authority obligations commit. Receipt arrival alone does not retire accepted work. [Writes](writes.md) owns replay and companion ordering.
