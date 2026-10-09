# Local operations

Local operations are the write and read side of the engine: every change the application makes is applied to the local tables at once, and every query reads those tables.

- [Writes](writes.md) — Apply named mutations and direct writes optimistically while keeping the last server-known row underneath.
- [Queries](queries.md) — Read records by identity, filter, order and relation, and run read-only SQL.

Writes keep two tables per model in step: the visible table the queries read, and a before-image table holding the server's last version of any record with pending mutations. The rest of the engine relies on that pair: [Push](../push/README.md) sends the queued mutations, [Pull](../../../../history/pre-protocol5/architecture/client/engine/pull.md) and [Settlement](../settlement.md) write server authority (a page, or the receipt's records) into the before image when a record is dirty, and Settlement promotes the before image back to the visible table once the receipt has answered.
