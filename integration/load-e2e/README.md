# Bound reads and finite Bootstrap

`run.sh` compiles the source contract, checks TypeScript and Dart, creates disposable PostgreSQL and runs generated Node/Dart clients through an HTTP/WebSocket fault proxy. The clients use real native actors and physical SQLite files. The child-process test sends a named Mutation, kills the actual client after backend acceptance, then reopens the same file and proves exact frozen-intent replay.

The former paged Load scenarios are expressed through current protocol 4 contracts:

| Concern | Current path |
| --- | --- |
| Finite history, committed coverage and reopen | Marked `Item` Bootstrap manifest and proven Delta prefix |
| Read snapshots, nullable arguments, once, refresh and invalidation | Named `ProjectItems`/`Catalog` Query and Fetch |
| Enrollment and later delivery | Bootstrap's explicit track or explicit backend publication; Query never enrolls |
| Independent holding and selected absence | Separate viewer-bound files and one Stream per file |
| Local atomicity and derived device state | Named Mutation callbacks and direct transaction rollback; `Seen` is device-only |
| Lost response and process recovery | Real proxy response hold, SIGKILL and native reopen |
| Loader failure | Whole ordinary read refusal with no partial cache commit |

`store=false` returns invocation snapshots without Model writes. Automatic Bootstrap may independently populate marked Models, so the no-enrollment tests query a project other than the bound viewer's Bootstrap selection. `Tag` is unmarked and exposes the same distinction in Dart. Delayed reads return their original business snapshot while current Stream authority continues to protect the Store.

There is no custom onStore hook, Load continuation, anonymous mutation, manual ACK/pull installer or multistream-in-one-store test seam. The proxy changes actual transport availability and response timing; read-only SQL observes durable state. Teardown observes and drains admitted backend transaction promises before closing PostgreSQL.
