# @axtonjs/postgres

> **Alpha.** AXTON is alpha software: its API is unstable and it is not ready for production use.

AXTON's PostgreSQL persistence: the framework tables (`@axtonjs/postgres/migration.sql`), every statement AXTON runs, and a two-method driver interface. Pick the shim for your access tool and pass it as `createBackend({ database })`:

```ts
import { pg } from "@axtonjs/postgres/pg";                // node-postgres pool
import { prisma } from "@axtonjs/postgres/prisma";        // Prisma client
import { drizzle } from "@axtonjs/postgres/drizzle";      // drizzle-orm/node-postgres
```

`pg` and `prisma` are also exported from `@axtonjs/postgres`; `drizzle` is not, because it imports `drizzle-orm`. Any other tool needs `persistence({ transaction, query })`. See the [database guide](https://github.com/zanminwang/axton/blob/main/website/docs/backend/database.md).

## Schema

`migration.sql` installs the eight framework tables in a new database: `axton_client`, `axton_call`, `axton_scope`, `axton_record` and the Scope tables `axton_scope_member`, `axton_scope_tag`, `axton_scope_member_tag` and `axton_scope_log`. Apply the whole file at once (for example `psql -v ON_ERROR_STOP=1 -f`); its trigger functions are dollar-quoted, so it cannot be split on semicolons. Re-applying it changes nothing.

An installed v0.2 database upgrades with `@axtonjs/postgres/migrations/2026-09-30-scopes.sql`. Stop all older backend writers and live connections, then apply the whole file once. It transactionally renames framework tables, ownership columns and catalog objects, replaces trigger bodies, and converts only top-level saved membership claims. Business JSON, request bytes, IDs, cursors, tags and removal evidence survive. Conflicting or incomplete layouts and malformed saved claims fail the whole transaction; after an error, issue `ROLLBACK` before retrying. Reapplying the file leaves saved rows unchanged.

A v0.1.x installation first applies the original `2026-09-30-channel-members.sql`, then `2026-09-30-scopes.sql`, with old writers stopped throughout. The first upgrade copies memberships and retained positions and gives never-published members positions above the existing head. The second renames ownership columns and indexes on retained `axton_membership` and `axton_invalidation` too. Do not reapply the original migration after Scope cutover. Empty databases use `migration.sql`; it refuses installed old layouts instead of creating parallel empty truth.

Clients reopen old SQLite files in place before reconciliation or network work. The upgrade preserves positive and negative membership evidence, subscriptions, request epochs and frozen queue/Load work. The membership marker retains its existing 0→1 reconciliation meaning. Updated peers require `scope-membership-v1`; old clients are rejected before handlers or progress effects.

A repeated upgrade still takes an `ACCESS EXCLUSIVE` lock on `axton_record`. Retain the old tables and compacted removal log: this release provides no pruning floor or snapshot replacement. Upgrade the backend, adapter, generated tooling and client runtimes together; see [cutover](https://github.com/zanminwang/axton/blob/main/website/docs/backend/deployment.md#scope-membership-cutover).
