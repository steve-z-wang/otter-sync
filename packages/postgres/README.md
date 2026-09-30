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

`migration.sql` installs the eight framework tables in a new database: `axton_client`, `axton_call`, `axton_channel`, `axton_record` and the Channel tables `axton_channel_member`, `axton_channel_tag`, `axton_channel_member_tag` and `axton_channel_log`. Apply the whole file at once (for example `psql -v ON_ERROR_STOP=1 -f`); its trigger functions are dollar-quoted, so it cannot be split on semicolons. Re-applying it changes nothing.

A database installed by 0.1.x upgrades with `@axtonjs/postgres/migrations/2026-09-30-channel-members.sql` instead. Stop every older AXTON writer, then apply it the same way: in one transaction it adds record IDs, copies memberships and retained positions into the Channel tables, gives never-published members positions above their Channel's head and verifies the result. It keeps `axton_membership` and `axton_invalidation` untouched, fails whole on inconsistent data, and a repeated run changes nothing.

