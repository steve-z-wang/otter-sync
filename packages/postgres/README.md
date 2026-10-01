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

`migration.sql` installs six tables in a fresh database: `axton_client`, `axton_call`, `axton_stream`, `axton_record`, `axton_stream_member` and `axton_stream_log`. Apply the whole file transactionally. Existing databases follow the [Channel → Scope → Stream forward migration chain](../../website/docs/backend/database.md#stream-forward-migration), with old writers stopped; fresh DDL refuses installed older layouts.

Migration preserves tracking, stamps, progress, removal evidence and saved work. Only framework-owned claim keys are rewritten; business JSON and opaque names remain unchanged. SQLite reopens its existing files in place. Upgrade the adapter, backend, tooling and client runtimes together using [stream-membership-v1 negotiation](../../website/docs/backend/deployment.md#stream-membership-cutover). Source manifests remain `0.2.0`; registry release selection/publication is separate work. Tracking/log retention is not automatic.
