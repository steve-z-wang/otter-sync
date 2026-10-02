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

`migration.sql` installs six tables in a fresh database: `axton_client`, `axton_call`, `axton_stream`, `axton_record`, `axton_stream_member` and `axton_stream_log`. Apply the whole file transactionally. Existing databases follow the [Channel → Scope → Stream forward migration chain](https://github.com/zanminwang/axton/blob/v0.3.0/website/docs/backend/database.md#stream-forward-migration), with old writers stopped; fresh DDL refuses installed older layouts.

Migration preserves tracking, stamps, progress and saved work. Earlier name migrations rewrite only framework-owned claim keys; business JSON and opaque names remain unchanged. SQLite reopens its existing files in place.

For the coordinated 0.3 upgrade, stop old writers/live sessions, apply the appropriate prior layout upgrades and current DDL, then run `migrations/2026-10-01-local-authority.sql` before resuming traffic. It repairs historical withdrawals with newer authority through current viewer Loaders. Upgrade the adapter, backend, tooling and client runtimes together using [stream-authority-v1 negotiation](https://github.com/zanminwang/axton/blob/v0.3.0/website/docs/backend/deployment.md#stream-membership-cutover). Tracking/log retention is not automatic.
