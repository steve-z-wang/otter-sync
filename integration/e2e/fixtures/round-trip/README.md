# Round-trip fixture

The generated backend uses Prisma and PostgreSQL. Node and Dart generated clients use one viewer-bound SQLite Store and Stream (`User:demo-user`) over real HTTP and WebSocket. The current named `EditEntry` Mutation trims text, explicitly invalidates existing holders, returns canonical business output and refuses `entry.denied`. The retained legacy `Edit` descriptor has an internal handler; current clients never submit it. `FindEntry` and Fetch return ordinary read snapshots.

`Entry` is marked for finite Bootstrap. The Bootstrap handler explicitly tracks visible rows; `notify`, `publishMany` and `publishOne` publish later authority. `invalidateRecords` targets existing Stream holding. `tombstone` deletes and globally invalidates the identity; its Loader returns null. Loader refusal controls and retained Stream inspection support bounded failure and recovery tests.

Run `bash integration/e2e/run.sh` from the repository root. The runner builds native libraries, creates disposable PostgreSQL and executes round-trip, file binding/recovery, finite Bootstrap and Node/Dart parity suites. The interactive fixture client accepts `edit TEXT`, `offline`, `online`, `status` and `quit`. Its direct Model observers read committed local projection; named Mutations remain durable across offline close/reopen.
