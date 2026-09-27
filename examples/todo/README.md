# Collaborative To-do

Two phones, one shared list. Alice and Bob each run the same React Native app against their own local SQLite database; the app writes locally first and synchronizes through an application-owned TypeScript backend on PostgreSQL. This is the example behind [#31](https://github.com/zanminwang/axton/issues/31).

The app has two Mutations: add a task and mark it done. Everything else (assignment, replies, editing, deleting) is deliberately absent so the schema, backend and screen stay readable.

## Layout

| Path | Owns |
| --- | --- |
| `models/todo.model` | The schema: `User`, `Todo`, `AddTodo`, `SetTodoDone` |
| `generate.sh` | Compiles the schema into `generated/node` (Node backend and tests) and `generated/mobile` (React Native) |
| `prisma/schema.prisma`, `server.mts`, `seed.mts` | Backend tables, handlers, loaders, development authentication and the seed data |
| `run.sh` | Builds, generates, starts a disposable PostgreSQL cluster and the backend on port 4242 |
| `mobile/` | The Expo app: one screen, a session adapter over the generated client, launch configuration |
| `../../integration/e2e/todo.test.mjs` | Backend scenarios through real Rust clients, HTTP/WebSocket and PostgreSQL |
| `../../integration/platform/run_todo_ios_smoke.sh` | Two independent simulators: live edits, lost response, offline add-then-done, relaunch, reconnect |

## Prerequisites

macOS with Xcode and an iOS simulator runtime, CocoaPods, Node, Rust with the `aarch64-apple-ios-sim` target, and PostgreSQL command-line tools (`initdb`, `pg_ctl`). The app pins Expo 57, React Native 0.86.3 and React 19.2.3 in `mobile/package-lock.json`; the root toolchain is unchanged.

## Run the backend

```sh
bash examples/todo/generate.sh
bash examples/todo/run.sh
```

`run.sh` creates a temporary cluster, seeds Alice, Bob and three tasks, and listens at `http://127.0.0.1:4242`. Stop it with Ctrl-C; the cluster is deleted. To keep data between runs, start the backend yourself with `DATABASE_URL` pointing at your own database:

```sh
(cd examples/todo && npm ci && npm run generate)
DATABASE_URL=postgresql://... PORT=4242 node examples/todo/server.mts
```

Seeding is create-if-missing, so an ordinary restart keeps edits. Reset by dropping the database (or the temporary cluster) and starting again.

A subscription starts at the first head the server acknowledges for it ([#150](https://github.com/zanminwang/axton/issues/150)): it delivers what is published from then on, not the tasks the Scope already held.

**What that means on a first run.** `run.sh` seeds Alice, Bob and three tasks before any phone exists, so subscribing alone would give a freshly installed app none of them: no tasks, and no `User` row. `mobile/src/todo.ts` therefore calls `subscription.bootstrap()` right after registering the Scope ([#151](https://github.com/zanminwang/axton/issues/151)). That call asks for everything published to the Scope before this installation's origin, and the seeds arrive on a fresh install with nothing republished.

It is deliberately not awaited. The registration is submitted when the call is made, so the load runs in the background while the screen renders whatever is already local, and the returned rejection goes to the same error channel as a connection failure. Readiness is unchanged: `mobile/src/useTodos.ts` still treats the arrival of the user's own `User` row as the signal that the list has loaded, because that is the row the screen needs, and it arrives from the load like any other. Waiting for `status.bootstrap.phase === "complete"` instead would hold the screen until the whole Scope had been processed, which is not what this list needs.

Completion means the historical interval and the live barrier were processed, not that a snapshot was taken or that every record resolved successfully: a record whose Loader failed is reported through the error channel and is corrected the next time it is delivered. `app.publishSeeds()` (which touches the seeds again) remains for tests that are about redistribution itself.

## Run two phones

Build the Rust simulator slice and the app once:

```sh
rustup target add aarch64-apple-ios-sim
bash packages/client-react-native/native-module/scripts/build-ios.sh simulator
cd examples/todo/mobile
npm ci
npx expo prebuild --platform ios --no-install
(cd ios && pod install)
npm run ios:build
```

`ios:build` produces `ios/build/Build/Products/Release-iphonesimulator/AXTONTodo.app` with embedded JavaScript. `npm run ios` (a development build with Metro) also works for iterating on the screen; Expo Go cannot load the native module.

Identity is launch configuration, not a login: without configuration the app is Alice on `http://127.0.0.1:4242`. To make a second simulator Bob, write `config.json` into that installation's Documents directory and launch:

```sh
udid=<simulator udid>
xcrun simctl install "$udid" ios/build/Build/Products/Release-iphonesimulator/AXTONTodo.app
docs="$(xcrun simctl get_app_container "$udid" dev.axton.Todo data)/Documents"
mkdir -p "$docs" && echo '{"user":"bob","url":"http://127.0.0.1:4242"}' > "$docs/config.json"
xcrun simctl launch "$udid" dev.axton.Todo
```

Each installation keeps its own database file and client identity under Application Support, so two simulators are two independent phones. Add a task on one; it appears on the other after the backend accepts it. Mark it done on the other; both converge.

## Walkthrough

1. **Two tables.** `models/todo.model` declares `User(id, name)` and `Todo(id, title, done, createdById)` with `createdBy` as a reference. The backend adds no other application tables; AXTON's own sync tables come from `packages/postgres/migration.sql`.
2. **Two Mutations.** `AddTodo` creates a task and declares no result; `SetTodoDone` updates only `done` and declares an explicit `{ todo Todo }` output. The generated client exposes `client.mutations.addTodo` and `client.mutations.setTodoDone` for durable local commits. Each returns a `Call` whose `wait()` resolves once the backend's authority for the input is applied locally, with the declared result (none for `AddTodo`) or a rejection. `client.mutations.call.setTodoDone` executes directly and returns its committed result. The generated backend requires one typed `mutations` handler per retained version.
3. **Backend rules.** `server.mts` trims the title and refuses empty ones, requires the creator to be the authenticated user and `done` to start false, and turns only a proven primary-key collision into `todo.id_conflict`. Handlers throw `CallRejected` for these business refusals. The Mutation input is already the changed record: AXTON stamps it and returns its authority to the caller. `addTodo` adds the new task to Channel `todo:demo` once (`ctx.channel(CHANNEL).todo.add(...)`), so that change and every later one reach the Channel's subscribers; `setTodoDone` needs no enrollment and returns `{ todo: { id } }`, which AXTON reads through the Loader for its result. `seed.mts` touches and enrolls the demo rows in one `backend.transaction`.
4. **Local watch.** `mobile/src/todo.ts` opens the generated client on a per-user database, registers the durable subscription with `client.scopes.subscribe('todo:demo')`, and exposes `watch`, `add` and `setDone`. The registration is durable and offline-capable, and its origin is the first acknowledged head, so the screen fills with what is published from then on. `add` and `setDone` return after the local commit; the screen renders from watch callbacks only, never from a second in-memory store.
5. **Offline and back.** Without a network, adds and completions commit locally and queue in SQLite. Killing and relaunching the app keeps the rows, the queue, the client identity and the subscription with its committed delivery position. On reconnect the engine pushes the queued create before its dependent update, catches up the changes it missed over HTTP from that position, then follows the live stream; reconnecting never rewinds the position and never reloads the Scope from the beginning. `integration/e2e/todo.test.mjs` verifies the current Mutation flow with native SQLite clients and PostgreSQL. After building a new app, `integration/platform/run_todo_ios_smoke.sh` can exercise the flow on two simulators with a per-phone network fault; the recorded simulator run below predates the Action migration.

Engine-owned tables, cursors and receipts are described in [client storage](../../docs/engineering/architecture/client/storage/README.md) and the [guarantees](../../docs/engineering/guarantees.md).

## Tests

```sh
bash integration/e2e/todo-run.sh
bash integration/platform/run_todo_ios_smoke.sh
```

The e2e runner covers the happy path, every rejection code, an unknown identity, a lost push response, a backend restart on the same database, offline add-then-done across a reopen, and opposing completions in both commit orders. It also checks that a direct result retains its Loader snapshot while a separate durable edit stays queued and optimistic, and that retrying a direct request returns its stored result after a later backend update. The simulator runner needs the Release app above; it creates two disposable simulators (newest installed iOS runtime and first iPhone device type by default, or `AXTON_TODO_SIM_RUNTIME`/`AXTON_TODO_SIM_DEVICE`) and deletes only those. Its harness backend (`integration/platform/todo-ios/server.mts`) republishes nothing: both phones meet the seeds through the app's own `subscription.bootstrap()`. The simulator runner has no host in the gate, so that path was last exercised by hand.

## Evidence

The original two-device smoke evidence below was recorded before the Action migration. It establishes the mobile host and screen behavior at that commit; the current Mutation schema and generated bindings are covered by the host E2E suite above.

Verified 2026-09-15 on branch `codex/todo-mobile` on the working tree later committed as `bd1678c` and rebased onto `c54ff70` (React Native support, #100); the SDK follow-up between those commits changed only build tooling and test timeouts with Xcode 26.5 (17F42), the iOS 26.5 simulator runtime (23F77), two disposable iPhone 17 simulators, Node 26.4.0, cargo 1.98.1, CocoaPods 1.16.2, Expo 57.0.22, React Native 0.86.3 and React 19.2.3.

```sh
bash integration/e2e/todo-run.sh                      # 15 scenarios, all passed
bash integration/platform/run_todo_ios_smoke.sh       # PASS: two To-do phones, lost-response retry, offline add-then-done, process restart and convergence
```

The Release app embedded `main.jsbundle` (1.5 MB, no Node built-ins, N-API binary or Node `ws`) and linked the Rust carrier. Phase results recorded by the app on each simulator:

| Phase | Result |
| --- | --- |
| alice/bob online | both loaded the seeded users and three tasks; Alice's add appeared on Bob live; Bob's completion appeared on Alice live; queues drained |
| alice offline | her proxy answered 503 and refused upgrades; a new task and its completion committed locally and showed through the watch; 2 pending |
| alice restart | after `simctl terminate`/`launch` while still disconnected: same client ID, task still done, 2 pending |
| bob remote | Bob added a task while Alice was disconnected |
| alice settle, bob observe | both converged to the six PostgreSQL rows with 0 pending |

Product screen: with the same backend, Alice and Bob were each launched without a test phase on their own simulator (one at a time on a memory-limited host); both showed the avatar header (A on blue, B on green), the To-do heading, the same three seeded tasks, and only the checkbox rows, the add field and the plus button. The SDK harness scenario was also rerun on the final SDK commit and passed.

Backend after the run: five handler executions (three adds, two completions) despite one deliberately dropped push response; Alice's and Bob's client IDs differed. Screenshots and JSON assertions were written to the printed evidence directory.

Limits: arm64 iOS simulator, app in the foreground. Physical devices, Android, background delivery while iOS suspends the app, and the x86_64 simulator slice are not covered. The web version is tracked separately in #72.
