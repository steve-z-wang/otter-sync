# Getting started

Run the collaborative To-do example from source: one React Native app installed on two iOS simulators as two independent phones, each with its own local SQLite database, synchronizing through your own TypeScript backend on Prisma/PostgreSQL. You will add a task on one phone, complete it on the other, and keep working while one phone is offline.

## Prerequisites

Use macOS with:

- Xcode with an installed iOS simulator runtime, and CocoaPods.
- Node.js 22.18 or newer and npm.
- Rust/rustup, using the repository's `rust-toolchain.toml`, plus the `aarch64-apple-ios-sim` target.
- Python 3 and a C/C++ build toolchain for the native Node addon.
- PostgreSQL tools `initdb` and `pg_ctl` on `PATH`.
- Internet access for the first dependency/build setup.

Run all commands below from the repository root. Packages have not been published; these instructions use the checked-in source and generated APIs. Linux hosts can run the backend and the host tests but not the simulators.

## 1. Read the schema

The whole application contract is [models/todo.model](https://github.com/zanminwang/axton/blob/main/examples/todo/models/todo.model):

```text
model User {
  id String
  name String
  @@id(id)
}

model Todo {
  id String
  title String
  done Bool
  createdById String
  createdBy User @reference(via: [createdById])
  @@id(id)
}

mutation AddTodo(todo Todo.create)
// Returns the committed Todo as its business result: an explicit output, read
// through the Loader, independent of the input it happens to share a name with.
mutation SetTodoDone(todo Todo.update<done>) { todo Todo }
```

`AddTodo` creates a task and `SetTodoDone` changes only `done`. A Model input is not a result: once either call completes, the server's version of the task is already in the local database. `AddTodo` declares no outputs; `SetTodoDone` declares a `todo` output, and its handler returns the identity of the Todo to read for it. Both are Mutations because they change business state. The compiler generates durable `client.mutations.addTodo` / `setTodoDone` and direct `client.mutations.call` methods, plus typed backend `Mutations` / `Loaders` from this file.

## 2. Start the backend

```sh
git clone https://github.com/zanminwang/axton.git
cd axton
bash examples/todo/run.sh
```

The runner builds the native runtime, generates the interfaces, installs example dependencies, starts a private disposable PostgreSQL cluster and seeds the users Alice and Bob with three tasks. Wait for:

```text
To-do backend listening at http://127.0.0.1:4242
```

The backend uses development authentication: the bearer token `alice` or `bob` is the whole credential. Keep this terminal open. Stopping the runner removes its temporary database; it is not a persistent deployment.

## 3. Run two phones

Build the Rust simulator slice and the Expo app once, then install the same app on two simulators. Without configuration the app is Alice; a `config.json` in the second installation's Documents directory makes it Bob. The exact commands are in the [example README](https://github.com/zanminwang/axton/blob/main/examples/todo/README.md#run-two-phones).

Each installation keeps its own database and client identity, so the two simulators behave as two phones. Expo Go cannot load the native module; use the native build.

## 4. Try it

1. Add a task on Alice's phone. It appears in her list at once and on Bob's phone after the backend accepts it.
2. Mark it done on Bob's phone. Both lists converge.
3. Interrupt one phone's connection to the backend (the simulator runner below does this with a per-phone proxy), add a task and complete it. Both writes commit locally and stay queued. Relaunch the app: the rows, the queue and the client identity survive. Restore networking: the create is pushed before its dependent update, missed changes are caught up over HTTP, and the live stream resumes.

The backend trims titles, rejects empty ones (`todo.title_empty`), requires the creator to be the authenticated user, and reports a primary-key collision as `todo.id_conflict`. A rejected Mutation's optimistic change is removed locally; its handle's `wait()` resolves with a `CallError` and the rejection remains inspectable.

## Understand the files

| File | Role |
| --- | --- |
| [models/todo.model](https://github.com/zanminwang/axton/blob/main/examples/todo/models/todo.model) | Model and Mutation contract |
| [generate.sh](https://github.com/zanminwang/axton/blob/main/examples/todo/generate.sh) | Compiles the schema into `generated/node` and `generated/mobile` |
| [server.mts](https://github.com/zanminwang/axton/blob/main/examples/todo/server.mts) | Handlers, loaders, development authentication and database setup |
| [seed.mts](https://github.com/zanminwang/axton/blob/main/examples/todo/seed.mts) | Create-if-missing demo users and tasks |
| [mobile/src/todo.ts](https://github.com/zanminwang/axton/blob/main/examples/todo/mobile/src/todo.ts) | Opens the generated client per user, subscribes to `todo:demo` for changes from then on, calls `bootstrap()` in the background so a fresh install also gets the tasks published earlier, exposes `watch`, `add` and `setDone` |
| [mobile/src/TodoScreen.tsx](https://github.com/zanminwang/axton/blob/main/examples/todo/mobile/src/TodoScreen.tsx) | The one screen, rendered from watch callbacks |

Next, [define your own schema](schema/define.md), browse the [API reference](api-index.md), or use the [client setup guide](frontend/setup.md).

## Verify

`bash integration/e2e/todo-run.sh` runs the backend scenarios (happy path, every rejection code, an unknown identity, a lost push response, offline add-then-done across a reopen, opposing completions) through real clients against a temporary backend. `bash integration/platform/run_todo_ios_smoke.sh` repeats the two-phone flow on disposable simulators with a real per-phone network fault. `bash integration/e2e/run.sh` runs the Node and Dart round-trip fixture that the API reference examples are written against.
