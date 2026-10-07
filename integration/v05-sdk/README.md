# Protocol 5 host gate

Run from the repository root:

```sh
bash integration/v05-sdk/run-host.sh
```

The runner builds this checkout's native libraries, regenerates typed SDKs,
checks TypeScript and Dart, and starts a disposable PostgreSQL cluster. Every
client uses Rust/SQLite and the generated SDK; every server uses the generated
backend with application handlers and viewer Loaders.

`server.mjs` forwards real HTTP and WebSocket bytes. Named faults discard a
completed Mutation response, hold initial handshakes, and delay a completed
Query response. These controls change transport timing, not protocol authority.
`client.mjs` also starts a process that exits immediately after a committed
local transaction, then reopens its Store without the original Call waiter.

The tests assert exact Batch retry, refusal isolation, private settlement,
later local ownership, independent Store identities, zero-cursor Bootstrap,
fresh read snapshots, Store modes, direct-delete refill, physical file aliases,
stale Query protection, initial-start ordering, durable completion lookup, and
Dart's typed completion across the same actual host.

The deterministic `crates/sim/src/scenario05.rs` runner complements this gate
with receipt/authority permutations and five SQLite reopen boundaries. Its
cloud acknowledgements are fixtures; it does not substitute for this host.
