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
completed Mutation response, hold initial handshakes and schema materializations,
and delay a completed Query response. SQLite triggers independently abort local
acknowledgement and settlement commits after the real cloud has accepted work. These controls change transport timing, not protocol authority.
`client.mjs` also starts a process that exits immediately after a committed
local transaction, then reopens its Store without the original Call waiter.

The P7 scenario sends supported/unsupported/supported calls through real HTTP and PostgreSQL, checks exact saved-result replay, retains structural HTTP400 for malformed JSON/digests, and reopens the SQLite client before the next Batch. Both an unretained name and an unsupported version become durable member refusals without executing their handler.

PostgreSQL's adapter reports the last original serialization error after its
initial attempt and three retries fail. The P7 oracle requires an exact object
bijection between reported errors and transaction escapes, four attempts and a
typed retryable failure; unmatched and nonretryable defects fail the test.
An independent case injects four raw failures inside the initial handshake's
adapter body, checks HTTP500 and no account framework/domain writes, then proves
SDK recovery through a successful handshake and one named Mutation execution.
Existing adapter/native tests own retry classification and rollback internals.

The tests assert exact Batch retry, refusal isolation, private settlement,
later local ownership, independent Store identities, zero-cursor Bootstrap,
fresh read snapshots, Store modes, direct-delete refill, physical file aliases,
stale Query protection, initial-start ordering, durable completion lookup, and
Dart's typed completion across the same actual host. The rollover fixture checks
desired-schema ownership, preserved Bootstrap coverage, and activation of a
newly selected held Model. A separate versioned fixture retains Model and
Mutation v1 history while serving v2; it adds a nullable field at the same
record cursor through owned rematerialization. Query enrollment is tested both for implicit absence
and deliberate tracking through the authenticated Stream context.

The deterministic `crates/sim/src/scenario05.rs` runner complements this gate
with receipt/authority permutations and five SQLite reopen boundaries. Its
cloud acknowledgements are fixtures; it does not substitute for this host.

`AXTON_GATE_TESTS` optionally selects test names for diagnosis; an unset value
runs the complete gate. `AXTON_GATE_TRACE=1` prints fixture requests and response
summaries. Neither control changes protocol behavior or bypasses native builds.
