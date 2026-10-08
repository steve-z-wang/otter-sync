# Internal refactor acceptance

Accepted locally on `codex/internal-refactor`, implementation commit `dd74ad55c15461c1afce2d0e5352e1230b0a798f`, from v0.4.2 (`98891f9be99aa89fe237612377d65e5acafce9a1`). The [specification](../specs/2026-10-07-internal-refactor-spec.md) and [plan](../plans/2026-10-07-internal-refactor.md) Tasks 1–9 are complete within the fresh-file scope. [Protocol 5](../../engineering/architecture/protocol/0.5.md) owns the resulting architecture.

## Result

Rust owns the durable Mutation Batch, retry, delivery queue, complete-unit apply, settlement and notifications. SDKs retain the existing binding carrier and perform network effects. One Client owns one physical SQLite file and one Stream. Explicit tracking, null-cursor Query/Fetch snapshots, finite Bootstrap, local transactions and independently settled Mutations remain.

Superseded protocol implementations, stamp admission, generic Load/once/refresh, store hooks, duplicate lifecycle tables and unused harnesses are removed. The final cleanup also removes the SDK Load transport/observer remnants and all five obsolete PostgreSQL upgrade scripts, their package exports and migration-only tests/fixtures. Useful retained behavior has current coverage, including delayed receipts after Remove, historical settlement below the public prefix, actual SIGKILL after server acceptance, whole mixed-Model Query failure and private Model-target no-op settlement. Independent reviews accepted these ports and both final cleanup batches.

There is no framework migration or compatibility bridge. Fresh DDL and immutable refusal of unsupported existing layouts remain. Published Model/Mutation history and current local-write semantics are retained. No old or deployed database was converted or erased.

## Verification

On the implementation commit, `CARGO_INCREMENTAL=0 bash scripts/test.sh` completed with **exit 0**. It rebuilt native artifacts and ran workspace tests, formatting/strict Clippy, TypeScript/generated positive and negative checks, JS/React Native/Dart suites, real PostgreSQL suites, 17 joined native host cases, paired capacity, generated persistence/reopen, end-to-end/To-do, documentation examples and installed-package verification.

The final installed gate packed the candidate, installed it outside the checkout and exercised the installed CLI, generated Node/Dart clients, SQLite reopen and actual PostgreSQL backend. This was local candidate verification, not a registry or publication test. The log is `.superpowers/sdd/task-final-clean-candidate.log`, SHA256 `225589830b56f4e606fcd4e1605ccde3ecf128e98d8e96c46044b6f8bd2d8c58`.

Earlier attempts caught a stale Dart helper import, rejection-ID documentation examples and a cancellation fixture that unintentionally allowed normal catch-up. Each was corrected and reviewed; the cancellation case retains the late-request and cursor assertions. A superseded run was deliberately interrupted. None of those attempts is reported as the final passing gate.

## Capacity and limits

The final debug gate passed four actual PostgreSQL-to-SQLite cases: selected and unique-constrained Models at 10,000 and 100,000 rows. Every case ended with zero queued plans and durable staging progress. Exact carriers are retained under `axton-task7-capacity-evidence.YNLzxB` in the host temporary directory.

Optimized measurements ran at `6b1436a4`; the capacity implementation is unchanged in the accepted candidate. Times below are milliseconds, measured on this host:

| Case | Server fenced request | Native SQLite apply total | Maximum atomic apply |
| --- | ---: | ---: | ---: |
| Selected 10,000 | 560.75 | 483.01 | 30.67 |
| Selected 100,000 | 5,997.37 | 4,795.93 | 131.35 |
| Unique 10,000 | 484.74 | 493.90 | 493.90 |
| Unique 100,000 | 6,105.79 | 5,001.70 | 5,001.70 |

The final debug unique 100,000 case applied its one atomic unit in 19,514 ms. These are explicit large-component costs, not an SLA or process-memory measurement. Optimized logs/carriers: `.superpowers/sdd/task-final-release-capacity-6b1436a4.log` and temporary evidence `axton-task7-capacity-evidence.j8JRtI`. Host installation does not establish iOS/Android device or every target's artifact behavior; [adoption](../../engineering/protocol5-adoption.md) defines those release boundaries.

## Preserved state

The original design remains unchanged: SHA256 `7822f6bf10a11c2775c4335e1356087b38079d76fa8767ae9ad7a4931ee68cbc`. Its notes checkout and user changes are preserved. The main checkout remains clean at `18c771a385a69f39d49e73c0c40b471a2bf5b521`; Capso is unchanged. This work is integrated on the isolated branch, with no PR, push, package-version change, publication or deployment.
