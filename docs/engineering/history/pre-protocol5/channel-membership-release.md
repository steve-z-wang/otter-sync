# Channel membership: 0.2 release boundary

Historical pre-Scope release observations, retained as evidence of the earlier implementation. These API names and measured costs do not describe the unreleased Scope cutover; [current deployment guidance](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/website/docs/backend/deployment.md#scope-membership-cutover) owns that boundary.

Prepared release notes; coordinated release acceptance and publication are pending. [Deployment cutover](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/website/docs/backend/deployment.md#scope-membership-cutover) owns the compatibility matrix and operator sequence. The [design](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/docs/superpowers/specs/2026-09-30-channel-tags-removal-design.md) records the binding decisions.

## Release notes

Backend Channel adds accept server-only tags; `channel.remove({tag})` removes every whole matching membership, including members with other tags. Ordered declarations settle to one final event per pair. Removing an absent member or unmatched selector allocates no cursor. Tags never become access grants or client state.

Channel delivery now carries per-record upsert/remove provenance. Last-hold release evicts replicated data while preserving pending and device-local work, stamp/absence evidence and independently held children. Authoritative Loader null remains a cascade-capable deletion; Loader error remains a diagnostic. Unsubscribe retains content and holds.

Enrolling Load pages, receipts and direct Mutation responses save explicit membership claims. Replay preserves their original cursors and never enrolls again. Request epoch admission prevents delayed positive bodies from restoring released replicas. Fresh authorized one-shot reads may cache again, with no inferred enrollment or automatic cleanup guarantee.

The PostgreSQL adapter installs eight tables and provides a forward upgrade retaining old tables and compacted removals. Custom hosts need the new operations and Channel/tag constraints. JS/Dart runtimes upgrade local storage additively and reconcile retained subscribed history; all new requests require `channel-membership-v1`. This is an intentional wire and host compatibility boundary.

## Release mechanism

`release-please-config.json` enables both `bump-minor-pre-major` and `bump-patch-for-minor-pre-major`. A plain `feat:` produces a patch while pre-major. Use `feat!:` or a `BREAKING CHANGE` footer for the coordinated 0.2 boundary. Do not manually edit versions. Inspect the release PR's produced package versions and dependency references; private workspace manifest versions are not publication evidence. Publish only after coordinated backend, adapter, tooling, JS and Dart verification.

## Oasis adoption handoff

Oasis's inspected pin is `0.1.1`. Adoption is separate work, targeting exactly `0.2.0` for backend/mobile dependencies and contract tooling:

1. Finish and verify the coordinated AXTON release.
2. Prepare a compatible mobile release and matching dependency/lock updates before backend cutover.
3. Coordinate stopped older writers, server migration and backend cutover with per-platform minimum-build floors and the capability gate.
4. Enable synchronized removal only after that boundary is enforced.

When several Journal domains share one User Channel, tags do not reference-count their access: removing X also removes an X/Y membership. Oasis must decide surviving domain access before declaring removal; automatic cross-Channel holds apply only to separate known Channels. See [Channel selection semantics](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/website/docs/backend/api.md#scopes).

The adoption task chooses actual build floors and runs Oasis's pin/contract checks. No pin, lock, build number or Oasis code is changed here. A package bump alone does not protect old clients.

## Costs and acceptance evidence

Removing N members costs O(N) database row/index/WAL work and O(N) identity delivery, in bounded batches, with no removal Loader calls. Bulk adds still guard each record. Enrolling Loads hold Channel locks across Loader reads. Multiple settlements in a push may deadlock and retry the whole transaction. Repeating the server migration still locks `axton_record` with `ACCESS EXCLUSIVE`.

Measured locally on an Apple M1 Pro (16 GiB), Node 26.4.0 and PostgreSQL 14.23, three fresh-fixture samples per size:

| Members | Public transaction ms (all samples) | SQL calls | WAL bytes (range) | Pull pages | JSON body bytes | Identity bytes | Loaders |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 5.850, 3.957, 3.770 | 8 | 880–904 | 1 | 173 | 21 | 0 |
| 1,000 | 73.106, 73.211, 77.561 | 8 | 591,952–591,976 | 20 | 108,560 | 21,000 | 0 |
| 10,000 | 698.491, 699.440, 775.696 | 26 | 5,897,184–5,897,200 | 200 | 1,096,200 | 210,000 | 0 |

These are observations, not latency guarantees. SQL calls include BEGIN/COMMIT and exclude trigger substatements. WAL is cluster insert-LSN difference, including triggers/commit/full-page images. Setup is excluded; there is no concurrent workload or percentile estimate. Pull byte totals include every actual <=50-event response through `to == head`, with no extra empty pull, HTTP framing or compression. Identities use fixed-width one-field keys. Loader counts cover removal and all pulls. See the [harness boundaries](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/performance/channel-removal/README.md) and [exact environment, samples and response bodies](https://github.com/zanminwang/axton/blob/71b14195b897bc8a42f19e0a83d59dfe8fc76677/integration/performance/channel-removal/evidence.json). Earlier preliminary timings are superseded by this artifact. Retained responses, old tables, log removals and local absence evidence have no TTL or pruning floor.

The repaired production-page capacity diagnostic completed locally: queue 10 observed enqueue p50/p95 0.294/0.417 ms, replay 0.601 ms and 12 SQLite commits; queue 1,000 observed 1.026/2.025 ms, replay 2.866 ms and 1,002 commits. This is a diagnostic, not a correctness gate or throughput guarantee.

Local validation includes 1,205 workspace Rust tests (one intentionally ignored), strict Clippy, formatting, native builds, generated API checks (12 Dart tests and typed negative cases), and 269 binding tests. Release-version checks verified the current 0.1.2 metadata across 20 release units; this is not evidence that 0.2.0 was produced or published.

The native iOS Release smoke passed all seven online/offline/restart/convergence phases on two iPhone 16 simulators running iOS 18.5. The iOS 26.5/iPhone 17 attempt failed after OS boot/launch delays and is not counted as passing. Physical devices, Android and CI platforms remain unverified here.

Review found that repeated valid Load adds could incorrectly validate their combined tags as one declaration. The corrected collector and Rust boundary preserve per-add validation while measuring the canonical union against existing page bounds: 64 tags plus one new tag succeeds, while one 65-tag add is still refused. Focused verification passed 82 Rust tests, 37 collector tests and 21 real-native Load tests.

The unchanged `scripts/test.sh` completed successfully on the final implementation after the obsolete raw protocol fixtures were updated. It covers PostgreSQL persistence, 204 Dart SDK tests, assembled e2e (16), Action e2e (28), Load e2e (21), Todo, documentation snippets and seven installed-package tests verifying all seven npm artifacts. The generated files remained unchanged. Combined implementation review passed after the Load correction.

Local acceptance does not establish coordinated release acceptance. macOS/Linux CI remains a PR gate; physical devices and Android remain unverified here. The eventual 0.2 release PR must verify produced versions and dependency references before publication.
