# Protocol 5 adoption

Protocol 5 changes wire and local storage contracts. It requires an explicitly authorized breaking release; the current package version is not a decision about the new release's semantic version. Use [release tooling](releasing.md) to choose and synchronize the version and exact package pins.

## Existing files and pending work

Open protocol 5 with a fresh SQLite file. An unsupported file is refused without changing its database, WAL or SHM; the old release can still open it. There is no automatic wipe or in-place upgrade. Keep the old release and backend available while unresolved protocol-4 work is drained or exported using the old release under a separately owned recovery process. A successful fresh-file test proves no recovery of old pending work. This candidate supplies no old-format migration or compatibility bridge.

Use a separate file per Stream binding and one active Client per physical file. Account switching, file retention and authentication remain application responsibilities. Update backend explicit tracking and generated interfaces together; returning Models does not enroll them. Query/Fetch snapshots have null cursors and cannot replace protected Stream authority. Mutation acknowledgment and locally committed settlement are separate completion boundaries.

## Candidate evidence

Before declaring implementation complete, review A1–A15, resilience and measured capacity against the final accepted commit. Record commands, exit status, logs, source hashes and faults in a new work record. Preserve frozen specifications and historical work logs.

Run from that candidate checkout:

```sh
bash scripts/test.sh
node scripts/release/version.mjs check
node --test integration/release/*.test.mjs
bash integration/release/verify-installed.sh
```

The joined native gate `integration/v05-sdk/run-host.sh` must remain in the full test script. Run the paired `integration/v05-sdk/run-capacity.sh` against actual PostgreSQL carriers and SQLite apply, retaining fragment/header costs, staging, fence duration and maximum atomic apply time. Distinguish missing tools, network or disk prerequisites from failed product assertions.

Installed verification must test the candidate's packed bytes. Registry mode tests a published version, not an unpublished candidate. A host-only pack proves only its selected host addon, CLI and Dart library; staged Dart verification requires the matching archive and libraries. The full release inventory includes darwin-arm64 and linux-x64-gnu hosts, iOS device and simulators, and Android arm64/v7a/x86_64. Matching tagged-commit mobile artifacts and native/device smoke remain separate evidence requirements.

Large required components are bounded but can hold the SQLite writer and backend publication fence for seconds. Treat the capacity measurements as explicit deployment costs, not a latency promise or process-memory measurement. Validate available temporary staging space on the target device.

Publishing, backend deployment, old-server retirement and legacy-table cleanup are separate authorized actions. Completing documentation or passing host gates authorizes none of them.
