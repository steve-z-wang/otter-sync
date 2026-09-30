# Channel removal measurement

A local diagnostic for `backend.transaction(({channel}) => channel(name).remove({tag: 'X'}))` over the production backend, native server and PostgreSQL adapter. It measures 1, 1,000 and 10,000 live members, each carrying X and Y, three times each. Removing X must release the whole member, including Y.

Run from the repository root with Node, Python, PostgreSQL tools and an already built compatible native addon:

```sh
bash integration/performance/channel-removal/run.sh /tmp/channel-removal-evidence.json
```

The runner creates and removes its own temporary PostgreSQL cluster. It does not build native artifacts or generate fixtures. The default output is `evidence.json` beside the harness; that checked-in file records one measured local snapshot, not a performance guarantee. Review the recorded addon fingerprint when comparing snapshots.

Each sample starts with empty framework and business tables. The fixture has N business records, N framework records, one channel, N live memberships, two tags, 2N tag associations and N upsert log rows. Setup is excluded from the timer, SQL count and WAL interval. No concurrent workload is generated.

The SQL count includes successful top-level `pg` calls between entry and return of the public removal transaction, including BEGIN and COMMIT. PostgreSQL trigger substatements are excluded. `returnedOrAffectedRows` reports the driver's rowCount per statement, while before/after fixture counts expose association cascades. The elapsed public transaction time includes native reduction, pool access, serialization and commit; the separate transaction time starts before BEGIN and ends after COMMIT. WAL is the difference between cluster insert LSNs before and after the committed removal, including triggers, commit and full-page images. Background PostgreSQL activity can affect this cluster-wide observation.

Pulls use the actual public server API with `channel-membership-v1`, beginning at the pre-removal cursor. Every <=50-event response is drained until the returned range has `to === head`; no extra empty client pull is counted. Assertions verify all N distinct identities, removal kind, terminal cursor, preserved business/record/log counts, emptied memberships/tags/associations, and zero Loader invocations during both removal and all pulls. Wire bytes are exact UTF-8 JSON body sizes, without HTTP framing or compression. Identity bytes count only the JSON identity object. The artifact includes the concatenated-body SHA256 and representative exact first/terminal response bodies (first repetition per size).

The simulation capacity diagnostic is a separate command owned by the release verification task. This harness does not run the application client or establish client reconciliation correctness.
