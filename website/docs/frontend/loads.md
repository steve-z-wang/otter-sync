# Bootstrap and history

Protocol 5 has no Load declarations, handlers or job manager. Choose `@@bootstrap` on Models needed at initial open; use named Queries for business reads and Fetch for one Model identity.

```model
model Entry {
  id String
  text String
  @@id(id)
  @@bootstrap
}
```

Open the client with its single Stream and fresh SQLite file, then await the full initial load:

```ts
await client.bootstrap();
```

```dart
await client.bootstrap();
```

The backend's typed `bootstrap({ctx})` callback may track visible identities through `ctx.stream.track` or explicit `ctx.streams([...]).track`. It cannot invalidate or perform business writes. Loaders remain the viewer authorization authority for materialized content and canonical absence.

Handshake commits initial head S and committed prefix C=S. Bootstrap completion B remains absent until the final complete unit commits. Bootstrap selects marked Models and freezes a finite authority plan; an empty plan still completes. Reopen retains committed progress and resumes the remaining work.

Initial selection preserves `@@bootstrap`; rematerialization also covers held authoritative identities and absence. Internal Mutation recovery materializes exact saved settlement targets without rerunning application Bootstrap preparation or implicitly tracking records.

Every required uniqueness/cascade component commits whole, even when transported in several fragments. Earlier independent units can remain committed after a later unit fails, but progress never crosses its missing coverage. Expiry or capacity refusal claims no completion. Bootstrap completion describes the captured head, not perpetual freshness.

Query and Fetch are fresh invocation snapshots. They do not materialize a Stream or replace Bootstrap. See [request storage](client-api.md#storing-model-results).
