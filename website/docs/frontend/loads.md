# Bootstrap and history

Protocol 4 has no Load declarations, handlers or job manager. Choose `@@bootstrap` on Models needed at initial open; use named Queries for business reads and Fetch for one Model identity.

```model
model Entry {
  id String
  text String
  @@id(id)
  @@bootstrap
}
```

Open the client with its single Stream and stable connection identity, then await the full initial load:

```ts
await client.bootstrap();
```

```dart
await client.bootstrap();
```

The backend's typed `bootstrap({ctx})` callback may track visible identities through `ctx.stream.track` or explicit `ctx.streams([...]).track`. It cannot invalidate or perform business writes. Loaders remain the viewer authorization authority for materialized content and canonical absence.

Bootstrap persists an immutable bounded identity manifest and captured starting boundary. Each returned unit includes its required constraint closure and commits atomically with ordinal coverage. Tail capture does not advance delivery progress; completion also requires real Stream delta catch-up through the captured tail. Empty manifests have the same completion predicate. Closing/reopening resumes the saved work under the same Store incarnation.

Initial selection preserves `@@bootstrap`: an unmarked Model does not become historical initial data merely because it shared a publication transaction with a marked Model. Rematerialization additionally covers explicitly held authoritative identities and absences at their existing positions. Internal Mutation receipt recovery uses a receipt-proved target manifest, without rerunning the application's Bootstrap callback or implicitly enrolling records.

A transport page can contain several independent units. A failed atomic unit advances no content or progress from that unit. The engine may reduce subsequent page requests so earlier independent units can proceed; it cannot split required uniqueness/dependency closure. An explicitly oversized required group fails within configured bounds instead of silently skipping it.

Query `once` is a separate saved invocation result. It does not materialize a Stream or replace Bootstrap. See [request storage and once](client-api.md#storing-model-results).
