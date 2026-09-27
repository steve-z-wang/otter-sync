// Generated Dart API misuse that must NOT analyze. verify.sh runs `dart analyze`
// on this directory alone and requires every error below to be reported; the
// TypeScript twin is the `@ts-expect-error` file beside it, misuse.ts, with the
// older negatives still in ../test.ts.
// ignore_for_file: unused_local_variable
import '../generated.dart';

void storeMisuse() {
  final hooks=StoreHooks(entry:(tx,changes) async {
    final change=changes.first;
    if(change is StoreUpsert<EntryIdentity,Entry>){
      change.row.missing;
    }
    if(change is StoreDelete<EntryIdentity,Entry>){
      change.row;
    }
    tx.mutations;
  });
  final unknown=StoreHooks(unknown:(tx,changes){});
}

void misuse(GeneratedClient client, GeneratedTransaction tx, Entry row) {
  // lists cannot be query predicates
  client.models.entry.query(where: const EntryFilter(tags: Present(['x'])));
  // enum ordering is not defined
  client.models.entry.query(orderBy: [EntryOrder(EntryOrderField.byStatus)]);
  // date filter must be a DateTime
  client.models.entry.query(where: const EntryFilter(at: Present('2026-01-01')));
  // watch is not available inside a transaction
  tx.models.entry.watch();
  // generated transactions expose neither named mutations nor actions
  tx.mutate;
  tx.actions;
  // raw transactions expose neither named mutations nor actions
  tx.transaction.mutate;
  tx.transaction.actions;
  // identity is immutable in patch
  client.mutate.editEntry(entry: EditEntryEntryUpdate(identity: EntryIdentity(id: row.id), id: 'bad'));
  // mutation forbids tags
  client.mutate.editEntry(entry: EditEntryEntryUpdate(identity: EntryIdentity(id: row.id), tags: const Present(['x'])));
  // nonnullable title
  client.mutate.editEntry(entry: EditEntryEntryUpdate(identity: EntryIdentity(id: row.id), title: const Present(null)));
  // enum typo
  final Entry bad = Entry(id: row.id, title: row.title, note: row.note, at: row.at, tags: row.tags, status: Status.typo);
  // deprecated enum value, field and slot are reported with their reasons
  final Status old = Status.archived;
  final int? index = const Counter(id: 'c', index: 1).index;
  client.mutate.removeEntries(entries: const [], maybe: EntryIdentity(id: row.id));
  // a create input still requires fields without a creation default
  tx.models.draft.create(const DraftCreate());
  // a defaulted nullable field distinguishes omission from null through Present
  tx.models.draft.create(const DraftCreate(memo: null, note: 'plain'));
  // the complete record stays complete: defaulted fields are still required
  tx.models.draft.create(Draft(body: 'x', mood: Mood.calm, created: DateTime.utc(2020), note: null, memo: null));
}

// The Scope facade refuses the same misuse the TypeScript twin does
// ([#150](https://github.com/zanminwang/axton/issues/150)): an immutable status,
// a fixed Scope, and no get-only accessor.
void scopeMisuse(GeneratedClient client, Subscription subscription) {
  // a status snapshot is immutable
  subscription.status.active = false;
  // the Scope a handle names is fixed for its lifetime
  subscription.scope = 'other';
  // the first Scope API deliberately omits a get-only accessor
  client.scopes.get('project:123');
  // The load status is part of that immutable snapshot, and this milestone
  // introduces no task-cancel or forced-refresh API
  // ([#151](https://github.com/zanminwang/axton/issues/151)).
  subscription.status.bootstrap.phase = BootstrapPhase.complete;
  subscription.bootstrap.cancel();
  subscription.refresh();
}
