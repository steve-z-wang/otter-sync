// Run by integration/action-e2e/action.test.mts against its real backend: a
// generated Dart client queues PublishEntry in an application transaction and
// deletes its local Composition as that call's `local` companion. The TS side
// rejects the Entry identities ending in `-no` and checks what the backend
// received.
import 'dart:io';
import 'dart:isolate';
import 'package:axton/axton.dart' as sdk;

import '../action-e2e/generated.dart' as app;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

app.Composition draft(String id) => app.Composition(id: id, title: 'draft $id', body: 'body $id');

/// PublishEntry's business input for [entry], built from [composition].
({app.Entry entry, List<app.Media> media, app.Placement placement}) input(String entry, app.Composition composition) => (
  entry: app.Entry(id: entry, title: composition.title, body: composition.body),
  media: [
    app.Media(id: '$entry-m1', entryId: entry, url: 'one.jpg'),
    app.Media(id: '$entry-m2', entryId: entry, url: 'two.jpg'),
  ],
  placement: app.Placement(id: '$entry-p', entryId: entry, journal: 'daily', position: 1),
);

Future<int> pending(app.GeneratedClient client) async => (await client.syncState())['pending'] as int;

Future<app.Composition?> composition(app.GeneratedClient client, String id) => client.models.composition.get(app.CompositionIdentity(id: id));

bool same(app.Composition? row, app.Composition expected) => row != null && row.id == expected.id && row.title == expected.title && row.body == expected.body;

Future<void> main(List<String> args) async {
  final [url, path, libraryPath] = args;
  final server = sdk.SyncServer(url: url, token: () => 'alice');
  var localRuns = 0;
  // A pending `Call.wait()` does not keep a Dart isolate alive (pre-existing,
  // https://github.com/zanminwang/axton/issues/177: the bridge holds the
  // isolate only while an open, task or close is outstanding), so this script
  // holds it until it finishes.
  final keepAlive = ReceivePort();
  try {
    // Offline: one local commit queues two calls, each deleting its own Composition.
    var client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath);
    try {
      for (final id in ['dart-comp-ok', 'dart-comp-no', 'dart-comp-side']) await client.models.composition.create(draft(id));
      await client.transaction((tx) async {
        final ok = await tx.models.composition.get(const app.CompositionIdentity(id: 'dart-comp-ok'));
        final no = await tx.models.composition.get(const app.CompositionIdentity(id: 'dart-comp-no'));
        check(ok != null && no != null, 'the transaction reads both Compositions');
        final okInput = input('dart-entry-ok', ok!);
        final okCall = await tx.mutations.publishEntry(
          entry: okInput.entry,
          media: okInput.media,
          placement: okInput.placement,
          local: (local) async {
            localRuns++;
            check((await local.models.entry.get(const app.EntryIdentity(id: 'dart-entry-ok')))?.title == ok.title, 'local reads its own call\'s optimism');
            await local.models.composition.delete(const app.CompositionIdentity(id: 'dart-comp-ok'));
          },
        );
        try {
          await okCall.wait();
          throw StateError('a wait before commit must fail');
        } on sdk.CallError catch (error) {
          check(error.code == 'transaction_uncommitted', 'wait before commit: ${error.code}');
        }
        final noInput = input('dart-entry-no', no!);
        await tx.mutations.publishEntry(
          entry: noInput.entry,
          media: noInput.media,
          placement: noInput.placement,
          local: (local) async {
            localRuns++;
            await local.models.composition.delete(const app.CompositionIdentity(id: 'dart-comp-no'));
          },
        );
        await tx.models.composition.update(const app.CompositionIdentity(id: 'dart-comp-side'), const app.CompositionPatch(title: app.Present('edited in the transaction')));
      });
      check(localRuns == 2, 'each callback ran once: $localRuns');
      check(await pending(client) == 2, 'both calls committed together');
      check(await composition(client, 'dart-comp-ok') == null && await composition(client, 'dart-comp-no') == null, 'companion deletes committed');
      await client.models.composition.update(const app.CompositionIdentity(id: 'dart-comp-side'), const app.CompositionPatch(body: app.Present('edited after commit')));
    } finally {
      await client.close();
    }

    client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath);
    try {
      check(await pending(client) == 2, 'queued calls survived reopen');
      check(await composition(client, 'dart-comp-ok') == null && await composition(client, 'dart-comp-no') == null, 'companion deletes survived reopen');
    } finally {
      await client.close();
    }

    client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath, server: server);
    try {
      for (var attempt = 0; attempt < 500 && await pending(client) != 0; attempt++) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
      check(await pending(client) == 0, 'reopened calls settled');
      check(localRuns == 2, 'reopen and settlement never re-run local: $localRuns');
      check(await composition(client, 'dart-comp-ok') == null, 'acceptance keeps the deletion');
      check(same(await composition(client, 'dart-comp-no'), draft('dart-comp-no')), 'rejection restores the Composition');
      check(
        same(await composition(client, 'dart-comp-side'), const app.Composition(id: 'dart-comp-side', title: 'edited in the transaction', body: 'edited after commit')),
        'independent writes survive both outcomes',
      );
      check((await client.models.entry.get(const app.EntryIdentity(id: 'dart-entry-ok')))?.title == 'draft dart-comp-ok', 'accepted Entry holds authority');
      check(await client.models.entry.get(const app.EntryIdentity(id: 'dart-entry-no')) == null, 'rejection removes the optimism');

      // Online: the returned Call observes the rejection after the local commit.
      await client.models.composition.create(draft('dart-comp-live-no'));
      final live = await client.transaction((tx) async {
        final source = await tx.models.composition.get(const app.CompositionIdentity(id: 'dart-comp-live-no'));
        final liveInput = input('dart-entry-live-no', source!);
        return tx.mutations.publishEntry(
          entry: liveInput.entry,
          media: liveInput.media,
          placement: liveInput.placement,
          local: (local) async {
            localRuns++;
            await local.models.composition.delete(const app.CompositionIdentity(id: 'dart-comp-live-no'));
          },
        );
      });
      final outcome = await live.wait();
      check(outcome is app.CallFailure<app.PublishEntryOutput> && outcome.error.code == 'publish.rejected' && outcome.error.execution == 'rejected', 'live call rejected: $outcome');
      check(localRuns == 3, 'the live callback ran once: $localRuns');
      check(same(await composition(client, 'dart-comp-live-no'), draft('dart-comp-live-no')), 'the rejected live call restored its Composition');
      await client.models.composition.update(const app.CompositionIdentity(id: 'dart-comp-live-no'), const app.CompositionPatch(title: app.Present('editing again')));
      check((await composition(client, 'dart-comp-live-no'))?.title == 'editing again', 'the restored Composition is editable');
      stdout.writeln('Dart transactional PublishEntry companion: passed');
    } finally {
      await client.close();
    }
  } finally {
    keepAlive.close();
  }
}
