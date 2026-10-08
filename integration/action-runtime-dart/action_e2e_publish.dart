import 'dart:io';
import 'dart:isolate';
import 'package:axton/axton.dart' as sdk;
import '../action-e2e/generated.dart' as app;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

app.Composition draft(String id) =>
    app.Composition(id: id, title: 'draft $id', body: 'body $id');
app.PublishEntryInput input(String id, app.Composition source) =>
    app.PublishEntryInput(
      entry: app.Entry(id: id, title: source.title, body: source.body),
      media: [
        app.Media(id: '$id-m1', entryId: id, url: 'one.jpg'),
        app.Media(id: '$id-m2', entryId: id, url: 'two.jpg'),
      ],
      placement: app.Placement(
        id: '$id-p',
        entryId: id,
        journal: 'daily',
        position: 1,
      ),
    );
Future<void> main(List<String> args) async {
  final [url, path, library] = args;
  final keepAlive = ReceivePort();
  var runs = 0;
  Future<app.GeneratedClient> open() => app.GeneratedClient.open(
    path: path,
    stream: 'User:alice',
    connection: sdk.StoreConnection(
      url: url,
      token: () => 'alice',
    ),
    libraryPath: library,
  );
  var client = await open();
  try {
    await client.bootstrap();
    await client.connection!.pause();
    await client.transaction((tx) async {
      await tx.models.composition.create(draft('dart-comp-ok'));
      await tx.models.composition.create(draft('dart-comp-no'));
      await tx.models.composition.create(draft('dart-comp-side'));
    });
    await client.transaction((tx) async {
      final ok = await tx.mutations.publishEntry.withTransaction((local) async {
        runs++;
        final source = (await local.models.composition.get(
          const app.CompositionIdentity(id: 'dart-comp-ok'),
        ))!;
        await local.models.composition.delete(source.identity);
        return input('dart-entry-ok', source);
      });
      try {
        await ok.wait();
        throw StateError('uncommitted wait accepted');
      } on sdk.CallError catch (error) {
        check(
          error.code == 'transaction_uncommitted',
          'precommit wait refusal',
        );
      }
      await tx.mutations.publishEntry.withTransaction((local) async {
        runs++;
        final source = (await local.models.composition.get(
          const app.CompositionIdentity(id: 'dart-comp-no'),
        ))!;
        await local.models.composition.delete(source.identity);
        return input('dart-entry-no', source);
      });
      await tx.models.composition.update(
        const app.CompositionIdentity(id: 'dart-comp-side'),
        const app.CompositionPatch(title: app.Present('independent title')),
      );
    });
    check(
      (await client.syncState())['pending'] == 2,
      'two independent durable Calls',
    );
    check(
      await client.models.composition.get(
            const app.CompositionIdentity(id: 'dart-comp-ok'),
          ) ==
          null,
      'accepted companion optimism',
    );
    await client.close();
    client = await open();
    final deadline = DateTime.now().add(const Duration(seconds: 20));
    while ((await client.syncState())['pending'] != 0 &&
        DateTime.now().isBefore(deadline)) {
      await Future<void>.delayed(Duration.zero);
    }
    check(
      (await client.syncState())['pending'] == 0,
      'durable reopen settlement',
    );
    check(runs == 2, 'callback did not rerun');
    check(
      await client.models.composition.get(
            const app.CompositionIdentity(id: 'dart-comp-ok'),
          ) ==
          null,
      'accepted companion delete stays',
    );
    check(
      (await client.models.composition.get(
            const app.CompositionIdentity(id: 'dart-comp-no'),
          ))?.title ==
          'draft dart-comp-no',
      'rejection restores source',
    );
    check(
      (await client.models.composition.get(
            const app.CompositionIdentity(id: 'dart-comp-side'),
          ))?.title ==
          'independent title',
      'independent write survives',
    );
    check(
      await client.models.entry.get(
            const app.EntryIdentity(id: 'dart-entry-no'),
          ) ==
          null,
      'rejected aggregate removed',
    );
    await client.transaction((tx) async {
      await tx.models.composition.create(draft('dart-comp-live-no'));
    });
    final rejected = await client.mutations.publishEntry.withTransaction((
      local,
    ) async {
      runs++;
      final source = (await local.models.composition.get(
        const app.CompositionIdentity(id: 'dart-comp-live-no'),
      ))!;
      await local.models.composition.delete(source.identity);
      return input('dart-entry-live-no', source);
    });
    final outcome = await rejected.wait();
    check(
      outcome is sdk.CallFailure<app.PublishEntryOutput> &&
          outcome.error.code == 'publish.rejected',
      'online refusal',
    );
    check(
      await client.models.composition.get(
            const app.CompositionIdentity(id: 'dart-comp-live-no'),
          ) !=
          null,
      'online refusal restores companion',
    );
    stdout.writeln('Dart transactional PublishEntry companion: passed');
  } finally {
    await client.close();
    keepAlive.close();
  }
}
