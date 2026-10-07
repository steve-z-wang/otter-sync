import 'dart:io';
import 'generated.dart';

void check(bool value, String message) {
  if (!value) throw StateError(message);
}

Future<void> main(List<String> args) async {
  final dir = await Directory.systemTemp.createTemp('axton-sdk05-dart-');
  final client = await GeneratedClient.open(
    path: '${dir.path}/db',
    stream: 'User:dart',
    connection: StoreConnection(url: args.single, token: () => 'dart'),
    libraryPath: Platform.environment['AXTON_LIBRARY'],
  );
  try {
    await client.bootstrap();
    final accepted = await client.mutations.publish.withTransaction((tx) async {
      await tx.models.draft.create(
        const DraftCreate(id: 'dart-companion', text: 'keep'),
      );
      return const PublishInput(
        entry: EntryCreate(id: 'dart-entry', text: ' canonical dart '),
        call: 'private',
      );
    });
    final outcome = await accepted.wait();
    check(
      outcome is CallSuccess<PublishOutput> &&
          outcome.result.entry.text == 'canonical dart',
      'typed canonical completion',
    );
    check(
      (await client.models.entry.get(
            const EntryIdentity(id: 'dart-entry'),
          ))?.text ==
          'canonical dart',
      'canonical local write',
    );
    check(
      (await client.models.draft.get(
            const DraftIdentity(id: 'dart-companion'),
          ))?.text ==
          'keep',
      'accepted companion',
    );
    final refused = await client.mutations.publish.withTransaction((tx) async {
      await tx.models.draft.create(
        const DraftCreate(id: 'dart-refused-companion', text: 'undo'),
      );
      return const PublishInput(
        entry: EntryCreate(id: 'dart-refused', text: 'refuse'),
        call: 'normal',
      );
    });
    final no = await refused.wait();
    check(
      no is CallFailure<PublishOutput> && no.error.code == 'publish.refused',
      'typed refusal',
    );
    check(
      await client.models.entry.get(const EntryIdentity(id: 'dart-refused')) ==
          null,
      'refused optimism rolled back',
    );
    check(
      await client.models.draft.get(
            const DraftIdentity(id: 'dart-refused-companion'),
          ) ==
          null,
      'refused companion rolled back',
    );
    check(
      (await client.queries.find(id: 'dart-entry', store: false)).entry?.text ==
          'canonical dart',
      'Query snapshot',
    );
    check(
      (await client.fetch.entry(
            const EntryIdentity(id: 'dart-entry'),
            store: false,
          ))?.text ==
          'canonical dart',
      'Fetch snapshot',
    );
    check(
      (await client.queries.find(id: 'absent')).entry == null,
      'missing Query',
    );
    try {
      await client.queries.find(id: 'refused-query');
      throw StateError('refused Query returned success');
    } on CallError catch (error) {
      check(
        error.code == 'find.denied' && error.execution == 'rejected',
        'Query refusal keeps native completion ABI',
      );
    }
    stdout.writeln('Dart protocol5 actual host: PASS');
  } finally {
    await client.close();
    await dir.delete(recursive: true);
  }
}
