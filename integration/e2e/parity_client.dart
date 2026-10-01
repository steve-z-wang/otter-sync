// The Dart half of the cross-runtime parity check in parity.test.mjs. Runs the
// same script as the Node half against the same server and prints the same
// normalized state on the last line, prefixed with PARITY.
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';

Future<void> waitFor(Future<bool> Function() condition, String label) async {
  for (var i = 0; i < 1000; i++) {
    if (await condition()) return;
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw StateError('timed out waiting for $label');
}

Map<String, dynamic> edit(String text) => {
  'name': 'Edit',
  'operations': [
    {
      'model': 'Entry',
      'op': 'update',
      'identity': {'id': 'entry-1'},
      'values': {'text': text},
    },
  ],
};

Future<void> main(List<String> args) async {
  final schema =
      jsonDecode(await File('../../fixtures/schemas/entry.json').readAsString())
          as Map<String, dynamic>;
  final client = await Client.open(
    path: '${args[1]}/parity-dart.sqlite',
    schema: schema,
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
  );
  try {
    final subscription = await client.subscribe('book:demo');
    final connection = await client.connect(
      SyncServer(url: args[0], token: () => 'demo-user'),
    );
    Future<bool> settled() async => (await client.syncState())['pending'] == 0;
    // The origin is the first head this handshake acknowledges (#150): nothing
    // published earlier arrives, so READY asks the harness to publish again.
    await waitFor(
      () async =>
          subscription.status.initialization ==
          SubscriptionInitialization.ready,
      'first initialization',
    );
    if (await client.read('Entry', {'id': 'entry-1'}) != null)
      throw StateError(
        'a new subscription loaded a record published before it',
      );
    stdout.writeln('READY');
    await stdout.flush();
    await waitFor(
      () async => await client.read('Entry', {'id': 'entry-1'}) != null,
      'initial catch-up',
    );
    final initial = (await client.read('Entry', {'id': 'entry-1'}))!['text'];
    await client.mutate(edit('  parity  '));
    await waitFor(settled, 'accepted edit');
    final afterAccepted = (await client.read('Entry', {
      'id': 'entry-1',
    }))!['text'];
    await client.mutate(edit('reject'));
    await waitFor(settled, 'rejected edit');
    await client.transaction((tx) async {
      await tx.direct({
        'model': 'Entry',
        'op': 'create',
        'identity': {'id': 'local-only'},
        'values': {'text': 'local', 'note': null},
      });
    });
    await connection.close();
    final entries = (await client.query('Entry'))
      ..sort((a, b) => (a['id'] as String).compareTo(b['id'] as String));
    final status = await client.syncState();
    final dump = {
      'initial': initial,
      'afterAccepted': afterAccepted,
      'entries': [
        for (final row in entries)
          {'id': row['id'], 'text': row['text'], 'note': row['note']},
      ],
      'pending': status['pending'],
      'beforeImages': status['beforeImages'],
      'scopes': status['scopes'],
      'rejections': status['rejections'],
      'entry1': await client.recordSyncState('Entry', {'id': 'entry-1'}),
      'localOnly': await client.recordSyncState('Entry', {'id': 'local-only'}),
    };
    stdout.writeln('PARITY ${jsonEncode(dump)}');
  } finally {
    await client.close();
  }
}
