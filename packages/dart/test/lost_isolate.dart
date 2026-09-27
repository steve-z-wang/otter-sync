// Run by lost_isolate_test.dart as its own process: an isolate opens a client,
// leaves a transaction open behind a callback that never finishes, and exits.
// This process then reopens the same file and writes to it. It exits 0 only
// when the lost isolate's runtime was detached: the actor released the file
// and no wake reached the isolate's deleted wake callback, which would abort.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';

import 'package:axton/axton.dart';

Future<void> main(List<String> args) async {
  final path = args[0];
  final library = Platform.environment['AXTON_LIBRARY']!;
  final schema = File(args[1]).readAsStringSync();
  final entered = ReceivePort();
  final exited = ReceivePort();
  await Isolate.spawn(_holdAndExit, (
    path,
    library,
    schema,
    entered.sendPort,
  ), onExit: exited.sendPort);
  await exited.first;
  entered.close();
  final client = await Client.open(
    path: path,
    schema: jsonDecode(schema) as Map<String, dynamic>,
    libraryPath: library,
  );
  try {
    await client.transaction(
      (tx) => tx.direct({
        'model': 'Entry',
        'op': 'create',
        'identity': {'id': 'e'},
        'values': {'text': 'after'},
      }),
    );
    final row = await client.read('Entry', {'id': 'e'});
    if (row?['text'] != 'after') throw StateError('unexpected row $row');
  } finally {
    await client.close();
  }
}

/// Open a client, hold its transaction open, queue a task behind it, and exit
/// the isolate while both wait.
Future<void> _holdAndExit((String, String, String, SendPort) args) async {
  final (path, library, schema, entered) = args;
  final client = await Client.open(
    path: path,
    schema: jsonDecode(schema) as Map<String, dynamic>,
    libraryPath: library,
  );
  final inside = Completer<void>();
  unawaited(
    client.transaction((tx) async {
      await tx.direct({
        'model': 'Entry',
        'op': 'create',
        'identity': {'id': 'e'},
        'values': {'text': 'never'},
      });
      inside.complete();
      await Completer<void>().future;
    }),
  );
  await inside.future;
  // Parked behind the open transaction: its completion is published, and the
  // runtime wakes, once that transaction ends.
  unawaited(client.syncState());
  entered.send(null);
  Isolate.exit();
}
