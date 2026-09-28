// Run by integration/action-e2e/action.test.mts against its real backend
// (#189). A generated Dart client writes microsecond DateTimes, which AXTON
// keeps at UTC millisecond precision, and calls Restamp with its optional
// `note` left out and as an explicit null. The last stdout line is a JSON
// object with the Note id and the ISO strings the backend must have received;
// the TS side checks the handler's arguments and PostgreSQL.
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'package:axton/axton.dart' as sdk;

import '../action-e2e/generated.dart' as app;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

const id = '123e4567-e89b-42d3-a456-426614174189';
const note = app.NoteIdentity(id: id);

Future<DateTime?> createdAt(app.GeneratedClient client) async => (await client.models.note.get(note))?.createdAt;

Future<void> main(List<String> args) async {
  final [url, path, libraryPath] = args;
  final server = sdk.SyncServer(url: url, token: () => 'alice');
  final created = DateTime.utc(2026, 9, 28, 12, 34, 56, 789, 123);
  final moved = DateTime(2026, 9, 28, 15, 0, 0, 1, 999); // a local DateTime
  final at = DateTime.utc(2026, 9, 28, 16, 0, 0, 2, 500);
  // A pending `Call.wait()` does not keep the isolate alive (#177).
  final keepAlive = ReceivePort();
  try {
    // Offline: what the local store and the queue hold is already truncated.
    var client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath);
    try {
      await client.mutations.addNote(note: app.NoteCreate(id: id, createdAt: created));
      final optimistic = await createdAt(client);
      check(optimistic == created.toAxtonPrecision(), 'optimistic create reads back truncated: $optimistic');
      check(optimistic == DateTime.utc(2026, 9, 28, 12, 34, 56, 789) && optimistic!.isUtc, 'UTC milliseconds: $optimistic');
      await client.mutations.restamp(at: at);
      await client.mutations.restamp(note: null, at: at);
      await client.mutations.restamp(note: app.RestampNoteUpdate(id: id, createdAt: app.Present(moved)), at: at);
      final patched = await createdAt(client);
      check(patched == moved.toAxtonPrecision() && patched!.isUtc, 'a local DateTime reads back as the same instant in UTC: $patched');
      final queued = await client.readSql('SELECT name, args FROM axton_mutation ORDER BY ordinal');
      check(queued.map((row) => row['name']).join(',') == 'AddNote,Restamp,Restamp,Restamp', 'queue: $queued');
      final addArgs = jsonDecode(queued[0]['args'] as String) as Map;
      check((addArgs['note'] as Map)['createdAt'] == '2026-09-28T12:34:56.789Z', 'recorded create: $addArgs');
      // Leaving the optional operand out and passing null record the same args.
      check(queued[1]['args'] == queued[2]['args'], 'omitted and null record identical args: ${queued[1]['args']} vs ${queued[2]['args']}');
      final omitted = jsonDecode(queued[1]['args'] as String) as Map;
      check(omitted.containsKey('note') && omitted['note'] == null, 'recorded as present null: $omitted');
      check(omitted['at'] == '2026-09-28T16:00:00.002Z', 'recorded argument truncated: $omitted');
      final movedArgs = jsonDecode(queued[3]['args'] as String) as Map;
      check((movedArgs['note'] as Map)['createdAt'] == moved.toAxtonPrecision().toIso8601String(), 'recorded patch truncated: $movedArgs');
    } finally {
      await client.close();
    }

    // Online: the backend's authority delivers the same values, which compare equal.
    client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath, server: server);
    try {
      for (var attempt = 0; attempt < 500 && (await client.syncState())['pending'] != 0; attempt++) {
        await Future<void>.delayed(const Duration(milliseconds: 20));
      }
      check((await client.syncState())['pending'] == 0, 'queued calls drained');
      check(await createdAt(client) == moved.toAxtonPrecision(), 'canonical delivery equals the optimistic value');
      final direct = await client.mutations.call.restamp(note: app.RestampNoteUpdate(id: id, createdAt: app.Present(created)), at: at);
      check(direct.at == at.toAxtonPrecision() && direct.at.isUtc, 'direct result truncated: ${direct.at}');
      check(await createdAt(client) == created.toAxtonPrecision(), 'direct authority truncated');
      final durable = await (await client.mutations.restamp(at: at)).wait();
      final result = (durable as sdk.CallSuccess<app.RestampOutput>).result;
      check(result.at == at.toAxtonPrecision(), 'durable result truncated: ${result.at}');
    } finally {
      await client.close();
    }
    stdout.writeln('Dart DateTime precision and omitted operand: passed');
    stdout.writeln(jsonEncode({
      'note': id,
      'created': created.toAxtonPrecision().toIso8601String(),
      'moved': moved.toAxtonPrecision().toIso8601String(),
      'at': at.toAxtonPrecision().toIso8601String(),
    }));
  } finally {
    keepAlive.close();
  }
}
