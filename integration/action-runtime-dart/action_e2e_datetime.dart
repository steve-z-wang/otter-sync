import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'package:axton/axton.dart' as sdk;
import '../action-e2e/generated.dart' as app;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

const id = '123e4567-e89b-42d3-a456-426614174189';
Future<void> main(List<String> args) async {
  final [url, path, library] = args;
  final keepAlive = ReceivePort();
  final created = DateTime.utc(2026, 9, 28, 12, 34, 56, 789, 123);
  final moved = DateTime(2026, 9, 28, 15, 0, 0, 1, 999);
  final at = DateTime.utc(2026, 9, 28, 16, 0, 0, 2, 500);
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
    await client.mutations.addNote(
      app.AddNoteInput(
        note: app.NoteCreate(id: id, createdAt: created),
      ),
    );
    check(
      (await client.models.note.get(
            const app.NoteIdentity(id: id),
          ))?.createdAt ==
          created.toAxtonPrecision(),
      'optimistic millisecond precision',
    );
    await client.mutations.restamp(app.RestampInput(at: at));
    await client.mutations.restamp(app.RestampInput(note: null, at: at));
    await client.mutations.restamp(
      app.RestampInput(
        note: app.RestampNoteUpdate(id: id, createdAt: app.Present(moved)),
        at: at,
      ),
    );
    final queued = await client.readSql(
      "SELECT q.name,(SELECT value FROM axton_mutation_queue_operation WHERE mutation_id=q.id AND input_path='note' AND kind='argument') AS note,(SELECT value FROM axton_mutation_queue_operation WHERE mutation_id=q.id AND input_path='at' AND kind='argument') AS at FROM axton_mutation_queue q ORDER BY q.id",
    );
    check(
      queued.map((row) => row['name']).join(',') ==
          'AddNote,Restamp,Restamp,Restamp',
      'frozen action order',
    );
    check(
      queued[1]['note'] == queued[2]['note'] &&
          queued[1]['at'] == queued[2]['at'],
      'omitted and null canonicalize identically',
    );
    check(
      jsonDecode(queued[1]['note'] as String) == null,
      'optional operand recorded null',
    );
    await client.close();
    client = await open();
    final end = DateTime.now().add(const Duration(seconds: 20));
    while ((await client.syncState())['pending'] != 0 &&
        DateTime.now().isBefore(end)) {
      await Future<void>.delayed(Duration.zero);
    }
    check(
      (await client.syncState())['pending'] == 0,
      'DateTime durable queue settled',
    );
    check(
      (await client.models.note.get(
            const app.NoteIdentity(id: id),
          ))?.createdAt ==
          moved.toAxtonPrecision(),
      'canonical move equals optimistic instant',
    );
    final call = await client.mutations.restamp(
      app.RestampInput(
        note: app.RestampNoteUpdate(id: id, createdAt: app.Present(created)),
        at: at,
      ),
    );
    final outcome = await call.wait();
    check(
      outcome is sdk.CallSuccess<app.RestampOutput> &&
          outcome.result.at == at.toAxtonPrecision(),
      'typed scalar result precision',
    );
    stdout.writeln('Dart DateTime precision and omitted operand: passed');
    stdout.writeln(
      jsonEncode({
        'note': id,
        'created': created.toAxtonPrecision().toIso8601String(),
        'moved': moved.toAxtonPrecision().toIso8601String(),
        'at': at.toAxtonPrecision().toIso8601String(),
      }),
    );
  } finally {
    await client.close();
    keepAlive.close();
  }
}
