import 'dart:io';

import 'package:test/test.dart';
import 'package:axton/axton.dart' as sdk;
import 'generated.dart';
import 'model_only/generated.dart' as model_only;
import 'model_free/generated.dart' as model_free;

void main() {
  late Directory directory;
  late GeneratedClient client;
  setUp(() async {
    directory = await Directory.systemTemp.createTemp(
      'axton-generated-action-',
    );
    client = await GeneratedClient.open(
      path: '${directory.path}/state.sqlite',
      stream: 'User:viewer', connection: offline(),
      libraryPath: Platform.environment['AXTON_DART_LIBRARY']!,
    );
  });
  tearDown(() async {
    await client.close();
    await directory.delete(recursive: true);
  });

  test(
    'standalone and transaction models write locally and notify watch',
    () async {
      final date = DateTime.utc(2026, 9, 23);
      final watched = client.models.note.watch().firstWhere(
        (rows) => rows.isNotEmpty,
      );
      await client.models.note.create(
        Note(id: 'n', at: date, mood: Mood.calm, label: null),
      );
      expect(
        (await watched.timeout(const Duration(seconds: 2))).single.at,
        date,
      );
      expect(
        (await client.models.note.get(const NoteIdentity(id: 'n')))?.mood,
        Mood.calm,
      );
      await client.models.note.update(
        const NoteIdentity(id: 'n'),
        const NotePatch(label: Present('updated')),
      );
      await client.transaction((tx) async {
        await tx.models.note.update(
          const NoteIdentity(id: 'n'),
          NotePatch(at: Present(date.add(const Duration(days: 1)))),
        );
        expect(
          (await tx.models.note.get(const NoteIdentity(id: 'n')))?.label,
          'updated',
        );
      });
      final record = (await client.models.note.query()).single;
      expect(record.at, date.add(const Duration(days: 1)));
      expect(record.label, 'updated');
      expect((await client.syncState())['pending'], 0);
      await client.models.note.delete(const NoteIdentity(id: 'n'));
      expect(await client.models.note.get(const NoteIdentity(id: 'n')), isNull);
    },
  );

  test('typed input codecs retain millisecond dates, enums and nulls', () {
    final at = DateTime.utc(2026, 9, 23, 12);
    expect(EchoInput(at: at, moods: [Mood.calm, Mood.loud], maybe: null).toRecord(), {'at': '2026-09-23T12:00:00.000Z', 'moods': ['calm', 'loud'], 'maybe': null});
    expect(TouchInput(note: NoteCreate(id: 'n', at: at, mood: Mood.calm, label: null), changed: null).toRecord()['changed'], isNull);
  });
  test('typed callback commits companions and optimism in one durable transaction', () async {
    final at = DateTime.utc(2026, 9, 23);
    final call = await client.transaction((tx) async {
      final call = await tx.mutations.touch.withTransaction((local) async {
        expect(await local.models.note.get(const NoteIdentity(id: 'n')), isNull);
        await local.models.note.create(NoteCreate(id: 'companion', at: at, mood: Mood.loud, label: null));
        return TouchInput(note: NoteCreate(id: 'n', at: at, mood: Mood.calm, label: null), changed: null);
      });
      await expectLater(call.wait(), throwsA(isA<sdk.CallError>().having((e) => e.code, 'code', 'transaction_uncommitted')));
      return call;
    });
    expect(call.status, sdk.CallStatus.pending);
    expect((await client.models.note.get(const NoteIdentity(id: 'n')))!.at, at);
    expect((await client.models.note.get(const NoteIdentity(id: 'companion')))!.mood, Mood.loud);
    await client.close();
    expect(((await call.wait()) as sdk.CallFailure).error.code, 'client.closed');
    client = await GeneratedClient.open(path: '${directory.path}/state.sqlite', stream: 'User:viewer', connection: offline(), libraryPath: Platform.environment['AXTON_DART_LIBRARY']!);
    expect((await client.syncState())['pending'], 1);
    expect((await client.models.note.get(const NoteIdentity(id: 'n')))!.at, at);
  });
  test('model-only facade retains local CRUD without remote namespaces', () async {
    final only = await model_only.GeneratedClient.open(path: '${directory.path}/only', stream: 'User:viewer', connection: offline(), libraryPath: Platform.environment['AXTON_DART_LIBRARY']!);
    try {
      await only.models.item.create(const model_only.ItemCreate(id: 'i', label: 'local'));
      expect((await only.models.item.get(const model_only.ItemIdentity(id: 'i')))!.label, 'local');
      expect(() => (only as dynamic).mutations, throwsNoSuchMethodError);
      expect(() => (only as dynamic).queries, throwsNoSuchMethodError);
    } finally { await only.close(); }
  });
  test('model-free facade retains durable named scalar writes', () async {
    final free = await model_free.GeneratedClient.open(path: '${directory.path}/free', stream: 'User:viewer', connection: offline(), libraryPath: Platform.environment['AXTON_DART_LIBRARY']!);
    try {
      final call = await free.mutations.ping(const model_free.PingInput());
      expect(call.status, sdk.CallStatus.pending);
      expect((await free.syncState())['pending'], 1);
      expect(() => (free.models as dynamic).note, throwsNoSuchMethodError);
      expect(() => (free.queries as dynamic).enqueue, throwsNoSuchMethodError);
      await free.close();
      expect(((await call.wait()) as sdk.CallFailure).error.code, 'client.closed');
    } finally { await free.close(); }
  });
}
sdk.StoreConnection offline() => sdk.StoreConnection(url: 'http://127.0.0.1:1', token: () => 'offline', onError: (_) {});
