import 'store_fixture.dart';
// Watched read-only SQL over several Models
// (https://github.com/zanminwang/axton/issues/184) through the real native
// runtime: SQLite names the tables a statement reads, and the runtime re-runs
// it only after a commit that writes one of them.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:test/test.dart';

Map<String, dynamic> _text(String name, {bool nullable = false}) => {
  'name': name,
  'nullable': nullable,
  'type': {'kind': 'scalar', 'name': 'string'},
};

/// Entry, with Media and Person belonging to an Entry, and a lone Note.
Future<Map<String, dynamic>> _schema() async {
  final schema =
      jsonDecode(await File('../../fixtures/schemas/entry.json').readAsString())
          as Map<String, dynamic>;
  (schema['models'] as List).addAll([
    {
      'name': 'Media',
      'identity': ['id'],
      'fields': [
        _text('id'),
        _text('entryId'),
        _text('url'),
        _text('caption', nullable: true),
      ],
    },
    {
      'name': 'Person',
      'identity': ['id'],
      'fields': [_text('id'), _text('entryId'), _text('name')],
    },
    {
      'name': 'Note',
      'identity': ['id'],
      'fields': [_text('id'), _text('body')],
    },
  ]);
  return schema;
}

/// A Journal page: each Entry with its Media and its Person.
const _journal =
    'SELECT e.id AS entry, e.text AS text, m.url AS media, p.name AS person '
    'FROM Entry e JOIN Media m ON m.entryId = e.id '
    'JOIN Person p ON p.entryId = e.id ORDER BY e.id';

/// Every re-run publishes: its `random()` column differs.
const _probe = 'SELECT count(*) AS n, random() AS r FROM Entry';

Map<String, dynamic> _put(
  String model,
  String id,
  Map<String, dynamic> values,
) => {
  'model': model,
  'op': 'create',
  'identity': {'id': id},
  'values': values,
};

Map<String, dynamic> _change(
  String model,
  String id,
  Map<String, dynamic> values,
) => {
  'model': model,
  'op': 'update',
  'identity': {'id': id},
  'values': values,
};

Future<void> _eventually(bool Function() condition, String what) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('$what timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

Future<void> _settle() =>
    Future<void>.delayed(const Duration(milliseconds: 50));

void main() {
  late Directory dir;
  late Client client;
  setUp(() async {
    dir = await Directory.systemTemp.createTemp('axton-watch-sql-');
    client = await Client.open(
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
      path: '${dir.path}/db',
      schema: await _schema(),
      libraryPath: Platform.environment['AXTON_LIBRARY']!,
    );
  });
  tearDown(() async {
    await client.close();
    await dir.delete(recursive: true);
  });

  test(
    'a join over three Models re-emits after a commit to each, and never for another table',
    () async {
      await client.direct(_put('Entry', 'e', {'text': 'first'}));
      await client.direct(_put('Media', 'm', {'entryId': 'e', 'url': 'a.jpg'}));
      await client.direct(_put('Person', 'p', {'entryId': 'e', 'name': 'Ann'}));
      final page = <List<Map<String, dynamic>>>[];
      final probe = <Object?>[];
      final watching = client.watchSql(_journal).listen(page.add);
      final probing = client
          .watchSql(_probe)
          .listen((rows) => probe.add(rows.single['n']));
      await _eventually(
        () => page.length == 1 && probe.length == 1,
        'the first rows',
      );
      Map<String, dynamic> row(String text, String media, String person) => {
        'entry': 'e',
        'text': text,
        'media': media,
        'person': person,
      };
      expect(page.single, [row('first', 'a.jpg', 'Ann')]);
      await client.direct(_change('Entry', 'e', {'text': 'second'}));
      await _eventually(
        () => page.length == 2 && probe.length == 2,
        'the Entry commit',
      );
      await client.direct(_change('Media', 'm', {'url': 'b.jpg'}));
      await _eventually(() => page.length == 3, 'the Media commit');
      await client.direct(_change('Person', 'p', {'name': 'Bea'}));
      await _eventually(() => page.length == 4, 'the Person commit');
      expect(page.sublist(1), [
        [row('second', 'a.jpg', 'Ann')],
        [row('second', 'b.jpg', 'Ann')],
        [row('second', 'b.jpg', 'Bea')],
      ]);
      // A commit to an unrelated Model re-runs neither; one that leaves the
      // join's answer unchanged publishes nothing.
      await client.direct(_put('Note', 'n', {'body': 'aside'}));
      await client.direct(_change('Media', 'm', {'caption': 'unselected'}));
      await _settle();
      expect(page, hasLength(4));
      expect(probe, [1, 1], reason: 'only the Entry commit re-ran the probe');
      await watching.cancel();
      await probing.cancel();
      await client.direct(_change('Entry', 'e', {'text': 'after'}));
      await _settle();
      expect(page, hasLength(4), reason: 'a cancelled watch hears nothing');
    },
  );

  test('binds parameters', () async {
    await client.direct(_put('Entry', 'e', {'text': 'bound'}));
    final rows = await client
        .watchSql('SELECT text FROM Entry WHERE id = ?', parameters: ['e'])
        .first
        .timeout(const Duration(seconds: 5));
    expect(rows, [
      {'text': 'bound'},
    ]);
  });

  test(
    'a write, an engine table and a missing table end the stream with their error',
    () async {
      for (final sql in [
        'DELETE FROM Entry RETURNING id',
        'SELECT count(*) AS n FROM axton_record',
        'SELECT id FROM Missing',
      ]) {
        final errors = <Object>[];
        final rows = <Object>[];
        final done = Completer<void>();
        client
            .watchSql(sql)
            .listen(rows.add, onError: errors.add, onDone: done.complete);
        await done.future.timeout(
          const Duration(seconds: 5),
          onTimeout: () => fail('$sql did not end'),
        );
        expect(errors, [isA<StateError>()], reason: sql);
        expect(rows, isEmpty, reason: sql);
      }
    },
  );

  test(
    'inside a transaction callback it is refused with transaction_active',
    () async {
      Object? refusal;
      await client.transaction((tx) async {
        await tx.direct(_put('Entry', 'e', {'text': 'inside'}));
        final done = Completer<void>();
        client
            .watchSql('SELECT text FROM Entry')
            .listen(
              (_) => fail('nothing is delivered'),
              onError: (Object error) => refusal = error,
              onDone: done.complete,
            );
        await done.future.timeout(const Duration(seconds: 5));
      });
      expect(
        refusal,
        isA<StateError>().having(
          (e) => e.message,
          'message',
          'transaction_active',
        ),
      );
    },
  );

  test('closing the client completes the stream', () async {
    final seen = <List<Map<String, dynamic>>>[];
    final done = Completer<void>();
    client
        .watchSql('SELECT count(*) AS n FROM Entry')
        .listen(seen.add, onDone: done.complete);
    await _eventually(() => seen.isNotEmpty, 'the first rows');
    await client.close();
    await done.future.timeout(
      const Duration(seconds: 5),
      onTimeout: () => fail('the watch did not complete'),
    );
    expect(seen, [
      [
        {'n': 0},
      ],
    ], reason: 'the terminal snapshot repeats no result');
  });
}
