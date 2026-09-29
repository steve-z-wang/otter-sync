// Unsent work (#186, #205, #204): refused and failed acts as streams, their
// resolutions on the client and inside a transaction. The real runtime and
// SQLite underneath.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:test/test.dart';

Map<String, dynamic> _field(String name, {bool nullable = false}) => {
  'name': name,
  'type': {'kind': 'scalar', 'name': 'string'},
  'nullable': nullable,
};

Map<String, dynamic> _slot(String operation) => {
  'kind': 'model',
  'name': 'note',
  'model': 'Note',
  'operation': operation,
  'cardinality': 'single',
};

/// `Write` edits a Note and its `blob` requires `RemoteBlob(key: self)`; a
/// later `Write` of the same Note follows an earlier one. `Create` makes one.
final _schema = <String, dynamic>{
  'enums': [],
  'models': [
    {
      'name': 'Note',
      'version': 1,
      'identity': ['id'],
      'fields': [_field('id'), _field('text'), _field('blob', nullable: true)],
    },
  ],
  'prerequisites': [
    {
      'name': 'RemoteBlob',
      'fields': [
        {'name': 'key', 'type': 'String'},
      ],
    },
  ],
  'actions': [
    {
      'name': 'Write',
      'version': 1,
      'outputs': [],
      'inputs': [_slot('update')],
      'requirements': [
        {
          'model': 'Note',
          'field': 'blob',
          'name': 'RemoteBlob',
          'arguments': {'key': 'self'},
        },
      ],
      'sequence': {
        'after': [
          {
            'name': 'Write',
            'arguments': {'note': 'note'},
          },
        ],
      },
    },
    {
      'name': 'Create',
      'version': 1,
      'outputs': [],
      'inputs': [_slot('create')],
    },
  ],
};

String _blob(String key) => jsonEncode({
  'arguments': {'key': key},
  'name': 'RemoteBlob',
});

Map<String, dynamic> _write(String text, [String? blob]) => {
  'note': {'id': 'n', 'text': text, 'blob': blob},
};

dynamic _identity(dynamic value) => value;

void main() {
  late Directory dir;
  setUp(() async {
    dir = await Directory.systemTemp.createTemp('axton-dart-unsent-');
  });
  tearDown(() => dir.delete(recursive: true));

  Future<Client> open([
    Map<String, PrerequisiteHandler> prerequisites = const {},
  ]) async {
    final client = await Client.open(
      path: '${dir.path}/db',
      schema: _schema,
      libraryPath: Platform.environment['AXTON_LIBRARY']!,
      prerequisites: prerequisites,
    );
    await client.direct({
      'model': 'Note',
      'op': 'create',
      'identity': {'id': 'n'},
      'values': {'text': 'base', 'blob': null},
    });
    return client;
  }

  /// Poll [probe] until it holds; fail after five seconds.
  Future<void> until(FutureOr<bool> Function() probe) async {
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (!await probe()) {
      expect(DateTime.now().isBefore(deadline), isTrue, reason: 'timed out');
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
  }

  Future<bool> failed(Client client) async =>
      (await client.pendingTasks()).any((task) => task['state'] == 'failed');

  /// Freeze the queue and answer it: [refuse] maps an ordinal to its code.
  Future<void> settle(
    Client client,
    int sequence, {
    Map<int, String> refuse = const {},
    List<Map<String, dynamic>> records = const [],
  }) async {
    final request = jsonDecode((await client.freeze())!) as Map;
    final rejections = <Map<String, dynamic>>[];
    final completions = <Map<String, dynamic>>[];
    for (final call in request['mutations'] as List) {
      final code = refuse[call['ordinal']];
      if (code != null) {
        rejections.add({'ordinal': call['ordinal'], 'code': code});
      }
      completions.add({
        'callId': call['callId'],
        'outcome': code == null
            ? {'status': 'succeeded', 'result': null}
            : {'status': 'failed', 'code': code, 'execution': 'rejected'},
      });
    }
    await client.acknowledge(sequence, {
      'clientId': client.clientId,
      'batchSequence': sequence,
      'rejections': rejections,
      'completions': completions,
      'records': records,
    });
  }

  Future<String> text(Client client) async =>
      (await client.read('Note', {'id': 'n'}))!['text'] as String;

  String? code(CallOutcome<dynamic> outcome) =>
      outcome is CallFailure ? outcome.error.code : null;

  test('a refusal keeps the act as submitted until it is dismissed', () async {
    final client = await open();
    try {
      final refused = <List<RefusedAct>>[];
      final pending = <int>[];
      final a = client.rejections.watch().listen(refused.add);
      final b = client.outbound.watchPending().listen(pending.add);
      await until(() => refused.length == 1 && pending.length == 1);
      expect(refused.single, isEmpty);
      final call = await client.invokeAction(
        'Write',
        1,
        _write("the author's words"),
        _identity,
      );
      await settle(client, 1, refuse: {1: 'note.denied'});
      await until(() => refused.length == 2 && pending.length == 3);
      expect(pending, [0, 1, 0], reason: 'distinct values only');
      final item = refused.last.single;
      expect(
        [item.id, item.name, item.version, item.code],
        [1, 'Write', 1, 'note.denied'],
      );
      // The author's words come back from the retained act.
      expect(item.act.args, _write("the author's words"));
      final op = item.act.operations.single;
      expect(
        [op.model, op.op, op.identity, op.values],
        [
          'Note',
          'update',
          {'id': 'n'},
          {'text': "the author's words", 'blob': null},
        ],
      );
      expect(await text(client), 'base');
      expect(code(await call.wait()), 'note.denied');
      expect((await client.rejections.get(1))!.act.args, item.act.args);
      expect(await client.rejections.get(2), isNull);
      await client.rejections.dismiss(1);
      await until(() => refused.length == 3);
      expect(refused.last, isEmpty);
      await a.cancel();
      await b.cancel();
    } finally {
      await client.close();
    }
  });

  test(
    'a terminal handler failure lists the act; a retry runs the handler again',
    () async {
      var calls = 0;
      final client = await open({
        'RemoteBlob': (arguments, cancelled) async {
          if (++calls == 1) throw StateError('file is gone');
        },
      });
      try {
        final failures = <List<FailedAct>>[];
        final sub = client.failures.watch().listen(failures.add);
        await client.submitAction('Write', 1, _write('photo', 'X'));
        await until(() => failures.any((items) => items.isNotEmpty));
        final act = failures.last.single;
        expect([act.ordinal, act.name, act.version], [1, 'Write', 1]);
        expect(act.act.args, _write('photo', 'X'));
        final task = act.tasks.single;
        expect(
          [task.key, task.name, task.arguments, task.error],
          [
            _blob('X'),
            'RemoteBlob',
            {'key': 'X'},
            'Bad state: file is gone',
          ],
        );
        await client.failures.retry([_blob('X')]);
        await until(() => failures.last.isEmpty);
        await until(() async => (await client.pendingTasks()).isEmpty);
        expect(calls, 2);
        await sub.cancel();
      } finally {
        await client.close();
      }
    },
  );

  test(
    'a new requirement on a failed task is listed at once and one retry unblocks both (#204)',
    () async {
      var calls = 0;
      final client = await open({
        'RemoteBlob': (arguments, cancelled) async {
          if (++calls == 1) throw StateError('upload refused');
        },
      });
      try {
        final failures = <List<FailedAct>>[];
        final sub = client.failures.watch().listen(failures.add);
        await client.submitAction('Write', 1, _write('one', 'X'));
        await until(() => failures.any((items) => items.length == 1));
        await client.submitAction('Write', 1, _write('two', 'X'));
        await until(() => failures.any((items) => items.length == 2));
        expect(failures.last.map((act) => act.ordinal), [1, 2]);
        expect(calls, 1, reason: 'the failed task is not reset');
        await client.failures.retry([_blob('X')]);
        await until(() async => (await client.pendingTasks()).isEmpty);
        expect(calls, 2, reason: 'one handler run covers both acts');
        final request = jsonDecode((await client.freeze())!) as Map;
        expect((request['mutations'] as List).map((m) => m['ordinal']), [1, 2]);
        await sub.cancel();
      } finally {
        await client.close();
      }
    },
  );

  test(
    'drop removes the act without a refusal and refuses a dependent',
    () async {
      final client = await open();
      try {
        final created = await client.invokeAction('Create', 1, {
          'note': {'id': 'm', 'text': 'new', 'blob': null},
        }, _identity);
        final edited = await client.invokeAction('Write', 1, {
          'note': {'id': 'm', 'text': 'edited', 'blob': null},
        }, _identity);
        await client.failures.drop(1);
        expect(code(await created.wait()), 'dropped');
        expect(code(await edited.wait()), 'dependency.rejected');
        expect(await client.read('Note', {'id': 'm'}), isNull);
        final refused = await client.rejections.watch().first;
        expect(refused.map((r) => [r.id, r.code]), [
          [2, 'dependency.rejected'],
        ]);
      } finally {
        await client.close();
      }
    },
  );

  test(
    'a replacement and the drop of the failed original commit as one (#205)',
    () async {
      final client = await open({
        'RemoteBlob': (arguments, cancelled) async =>
            throw StateError('upload refused'),
      });
      try {
        final original = await client.invokeAction(
          'Write',
          1,
          _write('draft', 'X'),
          _identity,
        );
        await until(() => failed(client));
        expect(await text(client), 'draft');
        late Call<dynamic> replacement;
        final seen = await client.transaction((tx) async {
          await tx.failures.drop(1);
          final row = await tx.read('Note', {'id': 'n'});
          replacement = await tx.submitMutation(
            'Write',
            1,
            _write('fixed'),
            _identity,
          );
          return row!['text'];
        });
        expect(seen, 'base', reason: "planned without the original's optimism");
        expect(code(await original.wait()), 'dropped');
        expect(await text(client), 'fixed');
        expect(
          await client.readSql(
            'SELECT COUNT(*) AS n FROM axton_mutation_dependency',
          ),
          [
            {'n': 0},
          ],
          reason: 'not sequenced after the original',
        );
        await settle(
          client,
          1,
          records: [
            {
              'model': 'Note',
              'identity': {'id': 'n'},
              'stamp': 1,
              'state': {'text': 'fixed', 'blob': null},
            },
          ],
        );
        expect(code(await replacement.wait()), isNull, reason: 'accepted');
      } finally {
        await client.close();
      }
    },
  );

  test(
    'a throw after the drop rolls both back and the original is intact (#205)',
    () async {
      final client = await open({
        'RemoteBlob': (arguments, cancelled) async =>
            throw StateError('upload refused'),
      });
      try {
        await client.submitAction('Write', 1, _write('draft', 'X'));
        await until(() => failed(client));
        await expectLater(
          client.transaction((tx) async {
            await tx.failures.drop(1);
            await tx.submitMutation('Write', 1, _write('fixed'), _identity);
            await tx.failures.retry([_blob('X')]);
            await tx.rejections.dismiss(1);
            throw StateError('the author cancelled');
          }),
          throwsA(isA<StateError>()),
        );
        expect(await text(client), 'draft');
        expect((await client.syncState())['pending'], 1);
        expect((await client.failures.watch().first).single.ordinal, 1);
        expect(await client.rejections.get(1), isNull);
      } finally {
        await client.close();
      }
    },
  );

  test(
    'a stream delivers the first result, then distinct results; cancel and close end it',
    () async {
      final client = await open();
      final counts = <int>[];
      final sub = client.outbound.watchPending().listen(counts.add);
      await until(() => counts.length == 1);
      await client.direct({
        'model': 'Note',
        'op': 'update',
        'identity': {'id': 'n'},
        'values': {'text': 'local'},
      });
      await client.submitAction('Write', 1, _write('one'));
      await until(() => counts.length == 2);
      await sub.cancel();
      await client.submitAction('Write', 1, _write('two'));
      expect(counts, [0, 1], reason: 'no value after cancel');
      // Inside a transaction a stream is refused.
      await client.transaction((tx) async {
        await expectLater(
          client.failures.watch().first,
          throwsA(isA<StateError>()),
        );
      });
      // Close completes an open stream.
      final done = Completer<void>();
      client.rejections.watch().listen((_) {}, onDone: done.complete);
      await client.rejections.watch().first;
      await client.close();
      await done.future;
      await expectLater(
        client.rejections.watch().first,
        throwsA(isA<StateError>()),
      );
    },
  );
}
