import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:test/test.dart';
import 'store_fixture.dart';

Map<String, dynamic> create(String id, String text) => {
  'model': 'Entry',
  'op': 'create',
  'identity': {'id': id},
  'values': {'text': text, 'note': null},
};
Map<String, dynamic> update(String text) => {
  'model': 'Entry',
  'op': 'update',
  'identity': {'id': 'e'},
  'values': {'text': text},
};
Map<String, dynamic> input(String id) => {
  'entry': {'id': id, 'text': 'published', 'note': null},
};
Matcher stateError(String message) =>
    throwsA(isA<StateError>().having((e) => e.message, 'message', message));
Matcher callError(String code) =>
    throwsA(isA<CallError>().having((e) => e.code, 'code', code));

void main() {
  late Directory dir;
  late Client client;
  late Map<String, dynamic> schema;
  Future<Client> open([String suffix = 'db']) => Client.open(
    path: '${dir.path}/$suffix',
    schema: schema,
    stream: 'User:viewer',
    connection: offlineStoreConnection(),
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
  );
  Future<String?> text([String id = 'e']) async =>
      (await client.read('Entry', {'id': id}))?['text'] as String?;
  Future<Call<void>> publish(
    Transaction tx,
    String id, {
    Future<Map<String, dynamic>> Function(WritePort)? callback,
  }) => tx.submitMutation<void>(
    'Publish',
    1,
    callback == null ? input(id) : null,
    (_) {},
    input: callback,
  );
  setUp(() async {
    dir = await Directory.systemTemp.createTemp('axton-dart-client04-');
    schema =
        jsonDecode(
              await File('../../fixtures/schemas/entry.json').readAsString(),
            )
            as Map<String, dynamic>;
    declareEntryEdit(schema);
    final entry = (schema['models'] as List).single;
    (schema['actions'] as List).addAll(<Map<String, dynamic>>[
      {
        'kind': 'mutation',
        'name': 'Publish',
        'version': 1,
        'input': {
          'models': [entry],
          'enums': [],
        },
        'inputs': [
          {
            'kind': 'model',
            'name': 'entry',
            'model': 'Entry',
            'operation': 'create',
            'cardinality': 'single',
          },
        ],
        'outputs': [],
        'requirements': [],
        'prerequisites': [],
      },
      {
        'kind': 'mutation',
        'name': 'Ping',
        'version': 1,
        'inputs': [],
        'outputs': [],
      },
    ]);
    client = await open();
    await client.direct(create('e', 'hello'));
  });
  tearDown(() async {
    await client.close();
    await dir.delete(recursive: true);
  });

  test(
    'committed pending Call wait refuses own transaction and preserves terminal wait',
    () async {
      final call = await client.submitMutation<void>('Ping', 1, {}, (_) {});
      final outside = call.wait();
      await client.transaction((_) async {
        await expectLater(
          call.wait().timeout(const Duration(milliseconds: 200)),
          stateError('transaction_active'),
        );
        expect(call.status, CallStatus.pending);
      });
      await client.resetStore(discardPending: true);
      expect(((await outside) as CallFailure<void>).error.code, 'abandoned');
      await client.transaction((_) async {
        expect(
          ((await call.wait()) as CallFailure<void>).error.code,
          'abandoned',
        );
      });
    },
  );
  test(
    'local transaction reads its own writes and throw rolls them back',
    () async {
      await expectLater(
        client.transaction((tx) async {
          await tx.direct(update('bad'));
          expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'bad');
          throw StateError('rollback');
        }),
        stateError('rollback'),
      );
      expect(await text(), 'hello');
      expect((await client.syncState())['pending'], 0);
    },
  );
  test(
    'nested client transaction is refused without running its body',
    () async {
      var ran = false;
      await client.transaction((tx) async {
        await expectLater(
          client.transaction((_) async {
            ran = true;
          }),
          stateError('transaction_active'),
        );
        expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'hello');
      });
      expect(ran, isFalse);
    },
  );
  test(
    'captured outer client commands reject promptly inside its callback',
    () async {
      await client.transaction((tx) async {
        await expectLater(
          client
              .read('Entry', {'id': 'e'})
              .timeout(const Duration(milliseconds: 200)),
          stateError('transaction_active'),
        );
        await expectLater(
          client.submitMutation<void>('Ping', 1, {}, (_) {}),
          stateError('transaction_active'),
        );
        await tx.direct(update('inside'));
      });
      expect(await text(), 'inside');
    },
  );
  test(
    'savepoint rolls back its writes while prior and later writes commit',
    () async {
      await client.transaction((tx) async {
        await tx.direct(update('before'));
        await expectLater(
          tx.savepoint(() async {
            await tx.direct(update('discard'));
            throw StateError('undo');
          }),
          stateError('undo'),
        );
        expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'before');
        await tx.direct(update('after'));
      });
      expect(await text(), 'after');
    },
  );
  test('escaped transaction cannot change committed state', () async {
    late Transaction escaped;
    await client.transaction((tx) async {
      escaped = tx;
    });
    await expectLater(escaped.direct(update('escaped')), throwsStateError);
    expect(await text(), 'hello');
  });
  test('unawaited native write poisons the entire transaction', () async {
    await expectLater(
      client.transaction((tx) async {
        unawaited(tx.direct(update('unawaited')).catchError((Object _) {}));
      }),
      throwsStateError,
    );
    expect(await text(), 'hello');
  });
  test(
    'named callback reads before optimism, returns input and atomically saves companions',
    () async {
      final call = await client.transaction((tx) async {
        final call = await publish(
          tx,
          'p',
          callback: (local) async {
            expect(await local.read('Entry', {'id': 'p'}), isNull);
            await local.direct(create('companion', 'local'));
            return input('p');
          },
        );
        expect((await tx.read('Entry', {'id': 'p'}))!['text'], 'published');
        await expectLater(call.wait(), callError('transaction_uncommitted'));
        return call;
      });
      expect(call.status, CallStatus.pending);
      expect(await text('companion'), 'local');
      expect((await client.syncState())['pending'], 1);
      await client.close();
      expect((await call.wait()), isA<CallFailure<void>>());
      client = await open();
      expect(await text('p'), 'published');
      expect(await text('companion'), 'local');
      expect((await client.syncState())['pending'], 1);
    },
  );
  test(
    'callback exception undoes companions and creates no queue entry',
    () async {
      await expectLater(
        client.submitMutation<void>(
          'Publish',
          1,
          null,
          (_) {},
          input: (tx) async {
            await tx.direct(create('companion', 'bad'));
            throw StateError('callback failed');
          },
        ),
        stateError('callback failed'),
      );
      expect(await text('companion'), isNull);
      expect((await client.syncState())['pending'], 0);
    },
  );
  test(
    'invalid returned typed input rolls back earlier companion work',
    () async {
      await expectLater(
        client.submitMutation<void>(
          'Publish',
          1,
          null,
          (_) {},
          input: (tx) async {
            await tx.direct(create('companion', 'bad'));
            return {
              'entry': {'id': 'p', 'text': 42, 'note': null},
            };
          },
        ),
        throwsA(anything),
      );
      expect(await text('companion'), isNull);
      expect(await text('p'), isNull);
      expect((await client.syncState())['pending'], 0);
    },
  );
  test(
    'several named calls commit together and stay independently durable',
    () async {
      final calls = await client.transaction(
        (tx) async => [await publish(tx, 'p'), await publish(tx, 'q')],
      );
      expect(calls.map((c) => c.status), everyElement(CallStatus.pending));
      expect((await client.syncState())['pending'], 2);
      await client.close();
      client = await open();
      expect((await client.syncState())['pending'], 2);
      expect(await text('p'), 'published');
      expect(await text('q'), 'published');
    },
  );
  test(
    'Call leaked from rolled-back transaction fails without committing optimism',
    () async {
      late Call<void> call;
      await expectLater(
        client.transaction((tx) async {
          call = await publish(tx, 'p');
          throw StateError('undo');
        }),
        stateError('undo'),
      );
      await expectLater(call.wait(), callError('transaction_rolled_back'));
      expect(await text('p'), isNull);
      expect((await client.syncState())['pending'], 0);
    },
  );
  test('savepoint ends only Calls owned by its scope', () async {
    late Call<void> discarded;
    final kept = await client.transaction((tx) async {
      final kept = await publish(tx, 'p');
      await expectLater(
        tx.savepoint(() async {
          discarded = await publish(tx, 'q');
          throw StateError('undo');
        }),
        stateError('undo'),
      );
      await expectLater(discarded.wait(), callError('transaction_rolled_back'));
      await expectLater(kept.wait(), callError('transaction_uncommitted'));
      return kept;
    });
    expect(kept.status, CallStatus.pending);
    expect((await client.syncState())['pending'], 1);
    expect(await text('p'), 'published');
    expect(await text('q'), isNull);
  });
  test('companion adapter expires after returning its input', () async {
    late WritePort escaped;
    await client.submitMutation<void>(
      'Publish',
      1,
      null,
      (_) {},
      input: (tx) async {
        escaped = tx;
        return input('p');
      },
    );
    await expectLater(escaped.direct(create('late', 'bad')), throwsStateError);
    expect(await text('late'), isNull);
  });
  test('parent commands cannot join an active Mutation callback', () async {
    await expectLater(
      client.transaction((tx) async {
        await publish(
          tx,
          'p',
          callback: (local) async {
            await expectLater(
              tx.read('Entry', {'id': 'e'}),
              stateError('invalid transaction capability'),
            );
            return input('p');
          },
        );
      }),
      stateError('invalid transaction capability'),
    );
    expect(await text('p'), isNull);
    expect((await client.syncState())['pending'], 0);
  });
  test(
    'unawaited companion command rolls back its owned input and companion',
    () async {
      await expectLater(
        client.submitMutation<void>(
          'Publish',
          1,
          null,
          (_) {},
          input: (tx) async {
            unawaited(
              tx.direct(create('late', 'bad')).catchError((Object _) {}),
            );
            return input('p');
          },
        ),
        throwsStateError,
      );
      expect(await text('late'), isNull);
      expect(await text('p'), isNull);
      expect((await client.syncState())['pending'], 0);
    },
  );
  for (final savepoint in [false, true]) {
    test(
      'foreign captured ${savepoint ? 'empty savepoint' : 'write'} refuses before body/admission and poisons its owner',
      () async {
        final other = await open('other');
        var ran = false;
        try {
          await expectLater(
            client.transaction((a) async {
              await other.transaction((b) async {
                if (savepoint) {
                  await expectLater(
                    a.savepoint(() async {
                      ran = true;
                    }),
                    stateError('foreign transaction scope'),
                  );
                } else {
                  await expectLater(
                    a.direct(create('foreign', 'bad')),
                    stateError('foreign transaction scope'),
                  );
                }
              });
            }),
            stateError('foreign transaction scope'),
          );
          expect(ran, isFalse);
          expect(await text('foreign'), isNull);
          expect((await client.syncState())['pending'], 0);
        } finally {
          await other.close();
        }
      },
    );
  }
  test('independent other-file ordinary reads remain available', () async {
    final other = await open('other');
    try {
      await client.transaction((tx) async {
        expect(await other.read('Entry', {'id': 'e'}), isNull);
        await tx.direct(update('inside'));
      });
      expect(await text(), 'inside');
    } finally {
      await other.close();
    }
  });
  test(
    'close during callback rolls back input, companions and queue before reopen',
    () async {
      final entered = Completer<void>(), gate = Completer<void>();
      final running = client.submitMutation<void>(
        'Publish',
        1,
        null,
        (_) {},
        input: (tx) async {
          await tx.direct(create('companion', 'bad'));
          entered.complete();
          await gate.future;
          return input('p');
        },
      );
      await entered.future;
      final closed = client.close();
      gate.complete();
      await expectLater(running, throwsA(anything));
      await closed;
      client = await open();
      expect(await text('companion'), isNull);
      expect(await text('p'), isNull);
      expect((await client.syncState())['pending'], 0);
    },
  );
  test(
    'offline schema evolution retains named queue and local view under a new materialization',
    () async {
      await client.submitMutation<void>('Edit', 1, {
        'entry': {'id': 'e', 'text': 'pending'},
      }, (_) {});
      await client.close();
      schema = jsonDecode(jsonEncode(schema)) as Map<String, dynamic>;
      final entry = (schema['models'] as List).single as Map<String, dynamic>;
      final oldEntry = jsonDecode(jsonEncode(entry)) as Map<String, dynamic>;
      entry['version'] = 2;
      (entry['fields'] as List).add({
        'name': 'extra',
        'nullable': true,
        'type': {'kind': 'scalar', 'name': 'string'},
      });
      schema['resultModels'] = [
        {...oldEntry, 'version': 1, 'enums': []},
        {...entry, 'version': 2, 'enums': []},
      ];
      (schema['models'] as List).add({
        'name': 'NewModel',
        'version': 1,
        'bootstrap': true,
        'identity': ['id'],
        'fields': [
          {
            'name': 'id',
            'nullable': false,
            'type': {'kind': 'scalar', 'name': 'string'},
          },
        ],
      });
      client = await open();
      expect((await client.syncState())['pending'], 1);
      expect(await text(), 'pending');
      expect((await client.read('Entry', {'id': 'e'}))!['extra'], isNull);
    },
  );
  test(
    'one physical owner, ordinary close and offline reopen retain direct state',
    () async {
      await expectLater(open(), throwsA(anything));
      await client.direct(update('retained'));
      await client.close();
      await client.close();
      client = await open();
      expect(await text(), 'retained');
    },
  );
  test(
    'watch publishes initial and distinct committed local views and closes',
    () async {
      final rows = <List<Map<String, dynamic>>>[];
      final done = Completer<void>();
      final sub = client.watch('Entry').listen(rows.add, onDone: done.complete);
      await Future<void>.delayed(Duration.zero);
      await client.direct(update('changed'));
      await client.direct(update('changed'));
      await Future<void>.delayed(Duration.zero);
      expect(rows.map((r) => r.single['text']), ['hello', 'changed']);
      await client.close();
      await done.future;
      await sub.cancel();
    },
  );
  test(
    'filtered watch ignores unrelated rows and failed watch reports an error',
    () async {
      final rows = <List<Map<String, dynamic>>>[];
      final sub = client.watch('Entry', where: {'id': 'e'}).listen(rows.add);
      await Future<void>.delayed(Duration.zero);
      await client.direct(create('other', 'irrelevant'));
      await client.direct(update('filtered'));
      await Future<void>.delayed(Duration.zero);
      expect(rows.map((r) => r.single['text']), ['hello', 'filtered']);
      await sub.cancel();
      await expectLater(client.watch('Missing').first, throwsA(anything));
    },
  );
}
