import 'dart:convert';
import 'dart:async';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:test/test.dart';

import 'fake_carrier.dart';

/// One client over a fresh temporary file with the Entry schema, plus a way to
/// reopen the same file. Every clause below starts from its own copy.
class Fixture {
  Fixture(this.dir, this.schema);
  final Directory dir;
  final Map<String, dynamic> schema;
  String get path => '${dir.path}/db';

  static Future<Fixture> create(String prefix) async {
    final dir = await Directory.systemTemp.createTemp(prefix);
    final schema =
        jsonDecode(
              await File('../../fixtures/schemas/entry.json').readAsString(),
            )
            as Map<String, dynamic>;
    return Fixture(dir, schema);
  }

  Future<Client> open() => Client.open(
    path: path,
    schema: schema,
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
  );

  Future<void> dispose() => dir.delete(recursive: true);
}

Map<String, dynamic> update(String text) => {
  'model': 'Entry',
  'op': 'update',
  'identity': {'id': 'e'},
  'values': {'text': text},
};

/// Creates Entry `e` with text `hello` and reads it back inside the transaction.
Future<void> seed(Client client) => client.transaction((tx) async {
  await tx.direct({
    'model': 'Entry',
    'op': 'create',
    'identity': {'id': 'e'},
    'values': {'text': 'hello'},
  });
  expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'hello');
});

Future<String?> text(Client client) async =>
    (await client.read('Entry', {'id': 'e'}))?['text'] as String?;

void main() {
  test('a checkout bundles no library: open needs libraryPath', () async {
    await expectLater(
      Client.open(path: 'unused', schema: const {}),
      throwsA(
        isA<StateError>().having(
          (error) => error.message,
          'message',
          contains('libraryPath'),
        ),
      ),
    );
  });

  group('Dart callbacks through native Rust', () {
    late Fixture fixture;
    late Client client;
    setUp(() async {
      fixture = await Fixture.create('axton-dart-test-');
      client = await fixture.open();
      await seed(client);
    });
    tearDown(() async {
      await client.close();
      await fixture.dispose();
    });

    test('a transaction reads its own writes; a throw rolls it back', () async {
      await expectLater(
        client.transaction((tx) async {
          await tx.direct(update('bad'));
          expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'bad');
          throw StateError('rollback');
        }),
        throwsStateError,
      );
      expect(await text(client), 'hello');
    });

    test(
      'raw transaction has no mutation method and captured client calls reject promptly',
      () async {
        final gate = Completer<void>();
        final entered = Completer<void>();
        final transaction = client.transaction((tx) async {
          expect(
            () => (tx as dynamic).mutate({'name': 'Edit'}),
            throwsNoSuchMethodError,
          );
          entered.complete();
          final outcome = await client
              .mutate({
                'name': 'Edit',
                'operations': [update('inside')],
              })
              .then(
                (_) => 'committed',
                onError: (Object error) => error.toString(),
              )
              .timeout(
                const Duration(milliseconds: 200),
                onTimeout: () => 'timeout',
              );
          expect(outcome, contains('transaction_active'));
          await gate.future;
        });
        try {
          await entered.future;
          final independent = client.mutate({
            'name': 'Edit',
            'operations': [update('outside')],
          });
          final state = await independent
              .then(
                (_) => 'committed',
                onError: (Object error) => error.toString(),
              )
              .timeout(
                const Duration(milliseconds: 50),
                onTimeout: () => 'queued',
              );
          expect(state, 'queued');
          gate.complete();
          await transaction;
          expect(await independent, 1);
          expect(await text(client), 'outside');
        } finally {
          if (!gate.isCompleted) gate.complete();
          await transaction;
        }
      },
    );

    test(
      'every outer client task rejects transaction_active inside a callback',
      () async {
        Matcher active() => throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            'transaction_active',
          ),
        );
        var nested = false;
        await client.transaction((tx) async {
          // Each would park behind this open transaction and deadlock.
          await expectLater(
            client
                .read('Entry', {'id': 'e'})
                .timeout(const Duration(seconds: 2)),
            active(),
          );
          await expectLater(
            client
                .transaction((_) async => nested = true)
                .timeout(const Duration(seconds: 2)),
            active(),
          );
          await expectLater(
            client.watch('Entry').first.timeout(const Duration(seconds: 2)),
            active(),
          );
          await expectLater(
            client.syncState().timeout(const Duration(seconds: 2)),
            active(),
          );
          // The transaction's own commands are unaffected.
          expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'hello');
        });
        expect(nested, isFalse, reason: 'the nested body never ran');
        expect(await text(client), 'hello', reason: 'the outer client works');
      },
    );

    test(
      'failed standalone enqueue leaves no queue entry or optimistic record',
      () async {
        await expectLater(
          client.mutate({
            'name': 'Broken',
            'operations': [
              {
                'model': 'Entry',
                'op': 'create',
                'identity': {'id': 'failed'},
                'values': {'text': 'optimistic'},
              },
              {
                'model': 'Missing',
                'op': 'create',
                'identity': {'id': 'missing'},
                'values': {'text': 'invalid'},
              },
            ],
          }),
          throwsStateError,
        );
        expect((await client.syncState())['pending'], 0);
        expect(await client.read('Entry', {'id': 'failed'}), isNull);
        expect(
          await client.mutate({
            'name': 'Edit',
            'operations': [update('after')],
          }),
          1,
        );
      },
    );

    test('an unawaited native call fails the transaction', () async {
      await expectLater(
        client.transaction((tx) async {
          tx.direct(update('forgotten'));
        }),
        throwsStateError,
      );
      expect(await text(client), 'hello');
    });

    test(
      'a failed native call fails the transaction even when caught',
      () async {
        await expectLater(
          client.transaction((tx) async {
            try {
              await tx.direct({
                'model': 'Entry',
                'op': 'create',
                'identity': {'id': 'e'},
                'values': {'text': 'duplicate'},
              });
            } catch (_) {}
          }),
          throwsStateError,
        );
        expect(await text(client), 'hello');
      },
    );

    test('a savepoint confines its rollback to its own scope', () async {
      await client.transaction((tx) async {
        try {
          await tx.savepoint(() async {
            await tx.direct(update('savepoint'));
            throw StateError('rollback');
          });
        } catch (_) {}
        expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'hello');
        await tx.direct(update('after savepoint'));
      });
      expect(await text(client), 'after savepoint');
    });

    test(
      'a child savepoint finishing after its parent fails the transaction',
      () async {
        await expectLater(
          client.transaction((tx) async {
            final gate = Completer<void>();
            Future<void>? child;
            try {
              await tx.savepoint(() async {
                await tx.direct(update('outer'));
                child = tx
                    .savepoint<void>(() async {
                      await gate.future;
                      throw StateError('late child');
                    })
                    .catchError((Object _) {});
              });
            } catch (_) {}
            gate.complete();
            await child;
          }),
          throwsStateError,
        );
        expect(await text(client), 'hello');
      },
    );

    test(
      'a frozen batch and committed state survive reopen; a closed handle refuses reads',
      () async {
        await client.mutate({
          'name': 'Edit',
          'operations': [update('offline')],
        });
        final frozen = await client.freeze();
        await client.close();
        client = await fixture.open();
        expect(await client.freeze(), frozen);
        expect(await text(client), 'offline');
        await client.close();
        await expectLater(client.read('Entry', {'id': 'e'}), throwsStateError);
      },
    );
  });

  test(
    'client close settles connection setup and remains idempotent',
    () async {
      final fixture = await Fixture.create('axton-dart-close-');
      final client = await fixture.open();
      final errors = <Object>[];
      try {
        final starting = client
            .connect(
              SyncServer(url: 'http://127.0.0.1:1', token: () => 'secret'),
              onError: errors.add,
            )
            .then<Object>((connection) => connection, onError: (Object e) => e);
        final closing = client.close();
        final outcome = await starting;
        await closing;
        await Future<void>.delayed(Duration.zero);
        expect(errors, isEmpty);
        await client.close();
        if (outcome is RuntimeConnection) {
          await outcome.close();
        } else {
          expect(
            outcome,
            isA<StateError>().having(
              (e) => e.message,
              'message',
              'client_closed',
            ),
          );
        }
        await expectLater(
          client.connect(
            SyncServer(url: 'http://127.0.0.1:1', token: () => 'secret'),
          ),
          throwsStateError,
        );
      } finally {
        await client.close();
        await fixture.dispose();
      }
    },
  );

  test(
    'the runtime refuses a second active connection, concurrent or not',
    () async {
      final fixture = await Fixture.create('axton-dart-connect-');
      final client = await fixture.open();
      final server = SyncServer(
        url: 'http://127.0.0.1:1',
        token: () => 'secret',
      );
      final refused = throwsA(
        isA<StateError>().having(
          (e) => e.message,
          'message',
          'connection already active',
        ),
      );
      try {
        final first = await client.connect(server, onError: (_) {});
        await expectLater(client.connect(server), refused);
        await first.close();
        final racing = [
          for (var i = 0; i < 2; i++)
            client
                .connect(server, onError: (_) {})
                .then<Object>((c) => c, onError: (Object e) => e),
        ];
        final settled = await Future.wait(racing);
        expect(settled.whereType<RuntimeConnection>(), hasLength(1));
        expect(
          settled.whereType<StateError>().single.message,
          'connection already active',
        );
        await settled.whereType<RuntimeConnection>().single.close();
      } finally {
        await client.close();
        await fixture.dispose();
      }
    },
  );

  test(
    'an incompatible schema keeps unsent work in the old file until rebuild is asked to leave it',
    () async {
      final fixture = await Fixture.create('axton-dart-rebuild-');
      final breaking =
          jsonDecode(jsonEncode(fixture.schema)) as Map<String, dynamic>;
      (breaking['models'][0]['fields'] as List).add({
        'name': 'due',
        'nullable': false,
        'type': {'kind': 'scalar', 'name': 'string'},
      });
      try {
        var client = await fixture.open();
        expect((await client.syncState())['schema']['rebuilt'], false);
        await seed(client);
        await client.mutate({
          'name': 'Edit',
          'operations': [update('offline')],
        });
        expect(await client.freeze(), isNotNull);
        await client.close();

        client = await Client.open(
          path: fixture.path,
          schema: breaking,
          libraryPath: Platform.environment['AXTON_LIBRARY']!,
        );
        var status = await client.syncState();
        expect(status['schema']['rebuilt'], false);
        expect(status['schema']['pending']['pending'], 1);
        expect(status['schema']['pending']['reason'], contains('due'));
        await expectLater(client.rebuild(), throwsStateError);
        final report = await client.rebuild(discardPending: true);
        expect(report['leftPending'], 1);
        expect(report['newFile'], endsWith('db.1'));
        status = await client.syncState();
        expect(status['schema']['rebuilt'], true);
        expect(status['schema']['pending'], isNull);
        expect(await text(client), isNull);
        expect(File(fixture.path).existsSync(), isTrue);
        await client.close();
      } finally {
        await fixture.dispose();
      }
    },
  );

  /// A rebuild resets the worker behind a connected lane that is asleep with no
  /// timer; the client wakes it once the native rebuild answered, so the
  /// carried Scope is subscribed again without another `connect` or `start`.
  /// The TypeScript twin is `a rebuild wakes the sleeping downlink lane without
  /// another start` ([#162](https://github.com/zanminwang/axton/issues/162)).
  test(
    'a rebuild wakes the sleeping downlink lane without another start',
    () async {
      final fixture = await Fixture.create('axton-dart-rebuild-wake-');
      final breaking =
          jsonDecode(jsonEncode(fixture.schema)) as Map<String, dynamic>;
      (breaking['models'][0]['fields'] as List).add({
        'name': 'due',
        'nullable': false,
        'type': {'kind': 'scalar', 'name': 'string'},
      });
      // Sockets are accepted and never acknowledged, and HTTP never answers: once
      // it opened its socket, the lane has nothing to do until it is woken.
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final handshakes = <Map<String, dynamic>>[];
      final sockets = <WebSocket>[];
      final closed = <Completer<void>>[];
      final served = server.listen((request) async {
        if (!WebSocketTransformer.isUpgradeRequest(request)) return;
        final socket = await WebSocketTransformer.upgrade(request);
        final done = Completer<void>();
        sockets.add(socket);
        closed.add(done);
        socket.listen(
          (message) => handshakes.add(
            jsonDecode(message as String) as Map<String, dynamic>,
          ),
          onDone: done.complete,
          onError: (Object _) {},
        );
      });
      Client? client;
      RuntimeConnection? connection;
      try {
        client = await fixture.open();
        await client.subscribe('scope');
        // Unsent work keeps the incompatible file open, so the rebuild happens
        // with this client - and its lane - already connected.
        await client.mutate({
          'name': 'Create',
          'operations': [
            {
              'model': 'Entry',
              'op': 'create',
              'identity': {'id': 'e'},
              'values': {'text': 'A', 'note': null},
            },
          ],
        });
        await client.close();
        client = await Client.open(
          path: fixture.path,
          schema: breaking,
          libraryPath: Platform.environment['AXTON_LIBRARY']!,
        );
        final reported = <Object>[];
        connection = await client.connect(
          SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 't'),
          onError: reported.add,
        );
        await _eventually(() => handshakes.length == 1, 'the first handshake');
        await Future<void>.delayed(const Duration(milliseconds: 50));
        expect(handshakes, hasLength(1), reason: 'the lane sleeps until woken');
        await client.rebuild(discardPending: true);
        await _eventually(
          () => handshakes.length == 2,
          'the carried Scope subscribed again after the rebuild',
        );
        expect(handshakes[1]['scopes'], ['scope']);
        await closed[0].future.timeout(
          const Duration(seconds: 5),
          onTimeout: () => fail('the old socket was not abandoned'),
        );
        expect(closed[1].isCompleted, isFalse, reason: 'the new socket stays');
        await Future<void>.delayed(const Duration(milliseconds: 50));
        expect(handshakes, hasLength(2), reason: 'one session per wake');
        expect(reported, isEmpty);
      } finally {
        await connection?.close();
        await client?.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await served.cancel();
        await server.close(force: true);
        await fixture.dispose();
      }
    },
  );

  // Local watch: the runtime runs the query, re-runs it after every commit and
  // publishes only a different result; the stream delivers what it publishes
  // (#134).
  group('local watch', () {
    late Fixture fixture;
    late Client client;
    setUp(() async {
      fixture = await Fixture.create('axton-dart-watch-');
      client = await fixture.open();
      await seed(client);
    });
    tearDown(() async {
      await client.close();
      await fixture.dispose();
    });

    List<String?> texts(List<Map<String, dynamic>> rows) =>
        rows.map((row) => row['text'] as String?).toList();

    test(
      'delivers the committed rows, then each different result, until cancelled',
      () async {
        final seen = <List<String?>>[];
        final watching = client
            .watch('Entry')
            .listen((rows) => seen.add(texts(rows)));
        await _eventually(() => seen.isNotEmpty, 'the initial snapshot');
        expect(seen, [
          ['hello'],
        ]);
        // A commit that changes nothing this query reads is not a new result.
        await client.direct(update('hello'));
        await client.direct(update('world'));
        await _eventually(() => seen.length == 2, 'the changed result');
        await pumpEventQueue();
        expect(seen, [
          ['hello'],
          ['world'],
        ], reason: 'an equal result is suppressed');
        await watching.cancel();
        await client.direct(update('again'));
        await Future<void>.delayed(const Duration(milliseconds: 50));
        expect(seen, hasLength(2), reason: 'a cancelled watch hears nothing');
      },
    );

    test('a filtered watch sees only its own rows', () async {
      final seen = <List<String?>>[];
      final watching = client
          .watch('Entry', where: {'text': 'other'})
          .listen((rows) => seen.add(texts(rows)));
      await _eventually(() => seen.isNotEmpty, 'the initial snapshot');
      expect(seen.single, isEmpty);
      await client.direct(update('other'));
      await _eventually(() => seen.length == 2, 'the matching row');
      expect(seen.last, ['other']);
      await watching.cancel();
    });

    test(
      'a throwing listener is reported and later results still arrive',
      () async {
        final seen = <List<String?>>[];
        final reported = <Object>[];
        late StreamSubscription<List<Map<String, dynamic>>> watching;
        runZonedGuarded(() {
          watching = client.watch('Entry').listen((rows) {
            seen.add(texts(rows));
            if (seen.length == 1) throw StateError('listener failed');
          });
        }, (error, _) => reported.add(error));
        await _eventually(() => seen.isNotEmpty, 'the initial snapshot');
        await client.direct(update('after'));
        await _eventually(() => seen.length == 2, 'the next result');
        expect(seen.last, ['after']);
        expect(reported, [
          isA<StateError>().having(
            (e) => e.message,
            'message',
            'listener failed',
          ),
        ]);
        expect(await text(client), 'after', reason: 'the commit stands');
        await watching.cancel();
      },
    );

    test('closing the client completes every watch', () async {
      final seen = <List<String?>>[];
      final done = Completer<void>();
      client
          .watch('Entry')
          .listen((rows) => seen.add(texts(rows)), onDone: done.complete);
      await _eventually(() => seen.isNotEmpty, 'the initial snapshot');
      await client.close();
      await done.future.timeout(
        const Duration(seconds: 5),
        onTimeout: () => fail('the watch did not complete'),
      );
      expect(seen, [
        ['hello'],
      ], reason: 'the terminal snapshot repeats no result');
    });

    test('a watch whose query fails reports it and ends', () async {
      final errors = <Object>[];
      final done = Completer<void>();
      client
          .watch('Missing')
          .listen((_) {}, onError: errors.add, onDone: done.complete);
      await done.future.timeout(
        const Duration(seconds: 5),
        onTimeout: () => fail('the failed watch did not end'),
      );
      expect(errors, [isA<StateError>()]);
    });
  });

  group('Mutations in a transaction', _mutationTests);
}

/// Entry with the `Recent` Load, plus `Publish` (creates an Entry), `Ping`
/// and the `Find` Query.
Map<String, dynamic> _mutationSchema() {
  Map<String, dynamic> scalar(String name) => {'kind': 'scalar', 'name': name};
  final fields = [
    {'name': 'id', 'nullable': false, 'type': scalar('string')},
    {'name': 'text', 'nullable': false, 'type': scalar('string')},
    {'name': 'note', 'nullable': true, 'type': scalar('string')},
  ];
  return {
    'enums': [],
    'models': [
      {
        'name': 'Entry',
        'version': 1,
        'identity': ['id'],
        'fields': fields,
      },
    ],
    'resultModels': [
      {
        'name': 'Entry',
        'version': 1,
        'identity': ['id'],
        'fields': fields,
        'enums': [],
      },
    ],
    'loads': [
      {
        'name': 'Recent',
        'version': 1,
        'inputs': [],
        'outputs': [
          {
            'name': 'entries',
            'kind': 'model',
            'cardinality': 'list',
            'source': 'handlerIdentity',
            'model': 'Entry',
            'modelReadVersion': 1,
            'handlerType': {
              'kind': 'identity',
              'model': 'Entry',
              'fields': [
                {'name': 'id', 'type': scalar('string')},
              ],
            },
          },
        ],
        'input': {'models': [], 'enums': []},
        'outputEnums': [],
      },
    ],
    'actions': [
      {
        'name': 'Publish',
        'version': 1,
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
      },
      {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
      {
        'name': 'Find',
        'version': 1,
        'kind': 'query',
        'inputs': [],
        'outputs': [],
      },
    ],
  };
}

Map<String, dynamic> _create(String id, String text) => {
  'model': 'Entry',
  'op': 'create',
  'identity': {'id': id},
  'values': {'text': text, 'note': null},
};
Map<String, dynamic> _remove(String id) => {
  'model': 'Entry',
  'op': 'delete',
  'identity': {'id': id},
};
Map<String, dynamic> _publish(String id) => {
  'entry': {'id': id, 'text': 'published', 'note': null},
};
String _decode(dynamic _) => 'decoded';
const _capability = 'invalid transaction capability';
Matcher _stateError(Object message) => throwsA(
  isA<StateError>().having((error) => error.message, 'message', message),
);
Matcher _callError(String code) =>
    throwsA(isA<CallError>().having((error) => error.code, 'code', code));

/// The durable queue and the two Entries the Publish clauses touch.
Future<Map<String, Object?>> _mutationState(Client client) async => {
  'pending': (await client.syncState())['pending'],
  'draft': (await client.read('Entry', {'id': 'draft'}))?['text'],
  'published': (await client.read('Entry', {'id': 'p'}))?['text'],
};
const _untouched = {'pending': 0, 'draft': 'local', 'published': null};

/// Freeze the next push and accept every call in it, with the authority of
/// each Entry it creates; the freeze body is returned.
Future<Map<String, dynamic>> _accept(
  Client client, {
  List<Map<String, dynamic>> records = const [],
}) async {
  final batch = jsonDecode((await client.freeze())!) as Map<String, dynamic>;
  final mutations = (batch['mutations'] as List).cast<Map<String, dynamic>>();
  await client.acknowledge(batch['batchSequence'] as int, {
    'clientId': batch['clientId'],
    'batchSequence': batch['batchSequence'],
    'rejections': [],
    'completions': [
      for (final mutation in mutations)
        {
          'callId': mutation['callId'],
          'outcome': {'status': 'succeeded', 'result': null},
        },
    ],
    'records': [
      ...records,
      for (final mutation in mutations)
        if ((mutation['args'] as Map)['entry'] case final Map entry)
          {
            'model': 'Entry',
            'identity': {'id': entry['id']},
            'stamp': 1,
            'state': {'text': entry['text'], 'note': entry['note']},
          },
    ],
  });
  return batch;
}

void _mutationTests() {
  test('outer commands are refused without reaching the runtime while a '
      'local submission is unfinished', () async {
    // A runtime that parks the submission until the test answers it.
    String? transaction;
    String? submission;
    final carrier = FakeCarrier((envelope) {
      final requestId = envelope['requestId'] as String?;
      final command = envelope['command'] as Map<String, dynamic>?;
      if (envelope['type'] == 'callbackResult') {
        return [
          {
            'type': 'taskCompleted',
            'requestId': transaction,
            'ok': false,
            'value': null,
            'error': envelope['error'],
          },
        ];
      }
      if (envelope['type'] != 'task' &&
          envelope['type'] != 'transactionCommand') {
        return null;
      }
      final local = command?['local'] == true;
      switch (command?['kind']) {
        case 'transaction':
          transaction = requestId;
          return [
            {
              'type': 'effect',
              'effectId': '5',
              'operation': {
                'kind': 'callback',
                'transactionId': 'tx',
                'requestId': requestId,
              },
            },
          ];
        case 'submitMutation' when local:
          submission = requestId;
          return [];
        case 'submitMutation':
          return [
            completed(requestId!, {'callId': 'plain', 'ordinal': 2}),
          ];
        default:
          return [completed(requestId!)];
      }
    });
    final fake = await Client.open(
      path: 'unused',
      schema: const {},
      carrier: carrier,
    );
    try {
      await expectLater(
        fake.transaction((tx) async {
          final submitted = tx.submitMutation(
            'Publish',
            1,
            {'id': 'p'},
            _decode,
            local: (_) async {},
          );
          for (final refused in [
            tx.read('Entry', {'id': 'e'}),
            tx.direct(const {}),
            tx.submitMutation('Ping', 1, const {}, _decode),
            tx.scopes.subscribe('book'),
            tx.savepoint(() async {}),
          ]) {
            await expectLater(refused, _stateError(_capability));
          }
          carrier.publish([
            completed(submission!, {'callId': 'call', 'ordinal': 1}),
          ]);
          await submitted;
          // Completed: the parent handle is admitted again; a submission
          // without a callback blocks nothing.
          final plain = tx.submitMutation('Ping', 1, const {}, _decode);
          await tx.read('Entry', {'id': 'e'});
          await plain;
        }),
        _stateError(_capability),
      );
      expect(carrier.commands.map((command) => command['kind']), [
        'transaction',
        'submitMutation',
        'submitMutation',
        'read',
      ]);
      expect(carrier.commands[1], {
        'kind': 'submitMutation',
        'name': 'Publish',
        'version': 1,
        'args': {'id': 'p'},
        'local': true,
      });
    } finally {
      await fake.close();
    }
  });

  late Directory dir;
  late Client client;
  Map<String, StoreHook>? hooks;
  Future<Client> open() => Client.open(
    path: '${dir.path}/db',
    schema: _mutationSchema(),
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
    onStore: hooks,
  );
  setUp(() async {
    hooks = null;
    dir = await Directory.systemTemp.createTemp('axton-dart-mutations-');
    client = await open();
    await client.direct(_create('draft', 'local'));
  });
  tearDown(() async {
    await client.close();
    await dir.delete(recursive: true);
  });

  test('a local callback runs inside its submission and its Call is '
      'provisional until the commit', () async {
    final order = <String>[];
    final running = client.transaction((tx) async {
      final call = await tx.submitMutation(
        'Publish',
        1,
        _publish('p'),
        _decode,
        local: (local) async {
          order.add('local');
          // The callback reads the call's optimism and earlier writes.
          expect(
            (await local.read('Entry', {'id': 'p'}))!['text'],
            'published',
          );
          expect(local, isA<LocalTransaction>());
          await local.direct(_remove('draft'));
        },
      );
      order.add('submitted');
      // Durable absence before the commit is checked by an independent
      // SQLite reader in the Node harness; here the handle shows it.
      expect(call.status, CallStatus.pending);
      // An early wait is an observation error only: caught, the call stays
      // usable.
      await expectLater(call.wait(), _callError('transaction_uncommitted'));
      expect(call.status, CallStatus.pending);
      expect(await tx.read('Entry', {'id': 'draft'}), isNull);
      return (call, 7);
    });
    final (call, value) = await running;
    expect(value, 7);
    expect(order, ['local', 'submitted']);
    expect(await _mutationState(client), {
      'pending': 1,
      'draft': null,
      'published': 'published',
    });
    final waiting = call.wait();
    final batch = await _accept(client);
    // The companion delete is never sent.
    expect(jsonEncode(batch).contains('draft'), isFalse);
    expect(((await waiting) as CallSuccess<String>).result, 'decoded');
    expect(call.status, CallStatus.succeeded);
    expect(await client.read('Entry', {'id': 'draft'}), isNull);
  });

  test('several Calls commit together and complete separately', () async {
    final (first, second) = await client.transaction(
      (tx) async => (
        await tx.submitMutation('Ping', 1, const {}, _decode),
        await tx.submitMutation('Ping', 1, const {}, (_) => 'second'),
      ),
    );
    expect((await client.syncState())['pending'], 2);
    // Completion arrives before anybody waits: the handle was routed at
    // submission and keeps the outcome.
    await _accept(client);
    expect(((await first.wait()) as CallSuccess<String>).result, 'decoded');
    expect(((await second.wait()) as CallSuccess<String>).result, 'second');
    expect(await client.transaction((_) async => 'plain'), 'plain');
  });

  test('a Call leaked from a failed transaction is rolled back', () async {
    final thrown = StateError('rollback');
    late Call<String> leaked;
    await expectLater(
      client.transaction((tx) async {
        leaked = await tx.submitMutation(
          'Publish',
          1,
          _publish('p'),
          _decode,
          local: (local) => local.direct(_remove('draft')),
        );
        throw thrown;
      }),
      throwsA(same(thrown)),
    );
    expect(leaked.status, CallStatus.failed);
    await expectLater(leaked.wait(), _callError('transaction_rolled_back'));
    await expectLater(leaked.wait(), _callError('transaction_rolled_back'));
    expect(await _mutationState(client), _untouched);
    // An uncaught early wait fails the transaction like any thrown error.
    await expectLater(
      client.transaction((tx) async {
        final call = await tx.submitMutation('Ping', 1, const {}, _decode);
        await call.wait();
      }),
      _callError('transaction_uncommitted'),
    );
    expect(await _mutationState(client), _untouched);
  });

  test(
    'a failed local callback fails its submission and the transaction',
    () async {
      Future<void> attempt(
        Future<void> Function(WritePort local) local,
        Matcher submission,
      ) async {
        await expectLater(
          client.transaction((tx) async {
            await expectLater(
              tx.submitMutation(
                'Publish',
                1,
                _publish('p'),
                _decode,
                local: local,
              ),
              submission,
            );
          }),
          throwsA(anything),
        );
        expect(await _mutationState(client), _untouched);
      }

      // Thrown: the submission fails with that very value.
      final thrown = StateError('boom');
      await attempt((local) async {
        await local.direct(_remove('draft'));
        throw thrown;
      }, throwsA(same(thrown)));
      // A caught failed command still fails it.
      await attempt((local) async {
        await local.direct(_remove('draft'));
        await local.direct(_create('p', 'twice')).catchError((Object _) {});
      }, throwsA(isA<StateError>()));
      // Unawaited work fails it.
      await attempt((local) async {
        unawaited(local.direct(_remove('draft')));
      }, _stateError('unawaited transaction operation'));
      // A refused submission never runs its callback.
      var ran = false;
      await expectLater(
        client.transaction(
          (tx) => tx.submitMutation(
            'Find',
            1,
            const {},
            _decode,
            local: (_) async => ran = true,
          ),
        ),
        _stateError(contains('cannot be submitted in a transaction')),
      );
      expect(ran, isFalse);
      expect(await _mutationState(client), _untouched);
    },
  );

  test('the local adapter exposes only local reads and writes and expires '
      'with its callback', () async {
    late WritePort captured;
    Object? refusal;
    await expectLater(
      client.transaction((tx) async {
        await tx.submitMutation(
          'Publish',
          1,
          _publish('p'),
          _decode,
          local: (local) async {
            captured = local;
            expect(local, isNot(isA<Transaction>()));
            expect(local, isNot(isA<SubmitMutationPort>()));
            final raw = local as LocalTransaction;
            expect(await raw.readSql('SELECT 1 AS one'), [
              {'one': 1},
            ]);
            expect(await raw.query('Entry'), hasLength(2));
            await local.direct(_remove('draft'));
          },
        );
        // An expired handle is refused as the runtime refuses a stale
        // capability, and poisons the open transaction even when caught.
        try {
          await captured.direct(_create('late', 'late'));
        } catch (error) {
          refusal = error;
        }
      }),
      _stateError(_capability),
    );
    expect(refusal, isA<StateError>());
    expect((refusal as StateError).message, _capability);
    expect(await _mutationState(client), _untouched);
    expect(await client.read('Entry', {'id': 'late'}), isNull);
    // Once the transaction ended the handle is simply closed.
    await expectLater(
      captured.read('Entry', {'id': 'p'}),
      _stateError('transaction_closed'),
    );
  });

  test('outer transaction commands are refused while a local callback is '
      'unfinished', () async {
    // A captured parent handle inside the callback.
    await expectLater(
      client.transaction((tx) async {
        await tx.submitMutation(
          'Publish',
          1,
          _publish('p'),
          _decode,
          local: (local) async {
            await expectLater(
              tx.direct(_create('x', 'x')),
              _stateError(_capability),
            );
            await expectLater(
              tx.submitMutation('Ping', 1, const {}, _decode),
              _stateError(_capability),
            );
            await expectLater(
              tx.scopes.subscribe('book'),
              _stateError(_capability),
            );
            await expectLater(
              tx.savepoint(() async {}),
              _stateError(_capability),
            );
            await local.direct(_remove('draft'));
          },
        );
      }),
      _stateError(_capability),
    );
    expect(await _mutationState(client), _untouched);
    // Pipelined behind an unawaited submission: refused whatever the timing.
    await expectLater(
      client.transaction((tx) async {
        final submission = tx.submitMutation(
          'Publish',
          1,
          _publish('p'),
          _decode,
          local: (local) => local.direct(_remove('draft')),
        );
        await expectLater(
          tx.read('Entry', {'id': 'draft'}),
          _stateError(_capability),
        );
        await submission;
        // Once the submission completed the parent handle works again.
        expect(await tx.read('Entry', {'id': 'draft'}), isNull);
      }),
      _stateError(_capability),
    );
    expect(await _mutationState(client), _untouched);
  });

  test('client calls, Loads and Fetches stay refused inside a local '
      'callback', () async {
    final job = await client.startLoad('Recent', 1, const {});
    await client.transaction((tx) async {
      await tx.submitMutation(
        'Publish',
        1,
        _publish('p'),
        _decode,
        local: (local) async {
          Matcher load(String code) =>
              throwsA(isA<LoadException>().having((e) => e.code, 'code', code));
          await expectLater(
            client.startLoad('Recent', 1, const {}),
            load('transaction_active'),
          );
          await expectLater(job.wait(), load('transaction_active'));
          await expectLater(
            client.invalidateLoad('Recent', const {}),
            load('transaction_active'),
          );
          await expectLater(
            client.fetchModel('Entry', 1, {'id': 'draft'}, (row) => row),
            _callError('transaction_active'),
          );
          await expectLater(
            client.invokeQuery('Find', 1, const {}, _decode),
            _callError('transaction_active'),
          );
          await expectLater(
            client.invokeAction('Ping', 1, const {}, _decode),
            _callError('transaction_active'),
          );
          await expectLater(
            client.read('Entry', {'id': 'p'}),
            _stateError('transaction_active'),
          );
          await local.direct(_remove('draft'));
        },
      );
    });
    expect(await _mutationState(client), {
      'pending': 1,
      'draft': null,
      'published': 'published',
    });
  });

  test('close during a local callback releases its submission and commits '
      'nothing', () async {
    final entered = Completer<void>();
    final gate = Completer<void>();
    late Future<Call<String>> submission;
    final running = client.transaction((tx) async {
      submission = tx.submitMutation(
        'Publish',
        1,
        _publish('p'),
        _decode,
        local: (local) async {
          entered.complete();
          await gate.future;
          await local.direct(_remove('draft')).catchError((Object _) {});
        },
      );
      await submission;
    });
    await entered.future;
    final closing = client.close();
    gate.complete();
    await expectLater(running, throwsA(anything));
    await expectLater(submission, throwsA(anything));
    await closing;
    client = await open();
    expect(await _mutationState(client), _untouched);
    // A provisional Call the close rolled back.
    final inside = Completer<void>();
    final hold = Completer<void>();
    late Call<String> call;
    final holding = client.transaction((tx) async {
      call = await tx.submitMutation('Ping', 1, const {}, _decode);
      inside.complete();
      await hold.future;
    });
    await inside.future;
    final closed = client.close();
    hold.complete();
    await expectLater(holding, throwsA(anything));
    await closed;
    expect(call.status, CallStatus.failed);
    await expectLater(call.wait(), _callError('transaction_rolled_back'));
    client = await open();
    expect(await _mutationState(client), _untouched);
  });

  test('an onStore callback cannot submit a Mutation or run a local '
      'callback', () async {
    var refused = 0;
    var ran = false;
    await client.close();
    hooks = {
      'Entry': (tx, _) async {
        await expectLater(
          tx.submitMutation(
            'Ping',
            1,
            const {},
            _decode,
            local: (_) async => ran = true,
          ),
          _stateError('store hook cannot submit a Mutation'),
        );
        refused++;
      },
    };
    client = await open();
    await client.invokeAction('Ping', 1, const {}, _decode);
    await expectLater(
      _accept(
        client,
        records: [
          {
            'model': 'Entry',
            'identity': {'id': 's'},
            'stamp': 1,
            'state': {'text': 'server', 'note': null},
          },
        ],
      ),
      throwsA(anything),
    );
    expect(refused, 1);
    expect(ran, isFalse);
    expect(await client.read('Entry', {'id': 's'}), isNull);
  });

  test('a savepoint rollback ends only the Calls of its scope', () async {
    final (kept, discarded) = await client.transaction((tx) async {
      final kept = await tx.submitMutation('Ping', 1, const {}, _decode);
      late Call<String> discarded;
      await expectLater(
        tx.savepoint<void>(() async {
          discarded = await tx.submitMutation(
            'Publish',
            1,
            _publish('p'),
            _decode,
            local: (local) => local.direct(_remove('draft')),
          );
          throw StateError('undo');
        }),
        _stateError('undo'),
      );
      expect(discarded.status, CallStatus.failed);
      await expectLater(
        discarded.wait(),
        _callError('transaction_rolled_back'),
      );
      await expectLater(kept.wait(), _callError('transaction_uncommitted'));
      return (kept, discarded);
    });
    expect(await _mutationState(client), {
      'pending': 1,
      'draft': 'local',
      'published': null,
    });
    await _accept(client);
    expect(await kept.wait(), isA<CallSuccess<String>>());
    await expectLater(discarded.wait(), _callError('transaction_rolled_back'));
  });
}

/// Poll [condition] until it holds or five seconds pass.
Future<void> _eventually(bool Function() condition, String what) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('$what timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
