import 'dart:convert';
import 'dart:async';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:test/test.dart';

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
  test('default native loader is reserved for iOS process symbols', () async {
    if (Platform.isIOS) return;
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
    'client close waits for connection setup and remains idempotent',
    () async {
      final fixture = await Fixture.create('axton-dart-close-');
      final client = await fixture.open();
      final errors = <Object>[];
      try {
        final starting = client.connect(
          SyncServer(url: 'http://127.0.0.1:1', token: () => 'secret'),
          onError: errors.add,
        );
        await Future.wait([starting, client.close()]);
        await Future<void>.delayed(Duration.zero);
        expect(errors, isEmpty);
        await client.close();
        await (await starting).close();
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
  /// carried Channel is subscribed again without another `connect` or `start`.
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
          'the carried Channel subscribed again after the rebuild',
        );
        expect(handshakes[1]['channels'], ['scope']);
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
}

/// Poll [condition] until it holds or five seconds pass.
Future<void> _eventually(bool Function() condition, String what) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) fail('$what timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
