import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:axton/axton.dart';
import 'package:axton/src/bridge.dart';
import 'package:test/test.dart';

import 'fake_carrier.dart';

/// A fresh temporary file with the Entry schema, opened through the real
/// native runtime as a [Client] or a bare [Bridge].
class Fixture {
  Fixture(this.dir, this.schema);
  final Directory dir;
  final Map<String, dynamic> schema;
  String get path => '${dir.path}/db';
  static String get library => Platform.environment['AXTON_LIBRARY']!;

  static Future<Fixture> create(String prefix) async {
    final dir = await Directory.systemTemp.createTemp(prefix);
    final schema =
        jsonDecode(
              await File('../../fixtures/schemas/entry.json').readAsString(),
            )
            as Map<String, dynamic>;
    return Fixture(dir, schema);
  }

  Future<Client> client() =>
      Client.open(path: path, schema: schema, libraryPath: library);
  Future<Bridge> bridge() =>
      Bridge.open(path: path, schema: schema, libraryPath: library);
  Future<void> dispose() => dir.delete(recursive: true);
}

Map<String, dynamic> create(String text) => {
  'model': 'Entry',
  'op': 'create',
  'identity': {'id': 'e'},
  'values': {'text': text},
};

Map<String, dynamic> update(String text) => {
  'model': 'Entry',
  'op': 'update',
  'identity': {'id': 'e'},
  'values': {'text': text},
};

Future<String?> text(Client client) async =>
    (await client.read('Entry', {'id': 'e'}))?['text'] as String?;

Future<Map<String, dynamic>> envelopeFixtures() async =>
    jsonDecode(
          await File('../../fixtures/bridge/envelopes.json').readAsString(),
        )
        as Map<String, dynamic>;

/// Whether [future] settles within [wait]: `'settled'` or `'pending'`.
Future<String> settles(Future<Object?> future, [int wait = 50]) => future
    .then<String>((_) => 'settled', onError: (Object _) => 'settled')
    .timeout(Duration(milliseconds: wait), onTimeout: () => 'pending');

void main() {
  late Fixture fixture;
  setUp(() async => fixture = await Fixture.create('axton-bridge-'));
  tearDown(() => fixture.dispose());

  test('overlapping tasks return to the waiter that submitted them', () async {
    final client = await fixture.client();
    try {
      await client.transaction((tx) => tx.direct(create('hello')));
      final gate = Completer<void>();
      final entered = Completer<void>();
      final order = <String>[];
      final transaction = client.transaction((tx) async {
        await tx.direct(update('inside'));
        expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'inside');
        entered.complete();
        await gate.future;
        await tx.direct(update('committed'));
      });
      unawaited(transaction.then((_) => order.add('transaction')));
      await entered.future;
      final read = client.read('Entry', {'id': 'e'});
      final state = client.syncState();
      unawaited(read.then((_) => order.add('read')));
      unawaited(state.then((_) => order.add('syncState')));
      expect(await settles(read), 'pending');
      expect(await settles(state), 'pending');
      gate.complete();
      await transaction;
      expect((await read)!['text'], 'committed');
      expect((await state)['pending'], 0);
      expect(order, ['transaction', 'read', 'syncState']);
      expect(await text(client), 'committed');
    } finally {
      await client.close();
    }
  });

  test('a task after close fails client_closed; close is idempotent', () async {
    final bridge = await fixture.bridge();
    expect(bridge.opened['clientId'], isA<String>());
    await bridge.close();
    await expectLater(
      bridge.task({'kind': 'status'}),
      throwsA(
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
    await bridge.close();
    expect(Bridge.attached, isNot(contains(bridge.runtimeId)));
  });

  test(
    'a throwing callback rethrows the same object and rolls back; a caught failed command fails at commit',
    () async {
      final client = await fixture.client();
      try {
        await client.transaction((tx) => tx.direct(create('hello')));
        final thrown = _Thrown();
        Object? caught;
        try {
          await client.transaction((tx) async {
            await tx.direct(update('rolled back'));
            throw thrown;
          });
        } catch (error) {
          caught = error;
        }
        expect(identical(caught, thrown), isTrue);
        expect(await text(client), 'hello');

        Object? engine;
        Object? failed;
        try {
          await client.transaction((tx) async {
            try {
              await tx.direct(create('duplicate'));
            } catch (error) {
              engine = error;
            }
            await tx.direct(update('after failure'));
          });
        } catch (error) {
          failed = error;
        }
        expect(engine, isA<StateError>());
        expect(
          failed,
          isA<StateError>().having(
            (e) => e.message,
            'message',
            (engine! as StateError).message,
          ),
        );
        expect(await text(client), 'hello');
      } finally {
        await client.close();
      }
    },
  );

  test('nested savepoints carry the scope Rust issued for them', () async {
    final client = await fixture.client();
    try {
      await client.transaction((tx) async {
        await tx.direct(create('top'));
        await tx.savepoint(() async {
          await tx.direct(update('outer'));
          try {
            await tx.savepoint(() async {
              await tx.direct(update('inner'));
              throw StateError('inner');
            });
          } on StateError catch (_) {}
          expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'outer');
          await tx.savepoint(() => tx.direct(update('released')));
        });
        expect((await tx.read('Entry', {'id': 'e'}))!['text'], 'released');
      });
      expect(await text(client), 'released');
    } finally {
      await client.close();
    }
  });

  test('a failed open throws and leaves nothing attached', () async {
    final before = Bridge.attached.toSet();
    await expectLater(
      Bridge.open(
        path: '${fixture.dir.path}/missing/dir/db',
        schema: fixture.schema,
        libraryPath: Fixture.library,
      ),
      throwsStateError,
    );
    expect(Bridge.attached.toSet(), before);
    final client = await fixture.client();
    try {
      await client.transaction((tx) => tx.direct(create('after')));
      expect(await text(client), 'after');
    } finally {
      await client.close();
    }
    expect(Bridge.attached.toSet(), before);
  });

  test('close during an open callback fails its later commands', () async {
    final client = await fixture.client();
    final gate = Completer<void>();
    final entered = Completer<void>();
    final later = Completer<Object>();
    final transaction = client.transaction((tx) async {
      await tx.direct(create('never'));
      entered.complete();
      await gate.future;
      try {
        await tx.read('Entry', {'id': 'e'});
        later.complete('succeeded');
      } catch (error) {
        later.complete(error);
      }
    });
    await entered.future;
    final closing = client.close();
    await expectLater(
      transaction,
      throwsA(
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
    gate.complete();
    expect(
      await later.future,
      isA<StateError>().having(
        (e) => e.message,
        'message',
        anyOf('transaction_closed', 'client_closed'),
      ),
    );
    await closing;
    final reopened = await fixture.client();
    try {
      expect(await text(reopened), isNull, reason: 'the open unit rolled back');
    } finally {
      await reopened.close();
    }
  });

  test(
    'close is priority control while a connected callback holds the transaction',
    () async {
      final client = await fixture.client();
      await client.connect(
        SyncServer(url: 'http://127.0.0.1:1', token: () => 't'),
        onError: (_) {},
      );
      final gate = Completer<void>();
      final entered = Completer<void>();
      final later = Completer<Object>();
      final transaction = client.transaction((tx) async {
        await tx.direct(create('never'));
        entered.complete();
        await gate.future;
        try {
          await tx.read('Entry', {'id': 'e'});
          later.complete('succeeded');
        } catch (error) {
          later.complete(error);
        }
      });
      final outcome = transaction.then<Object?>(
        (_) => null,
        onError: (Object error) => error,
      );
      await entered.future;
      // The connection's stop would park behind the callback; close does not.
      await client.close().timeout(const Duration(seconds: 5));
      expect(gate.isCompleted, isFalse, reason: 'close did not need the gate');
      expect(
        await outcome,
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      );
      gate.complete();
      expect(
        await later.future,
        isA<StateError>().having(
          (e) => e.message,
          'message',
          anyOf('transaction_closed', 'client_closed'),
        ),
      );
      final reopened = await fixture.client();
      try {
        expect(await text(reopened), isNull, reason: 'the unit rolled back');
      } finally {
        await reopened.close();
      }
    },
  );

  test('close settles a connect parked behind an open callback', () async {
    final client = await fixture.client();
    final gate = Completer<void>();
    final entered = Completer<void>();
    final transaction = client.transaction((tx) async {
      entered.complete();
      await gate.future;
    });
    final failed = transaction.then<Object?>(
      (_) => null,
      onError: (Object error) => error,
    );
    await entered.future;
    final connecting = client
        .connect(
          SyncServer(url: 'http://127.0.0.1:1', token: () => 't'),
          onError: (_) {},
        )
        .then<Object?>((_) => null, onError: (Object error) => error);
    await client.close().timeout(const Duration(seconds: 5));
    expect(gate.isCompleted, isFalse);
    expect(
      await connecting,
      isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
    );
    expect(await failed, isA<StateError>());
    gate.complete();
  });

  test('a callback whose task close already refused never runs', () async {
    // The runtime refused the transaction in the batch that asked for its
    // callback: the effect, its cancellation, the refusal and the end.
    String? transaction;
    final carrier = FakeCarrier((envelope) {
      if ((envelope['command'] as Map?)?['kind'] == 'transaction') {
        transaction = envelope['requestId'] as String;
        return const [];
      }
      if (envelope['type'] != 'close') return null;
      return [
        {
          'type': 'effect',
          'effectId': '5',
          'operation': {
            'kind': 'callback',
            'transactionId': 'tx1',
            'requestId': transaction,
          },
        },
        {'type': 'cancelEffect', 'effectId': '5'},
        {
          'type': 'taskCompleted',
          'requestId': transaction,
          'ok': false,
          'error': 'client_closed',
        },
        {'type': 'runtimeClosed'},
      ];
    });
    final client = await Client.open(
      path: 'unused',
      schema: const {},
      carrier: carrier,
    );
    var ran = false;
    final refused = client.transaction((tx) async => ran = true);
    final closing = client.close();
    await expectLater(
      refused,
      throwsA(
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
    await closing;
    await pumpEventQueue();
    expect(ran, isFalse, reason: 'the callback of a refused task never runs');
    expect(
      carrier.admitted.where((e) => e['type'] == 'callbackResult'),
      isEmpty,
    );
  });

  test('a cancelled callback effect never runs its callback', () async {
    // The runtime cancelled the callback before the bridge started it; the
    // task is still pending until the runtime settles it.
    String? transaction;
    late FakeCarrier carrier;
    carrier = FakeCarrier((envelope) {
      if ((envelope['command'] as Map?)?['kind'] != 'transaction') return null;
      transaction = envelope['requestId'] as String;
      return [
        {
          'type': 'effect',
          'effectId': '5',
          'operation': {
            'kind': 'callback',
            'transactionId': 'tx1',
            'requestId': transaction,
          },
        },
        {'type': 'cancelEffect', 'effectId': '5'},
      ];
    });
    final client = await Client.open(
      path: 'unused',
      schema: const {},
      carrier: carrier,
    );
    var ran = false;
    final refused = client.transaction((tx) async => ran = true);
    await pumpEventQueue();
    expect(ran, isFalse);
    carrier.publish([
      {
        'type': 'taskCompleted',
        'requestId': transaction,
        'ok': false,
        'error': 'transaction.cancelled',
      },
    ]);
    await expectLater(refused, throwsStateError);
    expect(ran, isFalse);
    await client.close();
  });

  test('the bridge envelopes match the shared fixtures', () async {
    final fixtures = await envelopeFixtures();
    final inputs = (fixtures['inputs'] as List).cast<Map<String, dynamic>>();
    final events = (fixtures['events'] as List).cast<Map<String, dynamic>>();
    // Every input the bridge builds has the spelling Rust decodes.
    final built = [
      for (final input in inputs)
        switch (input['type']) {
          'task' => Bridge.taskEnvelope(
            input['requestId'] as String,
            input['command'] as Map<String, dynamic>,
          ),
          'transactionCommand' => Bridge.transactionCommandEnvelope(
            input['requestId'] as String,
            input['transactionId'] as String,
            input['scope'] as String?,
            input['command'] as Map<String, dynamic>,
          ),
          'callbackResult' => Bridge.callbackResultEnvelope(
            input['effectId'] as String,
            input['transactionId'] as String,
            ok: input['ok'] as bool,
            error: input['error'] as String?,
          ),
          'effectResult' => Bridge.effectResultEnvelope(
            input['effectId'] as String,
            ok: (input['outcome'] as Map)['ok'] as bool,
            value: (input['outcome'] as Map)['value'],
            error:
                ((input['outcome'] as Map)['error'] as Map?)?['message']
                    as String?,
            status:
                ((input['outcome'] as Map)['error'] as Map?)?['status'] as int?,
          ),
          'close' => Bridge.closeEnvelope,
          final type => fail('unknown input type $type'),
        },
    ];
    expect(jsonDecode(jsonEncode(built)), inputs);
    // Every event carries what the dispatcher switches on.
    final seen = <String>{};
    for (final event in events) {
      final type = event['type'] as String;
      seen.add(type);
      switch (type) {
        case 'taskCompleted':
          expect(event['requestId'], isA<String>());
          expect(event['ok'], isA<bool>());
          expect(event.containsKey('value'), isTrue);
          if (event['ok'] == false) expect(event['error'], isA<String>());
          // A failure's machine-readable reason, when it has one.
          if (event.containsKey('details')) {
            expect((event['details'] as Map)['code'], isA<String>());
          }
        case 'effect':
          expect(event['effectId'], isA<String>());
          final operation = event['operation'] as Map<String, dynamic>;
          expect(operation['kind'], isA<String>());
          if (operation['kind'] == 'callback') {
            expect(operation['transactionId'], isA<String>());
            expect(operation['requestId'], isA<String>());
          }
        case 'cancelEffect':
          expect(event['effectId'], isA<String>());
        case 'callCompleted':
          expect(event['callId'], isA<String>());
          expect(event.containsKey('outcome'), isTrue);
        case 'observerChanged':
          expect(event['observerId'], isA<String>());
          expect(event.containsKey('snapshot'), isTrue);
        case 'report':
          expect((event['diagnostic'] as Map)['kind'], isA<String>());
        case 'runtimeClosed':
          break;
        default:
          fail('unknown event type $type');
      }
    }
    expect(seen, {
      'taskCompleted',
      'effect',
      'cancelEffect',
      'callCompleted',
      'observerChanged',
      'report',
      'runtimeClosed',
    });
  });

  test('the commands the client builds match the shared fixtures', () async {
    final fixtures = await envelopeFixtures();
    final byId = <String, Map<String, dynamic>>{
      for (final input
          in (fixtures['inputs'] as List).cast<Map<String, dynamic>>())
        if (input['requestId'] case final String id) id: input,
    };
    Map<String, dynamic> command(String id) =>
        byId[id]!['command'] as Map<String, dynamic>;
    // A runtime that answers every command at once, and runs the one
    // transaction's callback.
    String? transaction;
    final carrier = FakeCarrier((envelope) {
      final requestId = envelope['requestId'] as String?;
      final kind = (envelope['command'] as Map?)?['kind'];
      if (envelope['type'] == 'callbackResult') {
        return [completed(transaction!)];
      }
      if (kind == 'transaction') {
        transaction = requestId;
        return [
          {
            'type': 'effect',
            'effectId': '5',
            'operation': {
              'kind': 'callback',
              'transactionId': 'tx7',
              'requestId': requestId,
            },
          },
        ];
      }
      if (requestId == null) return null;
      return [
        completed(requestId, switch (kind) {
          'query' || 'sql' || 'querySpec' || 'referencing' || 'tasks' => [],
          'status' || 'recordStatus' || 'rebuild' || 'pull' => {},
          'enqueue' => 1,
          'submitAction' => {'callId': 'c1', 'ordinal': 1},
          'invoke' => {
            'outcome': {'status': 'succeeded', 'result': null},
          },
          'scopeSubscribe' => {
            'state': {'scope': 'book', 'subscriptionId': 1},
            'observerId': '9',
          },
          'watch' => {'observerId': '3'},
          'savepoint' => {'scope': 'sp1'},
          _ => null,
        }),
      ];
    });
    final client = await Client.open(
      path: 'unused',
      schema: const {},
      carrier: carrier,
    );
    final expected = <Map<String, dynamic>>[];
    Future<T> step<T>(String id, Future<T> Function() call) {
      expected.add(command(id));
      return call();
    }

    try {
      await step('101', () => client.read('Todo', {'id': 't'}));
      await step('102', () => client.query('Todo', where: {'done': false}));
      await step(
        '103',
        () => client.readSql(
          'SELECT count(*) AS n FROM Todo WHERE done = ?',
          parameters: [false],
        ),
      );
      await step(
        '104',
        () => client.querySpec(
          'Todo',
          command('104')['query'] as Map<String, dynamic>,
        ),
      );
      await step('105', () => client.related('Todo', {'id': 't'}, 'owner'));
      await step(
        '106',
        () => client.referencing('User', {'id': 'u'}, 'Todo', 'owner'),
      );
      await step('107', client.syncState);
      await step('108', () => client.recordSyncState('Todo', {'id': 't'}));
      await step('109', client.pendingTasks);
      await step(
        '110',
        () => client.mutate(command('110')['mutation'] as Map<String, dynamic>),
      );
      await step(
        '113',
        () => client.submitAction('Ping', 1, {}, store: const _Store(false)),
      );
      await step('114', () => client.setReadiness('k', 'ready'));
      await step('115', () => client.drop(3));
      await step('116', () => client.dismissRejection(4));
      await step(
        '117',
        () => client.invalidateQuery('GetTodo', 1, {'id': 't'}),
      );
      await step('118', () => client.rebuild(discardPending: true));
      await step('119', client.freeze);
      await step(
        '120',
        () => client.acknowledge(
          1,
          command('120')['receipt'] as Map<String, dynamic>,
        ),
      );
      await step(
        '121',
        () => client.applyPull(command('121')['page'] as Map<String, dynamic>),
      );
      final subscription = await step(
        '122',
        () => client.subscribeScope('book'),
      );
      await step('124', subscription.bootstrap);
      await step('126', subscription.unsubscribe);
      await step(
        '127',
        () => client.transaction((tx) async {
          await step('134', () => tx.read('Todo', {'id': 't'}));
          await step('136', () => tx.readSql('SELECT 1 AS one'));
          await step(
            '137',
            () => tx.querySpec(
              'Todo',
              command('137')['query'] as Map<String, dynamic>,
            ),
          );
          await step('138', () => tx.related('Todo', {'id': 't'}, 'owner'));
          await step(
            '139',
            () => tx.referencing('User', {'id': 'u'}, 'Todo', 'owner'),
          );
          // A savepoint's own commands carry the scope Rust issued for it.
          await step(
            '143',
            () => tx.savepoint(
              () => step(
                '141',
                () => tx.direct(
                  command('141')['operation'] as Map<String, dynamic>,
                ),
              ),
            ),
          );
          expected
            ..add(command('144'))
            ..add(command('143'))
            // A rollback names the scope it closes, the spelling the
            // fixture shows for `release`; Rust accepts either.
            ..add({...command('145'), 'scope': 'sp1'});
          await tx
              .savepoint<void>(() async => throw StateError('rolled back'))
              .then((_) {}, onError: (Object _) {});
        }),
      );
      final connection = await step(
        '128',
        () => client.connect(
          SyncServer(url: 'http://127.0.0.1:1', token: () => 't'),
          refreshAuth: () async {},
        ),
      );
      await step('129', connection.pause);
      await step(
        '130',
        () => client.invokeQuery(
          'GetTodo',
          1,
          {'id': 't'},
          (value) => value,
          store: const _Store({'todo': false}),
          once: true,
          refresh: true,
        ),
      );
      await step(
        '131',
        () => client.runPrerequisites({'upload': (_) async {}}),
      );
      final rows = step(
        '132',
        () async => client.watch('Todo', where: {'done': false}).listen((_) {}),
      );
      await pumpEventQueue();
      await step('133', () async => (await rows).cancel());
      // Close is priority control: it stops the connection without a task.
    } finally {
      await client.close();
    }
    expect(carrier.commands, expected);
  });

  test(
    'a throwing observer listener cannot stop the completion in its batch',
    () async {
      final bridge = await fixture.bridge();
      try {
        final reported = <Object>[];
        final subscribed =
            await bridge.task({'kind': 'scopeSubscribe', 'scope': 'book'})
                as Map;
        runZonedGuarded(
          () => bridge.listen(
            subscribed['observerId'] as String,
            (_) => throw StateError('listener'),
          ),
          (error, _) => reported.add(error),
        );
        // The removal publishes the terminal snapshot before its own
        // completion, in the same batch.
        final state = subscribed['state'] as Map;
        await bridge
            .task({
              'kind': 'scopeUnsubscribe',
              'scope': 'book',
              'subscriptionId': state['subscriptionId'],
            })
            .timeout(const Duration(seconds: 5));
        expect(reported, [
          isA<StateError>().having((e) => e.message, 'message', 'listener'),
        ]);
        expect(await bridge.task({'kind': 'status'}), isA<Map>());
      } finally {
        await bridge.close();
      }
    },
  );

  test(
    'an observer claimed at its task completion hears the snapshot of the same batch',
    () async {
      final bridge = await fixture.bridge();
      try {
        final heard = <Map<String, dynamic>>[];
        String? claimed;
        final value = await bridge.task(
          {'kind': 'scopeSubscribe', 'scope': 'book'},
          onValue: (value) {
            claimed = (value as Map)['observerId'] as String;
            expect(heard, isEmpty, reason: 'claimed before its first snapshot');
            bridge.listen(claimed!, heard.add);
          },
        );
        expect((value as Map)['observerId'], claimed);
        expect(heard, hasLength(1), reason: 'the first snapshot was not lost');
        expect(heard.single['kind'], 'subscription');
        expect((heard.single['status'] as Map)['connection'], 'offline');
        // The runtime's close ends the observer with its terminal snapshot.
        await bridge.close();
        expect(heard, hasLength(2));
        expect(heard.last['closed'], isTrue);
      } finally {
        await bridge.close();
      }
    },
  );

  test('a failure with a code carries it beside the message', () async {
    final bridge = await fixture.bridge();
    try {
      await expectLater(
        bridge.task({
          'kind': 'scopeBootstrap',
          'scope': 'book',
          'subscriptionId': 99,
        }),
        throwsA(
          isA<TaskFailure>()
              .having(
                (e) => e.message,
                'message',
                startsWith('subscription.closed'),
              )
              .having((e) => e.details, 'details', {
                'code': 'subscription.closed',
              }),
        ),
      );
      await expectLater(
        bridge.task({'kind': 'nope'}),
        throwsA(
          isA<StateError>().having(
            (e) => e is TaskFailure ? e.details : null,
            'details',
            isNull,
          ),
        ),
      );
    } finally {
      await bridge.close();
    }
  });

  test('a malformed envelope is reported and completes nothing', () async {
    final bridge = await fixture.bridge();
    try {
      final report = bridge.reports.first;
      bridge.submitRaw({'type': 'nope'});
      final diagnostic = await report.timeout(const Duration(seconds: 5));
      expect(diagnostic['kind'], 'protocol');
      expect(await bridge.task({'kind': 'status'}), isA<Map>());
    } finally {
      await bridge.close();
    }
  });
}

class _Thrown {}

class _Store extends CallStore {
  const _Store(this.wire);
  final Object? wire;
  @override
  Object? toWire() => wire;
}
