import 'store_fixture.dart';
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

  Future<Client> client() async {
    final client = await Client.open(
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
      path: path,
      schema: schema,
      libraryPath: library,
    );
    await client.connection?.close();
    return client;
  }

  Future<Bridge> bridge() => Bridge.open(
    path: path,
    schema: schema,
    stream: 'User:viewer',
    libraryPath: library,
  );
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

  // Application store callbacks were removed in 0.4. Public negative analyzer
  // fixtures fence that API; current authority/cascade admission is tested by
  // the native engine and generated host fixtures, not a host callback lane.

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
        stream: 'User:viewer',
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

  test('close cancels connection setup before its completion', () async {
    String? connectingId;
    var initial = true;
    final carrier = FakeCarrier((envelope) {
      if ((envelope['command'] as Map?)?['kind'] == 'connect') {
        if (initial) {
          initial = false;
          return [completed(envelope['requestId'] as String)];
        }
        connectingId = envelope['requestId'] as String;
        return const [];
      }
      if (envelope['type'] == 'close') {
        return [
          {
            'type': 'taskCompleted',
            'requestId': connectingId,
            'ok': false,
            'error': 'client_closed',
          },
          {'type': 'runtimeClosed'},
        ];
      }
      return null;
    });
    final client = await Client.open(
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
      path: 'unused',
      schema: const {},
      carrier: carrier,
    );
    await client.connection?.close();
    final errors = <Object>[];
    final starting = client.connect(
      SyncServer(url: 'http://127.0.0.1:1', token: () => 't'),
      onError: errors.add,
    );
    final closing = client.close();
    await expectLater(
      starting,
      throwsA(
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
    await closing;
    await client.close();
    expect(errors, isEmpty);
    expect(Bridge.attached, isNot(contains(carrier.runtime)));
    await expectLater(
      client.connect(SyncServer(url: 'http://127.0.0.1:1', token: () => 't')),
      throwsA(
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
  });

  test('close cleans up a connection that completed first', () async {
    final carrier = FakeCarrier((envelope) {
      if ((envelope['command'] as Map?)?['kind'] == 'connect') {
        return [completed(envelope['requestId'] as String)];
      }
      return null;
    });
    final client = await Client.open(
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
      path: 'unused',
      schema: const {},
      carrier: carrier,
    );
    await client.connection?.close();
    final errors = <Object>[];
    final starting = client.connect(
      SyncServer(url: 'http://127.0.0.1:1', token: () => 't'),
      onError: errors.add,
    );
    final closing = client.close();
    final connection = await starting;
    await closing;
    await connection.close();
    await client.close();
    expect(errors, isEmpty);
    expect(Bridge.attached, isNot(contains(carrier.runtime)));
    await expectLater(
      client.connect(SyncServer(url: 'http://127.0.0.1:1', token: () => 't')),
      throwsA(
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
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
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
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

  test('a local callback effect for an unrouted request is refused', () async {
    // The same answer as the Node bridge gives, so diagnostics agree.
    final carrier = FakeCarrier((_) => null);
    final client = await Client.open(
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
      path: 'unused',
      schema: const {},
      carrier: carrier,
    );
    carrier.publish([
      {
        'type': 'effect',
        'effectId': '13',
        'operation': {
          'kind': 'mutationLocal',
          'transactionId': 'tx7',
          'companionId': 'c9',
          'requestId': 'nobody',
        },
      },
    ]);
    await pumpEventQueue();
    expect(carrier.admitted.where((e) => e['type'] == 'callbackResult'), [
      Bridge.callbackResultEnvelope(
        '13',
        'tx7',
        ok: false,
        error: 'unknown mutation',
        companionId: 'c9',
      ),
    ]);
    await client.close();
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
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
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
            companionId: input['companionId'] as String?,
          ),
          'callbackResult' => Bridge.callbackResultEnvelope(
            input['effectId'] as String,
            input['transactionId'] as String,
            ok: input['ok'] as bool,
            error: input['error'] as String?,
            companionId: input['companionId'] as String?,
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
            refusal:
                ((input['outcome'] as Map)['error'] as Map?)?['refusal']
                    as String?,
            retry:
                ((input['outcome'] as Map)['error'] as Map?)?['retry'] == true,
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
          if (operation['kind'] == 'mutationLocal') {
            expect(operation['transactionId'], isA<String>());
            expect(operation['companionId'], isA<String>());
            expect(operation['requestId'], isA<String>());
          }
        case 'cancelEffect':
          expect(event['effectId'], isA<String>());
        case 'callCompleted':
          expect(event['callId'], isA<String>());
          expect(event.containsKey('outcome'), isTrue);
        case 'transactionCallState':
          expect(event['callId'], isA<String>());
          expect(event['state'], anyOf('committed', 'rolledBack'));
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
      'transactionCallState',
      'observerChanged',
      'report',
      'runtimeClosed',
    });
  });

  test(
    'bound client commands use native ownership and boolean read policy',
    () async {
      final carrier = FakeCarrier((envelope) {
        final id = envelope['requestId'] as String?;
        if (id == null) return null;
        return [completed(id, null)];
      });
      final client = await Client.open(
        path: 'unused',
        schema: const {},
        stream: 'User:viewer',
        carrier: carrier,
      );
      expect(carrier.openedRequest['protocol'], 5);
      expect(carrier.openedRequest['stream'], 'User:viewer');
      expect(carrier.openedRequest.containsKey('binding'), false);
      expect(carrier.commands, isEmpty);
      try {
        await client.read('Todo', {'id': 't'});
        expect(carrier.commands.last, {
          'kind': 'read',
          'key': {
            'model': 'Todo',
            'identity': {'id': 't'},
          },
        });
      } finally {
        await client.close();
      }
    },
  );

  test(
    'a throwing observer listener cannot stop the completion in its batch',
    () async {
      final bridge = await fixture.bridge();
      try {
        final reported = <Object>[];
        final subscribed =
            await bridge.task({
                  'kind': 'streamSubscribe',
                  'stream': 'User:viewer',
                })
                as Map;
        runZonedGuarded(
          () => bridge.listen(
            subscribed['observerId'] as String,
            (_) => throw StateError('listener'),
          ),
          (error, _) => reported.add(error),
        );
        // Runtime close publishes the terminal observer snapshot while its
        // lifetime completion continues despite the listener's exception.
        await bridge.close();
        expect(reported, [
          isA<StateError>().having((e) => e.message, 'message', 'listener'),
        ]);
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
          {'kind': 'streamSubscribe', 'stream': 'User:viewer'},
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
          'kind': 'streamBootstrap',
          'stream': 'User:viewer',
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
