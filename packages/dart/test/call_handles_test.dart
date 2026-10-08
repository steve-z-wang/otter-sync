import 'store_fixture.dart';
import 'dart:async';
import 'dart:io';

import 'package:axton/axton.dart';
import 'package:axton/src/actions.dart' show ActionObservers, ActionWeakState;
import 'package:test/test.dart';
import '../../../integration/action-contract/generated.dart' as generated;

void main() {
  test(
    'registered typed observer decodes completion in the same dispatch turn',
    () async {
      final observers = ActionObservers();
      final call = observers.register<String>(
        'same-turn',
        (value) => value as String,
      );
      observers.complete({
        'callId': 'same-turn',
        'outcome': {'status': 'succeeded', 'result': 'pong'},
      });
      expect((await call.wait() as CallSuccess<String>).result, 'pong');
      expect(call.status, CallStatus.succeeded);
    },
  );

  test('weak routes sweep without retaining an abandoned handle', () {
    final refs = <_TestWeak>[];
    final observers = ActionObservers(
      weak: (state) {
        final ref = _TestWeak(state);
        refs.add(ref);
        return ref;
      },
    );
    observers.register<void>('gone', (_) {});
    refs.single.value = null;
    expect(observers.routingCount, 0);
    observers.complete({
      'callId': 'gone',
      'outcome': {'status': 'succeeded', 'result': null},
    });
  });

  test('active wait survives loss of its weak routing target', () async {
    final refs = <_TestWeak>[];
    final observers = ActionObservers(
      weak: (state) {
        final ref = _TestWeak(state);
        refs.add(ref);
        return ref;
      },
    );
    final call = observers.register<String>('held', (value) => value as String);
    final waiting = call.wait();
    refs.single.value = null;
    observers.complete({
      'callId': 'held',
      'outcome': {'status': 'succeeded', 'result': 'ready'},
    });
    final result =
        await waiting.timeout(const Duration(milliseconds: 100))
            as CallSuccess<String>;
    expect(result.result, 'ready');
  });

  Matcher lifecycle(String code, String execution) => throwsA(
    isA<CallError>()
        .having((error) => error.code, 'code', code)
        .having((error) => error.execution, 'execution', execution),
  );

  test('a provisional Call refuses an early wait, then observes normally '
      'once committed', () async {
    final observers = ActionObservers();
    final call = observers.register<int>(
      'p',
      (value) => value as int,
      provisional: true,
    );
    expect(call.status, CallStatus.pending);
    await expectLater(
      call.wait(),
      lifecycle('transaction_uncommitted', 'unknown'),
    );
    // The refusal neither settles nor retains the Call.
    expect(call.status, CallStatus.pending);
    expect(observers.routingCount, 1);
    observers.transition('p', 'committed');
    final waiting = call.wait();
    observers.complete({
      'callId': 'p',
      'outcome': {'status': 'succeeded', 'result': 1},
    });
    expect(((await waiting) as CallSuccess<int>).result, 1);
    expect(call.status, CallStatus.succeeded);
  });

  test('a rolled-back Call fails every wait and routes nothing more', () async {
    final observers = ActionObservers();
    final rolled = observers.register<int>(
      'r',
      (value) => value as int,
      provisional: true,
    );
    final kept = observers.register<int>(
      'k',
      (value) => value as int,
      provisional: true,
    );
    observers.transition('r', 'rolledBack');
    expect(rolled.status, CallStatus.failed);
    for (var i = 0; i < 2; i++) {
      await expectLater(
        rolled.wait(),
        lifecycle('transaction_rolled_back', 'rejected'),
      );
    }
    expect(observers.routingCount, 1);
    // A later transition or completion for it changes nothing.
    observers.transition('r', 'committed');
    observers.complete({
      'callId': 'r',
      'outcome': {'status': 'succeeded', 'result': 1},
    });
    observers.transition('unknown', 'rolledBack');
    await expectLater(
      rolled.wait(),
      lifecycle('transaction_rolled_back', 'rejected'),
    );
    expect(kept.status, CallStatus.pending);
    await expectLater(
      kept.wait(),
      lifecycle('transaction_uncommitted', 'unknown'),
    );
  });

  test('weak routing drops an unobserved provisional Call', () {
    final refs = <_TestWeak>[];
    final observers = ActionObservers(
      weak: (state) {
        final ref = _TestWeak(state);
        refs.add(ref);
        return ref;
      },
    );
    observers.register<void>('abandoned', (_) {}, provisional: true);
    refs.single.value = null;
    observers.register<void>('other', (_) {}, provisional: true);
    expect(observers.routingCount, 1);
    observers.transition('abandoned', 'committed');
    observers.transition('other', 'rolledBack');
    expect(observers.routingCount, 0);
  });

  test('close leaves provisional Calls to their transition and the '
      "runtime's end rolls back the rest", () async {
    final observers = ActionObservers();
    final durable = observers.register<void>('d', (_) {});
    final rolled = observers.register<void>('r', (_) {}, provisional: true);
    final committed = observers.register<void>('c', (_) {}, provisional: true);
    final orphan = observers.register<void>('o', (_) {}, provisional: true);
    observers.close();
    expect(
      ((await durable.wait()) as CallFailure<void>).error.code,
      'client.closed',
    );
    expect(rolled.status, CallStatus.pending);
    observers.transition('r', 'rolledBack');
    observers.transition('c', 'committed');
    await expectLater(
      rolled.wait(),
      lifecycle('transaction_rolled_back', 'rejected'),
    );
    expect(
      ((await committed.wait()) as CallFailure<void>).error.code,
      'client.closed',
    );
    expect(orphan.status, CallStatus.pending);
    observers.ended();
    await expectLater(
      orphan.wait(),
      lifecycle('transaction_rolled_back', 'rejected'),
    );
    final after = observers.register<void>('late', (_) {}, provisional: true);
    await expectLater(
      after.wait(),
      lifecycle('transaction_rolled_back', 'rejected'),
    );
    expect(observers.routingCount, 0);
  });

  test(
    'late wait reads durable completion when live event was not observed',
    () async {
      var lookups = 0;
      final observers = ActionObservers(
        lookup: (id) async {
          lookups++;
          return {
            'callId': id,
            'outcome': {'status': 'succeeded', 'result': 42},
          };
        },
      );
      final call = observers.register<int>('durable', (value) => value as int);
      expect((await call.wait() as CallSuccess<int>).result, 42);
      expect((await call.wait() as CallSuccess<int>).result, 42);
      expect(lookups, 1);
    },
  );
  test('durable pending lookup does not invent terminal completion', () async {
    final observers = ActionObservers(lookup: (_) async => null);
    final call = observers.register<void>('pending', (_) {});
    var ended = false;
    unawaited(
      call.wait().then((_) {
        ended = true;
      }),
    );
    await Future<void>.delayed(Duration.zero);
    expect(ended, isFalse);
    expect(call.status, CallStatus.pending);
    observers.close();
    expect((await call.wait()), isA<CallFailure<void>>());
  });
  test(
    'saved decoder failure is observation failure without a second outcome',
    () async {
      final observers = ActionObservers(
        lookup: (id) async => {
          'callId': id,
          'outcome': {'status': 'succeeded', 'result': 'wrong type'},
        },
      );
      final call = observers.register<int>('wrong', (value) => value as int);
      final outcome = await call.wait() as CallFailure<int>;
      expect(outcome.error.code, 'action.observation_failed');
      expect(call.status, CallStatus.failed);
    },
  );
  test(
    'typed creation defaults are resolved once for direct CRUD and named optimism',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'axton-dart-defaults05-',
      );
      var client = await generated.GeneratedClient.open(
        path: '${directory.path}/db',
        stream: 'User:viewer',
        connection: offlineStoreConnection(),
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      try {
        await client.models.note.create(const generated.NoteCreate(memo: null));
        final direct = (await client.models.note.query()).single;
        expect(direct.body, '');
        expect(direct.pinned, isFalse);
        expect(direct.tag, 't');
        expect(direct.memo, isNull);
        expect(direct.id, matches(RegExp(r'^[0-9a-f-]{36}$')));
        expect(direct.at.isUtc, isTrue);
        await client.mutations.addNotes(
          const generated.AddNotesInput(
            note: generated.NoteCreate(memo: null),
            maybe: null,
            many: [],
          ),
        );
        final identities = (await client.models.note.query())
            .map((n) => n.id)
            .toSet();
        expect(identities.length, 2);
        await client.close();
        client = await generated.GeneratedClient.open(
          path: '${directory.path}/db',
          stream: 'User:viewer',
          connection: offlineStoreConnection(),
          libraryPath: Platform.environment['AXTON_LIBRARY']!,
        );
        expect(
          (await client.models.note.query()).map((n) => n.id).toSet(),
          identities,
        );
        expect((await client.syncState())['pending'], 1);
      } finally {
        await client.close();
        await directory.delete(recursive: true);
      }
    },
  );
  test(
    'native unsent drop and explicit reset settle owned handles durably',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'axton-dart-terminal05-',
      );
      final client = await Client.open(
        path: '${directory.path}/db',
        schema: {
          'models': [],
          'enums': [],
          'actions': [
            {
              'kind': 'mutation',
              'name': 'Ping',
              'version': 1,
              'inputs': [],
              'outputs': [],
            },
          ],
        },
        stream: 'User:viewer',
        connection: offlineStoreConnection(),
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      try {
        await client.connection!.pause();
        final dropped = await client.submitMutation<void>(
          'Ping',
          1,
          {},
          (_) {},
        );
        await client.drop(1);
        expect(
          await dropped.wait().timeout(const Duration(seconds: 2)),
          isA<CallFailure<void>>(),
        );
        final abandoned = await client.submitMutation<void>(
          'Ping',
          1,
          {},
          (_) {},
        );
        await expectLater(client.resetStore(), throwsA(anything));
        await client.resetStore(discardPending: true);
        expect(
          await abandoned.wait().timeout(const Duration(seconds: 2)),
          isA<CallFailure<void>>(),
        );
        expect((await client.syncState())['pending'], 0);
      } finally {
        await client.close();
        await directory.delete(recursive: true);
      }
    },
  );
}

final class _TestWeak implements ActionWeakState {
  Object? value;
  _TestWeak(this.value);
  @override
  Object? get target => value;
}
