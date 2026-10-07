import 'store_fixture.dart';
// Prerequisite handlers registered at open (#185): the native runtime runs
// them whenever a task becomes pending and retries a PrerequisiteRetry with
// its own backoff. The real runtime and SQLite underneath.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:test/test.dart';

typedef _Handlers = Map<String, PrerequisiteHandler>;

void main() {
  late Directory dir;
  late Map<String, dynamic> schema;

  setUp(() async {
    dir = await Directory.systemTemp.createTemp('axton-dart-prerequisite-');
    schema =
        jsonDecode(
              await File('../../fixtures/schemas/entry.json').readAsString(),
            )
            as Map<String, dynamic>;
    schema['prerequisites'] = [
      {
        'name': 'RemoteBlob',
        'fields': [
          {'name': 'key', 'type': 'String'},
        ],
      },
    ];
    schema['requirements'] = [
      {
        'model': 'Entry',
        'field': 'note',
        'name': 'RemoteBlob',
        'arguments': {'key': 'self'},
      },
    ];
    declareEntryEdit(schema);
  });
  tearDown(() => dir.delete(recursive: true));

  Future<Client> open(_Handlers prerequisites) => Client.open(
    stream: 'User:viewer',
    path: '${dir.path}/db',
    schema: schema,
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
    prerequisites: prerequisites,
  );

  Future<void> attach(Client client, String key) async {
    await client.transaction(
      (tx) => tx.direct({
        'model': 'Entry',
        'op': 'create',
        'identity': {'id': key},
        'values': {'text': 'A'},
      }),
    );
    await client.submitMutation<void>('Edit', 1, {
      'entry': {'id': key, 'note': key},
    }, (_) {});
  }

  /// Poll [probe] until it holds; fail after five seconds.
  Future<void> until(FutureOr<bool> Function() probe) async {
    final deadline = DateTime.now().add(const Duration(seconds: 5));
    while (!await probe()) {
      expect(DateTime.now().isBefore(deadline), isTrue, reason: 'timed out');
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
  }

  Future<bool> settled(Client client) async =>
      (await client.pendingTasks()).isEmpty;

  test(
    'a commit that queues a task runs its handler with no host call',
    () async {
      final calls = <Map<String, dynamic>>[];
      final client = await open({
        'RemoteBlob': (arguments, cancelled) async => calls.add(arguments),
      });
      try {
        await attach(client, 'asset');
        await until(() => settled(client));
        expect(calls, [
          {'key': 'asset'},
        ]);
      } finally {
        await client.close();
      }
    },
  );

  test('a task pending at restart runs after reopen', () async {
    final started = Completer<void>();
    final first = await open({
      'RemoteBlob': (arguments, cancelled) {
        started.complete();
        return Completer<void>().future;
      },
    });
    await attach(first, 'asset');
    await started.future;
    await first.close();
    final calls = <Map<String, dynamic>>[];
    final second = await open({
      'RemoteBlob': (arguments, cancelled) async => calls.add(arguments),
    });
    try {
      await until(() => settled(second));
      expect(calls, [
        {'key': 'asset'},
      ]);
    } finally {
      await second.close();
    }
  });

  test('a transient failure retries with a growing backoff', () async {
    // The runtime decides every delay; this zone records the timers it asks
    // for and shortens them, so the test observes the backoff without
    // waiting it out.
    final delays = <int>[];
    var calls = 0;
    final client = await runZoned(
      () => open({
        'RemoteBlob': (arguments, cancelled) async {
          if (++calls <= 3) throw PrerequisiteRetry('offline');
        },
      }),
      zoneSpecification: ZoneSpecification(
        createTimer: (self, parent, zone, duration, callback) {
          if (duration >= const Duration(milliseconds: 500)) {
            delays.add(duration.inMilliseconds);
            duration = const Duration(milliseconds: 1);
          }
          return parent.createTimer(zone, duration, callback);
        },
      ),
    );
    try {
      await attach(client, 'asset');
      await until(() => settled(client));
      expect(calls, 4);
      expect(delays, hasLength(3));
      for (var i = 0; i < delays.length; i++) {
        // 1 s doubling per retry, within the runtime's ±20 % jitter.
        final base = 1000 << i;
        expect(delays[i], inInclusiveRange(base * 0.8, base * 1.2));
      }
    } finally {
      await client.close();
    }
  });

  test('a terminal failure stays failed and visible until a reset', () async {
    var calls = 0;
    final client = await open({
      'RemoteBlob': (arguments, cancelled) async {
        if (++calls == 1) throw StateError('file is gone');
      },
    });
    try {
      await attach(client, 'asset');
      late Map<String, dynamic> task;
      await until(() async {
        final tasks = await client.pendingTasks();
        if (tasks.isEmpty || tasks.single['state'] != 'failed') return false;
        task = tasks.single;
        return true;
      });
      expect(task['error'], 'Bad state: file is gone');
      expect(task['name'], 'RemoteBlob');
      final status = await client.recordSyncState('Entry', {'id': 'asset'});
      expect(
        (status['pending'] as List).first['prerequisites'].first['state'],
        'failed',
      );
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(calls, 1, reason: 'not retried');
      expect(
        (await client.pendingTasks()).single['state'],
        'failed',
        reason: 'failed prerequisite remains the native submission gate',
      );
      expect((await client.syncState())['pending'], 1);
      await client.setReadiness(task['key'] as String, 'pending');
      await until(() => settled(client));
      expect(calls, 2);
    } finally {
      await client.close();
    }
  });

  // An error surfacing after close would be uncaught in this test's zone,
  // which fails the test.
  test(
    'close during a run cancels it, does not hang and reports nothing',
    () async {
      final started = Completer<void>();
      var cancelled = false;
      final client = await open({
        'RemoteBlob': (arguments, cancellation) {
          started.complete();
          final run = Completer<void>();
          cancellation.then((_) {
            cancelled = true;
            run.completeError(StateError('aborted'));
          });
          return run.future;
        },
      });
      await attach(client, 'asset');
      await started.future;
      await client.close().timeout(const Duration(seconds: 5));
      await pumpEventQueue();
      expect(cancelled, isTrue, reason: 'the run was cancelled');
    },
  );

  test(
    'a handler for a prerequisite the schema does not declare fails open',
    () async {
      await expectLater(
        open({'Upload': (arguments, cancelled) async {}}),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            contains('invalid prerequisite handler Upload'),
          ),
        ),
      );
    },
  );
}
