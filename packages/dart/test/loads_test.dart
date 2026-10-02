// Native Loads through the real native runtime (#173): Rust persists every
// job, batches its pages, decides once reuse and projects its status; this
// host executes the `load` HTTP effect over a real HTTP server and keeps only
// the language handles.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:axton/axton.dart';
import 'package:test/test.dart';

const _fields = [
  {
    'name': 'id',
    'nullable': false,
    'type': {'kind': 'scalar', 'name': 'string'},
  },
  {
    'name': 'text',
    'nullable': false,
    'type': {'kind': 'scalar', 'name': 'string'},
  },
  {
    'name': 'note',
    'nullable': true,
    'type': {'kind': 'scalar', 'name': 'string'},
  },
];
Map<String, dynamic> _input(String name, String scalar, bool nullable) => {
  'kind': 'value',
  'name': name,
  'type': {'kind': 'scalar', 'name': scalar},
  'nullable': nullable,
  'list': false,
  'required': true,
  'cardinality': 'single',
};
Map<String, dynamic> _load(String name, List<Map<String, dynamic>> inputs) => {
  'name': name,
  'version': 1,
  'inputs': inputs,
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
          {
            'name': 'id',
            'type': {'kind': 'scalar', 'name': 'string'},
          },
        ],
      },
    },
  ],
  'input': {'models': [], 'enums': []},
  'outputEnums': [],
};
final Map<String, dynamic> _schema = {
  'enums': [],
  'models': [
    {
      'name': 'Entry',
      'version': 1,
      'identity': ['id'],
      'fields': _fields,
    },
  ],
  'resultModels': [
    {
      'name': 'Entry',
      'version': 1,
      'identity': ['id'],
      'fields': _fields,
      'enums': [],
    },
  ],
  'loads': [
    _load('Entries', [
      _input('projectId', 'uuid', false),
      _input('since', 'dateTime', true),
    ]),
    _load('Recent', []),
  ],
};
const _project = '0190f0e0-1111-7222-8333-444455556666';
const _args = {'projectId': _project, 'since': null};

Map<String, dynamic> _page(
  Map intent,
  List<(String, String)> rows, [
  Object? next,
]) => {
  'loadId': intent['loadId'],
  'callId': intent['callId'],
  'outcome': {
    'status': 'succeeded',
    'data': {
      'entries': [
        for (final (id, _) in rows) {'id': id},
      ],
    },
    'next': next,
  },
  'records': [
    for (final (i, (id, text)) in rows.indexed)
      {
        'model': 'Entry',
        'identity': {'id': id},
        'stamp': i + 1,
        'state': {'text': text, 'note': null},
      },
  ],
};
Map<String, dynamic> _failed(Map intent, String code) => {
  'loadId': intent['loadId'],
  'callId': intent['callId'],
  'outcome': {
    'status': 'failed',
    'error': {'code': code, 'message': 'refused'},
  },
  'records': [],
};

Matcher _code(String code) =>
    throwsA(isA<LoadException>().having((e) => e.code, 'code', code));

void main() {
  late Directory directory;
  late HttpServer server;
  final clients = <Client>[];
  final batches = <Map<String, dynamic>>[];
  late Map<String, dynamic> Function(Map intent) answer;
  int? status;
  Completer<void>? gate;

  List<Map> intents() => [
    for (final batch in batches) ...(batch['loads'] as List).cast<Map>(),
  ];
  Future<Client> open() async {
    final client = await Client.open(
      path: '${directory.path}/db',
      schema: _schema,
      libraryPath: Platform.environment['AXTON_LIBRARY']!,
    );
    clients.add(client);
    return client;
  }

  Future<RuntimeConnection> connect(
    Client client, {
    Future<void> Function()? refreshAuth,
  }) => client.connect(
    SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'a'),
    refreshAuth: refreshAuth,
  );
  Future<void> until(bool Function() condition, String what) async {
    for (var i = 0; i < 500; i++) {
      if (condition()) return;
      await Future<void>.delayed(const Duration(milliseconds: 5));
    }
    fail('timed out waiting for $what');
  }

  setUp(() async {
    batches.clear();
    status = null;
    gate = null;
    answer = (intent) => _page(intent, [
      ((intent['loadId'] as String).substring(0, 8), 'loaded'),
    ]);
    directory = await Directory.systemTemp.createTemp('axton-loads-');
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((request) async {
      if (request.uri.path != '/sync/loads') {
        request.response.statusCode = 404;
        await request.response.close();
        return;
      }
      final body =
          jsonDecode(await utf8.decoder.bind(request).join())
              as Map<String, dynamic>;
      expect(body['capabilities'], contains('stream-authority-v1'));
      batches.add(body);
      await gate?.future;
      if (status != null) {
        request.response.statusCode = status!;
        await request.response.close();
        return;
      }
      request.response.headers.contentType = ContentType.json;
      request.response.write(
        jsonEncode({
          'loads': [for (final intent in body['loads'] as List) answer(intent)],
        }),
      );
      await request.response.close();
    });
  });
  tearDown(() async {
    for (final client in clients) {
      await client.close();
    }
    clients.clear();
    await server.close(force: true);
    await directory.delete(recursive: true);
  });

  test(
    'authority and application cleanup roll back together without client holds',
    () async {
      var refuse = true;
      var hooks = 0;
      final fixtureSchema = {
        ..._schema,
        'models': [
          ..._schema['models'] as List,
          {
            'name': 'Composition',
            'identity': ['id'],
            'fields': _fields,
          },
        ],
      };
      final client = await Client.open(
        path: '${directory.path}/db',
        schema: fixtureSchema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
        onStore: {
          'Entry': (tx, changes) async {
            hooks++;
            await tx.direct({
              'model': 'Entry',
              'op': 'delete',
              'identity': {'id': 'child'},
            });
            if (refuse) throw StateError('cleanup refused');
          },
        },
      );
      clients.add(client);
      await client.transaction((tx) async {
        await tx.direct({
          'model': 'Entry',
          'op': 'create',
          'identity': {'id': 'child'},
          'values': {'text': 'cached', 'note': null},
        });
        await tx.direct({
          'model': 'Composition',
          'op': 'create',
          'identity': {'id': 'draft'},
          'values': {'text': 'device words', 'note': null},
        });
      });
      expect(
        await client.readSql(
          "SELECT name FROM sqlite_master WHERE name='axton_stream_member'",
        ),
        isEmpty,
      );
      answer = (intent) => _page(intent, [('access', 'current authority')]);
      final job = await client.startLoad('Entries', 1, _args);
      await connect(client);
      await expectLater(job.wait(), _code('load.hook_failed'));
      expect(await client.read('Entry', {'id': 'access'}), isNull);
      expect((await client.read('Entry', {'id': 'child'}))!['text'], 'cached');
      expect(job.status.pages, 0);
      refuse = false;
      await job.retry();
      await job.wait();
      expect(
        (await client.read('Entry', {'id': 'access'}))!['text'],
        'current authority',
      );
      expect(await client.read('Entry', {'id': 'child'}), isNull);
      expect(
        (await client.read('Composition', {'id': 'draft'}))!['text'],
        'device words',
      );
      expect(job.status.pages, 1);
      expect(hooks, 2);
      await client.subscribe('delivery');
      await client.unsubscribe('delivery');
      expect(
        (await client.read('Entry', {'id': 'access'}))!['text'],
        'current authority',
      );
      expect(hooks, 2, reason: 'registration changes run no Model hook');
    },
  );

  test('a start is accepted offline and wait completes after the final '
      'page committed', () async {
    final client = await open();
    final job = await client.startLoad('Entries', 1, _args);
    expect(job.status.id, job.id);
    expect(job.status.name, 'Entries');
    expect(job.status.version, 1);
    expect(job.status.phase, LoadPhase.waiting);
    expect(job.status.pages, 0);
    expect(job.status.error, isNull);
    final other = await client.startLoad('Entries', 1, _args);
    expect(other.id, isNot(job.id), reason: 'ordinary starts never share');
    answer = (intent) => intent['continuation'] == null
        ? _page(
            intent,
            [('a', 'A')],
            {
              'state': {'after': 'a'},
            },
          )
        : _page(intent, [('b', 'B')]);
    final seen = <LoadStatus>[];
    final observer = job.watch().listen(seen.add);
    await pumpEventQueue();
    expect(seen.map((s) => s.phase), [LoadPhase.waiting]);
    await connect(client);
    await job.wait();
    await other.wait();
    expect(job.status.phase, LoadPhase.complete);
    expect(job.status.pages, 2);
    await pumpEventQueue();
    expect(seen.last.phase, LoadPhase.complete);
    expect(seen.map((s) => s.phase), contains(LoadPhase.loading));
    for (var i = 1; i < seen.length; i++) {
      expect(seen[i], isNot(seen[i - 1]), reason: 'only distinct snapshots');
    }
    await observer.cancel();
    expect((await client.read('Entry', {'id': 'b'}))?['text'], 'B');
    final mine = intents().where((i) => i['loadId'] == job.id).toList();
    expect(mine.map((i) => i['continuation']), [
      null,
      {
        'state': {'after': 'a'},
      },
    ]);
    expect(mine.first['args'], _args);
    expect(mine.first.containsKey('once'), isFalse);
    expect(mine[0]['callId'], isNot(mine[1]['callId']));
    await job.wait();
  });

  test('once shares active and complete jobs, refresh joins or restarts, '
      'invalidation is offline', () async {
    final client = await open();
    Future<Load> once({bool refresh = false}) =>
        client.startLoad('Entries', 1, _args, once: true, refresh: refresh);
    final first = await once();
    final joined = await once();
    expect(joined.id, first.id);
    expect(identical(joined, first), isFalse);
    final refreshedActive = await once(refresh: true);
    expect(refreshedActive.id, first.id, reason: 'refresh joins active work');
    gate = Completer<void>();
    final connection = await connect(client);
    await until(() => batches.isNotEmpty, 'the first batch');
    expect(intents().where((i) => i['loadId'] == first.id), hasLength(1));
    gate!.complete();
    gate = null;
    await Future.wait([first.wait(), joined.wait(), refreshedActive.wait()]);
    final requests = batches.length;
    await connection.close();
    final hit = await once();
    expect(hit.id, first.id);
    expect(hit.status.phase, LoadPhase.complete);
    await hit.wait();
    final refreshed = await once(refresh: true);
    expect(refreshed.id, isNot(first.id));
    expect((await once()).id, refreshed.id);
    expect(first.status.phase, LoadPhase.complete);
    await client.invalidateLoad('Entries', _args);
    final fresh = await once();
    expect(fresh.id, isNot(refreshed.id));
    final recent = await client.startLoad('Recent', 1, {}, once: true);
    expect((await client.startLoad('Recent', 1, {}, once: true)).id, recent.id);
    expect(batches, hasLength(requests), reason: 'all of it offline');
    final again = await connect(client);
    await Future.wait([refreshed.wait(), fresh.wait()]);
    expect(
      (await once()).id,
      fresh.id,
      reason: 'an invalidated job cannot restore its mapping',
    );
    await again.close();
  });

  test('invalid options and transaction callbacks are refused before any '
      'job exists', () async {
    final client = await open();
    await expectLater(
      client.startLoad('Entries', 1, _args, refresh: true),
      _code('load.invalid_options'),
    );
    await expectLater(client.startLoad('Nope', 1, {}), _code('load.unknown'));
    await expectLater(
      client.startLoad('Entries', 1, {
        'projectId': 'not-a-uuid',
        'since': null,
      }),
      _code('load.invalid_args'),
    );
    await expectLater(
      client.listLoads(limit: 0),
      _code('load.invalid_options'),
    );
    await expectLater(
      client.listLoads(limit: 101),
      _code('load.invalid_options'),
    );
    await client.transaction((tx) async {
      await expectLater(
        client.startLoad('Entries', 1, _args),
        _code('transaction_active'),
      );
    });
    expect(await client.listLoads(), isEmpty);
  });

  test('failure, retry, cancel and forget keep once ownership with the job '
      'that holds it', () async {
    final client = await open();
    answer = (intent) => _failed(intent, 'handler.failed');
    final doomed = await client.startLoad('Entries', 1, _args, once: true);
    final connection = await connect(client);
    await expectLater(
      doomed.wait(),
      throwsA(
        isA<LoadException>()
            .having((e) => e.code, 'code', 'handler.failed')
            .having((e) => e.message, 'message', 'refused'),
      ),
    );
    expect(doomed.status.phase, LoadPhase.failed);
    expect(
      doomed.status.error,
      const LoadException('handler.failed', 'refused'),
    );
    final requests = batches.length;
    final reused = await client.startLoad('Entries', 1, _args, once: true);
    expect(reused.id, doomed.id);
    await expectLater(reused.wait(), _code('handler.failed'));
    expect(batches, hasLength(requests), reason: 'no hidden request');
    answer = (intent) => _page(intent, [('r', 'R')]);
    await reused.retry();
    await doomed.wait();
    final calls = intents().where((i) => i['loadId'] == doomed.id).toList();
    expect(calls, hasLength(2));
    expect(calls[0]['callId'], isNot(calls[1]['callId']));
    await expectLater(doomed.retry(), _code('load.not_retryable'));
    await connection.close();
    final pending = await client.startLoad('Entries', 1, _args);
    final waiting = expectLater(pending.wait(), _code('load.cancelled'));
    await pending.cancel();
    await waiting;
    expect(pending.status.phase, LoadPhase.cancelled);
    await doomed.cancel();
    expect(doomed.status.phase, LoadPhase.complete);
    final replacement = await client.startLoad(
      'Entries',
      1,
      _args,
      once: true,
      refresh: true,
    );
    expect(replacement.id, isNot(doomed.id));
    await doomed.forget();
    expect(
      (await client.startLoad('Entries', 1, _args, once: true)).id,
      replacement.id,
      reason: 'forgetting the old job kept the newer mapping',
    );
    expect(await client.getLoad(doomed.id), isNull);
    await expectLater(doomed.wait(), _code('load.not_found'));
    await expectLater(replacement.forget(), _code('load.not_terminal'));
    await replacement.cancel();
    expect(
      (await client.startLoad('Entries', 1, _args, once: true)).id,
      isNot(replacement.id),
    );
  });

  test('handles reattach by ID, list newest first, and dispose releases only '
      'one observer', () async {
    final client = await open();
    final job = await client.startLoad('Entries', 1, _args);
    final second = await client.startLoad('Recent', 1, {});
    final restored = (await client.getLoad(job.id))!;
    expect(identical(restored, job), isFalse);
    expect(restored.status, job.status);
    expect(
      await client.getLoad('00000000-0000-4000-8000-000000000000'),
      isNull,
    );
    expect((await client.listLoads()).map((s) => s.id), [second.id, job.id]);
    expect(await client.listLoads(limit: 1), hasLength(1));
    final heard = <LoadPhase>[];
    final disposedStream = restored.watch().map((s) => s.phase).toList();
    final kept = job.watch().listen((s) => heard.add(s.phase));
    await pumpEventQueue();
    restored.dispose();
    restored.dispose();
    expect(
      await disposedStream,
      [LoadPhase.waiting],
      reason: 'a disposed handle stream ends with its last status',
    );
    await connect(client);
    await job.wait();
    await pumpEventQueue();
    expect(
      job.status.phase,
      LoadPhase.complete,
      reason: 'dispose cancels nothing',
    );
    expect(heard.last, LoadPhase.complete);
    expect(restored.status.phase, LoadPhase.waiting);
    expect(await restored.watch().toList(), [restored.status]);
    await restored.wait();
    await second.wait();
    await kept.cancel();
  });

  test('close settles every waiter, keeps the work, and terminal status is '
      'readable after reopen', () async {
    final client = await open();
    final job = await client.startLoad('Entries', 1, _args);
    final ended = job.watch().toList();
    final waits = [
      expectLater(job.wait(), _code('client_closed')),
      expectLater(job.wait(), _code('client_closed')),
      expectLater(
        (await client.getLoad(job.id))!.wait(),
        _code('client_closed'),
      ),
    ];
    await client.close();
    await Future.wait(waits);
    expect((await ended).last.phase, LoadPhase.waiting);
    expect(job.status.phase, LoadPhase.waiting);
    await expectLater(job.wait(), _code('client_closed'));
    final reopened = await open();
    final restored = (await reopened.getLoad(job.id))!;
    expect(restored.status.phase, LoadPhase.waiting);
    final connection = await connect(reopened);
    await restored.wait();
    await connection.close();
    await reopened.close();
    final third = await open();
    final terminal = (await third.getLoad(job.id))!;
    expect(terminal.status.phase, LoadPhase.complete);
    expect(terminal.status.pages, 1);
    await terminal.wait();
  });

  test(
    'a refused credential refresh fails the batch load.unauthorized',
    () async {
      final client = await open();
      status = 401;
      var refreshes = 0;
      final job = await client.startLoad('Entries', 1, _args);
      await connect(
        client,
        refreshAuth: () async {
          refreshes++;
          throw HttpFailure('refresh', 403, 'forbidden');
        },
      );
      await expectLater(job.wait(), _code('load.unauthorized'));
      expect(refreshes, 1);
      expect(job.status.phase, LoadPhase.failed);
      expect(job.status.error?.code, 'load.unauthorized');
    },
  );

  test('a rebuild ends live handles and parked waiters with '
      'load.schema_changed', () async {
    final changed = jsonDecode(jsonEncode(_schema)) as Map<String, dynamic>;
    for (final models in ['models', 'resultModels']) {
      (changed[models][0]['fields'] as List).add({
        'name': 'extra',
        'nullable': false,
        'type': {'kind': 'scalar', 'name': 'string'},
      });
    }
    // An unsent Mutation keeps the old file open behind a pending rebuild.
    final old = await open();
    final started = await old.startLoad('Entries', 1, _args);
    await old.transaction((tx) async {
      await tx.direct({
        'model': 'Entry',
        'op': 'create',
        'identity': {'id': 'e'},
        'values': {'text': 'A', 'note': null},
      });
    });
    await old.mutate({
      'name': 'Edit',
      'operations': [
        {
          'model': 'Entry',
          'op': 'update',
          'identity': {'id': 'e'},
          'values': {'text': 'B'},
        },
      ],
    });
    await old.close();
    final client = await Client.open(
      path: '${directory.path}/db',
      schema: changed,
      libraryPath: Platform.environment['AXTON_LIBRARY']!,
    );
    clients.add(client);
    expect((await client.syncState())['schema']['pending'], isNotNull);
    final job = (await client.getLoad(started.id))!;
    expect(job.status.phase, LoadPhase.waiting);
    final watched = job.watch().toList();
    Object? settled;
    final waiting = job.wait().then<void>(
      (_) => settled = 'resolved',
      onError: (Object error) {
        settled = error;
      },
    );
    await Future<void>.delayed(const Duration(milliseconds: 10));
    expect(settled, isNull, reason: 'the waiter is parked');
    final report = await client.rebuild(discardPending: true);
    expect(report['abandonedLoads'], [started.id]);
    await waiting;
    expect(
      settled,
      isA<LoadException>().having((e) => e.code, 'code', 'load.schema_changed'),
    );
    // The handle's last status says why it ended, and its watch completes.
    expect(job.status.phase, LoadPhase.failed);
    expect(job.status.error?.code, 'load.schema_changed');
    expect((await watched).last, job.status);
    await expectLater(job.wait(), _code('load.schema_changed'));
    await expectLater(job.cancel(), _code('load.schema_changed'));
    await expectLater(job.retry(), _code('load.schema_changed'));
    await expectLater(job.forget(), _code('load.schema_changed'));
    expect(await client.getLoad(started.id), isNull);
    expect(await client.listLoads(), isEmpty);
  });
}
