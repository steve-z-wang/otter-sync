// Query once through the real native runtime (#158): Rust decides Cached /
// Join / Fetch; this host executes direct I/O over a real HTTP carrier,
// shares one flight per decision and decodes an independent result per caller.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:axton/axton.dart';
import 'package:test/test.dart';

const _fields = [
  {
    'name': 'id',
    'type': {'kind': 'scalar', 'name': 'string'},
    'nullable': false,
  },
  {
    'name': 'title',
    'type': {'kind': 'scalar', 'name': 'string'},
    'nullable': false,
  },
];
final Map<String, dynamic> _schema = {
  'enums': [],
  'models': [
    {
      'name': 'Todo',
      'version': 1,
      'identity': ['id'],
      'fields': _fields,
    },
  ],
  'resultModels': [
    {
      'name': 'Todo',
      'version': 1,
      'identity': ['id'],
      'fields': _fields,
      'enums': [],
    },
  ],
  'actions': [
    {
      'name': 'GetTodos',
      'version': 1,
      'kind': 'query',
      'inputs': [
        {
          'kind': 'value',
          'name': 'project',
          'type': {'kind': 'scalar', 'name': 'string'},
          'nullable': false,
        },
      ],
      'outputs': [
        {
          'name': 'todos',
          'kind': 'model',
          'model': 'Todo',
          'modelReadVersion': 1,
          'cardinality': 'list',
          'source': 'handlerIdentity',
          'handlerType': {
            'kind': 'identity',
            'model': 'Todo',
            'fields': [
              {
                'name': 'id',
                'type': {'kind': 'scalar', 'name': 'string'},
              },
            ],
          },
        },
        {
          'name': 'tags',
          'kind': 'value',
          'type': {'kind': 'scalar', 'name': 'string'},
          'cardinality': 'list',
          'source': 'handlerValue',
        },
      ],
    },
    {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
  ],
};

/// Keeps the raw collections: independence must come from the host.
Map<String, dynamic> _decode(dynamic value) =>
    (value as Map).cast<String, dynamic>();

void main() {
  late Directory directory;
  late Client client;
  late HttpServer server;
  late StreamSubscription<HttpRequest> served;
  var requests = 0;
  final gates = <int, Completer<void>>{};
  final failing = <int>{};
  final arrivals = <int, Completer<void>>{};

  Future<Client> open() => Client.open(
    path: '${directory.path}/db',
    schema: _schema,
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
  );
  Future<RuntimeConnection> connect(Client client) => client.connect(
    SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'a'),
  );
  Future<Map<String, dynamic>> once(
    Client client, {
    String project = 'p',
    bool refresh = false,
  }) => client.invokeQuery<Map<String, dynamic>>(
    'GetTodos',
    1,
    {'project': project},
    _decode,
    once: true,
    refresh: refresh,
  );

  setUp(() async {
    requests = 0;
    gates.clear();
    failing.clear();
    arrivals.clear();
    directory = await Directory.systemTemp.createTemp('axton-query-once-');
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    served = server.listen((request) async {
      if (request.uri.path != '/sync/actions') {
        request.response.statusCode = 404;
        await request.response.close();
        return;
      }
      final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
      final n = ++requests;
      arrivals[n]?.complete();
      await gates[n]?.future;
      if (failing.contains(n)) {
        request.response.statusCode = 500;
        await request.response.close();
        return;
      }
      final title = 'v$n';
      final call = body['call'] as Map;
      request.response.write(
        jsonEncode({
          'completion': {
            'callId': call['callId'],
            'outcome': {
              'status': 'succeeded',
              'result': {
                'todos': [
                  {'id': 'a', 'title': title},
                ],
                'tags': [title, 'x'],
              },
            },
          },
          'records': call['store'] == false
              ? []
              : [
                  {
                    'model': 'Todo',
                    'identity': {'id': 'a'},
                    'stamp': n,
                    'state': {'title': title},
                  },
                ],
        }),
      );
      await request.response.close();
    });
    client = await open();
  });
  tearDown(() async {
    await client.close();
    await served.cancel();
    await server.close(force: true);
    await directory.delete(recursive: true);
  });

  test(
    'concurrent callers share one request and decode independently',
    () async {
      final connection = await connect(client);
      final gate = Completer<void>();
      gates[1] = gate;
      final waiting = [once(client), once(client), once(client)];
      await Future<void>.delayed(const Duration(milliseconds: 50));
      gate.complete();
      final results = await Future.wait(waiting);
      expect(requests, 1);
      for (final result in results) {
        expect(result['tags'], ['v1', 'x']);
      }
      (results[0]['tags'] as List).add('mutated');
      ((results[0]['todos'] as List).first as Map)['title'] = 'mutated';
      expect(results[1]['tags'], ['v1', 'x']);
      expect(((results[1]['todos'] as List).first as Map)['title'], 'v1');
      final hit = await once(client);
      expect(requests, 1, reason: 'a hit issues no request');
      expect(hit['tags'], ['v1', 'x']);
      await connection.close();
    },
  );

  test('a hit needs no carrier; a miss or refresh without one fails', () async {
    final connection = await connect(client);
    await once(client);
    await connection.close();
    expect((await once(client))['tags'], ['v1', 'x']);
    await expectLater(
      once(client, project: 'other'),
      throwsA(
        isA<CallError>().having((e) => e.code, 'code', 'action.unavailable'),
      ),
    );
    await expectLater(
      once(client, refresh: true),
      throwsA(
        isA<CallError>().having((e) => e.code, 'code', 'action.unavailable'),
      ),
    );
    await client.close();
    client = await open();
    expect((await once(client))['tags'], ['v1', 'x'], reason: 'after reopen');
    expect(requests, 1);
  });

  test('refresh requires once and a Mutation has no once route', () async {
    final connection = await connect(client);
    await expectLater(
      client.invokeQuery<Map<String, dynamic>>(
        'GetTodos',
        1,
        {'project': 'p'},
        _decode,
        refresh: true,
      ),
      throwsA(
        isA<CallError>()
            .having((e) => e.code, 'code', 'action.invalid_options')
            .having((e) => e.execution, 'execution', 'rejected'),
      ),
    );
    await expectLater(
      client.invokeQuery<void>('Ping', 1, {}, (_) {}, once: true),
      throwsA(isA<CallError>()),
    );
    expect(requests, 0);
    await connection.close();
  });

  test('default calls stay fresh; refresh replaces only on success', () async {
    final connection = await connect(client);
    await client.invokeQuery('GetTodos', 1, {'project': 'p'}, _decode);
    expect(requests, 1);
    expect((await once(client))['tags'], ['v2', 'x']);
    await client.invokeQuery('GetTodos', 1, {'project': 'p'}, _decode);
    expect(requests, 3);
    expect((await once(client))['tags'], ['v2', 'x']);
    failing.add(4);
    await expectLater(once(client, refresh: true), throwsA(isA<CallError>()));
    expect((await once(client))['tags'], ['v2', 'x']);
    expect((await once(client, refresh: true))['tags'], ['v5', 'x']);
    expect((await once(client))['tags'], ['v5', 'x']);
    await connection.close();
  });

  test('invalidation forces a miss and fences an older result', () async {
    final connection = await connect(client);
    final gate = Completer<void>();
    gates[1] = gate;
    final firstArrived = Completer<void>();
    arrivals[1] = firstArrived;
    final older = once(client);
    // Observe every caller immediately so failure cleanup cannot leave an
    // unhandled result while another request is held.
    final settled = <Future<void>>[
      older.then<void>((_) {}, onError: (Object _) {}),
    ];
    try {
      await firstArrived.future;
      await client.invalidateQuery('GetTodos', 1, {'project': 'p'});
      final newer = once(client);
      settled.add(newer.then<void>((_) {}, onError: (Object _) {}));
      expect((await newer)['tags'], ['v2', 'x']);
      expect(requests, 2);
      gate.complete();
      expect((await older)['tags'], ['v1', 'x']);
      expect((await once(client))['tags'], ['v2', 'x']);
    } finally {
      if (!gate.isCompleted) gate.complete();
      await connection.close();
      await Future.wait(settled);
    }
  });

  test('once and invalidate refuse an active transaction callback', () async {
    await client.transaction((tx) async {
      await expectLater(
        once(client),
        throwsA(
          isA<CallError>().having((e) => e.code, 'code', 'transaction_active'),
        ),
      );
      await expectLater(
        client.invalidateQuery('GetTodos', 1, {'project': 'p'}),
        throwsA(
          isA<CallError>().having((e) => e.code, 'code', 'transaction_active'),
        ),
      );
    });
  });

  test('closing settles a waiting once caller', () async {
    await connect(client);
    final gate = Completer<void>();
    gates[1] = gate;
    final waiting = once(client);
    await Future<void>.delayed(const Duration(milliseconds: 50));
    final closing = client.close();
    await expectLater(
      waiting,
      throwsA(isA<CallError>().having((e) => e.code, 'code', 'client.closed')),
    );
    gate.complete();
    await closing;
  });
}
