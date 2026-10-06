import 'store_fixture.dart';
import 'protocol4_transport.dart';
// Model Fetch through the real native runtime (#153): Rust validates, joins or
// starts the request, stores the reply and completes every caller; this host
// only posts the `fetch` HTTP effect to `/sync/fetch` and decodes each
// caller's own result.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:axton/axton.dart';
import 'package:test/test.dart';

Map<String, dynamic> _scalar(String name) => {'kind': 'scalar', 'name': name};
final Map<String, dynamic> _schema = {
  'enums': [
    {
      'name': 'Status',
      'values': ['open', 'closed'],
    },
  ],
  'models': [
    {
      'name': 'Entry',
      'version': 2,
      'identity': ['id'],
      'fields': [
        {'name': 'id', 'type': _scalar('string'), 'nullable': false},
        {'name': 'title', 'type': _scalar('string'), 'nullable': false},
        {'name': 'at', 'type': _scalar('dateTime'), 'nullable': false},
        {
          'name': 'status',
          'type': {'kind': 'enum', 'name': 'Status'},
          'nullable': false,
        },
        {
          'name': 'tags',
          'type': {'kind': 'list', 'element': _scalar('string')},
          'nullable': false,
        },
      ],
    },
  ],
  'actions': [],
};
Map<String, dynamic> _state(String title) => {
  'title': title,
  'at': '2026-01-02T03:04:05.000Z',
  'status': 'open',
  'tags': [title],
};

/// Keeps the raw map: independence must come from the host, not a decoder.
Map<String, dynamic> _raw(Map<String, dynamic> row) => row;

/// What the fake backend answers for the n-th request.
typedef _Reply = Object Function(Map<String, dynamic> request, int n);

void main() {
  late Directory directory;
  late HttpServer server;
  late StreamSubscription<HttpRequest> served;
  final requests = <Map<String, dynamic>>[];
  final seen = <String>[];
  final gates = <int, Completer<void>>{};
  late _Reply reply;

  Future<Client> open() => Client.open(
    stream: 'User:viewer',
    connection: offlineStoreConnection(),
    path: '${directory.path}/db',
    schema: _schema,
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
  );
  var token = 'first';
  Future<RuntimeConnection> connect(
    Client client, {
    Future<void> Function()? refreshAuth,
  }) async {
    await client.connection?.close();
    return client.connect(
      SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => token),
      refreshAuth: refreshAuth,
    );
  }

  Future<Map<String, dynamic>?> fetch(
    Client client,
    String id, {
    bool store = true,
  }) => client.fetchModel('Entry', 2, {'id': id}, _raw, store: store);

  setUp(() async {
    requests.clear();
    seen.clear();
    gates.clear();
    token = 'first';
    reply = (request, n) => {
      'result': {'id': (request['identity'] as Map)['id'], ..._state('v$n')},
      'stamp': n,
    };
    directory = await Directory.systemTemp.createTemp('axton-fetch-');
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    served = server.listen((http) async {
      if (await answerEmptyBackground(http)) return;
      seen.add('${http.uri.path} ${http.headers.value('authorization')}');
      final request =
          jsonDecode(await utf8.decoder.bind(http).join())
              as Map<String, dynamic>;
      requests.add(request);
      final n = requests.length;
      await gates[n]?.future;
      final answer = reply(request, n);
      if (answer is int) {
        http.response.statusCode = answer;
        http.response.write('refused $answer');
        await http.response.close();
        return;
      }
      final a = answer as Map<String, dynamic>;
      final failure = a['failure'] as String?;
      final result = a['result'] as Map<String, dynamic>?;
      http.response.write(
        jsonEncode({
          'context': request['context'],
          'completion': {
            'callId': request['callId'],
            'outcome': failure == null
                ? {'status': 'succeeded', 'result': result}
                : {
                    'status': 'failed',
                    'code': failure,
                    'execution': 'rejected',
                  },
          },
          'records': failure != null
              ? []
              : [
                  {
                    'model': request['model'],
                    'identity': request['identity'],
                    'cursor': null,
                    'state': result == null
                        ? null
                        : ({...result}..remove('id')),
                  },
                ],
        }),
      );
      await http.response.close();
    });
  });
  tearDown(() async {
    await served.cancel();
    await server.close(force: true);
    await directory.delete(recursive: true);
  });

  test(
    'default storage applies the null-cursor snapshot before resolving',
    () async {
      final client = await open();
      try {
        await connect(client);
        expect(await fetch(client, 'a'), {'id': 'a', ..._state('v1')});
        expect(seen, ['/sync/fetch Bearer first']);
        expect(requests.single['model'], 'Entry');
        expect(requests.single['version'], 2);
        expect(requests.single['identity'], {'id': 'a'});
        expect(requests.single.containsKey('store'), isFalse);
        expect(await client.read('Entry', {'id': 'a'}), {
          'id': 'a',
          ..._state('v1'),
        });
        // Every sequential call reads remotely again.
        await fetch(client, 'a');
        expect(requests, hasLength(2));
        expect((await client.read('Entry', {'id': 'a'}))!['title'], 'v2');
      } finally {
        await client.close();
      }
    },
  );

  test('store false returns the snapshot without local storage', () async {
    final client = await open();
    try {
      await connect(client);
      expect(await fetch(client, 'a', store: false), {
        'id': 'a',
        ..._state('v1'),
      });
      expect(requests.single['store'], false);
      expect(await client.read('Entry', {'id': 'a'}), isNull);
    } finally {
      await client.close();
    }
  });

  test(
    'an absent null-cursor read returns null without deleting cached content',
    () async {
      final client = await open();
      try {
        await connect(client);
        await fetch(client, 'a');
        reply = (_, n) => {'result': null, 'stamp': n};
        expect(await fetch(client, 'a'), isNull);
        expect((await client.read('Entry', {'id': 'a'}))!['title'], 'v1');
      } finally {
        await client.close();
      }
    },
  );

  test('malformed cache state fails without storing partial state', () async {
    final client = await open();
    try {
      await connect(client);
      reply = (_, n) => {
        'result': {'id': 'a', 'title': 'missing required fields'},
      };
      await expectLater(fetch(client, 'a'), throwsA(isA<CallError>()));
      expect(await client.read('Entry', {'id': 'a'}), isNull);
    } finally {
      await client.close();
    }
  });

  test(
    'joined callers share one request and decode independent maps',
    () async {
      final client = await open();
      final gate = Completer<void>();
      gates[1] = gate;
      try {
        await connect(client);
        final waiting = [fetch(client, 'a'), fetch(client, 'a')];
        await Future<void>.delayed(const Duration(milliseconds: 50));
        gate.complete();
        final results = await Future.wait(waiting);
        expect(requests, hasLength(1));
        expect(results[0], results[1]);
        expect(identical(results[0], results[1]), isFalse);
        (results[0]!['tags'] as List).add('mutated');
        expect(results[1]!['tags'], ['v1']);
      } finally {
        // A failed step must not leave the fake server's request held.
        if (!gate.isCompleted) gate.complete();
        await client.close();
      }
    },
  );

  test('a backend refusal throws its typed code', () async {
    final client = await open();
    try {
      await connect(client);
      reply = (_, _) => {'failure': 'loader.failed'};
      await expectLater(
        fetch(client, 'a'),
        throwsA(
          isA<CallError>()
              .having((e) => e.code, 'code', 'loader.failed')
              .having((e) => e.execution, 'execution', 'rejected'),
        ),
      );
    } finally {
      await client.close();
    }
  });

  test('local failures keep their fetch codes and transport details', () async {
    final client = await open();
    try {
      await client.connection?.close();
      await expectLater(
        fetch(client, 'a'),
        throwsA(
          isA<CallError>().having((e) => e.code, 'code', 'fetch.unavailable'),
        ),
      );
      await connect(client);
      reply = (_, _) => 503;
      await expectLater(
        fetch(client, 'a'),
        throwsA(
          isA<CallError>()
              .having((e) => e.code, 'code', 'fetch.transport_failed')
              .having(
                (e) => (e.cause as HttpFailure).statusCode,
                'status',
                503,
              ),
        ),
      );
      final invalid = throwsA(
        isA<CallError>()
            .having((e) => e.code, 'code', 'fetch.invalid_options')
            .having((e) => e.execution, 'execution', 'rejected'),
      );
      final before = requests.length;
      await expectLater(client.fetchModel('Entry', 2, {}, _raw), invalid);
      await expectLater(
        client.fetchModel('Entry', 2, {'id': 1}, _raw),
        invalid,
      );
      await expectLater(
        client.fetchModel('Entry', 1, {'id': 'a'}, _raw),
        invalid,
      );
      await client.transaction((_) async {
        await expectLater(
          fetch(client, 'a'),
          throwsA(
            isA<CallError>().having(
              (e) => e.code,
              'code',
              'transaction_active',
            ),
          ),
        );
      });
      expect(requests, hasLength(before));
    } finally {
      await client.close();
    }
  });

  test('a 401 refreshes credentials once and resends the same call', () async {
    final client = await open();
    try {
      await connect(client, refreshAuth: () async => token = 'second');
      reply = (request, n) => n == 1
          ? 401
          : {
              'result': {'id': 'a', ..._state('v$n')},
              'stamp': n,
            };
      expect((await fetch(client, 'a', store: false))!['title'], 'v2');
      expect(seen, ['/sync/fetch Bearer first', '/sync/fetch Bearer second']);
      expect(requests[0]['callId'], requests[1]['callId']);
    } finally {
      await client.close();
    }
  });

  test('a Fetch on a closed client rethrows the admission error', () async {
    final client = await open();
    await connect(client);
    await client.close();
    // The raw admission error every task of a closed client gets; unlike a
    // direct call it is not mapped to a CallError.
    await expectLater(
      fetch(client, 'a'),
      throwsA(
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
    expect(requests, isEmpty);
  });

  test(
    'close while a Fetch waits on the network rejects it unavailable',
    () async {
      final client = await open();
      final gate = Completer<void>();
      gates[1] = gate;
      try {
        await connect(client);
        final waiting = [fetch(client, 'a'), fetch(client, 'a')];
        final outcomes = [
          for (final future in waiting)
            future.then<Object?>((value) => value, onError: (Object e) => e),
        ];
        while (requests.isEmpty) {
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        // The HTTP effect is still outstanding when the runtime closes.
        await client.close();
        for (final outcome in await Future.wait(outcomes)) {
          expect(
            outcome,
            isA<CallError>()
                .having((e) => e.code, 'code', 'fetch.unavailable')
                .having((e) => e.execution, 'execution', 'unknown'),
          );
        }
        // A reply released after close is fenced: nothing is stored.
        gate.complete();
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(requests, hasLength(1));
      } finally {
        // A failed step must not leave the fake server's request held.
        if (!gate.isCompleted) gate.complete();
        await client.close();
      }
    },
  );
}
