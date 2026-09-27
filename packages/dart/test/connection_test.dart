// The connection as an effect executor (#134): the runtime owns the lanes,
// direct calls, refresh coordination and timeouts; the SDK executes the
// effects it asks for and aborts each one when it is cancelled. The executor
// tests drive the handlers through a fake host that emits effects and
// cancellations and records every answer; the direct-call tests run the real
// client against a local server.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:axton/src/bridge.dart' show Effect, EffectHandler, RuntimeHost;
import 'package:axton/src/connection.dart'
    show deliverDiagnostic, prerequisiteHandler;
import 'package:axton/src/live.dart' show ServerSession, SocketEvents;
import 'package:test/test.dart';

/// A runtime stand-in: it records tasks, emits effects to the installed
/// handlers, cancels them, and records every answer by effect id.
class FakeHost implements RuntimeHost {
  final tasks = <Map<String, dynamic>>[];
  final handlers = <String, EffectHandler>{};
  final results = <String, List<Map<String, dynamic>>>{};
  final _effects = <String, Effect>{};
  int _next = 0;

  /// What a task answers; tasks succeed with null by default.
  Future<dynamic> Function(Map<String, dynamic> command)? answer;

  /// Records [command] and answers it; [onValue] runs with a successful
  /// value before the caller resumes, as the Bridge runs it while
  /// dispatching the completion.
  @override
  Future<dynamic> task(
    Map<String, dynamic> command, {
    void Function(dynamic value)? onValue,
  }) async {
    tasks.add(command);
    final value = await answer?.call(command);
    onValue?.call(value);
    return value;
  }

  @override
  void handleEffects(String kind, EffectHandler handler) =>
      handlers[kind] = handler;

  @override
  void stopHandling(String kind, EffectHandler handler) {
    if (!identical(handlers[kind], handler)) return;
    handlers.remove(kind);
    for (final effect in _effects.values.toList()) {
      if (effect.operation['kind'] == kind) effect.cancel();
    }
  }

  /// Emit one effect to its handler and answer its id.
  String effect(Map<String, dynamic> operation) {
    final id = '${++_next}';
    final effect = Effect(
      id,
      operation,
      (outcome) => results.putIfAbsent(id, () => []).add(outcome),
      () => _effects.remove(id),
    );
    _effects[id] = effect;
    handlers[operation['kind']]!(effect);
    return id;
  }

  /// `cancelEffect`.
  void cancel(String id) => _effects[id]?.cancel();

  List<Map<String, dynamic>> of(String id) => results[id] ?? const [];

  /// The [count]th answer of [id], once it arrived.
  Future<Map<String, dynamic>> answerOf(String id, [int count = 1]) async {
    await until(() => of(id).length >= count, 'answer $count of effect $id');
    return of(id)[count - 1];
  }
}

Future<void> until(bool Function() predicate, String what) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!predicate()) {
    if (DateTime.now().isAfter(deadline)) throw StateError('$what timed out');
    await Future<void>.delayed(const Duration(milliseconds: 2));
  }
}

/// A network whose sockets the test drives by hand.
class ScriptedNetwork extends ServerSession {
  ScriptedNetwork()
    : super(SyncServer(url: 'http://127.0.0.1:1', token: () => 'secret'));
  final sockets = <(String, Future<void>, SocketEvents)>[];
  @override
  void open(String subscribe, Future<void> cancellation, SocketEvents on) =>
      sockets.add((subscribe, cancellation, on));
}

Map<String, dynamic> get _pingSchema => {
  'enums': [],
  'models': [],
  'actions': [
    {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
  ],
};

/// A direct call the runtime could not complete, with [code] and the cause
/// the runtime's `details` named.
Matcher _transport(String code, [Object? cause = anything]) => throwsA(
  isA<ActionTransportException>()
      .having((e) => e.code, 'code', code)
      .having((e) => e.cause, 'cause', cause),
);

/// The cause of a deadline or a transport failure without a status.
Matcher _message(String message) =>
    isA<StateError>().having((e) => e.message, 'message', message);

void main() {
  group('effect executor', () {
    late HttpServer server;
    late FakeHost host;
    final seen = <String>[];
    final bodies = <String>[];
    var status = 200;
    Completer<void>? hold;

    setUp(() async {
      seen.clear();
      bodies.clear();
      status = 200;
      hold = null;
      host = FakeHost();
      server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      server.listen((request) async {
        seen.add(request.uri.path);
        bodies.add(await utf8.decoder.bind(request).join());
        await hold?.future;
        request.response.statusCode = status;
        request.response.write('answer ${request.uri.path}');
        await request.response.close();
      });
    });
    tearDown(() => server.close(force: true));

    Future<RuntimeConnection> connect({
      FutureOr<String> Function()? token,
      Future<void> Function()? refreshAuth,
      void Function(Object)? onError,
      ServerSession? network,
      Duration directTimeout = const Duration(seconds: 30),
    }) => RuntimeConnection.connect(
      host: host,
      network:
          network ??
          ServerSession(
            SyncServer(
              url: 'http://127.0.0.1:${server.port}',
              token: token ?? () => 'alice',
            ),
          ),
      refreshAuth: refreshAuth,
      onError: onError,
      directTimeout: directTimeout,
    );

    test(
      'connect submits the intent; controls and close submit connection events',
      () async {
        final connection = await connect(
          refreshAuth: () async {},
          directTimeout: const Duration(microseconds: 1500),
        );
        expect(host.tasks.single, {
          'kind': 'connect',
          'directTimeoutMs': 2,
          'refreshAuth': true,
        });
        expect(host.handlers.keys, {'http', 'socket', 'timer', 'refreshAuth'});
        await connection.pause();
        await connection.resume();
        await connection.wake();
        await connection.close();
        expect(host.tasks.skip(1).map((t) => t['event']), [
          'pause',
          'resume',
          'wake',
          'stop',
        ]);
        expect(host.handlers, isEmpty, reason: 'close removes its handlers');
        // Closed controls reach no runtime: a later connection's lanes are
        // not this one's to alter.
        await connection.pause();
        await connection.resume();
        await connection.wake();
        await connection.close();
        expect(host.tasks, hasLength(5));
      },
    );

    test(
      'without refreshAuth the runtime is told and no handler runs',
      () async {
        final connection = await connect();
        expect(host.tasks.single['refreshAuth'], false);
        expect(host.tasks.single['directTimeoutMs'], 30000);
        expect(host.handlers.keys, {'http', 'socket', 'timer'});
        await connection.close();
      },
    );

    test(
      'a non-positive direct timeout is refused before connecting',
      () async {
        await expectLater(
          connect(directTimeout: Duration.zero),
          throwsArgumentError,
        );
        expect(host.tasks, isEmpty);
        expect(host.handlers, isEmpty);
      },
    );

    test('a refused connect installs no handlers', () async {
      host.answer = (_) async => throw StateError('connection already active');
      await expectLater(
        connect(),
        throwsA(
          isA<StateError>().having(
            (e) => e.message,
            'message',
            'connection already active',
          ),
        ),
      );
      expect(host.handlers, isEmpty);
    });

    test(
      'a second connect the runtime refuses leaves the active connection its handlers',
      () async {
        final first = await connect();
        final installed = Map.of(host.handlers);
        host.answer = (_) async =>
            throw StateError('connection already active');
        await expectLater(connect(), throwsStateError);
        expect(host.handlers, installed);
        host.answer = null;
        await first.close();
        expect(host.handlers, isEmpty);
      },
    );

    test(
      'http posts each route to its endpoint and answers the text',
      () async {
        final connection = await connect();
        const paths = {
          'push': '/sync/mutations',
          'pull': '/sync/pull',
          'action': '/sync/actions',
        };
        final ids = {
          for (final route in paths.keys)
            route: host.effect({'kind': 'http', 'route': route, 'body': route}),
        };
        for (final MapEntry(key: route, value: id) in ids.entries) {
          expect(await host.answerOf(id), {
            'ok': true,
            'value': 'answer ${paths[route]}',
          });
        }
        expect(seen.toSet(), paths.values.toSet());
        expect(bodies.toSet(), paths.keys.toSet());
        await connection.close();
      },
    );

    test('http failures carry the status the server answered', () async {
      final connection = await connect();
      for (final (route, code) in [
        ('push', 503),
        ('pull', 409),
        ('action', 500),
        ('push', 401),
      ]) {
        status = code;
        final id = host.effect({'kind': 'http', 'route': route, 'body': '{}'});
        final answer = await host.answerOf(id);
        expect(answer['ok'], false);
        final error = answer['error'] as Map;
        expect(error['status'], code);
        if (code != 401) {
          expect(error['message'], '$route failed: $code answer ${seen.last}');
        }
      }
      await connection.close();
    });

    test('a transport failure without a status carries none', () async {
      final closed = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final port = closed.port;
      await closed.close(force: true);
      final connection = await connect(
        network: ServerSession(
          SyncServer(url: 'http://127.0.0.1:$port', token: () => 'alice'),
        ),
      );
      final id = host.effect({'kind': 'http', 'route': 'push', 'body': '{}'});
      final answer = await host.answerOf(id);
      expect(answer['ok'], false);
      expect((answer['error'] as Map).containsKey('status'), isFalse);
      await connection.close();
    });

    test(
      'a cancelled request is aborted in flight and answers nothing',
      () async {
        final connection = await connect();
        hold = Completer<void>();
        final id = host.effect({'kind': 'http', 'route': 'push', 'body': '{}'});
        await until(() => seen.isNotEmpty, 'the request');
        host.cancel(id);
        hold!.complete();
        await Future<void>.delayed(const Duration(milliseconds: 30));
        expect(host.of(id), isEmpty);
        await connection.close();
      },
    );

    // The runtime cancels the push it abandons on `pause`: a request whose
    // token was still pending never goes out, and a direct request beside it
    // is not touched.
    test(
      'cancelling a push before its token resolves sends nothing and leaves a direct request alive',
      () async {
        final token = Completer<String>();
        final connection = await connect(token: () => token.future);
        final push = host.effect({
          'kind': 'http',
          'route': 'push',
          'body': 'p',
        });
        final direct = host.effect({
          'kind': 'http',
          'route': 'action',
          'body': 'a',
        });
        host.cancel(push);
        token.complete('late');
        expect(await host.answerOf(direct), {
          'ok': true,
          'value': 'answer /sync/actions',
        });
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(seen, ['/sync/actions']);
        expect(host.of(push), isEmpty);
        await connection.close();
      },
    );

    test(
      'a socket streams frames, overflow and its end under one id',
      () async {
        final network = ScriptedNetwork();
        final connection = await connect(network: network);
        final id = host.effect({'kind': 'socket', 'subscribe': 'hello'});
        final (subscribe, _, on) = network.sockets.single;
        expect(subscribe, 'hello');
        await on.message('one');
        await on.overflow();
        await on.message('two');
        on.closed(const AuthenticationExpired(), null);
        on.closed(StateError('twice'), null);
        await on.message('late');
        expect(host.of(id), [
          {
            'ok': true,
            'value': {'event': 'message', 'body': 'one'},
          },
          {
            'ok': true,
            'value': {'event': 'overflow'},
          },
          {
            'ok': true,
            'value': {'event': 'message', 'body': 'two'},
          },
          {
            'ok': false,
            'error': {
              'message': "Instance of 'AuthenticationExpired'",
              'status': 401,
            },
          },
        ]);
        await connection.close();
      },
    );

    test('cancelling a socket aborts it and silences its callbacks', () async {
      final network = ScriptedNetwork();
      final connection = await connect(network: network);
      final id = host.effect({'kind': 'socket', 'subscribe': 'hello'});
      final (_, cancellation, on) = network.sockets.single;
      var aborted = false;
      unawaited(cancellation.then((_) => aborted = true));
      host.cancel(id);
      await Future<void>.delayed(Duration.zero);
      expect(aborted, isTrue);
      await on.message('late');
      on.closed(StateError('late'), null);
      expect(host.of(id), isEmpty);
      await connection.close();
    });

    test(
      'a real socket ends with a failure when the server closes it',
      () async {
        final live = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
        live.listen((request) async {
          final socket = await WebSocketTransformer.upgrade(request);
          socket.listen((message) async {
            socket.add('echo $message');
            await socket.close();
          });
        });
        try {
          final connection = await connect(
            network: ServerSession(
              SyncServer(
                url: 'http://127.0.0.1:${live.port}',
                token: () => 'a',
              ),
            ),
          );
          final id = host.effect({'kind': 'socket', 'subscribe': 'hi'});
          expect(await host.answerOf(id), {
            'ok': true,
            'value': {'event': 'message', 'body': 'echo hi'},
          });
          final end = await host.answerOf(id, 2);
          expect(end['ok'], false);
          expect(
            (end['error'] as Map)['message'],
            contains('live disconnected'),
          );
          await connection.close();
        } finally {
          await live.close(force: true);
        }
      },
    );

    test('a timer answers when it fires; a cancelled one never does', () async {
      final connection = await connect();
      final fired = host.effect({'kind': 'timer', 'millis': 5});
      final cancelled = host.effect({'kind': 'timer', 'millis': 20});
      host.cancel(cancelled);
      expect(await host.answerOf(fired), {'ok': true});
      await Future<void>.delayed(const Duration(milliseconds: 40));
      expect(host.of(cancelled), isEmpty);
      await connection.close();
    });

    test('close aborts every effect its handlers still hold', () async {
      final network = ScriptedNetwork();
      final connection = await connect(network: network);
      final socket = host.effect({'kind': 'socket', 'subscribe': 's'});
      final timer = host.effect({'kind': 'timer', 'millis': 20});
      await connection.close();
      final (_, cancellation, on) = network.sockets.single;
      var aborted = false;
      unawaited(cancellation.then((_) => aborted = true));
      await Future<void>.delayed(const Duration(milliseconds: 40));
      expect(aborted, isTrue);
      await on.message('late');
      expect(host.of(socket), isEmpty);
      expect(host.of(timer), isEmpty);
    });

    test('refreshAuth answers ok, or the failure it threw', () async {
      var refreshes = 0;
      final connection = await connect(
        refreshAuth: () async {
          if (++refreshes == 2) throw StateError('refresh failed');
        },
      );
      final ok = host.effect({'kind': 'refreshAuth'});
      expect(await host.answerOf(ok), {'ok': true});
      final failed = host.effect({'kind': 'refreshAuth'});
      expect(await host.answerOf(failed), {
        'ok': false,
        'error': {'message': 'Bad state: refresh failed'},
      });
      expect(refreshes, 2);
      await connection.close();
    });

    test('a prerequisite handler answers ok, or the reason it threw', () async {
      final calls = <Map<String, dynamic>>[];
      host.handleEffects(
        'prerequisite',
        prerequisiteHandler({
          'Upload': (arguments) async {
            calls.add(arguments);
            if (arguments['key'] == 'bad') throw StateError('offline');
          },
        }),
      );
      final ok = host.effect({
        'kind': 'prerequisite',
        'key': 't1',
        'name': 'Upload',
        'arguments': {'key': 'good'},
      });
      final failed = host.effect({
        'kind': 'prerequisite',
        'key': 't2',
        'name': 'Upload',
        'arguments': {'key': 'bad'},
      });
      final missing = host.effect({
        'kind': 'prerequisite',
        'key': 't3',
        'name': 'Other',
        'arguments': <String, dynamic>{},
      });
      expect(await host.answerOf(ok), {'ok': true});
      expect(await host.answerOf(failed), {
        'ok': false,
        'error': {'message': 'Bad state: offline'},
      });
      expect(await host.answerOf(missing), {
        'ok': false,
        'error': {'message': 'missing prerequisite handler'},
      });
      expect(calls, [
        {'key': 'good'},
        {'key': 'bad'},
      ]);
    });
  });

  group('reports', () {
    final record = {
      'kind': 'conflict',
      'model': 'Entry',
      'identity': {'id': 'e'},
      'stamp': 3,
    };

    test('records become AxtonReports; errors become StateErrors', () {
      final reported = <Object>[];
      deliverDiagnostic({
        'kind': 'records',
        'reports': [record, record],
      }, reported.add);
      deliverDiagnostic({'kind': 'error', 'message': 'lane'}, reported.add);
      deliverDiagnostic({'kind': 'protocol', 'message': 'dup'}, reported.add);
      expect(reported, hasLength(4));
      expect(
        reported.take(2),
        everyElement(
          isA<AxtonReport>()
              .having((r) => r.kind, 'kind', 'conflict')
              .having((r) => r.stamp, 'stamp', 3),
        ),
      );
      expect(reported.skip(2).map((e) => (e as StateError).message), [
        'lane',
        'dup',
      ]);
    });

    test('a throwing onError reaches the zone and stops nothing', () {
      final reported = <Object>[];
      final thrown = <Object>[];
      runZonedGuarded(() {
        deliverDiagnostic(
          {
            'kind': 'records',
            'reports': [record, record],
          },
          (error) {
            reported.add(error);
            throw StateError('application failed');
          },
        );
      }, (error, _) => thrown.add(error));
      expect(reported, hasLength(2));
      expect(thrown.map((e) => '$e'), [
        'Bad state: application failed',
        'Bad state: application failed',
      ]);
    });

    test(
      'a connection hands reports to onError in the zone that connected',
      () async {
        final host = FakeHost();
        final reported = <Object>[];
        final thrown = <Object>[];
        final connected = Completer<RuntimeConnection>();
        runZonedGuarded(() {
          RuntimeConnection.connect(
            host: host,
            network: ScriptedNetwork(),
            onError: (error) {
              reported.add(error);
              throw StateError('diagnostic failed');
            },
          ).then(connected.complete);
        }, (error, _) => thrown.add(error));
        final connection = await connected.future;
        connection.report({'kind': 'error', 'message': 'push failed: 503'});
        expect(
          reported.single,
          isA<StateError>().having(
            (e) => e.message,
            'message',
            'push failed: 503',
          ),
        );
        expect(thrown.single.toString(), contains('diagnostic failed'));
        await connection.close();
        connection.report({'kind': 'error', 'message': 'after close'});
        expect(
          reported,
          hasLength(1),
          reason: 'a closed connection hears nothing',
        );
      },
    );
  });

  group('direct calls through the runtime', () {
    late Directory directory;
    late HttpServer server;
    late Client client;

    setUp(() async {
      directory = await Directory.systemTemp.createTemp('axton-direct-');
      server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      client = await Client.open(
        path: '${directory.path}/db',
        schema: _pingSchema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
    });
    tearDown(() async {
      await client.close();
      await server.close(force: true);
      await directory.delete(recursive: true);
    });

    SyncServer config() =>
        SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'a');

    String completion(String body) => jsonEncode({
      'completion': {
        'callId': ((jsonDecode(body) as Map)['call'] as Map)['callId'],
        'outcome': {'status': 'succeeded', 'result': null},
      },
      'records': [],
    });

    test('direct attempt times out while the server never answers', () async {
      server.listen((request) async {
        await utf8.decoder.bind(request).join();
      });
      final connection = await client.connect(
        config(),
        directTimeout: const Duration(milliseconds: 15),
      );
      await expectLater(
        client.callAction('Ping', 1, {}),
        _transport(
          'action.execution_unknown',
          _message('direct call timed out'),
        ),
      );
      await connection.close();
    });

    test('direct authentication retry resends the same request once', () async {
      final bodies = <String>[];
      server.listen((request) async {
        final body = await utf8.decoder.bind(request).join();
        bodies.add(body);
        if (bodies.length == 1) {
          request.response.statusCode = 401;
        } else {
          request.response.write(completion(body));
        }
        await request.response.close();
      });
      var refreshes = 0;
      final connection = await client.connect(
        config(),
        refreshAuth: () async => refreshes++,
      );
      final invoked = await client.callAction('Ping', 1, {});
      expect((invoked['outcome'] as Map)['status'], 'succeeded');
      expect(refreshes, 1);
      expect(bodies, hasLength(2));
      expect(bodies[1], bodies[0], reason: 'the same bytes are resent');
      await connection.close();
    });

    test('close ends a pending direct attempt as unavailable', () async {
      final entered = Completer<void>();
      server.listen((request) async {
        await utf8.decoder.bind(request).join();
        entered.complete();
      });
      final connection = await client.connect(config());
      final pending = expectLater(
        client.callAction('Ping', 1, {}),
        _transport('action.unavailable', isNull),
      );
      await entered.future;
      await connection.close();
      await pending;
    });

    test('direct timeout includes a stalled authentication refresh', () async {
      server.listen((request) async {
        await utf8.decoder.bind(request).join();
        request.response.statusCode = 401;
        await request.response.close();
      });
      final connection = await client.connect(
        config(),
        refreshAuth: () => Completer<void>().future,
        directTimeout: const Duration(milliseconds: 15),
      );
      await expectLater(
        client.callAction('Ping', 1, {}),
        _transport(
          'action.execution_unknown',
          _message('direct call timed out'),
        ),
      );
      await connection.close();
    });

    test('a transport failure keeps its message and status', () async {
      server.listen((request) async {
        await utf8.decoder.bind(request).join();
        request.response.statusCode = 503;
        request.response.write('down');
        await request.response.close();
      });
      final connection = await client.connect(config());
      final cause = isA<HttpFailure>()
          .having((e) => e.statusCode, 'statusCode', 503)
          .having((e) => e.message, 'message', 'action failed: 503 down');
      await expectLater(
        client.callAction('Ping', 1, {}),
        _transport('action.execution_unknown', cause),
      );
      await expectLater(
        client.invokeDirectAction<void>('Ping', 1, {}, (_) {}),
        throwsA(
          isA<CallError>()
              .having((e) => e.code, 'code', 'action.execution_unknown')
              .having((e) => e.execution, 'execution', 'unknown')
              .having((e) => e.cause, 'cause', cause),
        ),
      );
      await connection.close();
    });

    test('a refused refresh keeps its reason', () async {
      server.listen((request) async {
        await utf8.decoder.bind(request).join();
        request.response.statusCode = 401;
        await request.response.close();
      });
      final connection = await client.connect(
        config(),
        refreshAuth: () async => throw StateError('login required'),
      );
      await expectLater(
        client.callAction('Ping', 1, {}),
        _transport(
          'action.execution_unknown',
          isA<Object>().having(
            (e) => e.toString(),
            'text',
            contains('login required'),
          ),
        ),
      );
      await connection.close();
    });

    test('without a connection a direct call is unavailable', () async {
      await expectLater(
        client.callAction('Ping', 1, {}),
        _transport('action.unavailable'),
      );
      await expectLater(
        client.invokeDirectAction<void>('Ping', 1, {}, (_) {}),
        throwsA(
          isA<CallError>()
              .having((e) => e.code, 'code', 'action.unavailable')
              .having((e) => e.execution, 'execution', 'unknown'),
        ),
      );
    });
  });

  Future<void> assertSocketClosed({required bool closeConnection}) async {
    final directory = await Directory.systemTemp.createTemp(
      'axton-direct-abort-',
    );
    final server = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final entered = Completer<void>();
    final disconnected = Completer<void>();
    Socket? accepted;
    final subscription = server.listen((socket) {
      accepted = socket;
      socket.listen(
        (_) {
          if (!entered.isCompleted) entered.complete();
        },
        onDone: () {
          if (!disconnected.isCompleted) disconnected.complete();
        },
        onError: (Object _) {
          if (!disconnected.isCompleted) disconnected.complete();
        },
      );
    });
    final client = await Client.open(
      path: '${directory.path}/db',
      schema: _pingSchema,
      libraryPath: Platform.environment['AXTON_LIBRARY']!,
    );
    RuntimeConnection? connection;
    try {
      connection = await client.connect(
        SyncServer(
          url: 'http://127.0.0.1:${server.port}',
          token: () => 'alice',
        ),
        directTimeout: Duration(milliseconds: closeConnection ? 1000 : 100),
      );
      final pending = client.callAction('Ping', 1, {});
      final observed = expectLater(
        pending,
        throwsA(isA<ActionTransportException>()),
      );
      await entered.future.timeout(const Duration(seconds: 1));
      if (closeConnection) await connection.close();
      await observed;
      await disconnected.future.timeout(const Duration(milliseconds: 300));
    } finally {
      await connection?.close();
      await client.close();
      accepted?.destroy();
      await subscription.cancel();
      await server.close();
      await directory.delete(recursive: true);
    }
  }

  test(
    'direct timeout closes its actual HTTP socket',
    () async => assertSocketClosed(closeConnection: false),
  );
  test(
    'direct close closes its actual HTTP socket',
    () async => assertSocketClosed(closeConnection: true),
  );

  test(
    'cancelling one direct HTTP attempt leaves a sibling request alive',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final firstSeen = Completer<void>();
      final served = server.listen((request) async {
        final body = await utf8.decoder.bind(request).join();
        if (body == 'first') {
          firstSeen.complete();
          return;
        }
        request.response.write('second-ok');
        await request.response.close();
      });
      final session = ServerSession(
        SyncServer(
          url: 'http://127.0.0.1:${server.port}',
          token: () => 'alice',
        ),
      );
      final cancelFirst = Completer<void>();
      final cancelSecond = Completer<void>();
      try {
        final first = session.action('first', cancelFirst.future);
        final failed = expectLater(first, throwsA(anything));
        await firstSeen.future;
        final second = session.action('second', cancelSecond.future);
        cancelFirst.complete();
        await failed;
        expect(await second, 'second-ok');
      } finally {
        if (!cancelFirst.isCompleted) cancelFirst.complete();
        if (!cancelSecond.isCompleted) cancelSecond.complete();
        await served.cancel();
        await server.close(force: true);
      }
    },
  );
  test(
    'direct HTTP wait leaves local work free and late response after close is ignored',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'axton-dart-direct-',
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final entered = Completer<Map<String, dynamic>>();
      final release = Completer<void>();
      final served = server.listen((request) async {
        if (request.uri.path != '/sync/actions') {
          request.response.statusCode = 404;
          await request.response.close();
          return;
        }
        final body =
            jsonDecode(await utf8.decoder.bind(request).join())
                as Map<String, dynamic>;
        entered.complete(body);
        await release.future;
        request.response.write(
          jsonEncode({
            'completion': {
              'callId': (body['call'] as Map)['callId'],
              'outcome': {'status': 'succeeded', 'result': null},
            },
            'records': [],
          }),
        );
        await request.response.close();
      });
      final client = await Client.open(
        path: '${directory.path}/db',
        schema: _pingSchema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      RuntimeConnection? connection;
      try {
        connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'alice',
          ),
        );
        final pending = client.callAction('Ping', 1, {});
        final observed = expectLater(
          pending,
          throwsA(isA<ActionTransportException>()),
        );
        await entered.future;
        expect((await client.syncState())['pending'], 0);
        expect(await client.transaction((_) async => 42), 42);
        await connection.close();
        await observed;
        release.complete();
        await Future<void>.delayed(const Duration(milliseconds: 10));
        expect((await client.syncState())['pending'], 0);
      } finally {
        if (!release.isCompleted) release.complete();
        await connection?.close();
        await client.close();
        await served.cancel();
        await server.close(force: true);
        await directory.delete(recursive: true);
      }
    },
  );
  // The runtime treats a response that already arrived as local work: `stop`
  // fails only the calls still waiting on the network, so a response held
  // behind a local transaction applies once it commits, and its completion
  // is delivered after that commit
  // ([direct.rs](../../../crates/client/src/runtime/direct.rs)).
  test(
    'a response that arrived before close applies once the local transaction commits',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'axton-direct-close-race-',
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final entered = Completer<Map<String, dynamic>>();
      final releaseResponse = Completer<void>();
      final responseSent = Completer<void>();
      final served = server.listen((request) async {
        final body =
            jsonDecode(await utf8.decoder.bind(request).join())
                as Map<String, dynamic>;
        entered.complete(body);
        await releaseResponse.future;
        request.response.write(
          jsonEncode({
            'completion': {
              'callId': (body['call'] as Map)['callId'],
              'outcome': {'status': 'succeeded', 'result': null},
            },
            'records': [],
          }),
        );
        await request.response.close();
        responseSent.complete();
      });
      final client = await Client.open(
        path: '${directory.path}/db',
        schema: _pingSchema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final completions = <Map<String, dynamic>>[];
      final observedCompletions = client.actionCompletions.listen(
        completions.add,
      );
      RuntimeConnection? connection;
      final hold = Completer<void>();
      try {
        connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'alice',
          ),
        );
        final pending = client.callAction('Ping', 1, {});
        final call = await entered.future;
        final txEntered = Completer<void>();
        final transaction = client.transaction((_) async {
          txEntered.complete();
          await hold.future;
        });
        await txEntered.future;
        releaseResponse.complete();
        await responseSent.future;
        await Future<void>.delayed(const Duration(milliseconds: 50));
        final closing = connection.close();
        expect(completions, isEmpty, reason: 'nothing applied behind it');
        hold.complete();
        await transaction;
        await closing;
        expect(((await pending)['outcome'] as Map)['status'], 'succeeded');
        expect(completions.single['callId'], (call['call'] as Map)['callId']);
      } finally {
        if (!hold.isCompleted) hold.complete();
        if (!releaseResponse.isCompleted) releaseResponse.complete();
        await connection?.close();
        await observedCompletions.cancel();
        await client.close();
        await served.cancel();
        await server.close(force: true);
        await directory.delete(recursive: true);
      }
    },
  );
  test(
    'Dart Action discard and rebuild streams carry terminal call identities',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'axton-dart-action-discard-',
      );
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      schema['actions'] = [
        {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
      ];
      final breaking = jsonDecode(jsonEncode(schema)) as Map<String, dynamic>;
      ((breaking['models'] as List).first['fields'] as List).add({
        'name': 'due',
        'nullable': false,
        'type': {'kind': 'scalar', 'name': 'string'},
      });
      try {
        for (final frozen in [false, true]) {
          final path = '${directory.path}/${frozen ? 'frozen' : 'unsent'}';
          final original = await Client.open(
            path: path,
            schema: schema,
            libraryPath: Platform.environment['AXTON_LIBRARY']!,
          );
          final delivered = <Map<String, dynamic>>[];
          final sub = original.actionCompletions.listen(delivered.add);
          final dropped = await original.submitAction('Ping', 1, {});
          await original.drop(dropped['ordinal'] as int);
          expect(delivered.single['callId'], dropped['callId']);
          expect((delivered.single['outcome'] as Map)['code'], 'dropped');
          final pending = await original.submitAction('Ping', 1, {});
          if (frozen) await original.freeze();
          await sub.cancel();
          await original.close();
          final reopened = await Client.open(
            path: path,
            schema: breaking,
            libraryPath: Platform.environment['AXTON_LIBRARY']!,
          );
          final abandoned = <Map<String, dynamic>>[];
          final rebuildSub = reopened.actionCompletions.listen(abandoned.add);
          try {
            final report = await reopened.rebuild(discardPending: true);
            expect(report['abandonedCalls'], [
              {'callId': pending['callId'], 'frozen': frozen},
            ]);
            expect(abandoned.single['callId'], pending['callId']);
            expect(
              (abandoned.single['outcome'] as Map)['execution'],
              frozen ? 'unknown' : 'rejected',
            );
          } finally {
            await rebuildSub.cancel();
            await reopened.close();
          }
        }
      } finally {
        await directory.delete(recursive: true);
      }
    },
  );

  /// A page the lane abandoned itself is not the application's failure:
  /// `pause` aborts it silently and `resume` fetches again. The TypeScript
  /// twin is `pausing the downlink lane abandons its bootstrap page without
  /// reporting it` ([#151](https://github.com/zanminwang/axton/issues/151)).
  test(
    'pausing the connection abandons its bootstrap page without reporting it',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'axton-dart-bootstrap-pause-',
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var loads = 0;
      server.listen((request) async {
        if (request.uri.path == '/sync/pull') {
          final body =
              jsonDecode(await utf8.decoder.bind(request).join()) as Map;
          if (body['mode'] == 'bootstrap') {
            // Held: only the client's abort ends it.
            loads++;
            return;
          }
          request.response.write(
            jsonEncode({
              'cursors': {
                for (final entry in (body['cursors'] as Map).entries)
                  entry.key: {
                    'from': entry.value,
                    'to': entry.value,
                    'head': entry.value,
                  },
              },
              'changes': <Object>[],
            }),
          );
          await request.response.close();
          return;
        }
        final socket = await WebSocketTransformer.upgrade(request);
        socket.listen((message) {
          final sub = jsonDecode(message as String) as Map;
          socket.add(
            jsonEncode({
              'type': 'subscribed',
              'cursors': {for (final c in sub['channels'] as List) c: 0},
            }),
          );
        }, onError: (Object _) {});
      });
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${directory.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final reported = <Object>[];
      try {
        final connection = await client.connect(
          SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'a'),
          onError: reported.add,
        );
        final subscription = await client.subscribe('a');
        unawaited(subscription.bootstrap().then((_) {}, onError: (_) {}));
        await until(() => loads == 1, 'the bootstrap page');
        await connection.pause();
        await Future<void>.delayed(const Duration(milliseconds: 30));
        expect(
          reported,
          isEmpty,
          reason: 'its own cancellation is not an application failure',
        );
        await connection.resume();
        await until(() => loads == 2, 'the resumed page');
        await connection.close();
      } finally {
        await client.close();
        await server.close(force: true);
        await directory.delete(recursive: true);
      }
    },
  );
}
