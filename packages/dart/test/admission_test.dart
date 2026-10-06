import 'store_fixture.dart';
import 'protocol4_transport.dart';
// Client metadata and admission refusals (#181): the headers a SyncServer
// declares ride every request and the live upgrade, and a response the server
// marks `axton-admission: refused` stops the connection and reaches onError
// once as an AdmissionRefused - never as an authentication failure and never
// retried.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:axton/src/connection.dart' show statusOf;
import 'package:axton/src/live.dart'
    show ServerSession, SocketEvents, refusalOf;
import 'package:test/test.dart';

const refusal = '{"minimumBuild":7}';

/// A fake backend admitting only `x-app-build` 7 or later, on every route and
/// the upgrade. It records what each request carried.
class AdmissionServer {
  AdmissionServer._(this._server);
  final HttpServer _server;
  final seen = <(String, String?, String?)>[];
  final sockets = <WebSocket>[];
  final envelopes = <Map>[];
  var acknowledged = 0;
  bool Function(HttpRequest) marked = (_) => true;

  static Future<AdmissionServer> start() async {
    final server = AdmissionServer._(
      await HttpServer.bind(InternetAddress.loopbackIPv4, 0),
    );
    server._server.listen(server._handle);
    return server;
  }

  String get url => 'http://127.0.0.1:${_server.port}';

  Future<void> _handle(HttpRequest request) async {
    seen.add((
      request.uri.path,
      request.headers.value('x-app-build'),
      request.headers.value('authorization'),
    ));
    final build = int.tryParse(request.headers.value('x-app-build') ?? '');
    if (build == null || build < 7) {
      request.response.statusCode = 426;
      request.response.headers.contentType = ContentType.json;
      if (marked(request)) {
        request.response.headers.set('axton-admission', 'refused');
      }
      request.response.write(refusal);
      await request.response.close();
      return;
    }
    if (request.uri.path == '/sync/live') {
      final socket = await WebSocketTransformer.upgrade(request);
      sockets.add(socket);
      socket.listen((message) {
        final sub = jsonDecode(message as String) as Map;
        envelopes.add(sub);
        acknowledged++;
        socket.add(
          jsonEncode({
            'context': sub['context'],
            'cursor': sub['cursor'],
            'head': sub['cursor'],
          }),
        );
      });
      return;
    }
    final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
    envelopes.add(body);
    request.response.write(
      jsonEncode(
        body['context'] == null
            ? <String, Object?>{}
            : request.uri.path == '/sync/pull'
            ? emptyPull(body)
            : body.containsKey('models')
            ? emptyMutation(body)
            : emptyRead(body),
      ),
    );
    await request.response.close();
  }

  Future<void> close() async {
    for (final socket in sockets) {
      await socket.close();
    }
    await _server.close(force: true);
  }
}

Future<void> until(FutureOr<bool> Function() predicate, String what) async {
  final deadline = DateTime.now().add(const Duration(seconds: 5));
  while (!await predicate()) {
    if (DateTime.now().isAfter(deadline)) throw StateError('$what timed out');
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

SocketEvents events({
  Future<void> Function(String)? message,
  void Function(Object, StackTrace?)? closed,
}) => SocketEvents(
  message: message ?? (_) async {},
  overflow: () async {},
  closed: closed ?? (_, _) {},
);

Future<Map<String, dynamic>> entrySchema() async =>
    jsonDecode(await File('../../fixtures/schemas/entry.json').readAsString())
        as Map<String, dynamic>;

void main() {
  late AdmissionServer server;
  setUp(() async => server = await AdmissionServer.start());
  tearDown(() => server.close());

  SyncServer config(String build) => SyncServer(
    url: server.url,
    token: () => 'secret',
    headers: {'x-app-build': build},
  );

  test('the headers reach every route and the upgrade beside the credential, '
      'which they cannot replace', () async {
    final session = ServerSession(config('7'));
    final never = Completer<void>().future;
    const body =
        '{"cursors":{},"mutations":[],"clientId":"c","batchSequence":1}';
    await session.push(body, never);
    await session.pull(body, never);
    await session.action(body, never);
    await session.fetch(body, never);
    await session.load(body, never);
    final opened = Completer<void>();
    final cancel = Completer<void>();
    session.open(
      jsonEncode({
        'type': 'subscribe',
        'streams': ['scope'],
      }),
      cancel.future,
      events(message: (_) async => opened.complete()),
    );
    await opened.future;
    cancel.complete();
    expect(server.seen, [
      for (final path in [
        '/sync/mutations',
        '/sync/pull',
        '/sync/actions',
        '/sync/fetch',
        '/sync/loads',
        '/sync/live',
      ])
        (path, '7', 'Bearer secret'),
    ]);
    for (final name in [
      'Authorization',
      'content-type',
      'Sec-WebSocket-Key',
      'upgrade',
    ]) {
      expect(
        () => ServerSession(
          SyncServer(url: server.url, token: () => 't', headers: {name: 'x'}),
        ),
        throwsArgumentError,
      );
    }
  });

  test('a marked refusal carries its status and body on HTTP and on the '
      'upgrade; an unmarked 426 is an ordinary failure', () async {
    final session = ServerSession(config('6'));
    final never = Completer<void>().future;
    final refused = await session
        .push('{}', never)
        .then<Object>((_) => fail('admitted'), onError: (Object e) => e);
    expect((statusOf(refused), refusalOf(refused)), (426, refusal));
    final closed = Completer<Object>();
    session.open(
      '{}',
      never,
      events(closed: (error, _) => closed.complete(error)),
    );
    final socket = await closed.future;
    expect((statusOf(socket), refusalOf(socket)), (426, refusal));

    server.marked = (_) => false;
    final plain = await session
        .pull('{}', never)
        .then<Object>((_) => fail('admitted'), onError: (Object e) => e);
    expect((statusOf(plain), refusalOf(plain)), (426, null));
    final unmarked = Completer<Object>();
    session.open(
      '{}',
      never,
      events(closed: (error, _) => unmarked.complete(error)),
    );
    final upgrade = await unmarked.future;
    expect((statusOf(upgrade), refusalOf(upgrade)), (426, null));
  });

  test('a refused client hears one AdmissionRefused, stops reconnecting, and '
      'a later client with current headers syncs', () async {
    final dir = await Directory.systemTemp.createTemp('axton-dart-admission-');
    final path = '${dir.path}/db';
    final schema = await entrySchema();
    schema['actions'] = [
      {
        'name': 'Ping',
        'version': 1,
        'kind': 'mutation',
        'inputs': <Object>[],
        'outputs': <Object>[],
      },
    ];
    final library = Platform.environment['AXTON_LIBRARY']!;
    var client = await Client.open(
      stream: 'User:viewer',
      connection: offlineStoreConnection(),
      path: path,
      schema: schema,
      libraryPath: library,
    );
    try {
      final errors = <Object>[];
      var refreshes = 0;
      await client.connection?.close();
      await client.connect(
        config('6'),
        onError: errors.add,
        refreshAuth: () async => refreshes++,
      );
      await until(() => errors.isNotEmpty, 'the refusal');
      await Future<void>.delayed(const Duration(milliseconds: 700));
      expect(errors, hasLength(1));
      final refused = errors.single as AdmissionRefused;
      expect(refused.status, 426);
      expect(refused.body, {'minimumBuild': 7});
      expect(
        refreshes,
        0,
        reason: 'a refusal is not an authentication failure',
      );
      expect(server.seen, [
        ('/sync/pull', '6', 'Bearer secret'),
      ], reason: 'the refused request is not retried');

      // The refused connection ended its handle: the same client connects
      // again, and a write reaches the server.
      final call = await client.submitMutation<void>('Ping', 1, {}, (_) {});
      server.seen.clear();
      final connection = await client.connect(config('7'), onError: errors.add);
      await until(
        () async => (await client.syncState())['pending'] == 0,
        'the push',
      );
      expect(await call.wait(), isA<CallSuccess<void>>());
      await until(() => server.acknowledged == 1, 'the socket');
      await connection.close();
      await client.close();

      // A later client over the same database, with current headers.
      client = await Client.open(
        stream: 'User:viewer',
        connection: offlineStoreConnection(),
        path: path,
        schema: schema,
        libraryPath: library,
      );
      await client.connection?.close();
      await client.connect(config('8'), onError: errors.add);
      await until(() => server.acknowledged == 2, 'the later socket');
      expect(server.envelopes, isNotEmpty);
      for (final envelope in server.envelopes) {
        final context = envelope['context'] as Map;
        expect(context['protocol'], 4);
        expect((context['binding'] as Map)['stream'], 'User:viewer');
      }
      expect(errors, hasLength(1));
      expect(
        server.seen.map((s) => s.$2).toSet(),
        {'7', '8'},
        reason: 'every request after the refusal carried current headers',
      );
    } finally {
      await client.close();
      await dir.delete(recursive: true);
    }
  });
}
