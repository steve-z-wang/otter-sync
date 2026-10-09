import 'protocol5_transport.dart';
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:axton/src/bindings/live.dart' show ServerSession, SocketEvents;
import 'package:test/test.dart';

final subscribe = {
  'protocol': 5,
  'storeId': 'store',
  'stream': 'User:viewer',
  'cursor': 7,
};
final subscribeFrame = jsonEncode(subscribe);

SocketEvents events({
  Future<void> Function(String)? message,
  void Function(Object, StackTrace?)? closed,
}) => SocketEvents(
  message: message ?? (_) async {},
  overflow: () async {},
  closed: closed ?? (_, _) {},
);

void main() {
  test('cancellation ends a stalled WebSocket token', () async {
    final cancel = Completer<void>();
    var closed = 0;
    final live = ServerSession(
      SyncServer(
        url: 'http://127.0.0.1:1',
        token: () => Completer<String>().future,
      ),
    );
    live.open(
      subscribeFrame,
      cancel.future,
      events(closed: (_, _) => closed++),
    );
    cancel.complete();
    await Future<void>.delayed(const Duration(milliseconds: 20));
    expect(closed, 0, reason: 'a cancelled socket is not reported as closed');
  });
  test(
    'the socket sends the subscribe frame and delivers frames in order',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final handshake = Completer<Map>();
      final finished = Completer<void>();
      server.listen((request) async {
        expect(request.headers.value('authorization'), 'Bearer secret');
        final socket = await WebSocketTransformer.upgrade(request);
        socket.listen((message) {
          handshake.complete(jsonDecode(message as String) as Map);
          socket.add(
            jsonEncode({
              ...emptyHandshake(jsonDecode(message) as Map),
              'head': 7,
            }),
          );
          socket.add(
            jsonEncode({
              'protocol': 5,
              'storeId': 'store',
              'stream': 'User:viewer',
              'head': 8,
            }),
          );
        }, onDone: () => finished.complete());
      });
      final cancel = Completer<void>();
      final frames = <Map>[];
      final second = Completer<void>();
      final live = ServerSession(
        SyncServer(
          url: 'http://127.0.0.1:${server.port}',
          token: () => 'secret',
        ),
      );
      live.open(
        subscribeFrame,
        cancel.future,
        events(
          message: (text) async {
            frames.add(jsonDecode(text) as Map);
            if (frames.length == 2) second.complete();
          },
        ),
      );
      try {
        expect(
          await handshake.future.timeout(const Duration(seconds: 2)),
          subscribe,
        );
        await second.future.timeout(const Duration(seconds: 2));
        expect(
          frames[0]['head'],
          7,
          reason: 'the transport does not interpret frames',
        );
        expect(frames[1]['head'], 8);
        cancel.complete();
        await finished.future.timeout(const Duration(seconds: 2));
      } finally {
        if (!cancel.isCompleted) cancel.complete();
        await server.close(force: true);
      }
    },
  );
  test(
    'cancel push before token resolution prevents any later HTTP request',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var requests = 0;
      server.listen((r) {
        requests++;
        r.response.close();
      });
      final token = Completer<String>();
      final live = ServerSession(
        SyncServer(
          url: 'http://127.0.0.1:${server.port}',
          token: () => token.future,
        ),
      );
      final cancel = Completer<void>();
      final pushing = live.push('{}', cancel.future);
      cancel.complete();
      token.complete('late');
      await expectLater(pushing, throwsStateError);
      expect(requests, 0);
      await server.close(force: true);
    },
  );

  test(
    'HTTP catch-up cancellation ends stalled token and in-flight response',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var requests = 0;
      final entered = Completer<void>();
      server.listen((request) {
        requests++;
        entered.complete();
      });
      final token = Completer<String>();
      final session = ServerSession(
        SyncServer(
          url: 'http://127.0.0.1:${server.port}',
          token: () => token.future,
        ),
      );
      final firstCancel = Completer<void>();
      final first = session.pull('{}', firstCancel.future);
      final stopped = expectLater(first, throwsStateError);
      firstCancel.complete();
      await stopped.timeout(const Duration(seconds: 2));
      token.complete('late');
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(requests, 0);
      final secondCancel = Completer<void>();
      final second = session.pull('{}', secondCancel.future);
      final aborted = expectLater(second, throwsStateError);
      await entered.future.timeout(const Duration(seconds: 2));
      secondCancel.complete();
      await aborted.timeout(const Duration(seconds: 2));
      expect(requests, 1);
      await server.close(force: true);
    },
  );

  test('close during an opening handshake cancels its socket', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final entered = Completer<void>();
    final requests = <HttpRequest>[];
    server.listen((r) {
      requests.add(r);
      entered.complete();
    });
    final cancelled = Completer<void>();
    var closed = 0;
    final live = ServerSession(
      SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'secret'),
    );
    live.open(
      subscribeFrame,
      cancelled.future,
      events(closed: (_, _) => closed++),
    );
    await entered.future.timeout(const Duration(seconds: 2));
    cancelled.complete();
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(closed, 0);
    await server.close(force: true);
  });

  test(
    'bound Store follows exactly one Stream and retains context on reopen',
    () async {
      final directory = await Directory.systemTemp.createTemp(
        'axton-bound-live-',
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final frames = <Map>[];
      final arrivals = <Completer<void>>[Completer<void>(), Completer<void>()];
      final served = server.listen((request) async {
        if (request.uri.path == '/sync/live') {
          final socket = await WebSocketTransformer.upgrade(request);
          socket.listen((message) {
            final body = jsonDecode(message as String) as Map;
            frames.add(body);
            socket.add(jsonEncode({...emptyHandshake(body)}));
            arrivals[frames.length - 1].complete();
          }, onError: (Object _) {});
        } else {
          await answerEmptyBackground(request);
        }
      });
      Future<Client> open() => Client.open(
        path: '${directory.path}/db',
        schema: {
          'models': [],
          'actions': [
            {
              'name': 'Ping',
              'version': 1,
              'kind': 'query',
              'inputs': [],
              'outputs': [],
            },
          ],
          'enums': [],
        },
        stream: 'User:viewer',
        connection: StoreConnection(
          url: 'http://127.0.0.1:${server.port}',
          token: () => 'token',
          onError: (_) {},
        ),
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      Client? client;
      try {
        client = await open();
        await client.bootstrap();
        await arrivals[0].future.timeout(const Duration(seconds: 5));
        await client.close();
        client = await open();
        await arrivals[1].future.timeout(const Duration(seconds: 5));
        expect(frames[0], frames[1]);
        expect(frames[0]['protocol'], 5);
        expect(frames[0]['stream'], 'User:viewer');
        expect(frames[0]['storeId'], matches(RegExp(r'^[0-9a-f-]{36}$')));
        expect(frames[0].containsKey('streams'), isFalse);
      } finally {
        await client?.close();
        await served.cancel();
        await server.close(force: true);
        await directory.delete(recursive: true);
      }
    },
  );
}
