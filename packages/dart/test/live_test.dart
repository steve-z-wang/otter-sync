import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:axton/src/live.dart' show ServerSession, SocketEvents;
import 'package:test/test.dart';

final subscribeFrame = jsonEncode({
  'type': 'subscribe',
  'channels': ['scope'],
});

/// The acknowledgement: every subscribed channel at `head`.
String ack(Map sub, [int head = 0]) => jsonEncode({
  'type': 'subscribed',
  'cursors': {for (final channel in sub['channels'] as List) channel: head},
});
Map<String, dynamic> range(int from, int to, [int? head]) => {
  'from': from,
  'to': to,
  'head': head ?? to,
};

/// An HTTP answer that moves nothing: every requested channel stays where it is.
Map<String, dynamic> emptyPage(Map pull) => {
  'cursors': {
    for (final entry in (pull['cursors'] as Map).entries)
      entry.key: range(entry.value as int, entry.value as int),
  },
  'changes': <Object>[],
};
SocketEvents events({
  Future<void> Function(String)? message,
  void Function(Object, StackTrace?)? closed,
}) => SocketEvents(
  message: message ?? (_) async {},
  overflow: () async {},
  closed: closed ?? (_, _) {},
);

/// The stamps a fake server hands out, monotonically, across its receipts.
class FakeStamps {
  int next = 0;
}

/// A push receipt in the wire shape the client accepts: it answers the batch it
/// was asked (clientId and batchSequence echoed) and carries the authoritative
/// state of every record the batch's wire operations target, once per record,
/// at the next stamp. The "server" normalizes text by trimming it, so a test
/// can tell the receipt's content from the client's prediction.
Map<String, Object?> receiptFor(Map body, FakeStamps stamps) {
  final records = <String, Map<String, Object?>>{};
  for (final mutation in body['mutations'] as List) {
    final operations = (mutation as Map)['operations'] as List? ?? const [];
    for (final entry in operations) {
      final operation = entry as Map;
      final values = operation['values'] as Map?;
      final state = operation['op'] == 'delete'
          ? null
          : {
              'text': (values?['text'] ?? '').toString().trim(),
              'note': values?['note'],
            };
      final key = '${operation['model']}|${jsonEncode(operation['identity'])}';
      records[key] = {
        'model': operation['model'],
        'identity': operation['identity'],
        'stamp': ++stamps.next,
        'state': state,
      };
    }
  }
  return {
    'clientId': body['clientId'],
    'batchSequence': body['batchSequence'],
    'rejections': <Object>[],
    'records': records.values.toList(),
  };
}

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
          socket.add(ack(jsonDecode(message) as Map));
          socket.add(
            jsonEncode({
              'cursors': {'scope': range(7, 8)},
              'changes': [],
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
        expect(await handshake.future.timeout(const Duration(seconds: 2)), {
          'type': 'subscribe',
          'channels': ['scope'],
        });
        await second.future.timeout(const Duration(seconds: 2));
        expect(
          frames[0]['type'],
          'subscribed',
          reason: 'the transport does not interpret frames',
        );
        expect(frames[1]['cursors']['scope']['to'], 8);
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

  // The SDK no longer cancels the socket before it writes: an unsubscribe is a
  // local commit, and the Downlink worker decides what the committed membership
  // means for the session it holds
  // ([#150](https://github.com/zanminwang/axton/issues/150)).
  test(
    'an unsubscribe waiting on the database leaves the socket alone; the committed change ends it',
    () async {
      final dir = await Directory.systemTemp.createTemp(
        'axton-dart-token-generation-',
      );
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var requests = 0;
      final sockets = <WebSocket>[];
      final subscribes = <Map>[];
      server.listen((request) async {
        if (WebSocketTransformer.isUpgradeRequest(request)) {
          final socket = await WebSocketTransformer.upgrade(request);
          sockets.add(socket);
          socket.listen((message) {
            final sub = jsonDecode(message as String) as Map;
            subscribes.add(sub);
            socket.add(ack(sub));
          }, onError: (Object _) {});
          return;
        }
        requests++;
        await request.response.close();
      });
      final token = Completer<String>(), tokenEntered = Completer<void>();
      final txEntered = Completer<void>(), held = Completer<void>();
      final errors = <Object>[];
      try {
        await client.subscribe('scope');
        await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () {
              tokenEntered.complete();
              return token.future;
            },
          ),
          onError: errors.add,
        );
        await tokenEntered.future.timeout(const Duration(seconds: 2));
        final transaction = client.transaction((_) async {
          txEntered.complete();
          await held.future;
        });
        await txEntered.future;
        final removing = client.unsubscribe('scope');
        token.complete('secret');
        final deadline = DateTime.now().add(const Duration(seconds: 5));
        while (subscribes.isEmpty) {
          if (DateTime.now().isAfter(deadline)) {
            throw StateError('the pending authentication opened no socket');
          }
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        expect(
          requests,
          0,
          reason: 'an acknowledged session at its head pulls nothing',
        );
        held.complete();
        await transaction;
        await removing;
        // The committed change invalidates the session; with no Scope left the
        // lane opens nothing in its place.
        final ended = DateTime.now().add(const Duration(seconds: 5));
        while (sockets.first.closeCode == null) {
          if (DateTime.now().isAfter(ended)) {
            throw StateError('the committed change did not end the session');
          }
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        await Future<void>.delayed(const Duration(milliseconds: 50));
        expect(subscribes, hasLength(1));
        expect((await client.syncState())['channels'], isEmpty);
        expect(errors, isEmpty);
      } finally {
        if (!held.isCompleted) held.complete();
        if (!token.isCompleted) token.complete('cleanup');
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  test(
    'native live client retries failed authentication refresh and closes without leaks',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-dart-live-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var token = 'expired', refreshes = 0;
      final accepted = Completer<void>();
      final errors = <Object>[];
      final sockets = <WebSocket>[];
      server.listen((request) async {
        if (request.headers.value('authorization') != 'Bearer valid') {
          request.response.statusCode = 401;
          await request.response.close();
          return;
        }
        if (request.uri.path == '/sync/pull') {
          final pull =
              jsonDecode(await utf8.decoder.bind(request).join()) as Map;
          request.response.write(jsonEncode(emptyPage(pull)));
          await request.response.close();
          return;
        }
        final socket = await WebSocketTransformer.upgrade(request);
        sockets.add(socket);
        socket.listen((message) {
          final sub = jsonDecode(message as String) as Map;
          socket.add(ack(sub));
          if (!accepted.isCompleted) accepted.complete();
        });
      });
      try {
        await client.subscribe('scope');
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => token,
          ),
          onError: errors.add,
          refreshAuth: () async {
            if (++refreshes == 1) throw StateError('refresh failed');
            token = 'valid';
          },
        );
        await accepted.future.timeout(
          const Duration(seconds: 5),
          onTimeout: () =>
              throw StateError('refreshes=$refreshes errors=$errors'),
        );
        expect(refreshes, 2);
        expect(
          errors.any((e) => e.toString().contains('refresh failed')),
          isTrue,
        );
        await connection.pause();
        await connection.resume();
        await connection.close();
      } finally {
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );
  test(
    'native subscription changes discard old pages and HTTP recovers live gaps',
    () async {
      final dir = await Directory.systemTemp.createTemp(
        'axton-dart-generation-',
      );
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <WebSocket>[];
      final handshakes = <Map>[];
      final errors = <Object>[];
      Future<void> until(FutureOr<bool> Function() check) async {
        final deadline = DateTime.now().add(const Duration(seconds: 5));
        while (DateTime.now().isBefore(deadline)) {
          if (await check()) return;
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        throw StateError('condition timed out: $errors');
      }

      // A resubscribed channel restarts at cursor 0 while the record it
      // delivered before is retained at its stamp, so pages of the fresh
      // session carry newer stamps than the first session's did.
      var stampBase = 0;
      Map<String, dynamic> page(String text, int cursor) => {
        'cursors': {'scope': range(cursor, cursor + 1)},
        'changes': [
          {
            'model': 'Entry',
            'identity': {'id': 'live'},
            'stamp': stampBase + cursor + 1,
            'state': {'text': text, 'note': null},
          },
        ],
      };
      var pulls = 0;
      Map<String, dynamic> Function(int from)? recovery;
      server.listen((r) async {
        if (r.uri.path == '/sync/pull') {
          pulls++;
          final pull = jsonDecode(await utf8.decoder.bind(r).join()) as Map;
          r.response.write(
            jsonEncode(
              recovery?.call((pull['cursors'] as Map)['scope'] as int) ??
                  emptyPage(pull),
            ),
          );
          await r.response.close();
          return;
        }
        final socket = await WebSocketTransformer.upgrade(r);
        sockets.add(socket);
        socket.listen((message) {
          final sub = jsonDecode(message as String) as Map;
          handshakes.add(sub);
          socket.add(ack(sub));
        });
      });
      try {
        await client.subscribe('scope');
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        await until(() => handshakes.length == 1);
        sockets.first.add(jsonEncode(page('first', 0)));
        await until(
          () async =>
              (await client.read('Entry', {'id': 'live'}))?['text'] == 'first',
        );
        final held = Completer<void>(), entered = Completer<void>();
        final tx = client.transaction((_) async {
          entered.complete();
          await held.future;
        });
        await entered.future;
        sockets.first.add(jsonEncode(page('obsolete', 1)));
        final remove = client.unsubscribe('scope'),
            restore = client.subscribe('scope');
        held.complete();
        await tx;
        await remove;
        await restore;
        await until(() => handshakes.length >= 2);
        // Unsubscribing retains the downloaded record, and the obsolete frame
        // that arrived behind the transaction is dropped, not applied: the
        // runtime keeps the arrival order between the membership tasks and the
        // inbound frame, so the worker sees the frame after the session it
        // belonged to became stale.
        expect((await client.read('Entry', {'id': 'live'}))?['text'], 'first');
        expect(handshakes.last.containsKey('cursors'), isFalse);
        // The resubscribed channel restarts at cursor 0, but the record is
        // retained at stamp 1: the fresh session's pages need newer stamps.
        stampBase = 10;
        sockets.last.add(jsonEncode(page('fresh', 0)));
        await until(
          () async =>
              (await client.read('Entry', {'id': 'live'}))?['text'] == 'fresh',
        );
        expect(errors, isEmpty);
        final beforeOverlap = pulls;
        recovery = (from) => page('overlap recovered', from);
        sockets.last.add(
          jsonEncode({
            ...page('overlap', 1),
            'cursors': {'scope': range(0, 2)},
          }),
        );
        await until(
          () async =>
              (await client.read('Entry', {'id': 'live'}))?['text'] ==
              'overlap',
        );
        expect(
          pulls,
          beforeOverlap,
          reason: 'overlap applies directly without HTTP',
        );
        expect((await client.syncState())['cursors']['scope'], 2);
        sockets.last.add(
          jsonEncode({
            ...page('duplicate', 1),
            'cursors': {'scope': range(0, 2)},
          }),
        );
        await Future<void>.delayed(const Duration(milliseconds: 30));
        expect(pulls, beforeOverlap);
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          'overlap',
        );
        final before = pulls;
        // The pull covers the gap frame: from the cursor up to the frame's end.
        recovery = (from) => {
          ...page('recovered', from),
          'cursors': {'scope': range(from, 11)},
        };
        sockets.last.add(jsonEncode(page('gap', 10)));
        await until(
          () async =>
              (await client.read('Entry', {'id': 'live'}))?['text'] ==
              'recovered',
        );
        expect(pulls, greaterThan(before));
        expect(errors, isEmpty);
        await connection.close();
      } finally {
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );
  test(
    'HTTP catch-up pages after ack, queues overlap, and rejects obsolete HTTP completion',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-dart-http-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <WebSocket>[];
      final errors = <Object>[];
      final requests = <int>[];
      var acknowledged = false;
      var hold = Completer<void>();
      var entered = Completer<void>();
      var held = true;
      var version = 'initial';
      // A subscription's origin is the head its first handshake acknowledged,
      // so this server starts empty and publishes once the client has its
      // boundary ([#150](https://github.com/zanminwang/axton/issues/150)).
      var serverHead = 0;
      // A recreated subscription starts over at the next acknowledged head
      // while the records the old one delivered are retained at their stamps,
      // so a "fresh" page carries newer stamps than the "initial" ones.
      var stampBase = 0;
      Map<String, dynamic> page(int from, int to, String text) => {
        'cursors': {
          'scope': range(from, to, to > serverHead ? to : serverHead),
        },
        'changes': [
          for (var cursor = from + 1; cursor <= to; cursor++)
            {
              'model': 'Entry',
              'identity': {'id': 'e$cursor'},
              'stamp': stampBase + cursor,
              'state': {'text': text, 'note': null},
            },
        ],
      };
      Future<void> until(FutureOr<bool> Function() check) async {
        final deadline = DateTime.now().add(const Duration(seconds: 5));
        while (DateTime.now().isBefore(deadline)) {
          if (await check()) return;
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        throw StateError('timeout: $errors requests=$requests');
      }

      server.listen((request) async {
        if (request.uri.path == '/sync/pull') {
          expect(acknowledged, isTrue, reason: 'listeners must precede HTTP');
          final body =
              jsonDecode(await utf8.decoder.bind(request).join()) as Map;
          final from = (body['cursors'] as Map)['scope'] as int;
          requests.add(from);
          final result = page(
            from,
            from == 0 ? 50 : (from < 55 ? 55 : serverHead),
            version,
          );
          if (held) {
            held = false;
            entered.complete();
            await hold.future;
          }
          try {
            request.response.write(jsonEncode(result));
            await request.response.close();
          } catch (_) {}
          return;
        }
        final socket = await WebSocketTransformer.upgrade(request);
        sockets.add(socket);
        socket.listen((message) {
          final sub = jsonDecode(message as String) as Map;
          expect(sub.containsKey('cursors'), isFalse);
          acknowledged = true;
          socket.add(ack(sub, serverHead));
        });
      });
      try {
        await client.subscribe('scope');
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        // The first handshake is the origin: head zero, no history fetched.
        await until(
          () async => (await client.syncState())['cursors']['scope'] == 0,
        );
        expect(requests, isEmpty, reason: 'a fresh subscription pulls nothing');
        expect(await client.query('Entry'), isEmpty);
        // The server publishes while the socket is closed, so the next
        // handshake acknowledges a head above the committed cursor and the
        // catch-up this test holds begins.
        await connection.pause();
        serverHead = 55;
        await connection.resume();
        await entered.future.timeout(const Duration(seconds: 3));
        expect(await client.query('Entry'), isEmpty);
        // A commit observed live during catch-up is buffered, then recognized as covered.
        sockets.last.add(jsonEncode(page(50, 55, 'initial')));
        hold.complete();
        await until(() async => (await client.query('Entry')).length == 55);
        expect(requests, [0, 50]);
        await Future<void>.delayed(const Duration(milliseconds: 30));
        expect(requests, [0, 50], reason: 'steady state must not poll HTTP');
        sockets.last.add(jsonEncode(page(55, 56, 'live')));
        await until(() async => (await client.query('Entry')).length == 56);
        expect(requests, [0, 50]);
        // Reconnect catches up from the durable cursor. Hold that obsolete response
        // while unsubscribe/resubscribe resets the scope and starts a fresh session.
        await connection.pause();
        held = true;
        hold = Completer<void>();
        entered = Completer<void>();
        // The server moved on: the acknowledgement's head is beyond the
        // durable cursor, so the reconnect pulls from it.
        serverHead = 57;
        await connection.resume();
        await entered.future.timeout(const Duration(seconds: 3));
        expect(requests.last, 56);
        await client.unsubscribe('scope');
        version = 'fresh';
        stampBase = 100;
        await client.subscribe('scope');
        hold.complete();
        // The recreated subscription starts over at the head its own handshake
        // acknowledges and loads no history; the stream is the truth from there.
        await until(() async => sockets.length >= 4);
        await until(
          () async => (await client.syncState())['cursors']['scope'] == 57,
        );
        sockets.last.add(jsonEncode(page(57, 58, 'fresh')));
        await until(
          () async =>
              (await client.read('Entry', {'id': 'e58'}))?['text'] == 'fresh',
        );
        await Future<void>.delayed(const Duration(milliseconds: 30));
        expect(
          await client.read('Entry', {'id': 'e57'}),
          isNull,
          reason: 'the obsolete HTTP completion applied nothing',
        );
        expect(
          (await client.read('Entry', {'id': 'e1'}))?['text'],
          'initial',
          reason: 'unsubscribing retains what was delivered',
        );
        expect(
          (await client.read('Entry', {'id': 'e56'}))?['text'],
          'live',
          reason: 'the page the old session streamed is retained too',
        );
        expect((await client.query('Entry')).length, 57);
        expect((await client.syncState())['cursors']['scope'], 58);
        expect(errors, isEmpty);
        await connection.close();
      } finally {
        if (!hold.isCompleted) hold.complete();
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  test(
    'shared live factory isolates cancellation and pushes with no subscribed channels',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-shared-live-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final first = await Client.open(
        path: '${dir.path}/first',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final second = await Client.open(
        path: '${dir.path}/second',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var requests = 0;
      final stamps = FakeStamps();
      final entered = Completer<void>(), token = Completer<String>();
      final errors = <Object>[];
      server.listen((request) async {
        requests++;
        expect(request.uri.path, '/sync/mutations');
        expect(WebSocketTransformer.isUpgradeRequest(request), isFalse);
        final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
        request.response.headers.contentType = ContentType.json;
        request.response.write(jsonEncode(receiptFor(body, stamps)));
        await request.response.close();
      });
      final live = SyncServer(
        url: 'http://127.0.0.1:${server.port}',
        token: () {
          if (!entered.isCompleted) entered.complete();
          return token.future;
        },
      );
      try {
        await second.transaction(
          (tx) => tx.direct({
            'model': 'Entry',
            'op': 'create',
            'identity': {'id': 'local'},
            'values': {'text': 'base'},
          }),
        );
        await second.mutate({
          'name': 'Edit',
          'operations': [
            {
              'model': 'Entry',
              'op': 'update',
              'identity': {'id': 'local'},
              'values': {'text': 'edited'},
            },
          ],
        });
        final a = await first.connect(live);
        await second.connect(live, onError: errors.add);
        await entered.future.timeout(const Duration(seconds: 2));
        await a.pause();
        await a.close();
        token.complete('secret');
        final deadline = DateTime.now().add(const Duration(seconds: 3));
        while (DateTime.now().isBefore(deadline) &&
            (await second.syncState())['pending'] != 0 &&
            errors.isEmpty) {
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        expect(
          errors,
          isEmpty,
          reason: 'pausing another client must not cancel this push',
        );
        expect((await second.syncState())['pending'], 0);
        expect(requests, 1);
        expect(
          (await second.read('Entry', {'id': 'local'}))?['text'],
          'edited',
          reason:
              'the receipt alone completed the batch; the row shows the server-returned state',
        );
      } finally {
        if (!token.isCompleted) token.complete('cleanup');
        await first.close();
        await second.close();
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  moreTests();
}

void moreTests() {
  test(
    'push completes from its receipt while the WebSocket upgrade is refused; HTTP catch-up runs only once the upgrade is allowed',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-dart-blocked-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <WebSocket>[];
      final errors = <Object>[];
      final stamps = FakeStamps();
      var allowUpgrades = false,
          upgradeAttempts = 0,
          pushes = 0,
          pulls = 0,
          acks = 0,
          head = 0;
      Future<void> until(FutureOr<bool> Function() check) async {
        final deadline = DateTime.now().add(const Duration(seconds: 5));
        while (DateTime.now().isBefore(deadline)) {
          if (await check()) return;
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        throw StateError('condition timed out: $errors');
      }

      server.listen((r) async {
        if (WebSocketTransformer.isUpgradeRequest(r)) {
          upgradeAttempts++;
          if (!allowUpgrades) {
            r.response.statusCode = HttpStatus.serviceUnavailable;
            await r.response.close();
            return;
          }
          final socket = await WebSocketTransformer.upgrade(r);
          sockets.add(socket);
          socket.listen((message) {
            final sub = jsonDecode(message as String) as Map;
            acks++;
            socket.add(ack(sub, head));
          });
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(r).join()) as Map;
        r.response.headers.contentType = ContentType.json;
        if (r.uri.path == '/sync/mutations') {
          pushes++;
          r.response.write(jsonEncode(receiptFor(body, stamps)));
        } else {
          pulls++;
          final from = (body['cursors'] as Map)['scope'] as int;
          // The catch-up page carries a stamp newer than the receipt's, so it
          // is authority that updates the row.
          r.response.write(
            jsonEncode({
              'cursors': {'scope': range(from, from + 1)},
              'changes': [
                {
                  'model': 'Entry',
                  'identity': {'id': 'live'},
                  'stamp': stamps.next + 1,
                  'state': {'text': 'from catch-up', 'note': null},
                },
              ],
            }),
          );
        }
        await r.response.close();
      });
      try {
        await client.transaction(
          (tx) => tx.direct({
            'model': 'Entry',
            'op': 'create',
            'identity': {'id': 'live'},
            'values': {'text': 'local'},
          }),
        );
        await client.subscribe('scope');
        await client.mutate({
          'name': 'Edit',
          'operations': [
            {
              'model': 'Entry',
              'op': 'update',
              'identity': {'id': 'live'},
              'values': {'text': '  edited offline  '},
            },
          ],
        });
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          '  edited offline  ',
          reason: 'the local prediction is visible before the push',
        );
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        await until(() => pushes == 1 && upgradeAttempts >= 2);
        expect(
          pulls,
          0,
          reason: 'no HTTP catch-up without an acknowledged WebSocket',
        );
        await until(() async => (await client.syncState())['pending'] == 0);
        expect(
          pulls,
          0,
          reason:
              'the batch completed from its receipt alone: no page was delivered',
        );
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          'edited offline',
          reason:
              'the row shows the server-returned state as soon as the response is applied',
        );
        expect(
          errors.any((e) => e.toString().contains('503')),
          isTrue,
          reason: 'upgrade refusals reach onError: $errors',
        );
        allowUpgrades = true;
        await until(() => acks == 1);
        await until(
          () async => (await client.syncState())['cursors']['scope'] == 0,
        );
        expect(
          pulls,
          0,
          reason: 'an acknowledged session at its head pulls no history',
        );
        // Only a publication the closed socket missed leaves a gap for HTTP.
        await connection.pause();
        head = 1;
        await connection.resume();
        await until(
          () async =>
              (await client.read('Entry', {'id': 'live'}))?['text'] ==
              'from catch-up',
        );
        expect(pushes, 1, reason: 'the receipt was not re-requested');
        expect(pulls, greaterThanOrEqualTo(1));
        expect((await client.syncState())['pending'], 0);
        await connection.close();
      } finally {
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  test(
    'bounded receive buffer (128 pages) overflows into recovery without restarting the in-flight HTTP catch-up',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-dart-overflow-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <WebSocket>[];
      final errors = <Object>[];
      final entered = Completer<void>(), gate = Completer<void>();
      // The subscription's origin is the head of its first handshake, so this
      // server acknowledges zero, then publishes while the socket is closed:
      // the session after that starts the catch-up this test holds
      // ([#150](https://github.com/zanminwang/axton/issues/150)).
      var head = 1, pulls = 0, ackHead = 0;
      final pullCursors = <int>[];
      Map<String, dynamic> page(String text, int cursor, int to) => {
        'cursors': {'scope': range(cursor, to)},
        'changes': [
          {
            'model': 'Entry',
            'identity': {'id': 'live'},
            'stamp': to,
            'state': {'text': text, 'note': null},
          },
        ],
      };
      Future<void> until(FutureOr<bool> Function() check) async {
        final deadline = DateTime.now().add(const Duration(seconds: 10));
        while (DateTime.now().isBefore(deadline)) {
          if (await check()) return;
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        throw StateError('condition timed out: $errors');
      }

      server.listen((r) async {
        if (WebSocketTransformer.isUpgradeRequest(r)) {
          final socket = await WebSocketTransformer.upgrade(r);
          sockets.add(socket);
          socket.listen((message) {
            final sub = jsonDecode(message as String) as Map;
            socket.add(ack(sub, ackHead));
          });
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(r).join()) as Map;
        pulls++;
        final from = (body['cursors'] as Map)['scope'] as int;
        pullCursors.add(from);
        final response = page('head $head', from, head);
        if (pulls == 1) {
          entered.complete();
          await gate.future;
        }
        r.response.headers.contentType = ContentType.json;
        r.response.write(jsonEncode(response));
        await r.response.close();
      });
      try {
        await client.subscribe('scope');
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        await until(
          () async => (await client.syncState())['cursors']['scope'] == 0,
        );
        expect(pulls, 0, reason: 'a fresh subscription pulls no history');
        await connection.pause();
        ackHead = 1;
        await connection.resume();
        await entered.future.timeout(const Duration(seconds: 5));
        final opened = sockets.length;
        // Flush more pages than the 128-page bound, then let the listener
        // receive them while the initial HTTP response remains held.
        await sockets.last.addStream(
          Stream.fromIterable([
            for (var cursor = 1; cursor <= 200; cursor++)
              jsonEncode(page('live $cursor', cursor, cursor + 1)),
          ]),
        );
        await Future<void>.delayed(const Duration(milliseconds: 100));
        // Only HTTP can reveal this state: replaying every buffered live page
        // reaches 201, so an unbounded buffer cannot satisfy this assertion.
        head = 202;
        gate.complete();
        await until(
          () async => (await client.syncState())['cursors']['scope'] >= 201,
        );
        expect((await client.syncState())['cursors']['scope'], 202);
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          'head 202',
        );
        expect(
          pullCursors,
          contains(1),
          reason: 'recovery preserves the held HTTP page before pulling again',
        );
        expect(
          sockets.length,
          opened,
          reason: 'overflow must not restart the socket and starve catch-up',
        );
        expect(
          pulls,
          inInclusiveRange(2, 4),
          reason: 'overflow requires HTTP recovery and coalesces its work',
        );
        expect(errors, isEmpty);
        await connection.close();
      } finally {
        if (!gate.isCompleted) gate.complete();
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  test(
    'a 401 on both lanes at once shares one refreshAuth; both lanes recover with the new token',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-dart-refresh-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var token = 'expired', refreshes = 0, unauthorized = 0, pushes = 0;
      final stamps = FakeStamps();
      final gate = Completer<void>();
      final accepted = Completer<void>();
      final errors = <Object>[];
      final sockets = <WebSocket>[];
      server.listen((request) async {
        if (request.headers.value('authorization') != 'Bearer valid') {
          unauthorized++;
          request.response.statusCode = 401;
          await request.response.close();
          return;
        }
        if (request.uri.path == '/sync/mutations') {
          pushes++;
          // The receipt names no channel: the push completes on its own,
          // whatever the live lane is doing.
          final body =
              jsonDecode(await utf8.decoder.bind(request).join()) as Map;
          request.response.write(jsonEncode(receiptFor(body, stamps)));
          await request.response.close();
          return;
        }
        if (request.uri.path == '/sync/pull') {
          final pull =
              jsonDecode(await utf8.decoder.bind(request).join()) as Map;
          request.response.write(jsonEncode(emptyPage(pull)));
          await request.response.close();
          return;
        }
        final socket = await WebSocketTransformer.upgrade(request);
        sockets.add(socket);
        socket.listen((message) {
          final sub = jsonDecode(message as String) as Map;
          socket.add(ack(sub));
          if (!accepted.isCompleted) accepted.complete();
        });
      });
      Future<void> until(bool Function() predicate, String label) async {
        final deadline = DateTime.now().add(const Duration(seconds: 5));
        while (!predicate()) {
          if (DateTime.now().isAfter(deadline)) {
            throw StateError(
              '$label: refreshes=$refreshes unauthorized=$unauthorized '
              'pushes=$pushes errors=$errors',
            );
          }
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
      }

      try {
        await client.transaction((tx) async {
          await tx.direct({
            'model': 'Entry',
            'op': 'create',
            'identity': {'id': 'live'},
            'values': {'text': 'local'},
          });
        });
        await client.subscribe('scope');
        await client.mutate({
          'name': 'Edit',
          'operations': [
            {
              'model': 'Entry',
              'op': 'update',
              'identity': {'id': 'live'},
              'values': {'text': 'edited offline'},
            },
          ],
        });
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => token,
          ),
          onError: errors.add,
          refreshAuth: () async {
            refreshes++;
            await gate.future;
            token = 'valid';
          },
        );
        await until(() => unauthorized >= 2, 'both lanes refused');
        await Future<void>.delayed(const Duration(milliseconds: 100));
        expect(
          unauthorized,
          2,
          reason:
              'each lane was refused once and neither retried while the refresh was pending',
        );
        expect(
          refreshes,
          1,
          reason:
              'the second lane joined the pending refresh instead of starting another',
        );
        gate.complete();
        await until(
          () => accepted.isCompleted && pushes >= 1,
          'both lanes recovered',
        );
        var status = await client.syncState();
        final settled = DateTime.now().add(const Duration(seconds: 5));
        while (status['pending'] != 0 && DateTime.now().isBefore(settled)) {
          await Future<void>.delayed(const Duration(milliseconds: 5));
          status = await client.syncState();
        }
        expect(status['pending'], 0);
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          'edited offline',
          reason: 'the receipt completed the batch without any page',
        );
        expect(
          refreshes,
          1,
          reason: 'no further refresh once the token is valid',
        );
        expect(unauthorized, 2);
        await connection.close();
      } finally {
        if (!gate.isCompleted) gate.complete();
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  test(
    'a socket the server closes is reconnected after the backoff, resubscribed, and streaming resumes',
    () async {
      final dir = await Directory.systemTemp.createTemp(
        'axton-dart-reconnect-',
      );
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final started = DateTime.now();
      final upgrades = <int>[];
      final subscribes = <Map<String, dynamic>>[];
      final sockets = <WebSocket>[];
      final errors = <Object>[];
      server.listen((request) async {
        if (request.uri.path == '/sync/pull') {
          final pull =
              jsonDecode(await utf8.decoder.bind(request).join()) as Map;
          request.response.write(jsonEncode(emptyPage(pull)));
          await request.response.close();
          return;
        }
        upgrades.add(DateTime.now().difference(started).inMilliseconds);
        final socket = await WebSocketTransformer.upgrade(request);
        sockets.add(socket);
        socket.listen((message) {
          final sub = jsonDecode(message as String) as Map<String, dynamic>;
          subscribes.add(sub);
          socket.add(ack(sub));
        });
      });
      Future<void> until(
        FutureOr<bool> Function() predicate,
        String label,
      ) async {
        final deadline = DateTime.now().add(const Duration(seconds: 5));
        while (!await predicate()) {
          if (DateTime.now().isAfter(deadline)) {
            throw StateError(
              '$label: upgrades=$upgrades subscribes=${subscribes.length} '
              'errors=$errors',
            );
          }
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
      }

      try {
        await client.transaction((tx) async {
          await tx.direct({
            'model': 'Entry',
            'op': 'create',
            'identity': {'id': 'live'},
            'values': {'text': 'local'},
          });
        });
        await client.subscribe('scope');
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        await until(() => subscribes.length == 1, 'first subscribe');
        final closedAt = DateTime.now().difference(started).inMilliseconds;
        await sockets[0].close(1001, 'closing');
        await until(() => upgrades.length == 2, 'reconnect');
        final waited = upgrades[1] - closedAt;
        expect(
          waited,
          greaterThanOrEqualTo(180),
          reason:
              'the reconnect waited $waited ms; the first retry is due 250 ms later, minus 20% jitter',
        );
        expect(errors, isNotEmpty, reason: 'the close reaches onError');
        await until(() => subscribes.length == 2, 'second subscribe');
        expect(subscribes[1], {
          'type': 'subscribe',
          'channels': ['scope'],
          'models': {'Entry': 1},
        });
        sockets[1].add(
          jsonEncode({
            'cursors': {'scope': range(0, 1)},
            'changes': [
              {
                'model': 'Entry',
                'identity': {'id': 'live'},
                'stamp': 1,
                'state': {'text': 'after reconnect', 'note': null},
              },
            ],
          }),
        );
        await until(
          () async =>
              (await client.read('Entry', {'id': 'live'}))?['text'] ==
              'after reconnect',
          'page on the new socket applies',
        );
        expect(upgrades.length, 2, reason: 'one reconnect; no busy loop');
        await connection.close();
      } finally {
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );
  test(
    'bounded receive buffer (8 MiB) overflows on ten large pages without restarting the in-flight HTTP catch-up',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-dart-bytes-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <WebSocket>[];
      final errors = <Object>[];
      final entered = Completer<void>(), gate = Completer<void>();
      // The subscription's origin is the head of its first handshake, so this
      // server acknowledges zero, then publishes while the socket is closed:
      // the session after that starts the catch-up this test holds
      // ([#150](https://github.com/zanminwang/axton/issues/150)).
      var head = 1, pulls = 0, ackHead = 0;
      final pullCursors = <int>[];
      Map<String, dynamic> page(String text, int cursor, int to) => {
        'cursors': {'scope': range(cursor, to)},
        'changes': [
          {
            'model': 'Entry',
            'identity': {'id': 'live'},
            'stamp': to,
            'state': {'text': text, 'note': null},
          },
        ],
      };
      // Each page carries one change holding a 1 MiB value, so eight buffered
      // pages already exceed the 8 MiB byte bound: the page count never comes
      // close to 128 and cannot be what trips the buffer.
      const largePages = 10;
      final filler = 'x' * (1024 * 1024);
      expect(largePages, lessThan(128));
      expect(
        jsonEncode(page('live 1 $filler', 1, 2)).length * 8,
        greaterThan(8 * 1024 * 1024),
      );
      Future<void> until(FutureOr<bool> Function() check) async {
        final deadline = DateTime.now().add(const Duration(seconds: 10));
        while (DateTime.now().isBefore(deadline)) {
          if (await check()) return;
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        throw StateError('condition timed out: $errors');
      }

      server.listen((r) async {
        if (WebSocketTransformer.isUpgradeRequest(r)) {
          final socket = await WebSocketTransformer.upgrade(r);
          sockets.add(socket);
          socket.listen((message) {
            final sub = jsonDecode(message as String) as Map;
            socket.add(ack(sub, ackHead));
          });
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(r).join()) as Map;
        pulls++;
        final from = (body['cursors'] as Map)['scope'] as int;
        pullCursors.add(from);
        final response = page('head $head', from, head);
        if (pulls == 1) {
          entered.complete();
          await gate.future;
        }
        r.response.headers.contentType = ContentType.json;
        r.response.write(jsonEncode(response));
        await r.response.close();
      });
      try {
        await client.subscribe('scope');
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        await until(
          () async => (await client.syncState())['cursors']['scope'] == 0,
        );
        expect(pulls, 0, reason: 'a fresh subscription pulls no history');
        await connection.pause();
        ackHead = 1;
        await connection.resume();
        await entered.future.timeout(const Duration(seconds: 5));
        final opened = sockets.length;
        // Flush fewer pages than the 128-page bound but more bytes than the
        // 8 MiB bound while the initial HTTP response remains held.
        await sockets.last.addStream(
          Stream.fromIterable([
            for (var cursor = 1; cursor <= largePages; cursor++)
              jsonEncode(page('live $cursor $filler', cursor, cursor + 1)),
          ]),
        );
        await Future<void>.delayed(const Duration(milliseconds: 100));
        // Only HTTP can reveal this state: replaying every buffered live page
        // reaches 11, so a buffer bounded by pages alone cannot satisfy this.
        head = 12;
        gate.complete();
        await until(
          () async => (await client.syncState())['cursors']['scope'] >= 11,
        );
        expect((await client.syncState())['cursors']['scope'], 12);
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          'head 12',
        );
        expect(
          pullCursors,
          contains(1),
          reason: 'recovery preserves the held HTTP page before pulling again',
        );
        expect(
          sockets.length,
          opened,
          reason: 'overflow must not restart the socket and starve catch-up',
        );
        expect(
          pulls,
          inInclusiveRange(2, 4),
          reason: 'overflow requires HTTP recovery and coalesces its work',
        );
        expect(errors, isEmpty);
        await connection.close();
      } finally {
        if (!gate.isCompleted) gate.complete();
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  test(
    'an owner-mismatch refusal reaches onError and leaves the batch frozen for a resend',
    () async {
      final dir = await Directory.systemTemp.createTemp(
        'axton-owner-mismatch-',
      );
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/client.sqlite',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final bodies = <Map>[];
      final errors = <Object>[];
      server.listen((request) async {
        final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
        bodies.add(body);
        request.response.statusCode = 403;
        request.response.headers.contentType = ContentType.json;
        request.response.write(jsonEncode({'code': 'client.owner_mismatch'}));
        await request.response.close();
      });
      try {
        await client.mutate({
          'name': 'Create',
          'operations': [
            {
              'model': 'Entry',
              'op': 'create',
              'identity': {'id': 'live'},
              'values': {'text': 'local', 'note': null},
            },
          ],
        });
        expect((await client.syncState())['pending'], 1);
        await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        final deadline = DateTime.now().add(const Duration(seconds: 3));
        while (DateTime.now().isBefore(deadline) && bodies.length < 2) {
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        expect(
          bodies.length,
          greaterThanOrEqualTo(2),
          reason: 'the frozen batch is resent after the refusal',
        );
        expect(
          errors.any((e) => e.toString().contains('client.owner_mismatch')),
          isTrue,
          reason: "the refusal's code reaches onError: $errors",
        );
        expect(
          (await client.syncState())['pending'],
          1,
          reason: 'the refused batch stays pending, not dropped or completed',
        );
        expect(
          bodies[1],
          equals(bodies[0]),
          reason: 'the same request body is resent on the next cycle',
        );
      } finally {
        await client.close();
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );

  test(
    'what a page cannot apply reaches onError as an AxtonReport: read failures, skipped changes and divergence',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-dart-reports-');
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      final client = await Client.open(
        path: '${dir.path}/db',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final sockets = <WebSocket>[];
      final errors = <Object>[];
      var allowPush = false;
      var breakReceipt = false;
      var acknowledged = 0;
      final stamps = FakeStamps()..next = 10;
      Future<void> until(FutureOr<bool> Function() check) async {
        final deadline = DateTime.now().add(const Duration(seconds: 5));
        while (DateTime.now().isBefore(deadline)) {
          if (await check()) return;
          await Future<void>.delayed(const Duration(milliseconds: 5));
        }
        throw StateError('condition timed out: $errors');
      }

      server.listen((r) async {
        if (WebSocketTransformer.isUpgradeRequest(r)) {
          final socket = await WebSocketTransformer.upgrade(r);
          sockets.add(socket);
          socket.listen((message) {
            socket.add(ack(jsonDecode(message as String) as Map));
            acknowledged++;
          });
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(r).join()) as Map;
        r.response.headers.contentType = ContentType.json;
        if (r.uri.path == '/sync/mutations') {
          if (!allowPush) {
            r.response.statusCode = HttpStatus.serviceUnavailable;
            await r.response.close();
            return;
          }
          final receipt = receiptFor(body, stamps);
          if (breakReceipt) {
            ((receipt['records'] as List).first as Map)['state'] = {
              'text': 5,
              'note': null,
            };
          }
          r.response.write(jsonEncode(receipt));
        } else {
          r.response.write(jsonEncode(emptyPage(body)));
        }
        await r.response.close();
      });
      Map<String, dynamic> record(
        String id,
        int stamp,
        Map<String, dynamic>? state,
      ) => {
        'model': 'Entry',
        'identity': {'id': id},
        'stamp': stamp,
        'state': state,
      };
      try {
        await client.subscribe('scope');
        final connection = await client.connect(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'secret',
          ),
          onError: errors.add,
        );
        // A page before the acknowledgement is a protocol violation: the
        // server streams only once it acknowledged the handshake.
        await until(() => acknowledged == 1);
        sockets.first.add(
          jsonEncode({
            'cursors': {'scope': range(0, 1)},
            'changes': [
              record('live', 1, {'text': 'first', 'note': null}),
            ],
          }),
        );
        await until(
          () async =>
              (await client.read('Entry', {'id': 'live'}))?['text'] == 'first',
        );
        // A record the server could not read, and one whose state does not
        // fit the schema: each is reported, the page still lands.
        sockets.first.add(
          jsonEncode({
            'cursors': {'scope': range(1, 3)},
            'changes': [
              {
                'model': 'Entry',
                'identity': {'id': 'live'},
                'stamp': 9,
                'error': 'loader.failed',
              },
              record('bad', 2, {'text': 5, 'note': null}),
            ],
          }),
        );
        await until(() => errors.length == 2);
        final reports = errors.cast<AxtonReport>();
        expect(reports[0].kind, 'readFailed');
        expect(reports[0].code, 'loader.failed');
        expect(reports[0].identity, {'id': 'live'});
        expect(reports[0].stamp, 9);
        expect(reports[0].toString(), contains('loader.failed'));
        expect(reports[1].kind, 'skipped');
        expect(reports[1].identity, {'id': 'bad'});
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          'first',
          reason: 'a read failure keeps the local content',
        );
        expect((await client.syncState())['cursors']['scope'], 3);
        // A queued edit whose replay fails over new authority: the server's
        // row is visible, the edit is reported diverged and still sent.
        errors.clear();
        await client.mutate({
          'name': 'Edit',
          'operations': [
            {
              'model': 'Entry',
              'op': 'update',
              'identity': {'id': 'live'},
              'values': {'text': 'edited offline'},
            },
          ],
        });
        sockets.first.add(
          jsonEncode({
            'cursors': {'scope': range(3, 4)},
            'changes': [record('live', 2, null)],
          }),
        );
        await until(() => errors.whereType<AxtonReport>().isNotEmpty);
        final diverged = errors.whereType<AxtonReport>().first;
        expect(diverged.kind, 'diverged');
        expect(diverged.ordinal, isA<int>());
        expect(diverged.identity, {'id': 'live'});
        expect(
          await client.read('Entry', {'id': 'live'}),
          isNull,
          reason: "the server's row (a deletion) is visible",
        );
        expect((await client.syncState())['pending'], 1);
        allowPush = true;
        await until(() async => (await client.syncState())['pending'] == 0);
        expect(
          (await client.read('Entry', {'id': 'live'}))?['text'],
          'edited offline',
          reason: 'the diverged edit was sent and completed from its receipt',
        );
        // A receipt record that does not fit is reported with its batch, and
        // the batch still completes.
        errors.clear();
        breakReceipt = true;
        await client.mutate({
          'name': 'Create',
          'operations': [
            {
              'model': 'Entry',
              'op': 'create',
              'identity': {'id': 'odd'},
              'values': {'text': 'local', 'note': null},
            },
          ],
        });
        await until(() async => (await client.syncState())['pending'] == 0);
        final skipped = errors.whereType<AxtonReport>().single;
        expect(skipped.kind, 'skipped');
        expect(skipped.identity, {'id': 'odd'});
        expect((skipped.detail as Map)['batch'], isA<int>());
        await connection.close();
      } finally {
        await client.close();
        for (final socket in sockets) {
          await socket.close();
        }
        await server.close(force: true);
        await dir.delete(recursive: true);
      }
    },
  );
}
