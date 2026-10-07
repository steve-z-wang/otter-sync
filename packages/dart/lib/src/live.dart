import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';
import 'connection.dart';

/// A request the server refused, with the status it refused it with. It is an
/// `HttpException` like the failure it replaces, so existing handling is
/// unchanged; the status travels to the runtime, which tells a refusal the
/// server decided from a transport failure by it. A direct call whose
/// transport failed with a status carries one as its `cause`.
class HttpFailure extends HttpException {
  final int statusCode;
  HttpFailure(String what, this.statusCode, String body)
    : super('$what failed: $statusCode $body');

  /// The failure the runtime reported: its [message] and [statusCode].
  HttpFailure.reported(super.message, this.statusCode);
}

/// A response the server marked as an admission refusal
/// (`axton-admission: refused`), with its body text. The runtime reports it
/// to the application as an [AdmissionRefused].
class RefusedResponse extends HttpFailure {
  final String body;
  RefusedResponse(String what, int statusCode, this.body)
    : super(what, statusCode, body);
}

/// The body of an admission refusal a transport error carries, if it is one.
String? refusalOf(Object error) => error is RefusedResponse ? error.body : null;

/// Immutable configuration reusable across independent client connections.
class SyncServer {
  final String url;
  final FutureOr<String> Function() token;

  /// Sent with every request and the live upgrade, e.g. the application's
  /// platform and build for the backend's `admit`. The headers AXTON sets
  /// itself (`authorization`, `content-type`, the WebSocket handshake) are
  /// refused when the client connects.
  final Map<String, String> headers;
  const SyncServer({
    required this.url,
    required this.token,
    this.headers = const {},
  });
}

class StoreConnection extends SyncServer {
  final String projectionGeneration;
  final void Function(Object)? onError;
  final Future<void> Function()? refreshAuth;
  final Duration directTimeout;
  const StoreConnection({
    required super.url,
    required super.token,
    super.headers,
    this.projectionGeneration = '1',
    this.onError,
    this.refreshAuth,
    this.directTimeout = const Duration(seconds: 30),
  });
}

/// The response header that marks an admission refusal (`refused`).
const _admissionHeader = 'axton-admission';

/// Headers AXTON sets itself: the credential, the body and the handshake.
final _reserved = RegExp(
  r'^(authorization|content-type|content-length|host|connection|upgrade|sec-websocket-.*)$',
  caseSensitive: false,
);

/// The key a server answers a WebSocket handshake with (RFC 6455 §4.2.2).
const _handshakeGuid = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

/// Internal per-client network session: the platform side of the runtime's
/// `http` and `socket` effects. Every request and socket owns its
/// cancellation, so aborting one never touches another.
class ServerSession {
  final Uri _base;
  final FutureOr<String> Function() _token;
  final Map<String, String> _headers;
  ServerSession(SyncServer server)
    : _base = Uri.parse(server.url),
      _token = server.token,
      _headers = Map.unmodifiable(server.headers) {
    for (final name in _headers.keys) {
      if (_reserved.hasMatch(name)) {
        throw ArgumentError.value(name, 'headers', 'reserved header');
      }
    }
  }

  /// `POST /sync/handshake`: the Store's current Stream head.
  Future<String> handshake(String body, Future<void> cancellation) =>
      _post('handshake', 'handshake', body, cancellation);

  /// `POST /sync/materialize`: an owned settlement or schema plan.
  Future<String> materialize(String body, Future<void> cancellation) =>
      _post('materialize', 'materialize', body, cancellation);

  /// `POST /sync/mutations`: one frozen push batch.
  Future<String> push(String body, Future<void> cancellation) =>
      _post('mutations', 'push', body, cancellation);

  /// `POST /sync/actions`: one direct attempt. Its cancellation closes the
  /// socket even while the response is stalled.
  Future<String> action(String body, Future<void> cancellation) =>
      _post('actions', 'action', body, cancellation);

  /// `POST /sync/fetch`: one Model Fetch. Its cancellation closes the socket
  /// even while the response is stalled.
  Future<String> fetch(String body, Future<void> cancellation) =>
      _post('fetch', 'fetch', body, cancellation);

  /// `POST /sync/loads`: one batch of native Load pages.
  Future<String> load(String body, Future<void> cancellation) =>
      _post('loads', 'load', body, cancellation);

  /// `POST /sync/pull`: an ordinary catch-up or a Bootstrap page.
  Future<String> pull(String body, Future<void> cancellation) =>
      _post('pull', 'pull', body, cancellation);

  /// One request on its own HTTP client. An answer marked as an admission
  /// refusal is a [RefusedResponse], a 401 [AuthenticationExpired], any other
  /// non-2xx answer an [HttpFailure] with its status; once
  /// [cancellation] completes, the token wait, the request and a stalled
  /// response are abandoned and it fails with [_cancelled]. The runtime
  /// fences a cancelled effect, so that failure only releases the caller.
  Future<String> _post(
    String path,
    String what,
    String body,
    Future<void> cancellation,
  ) async {
    var aborted = false;
    HttpClient? http;
    final stopped = Completer<String>();
    unawaited(
      cancellation.then((_) {
        aborted = true;
        http?.close(force: true);
        if (!stopped.isCompleted) stopped.completeError(_cancelled);
      }),
    );
    final sending = Future<String>(() async {
      final token = await _token();
      if (aborted) throw _cancelled;
      final client = HttpClient();
      http = client;
      try {
        final request = await client.postUrl(_endpoint(path, false));
        if (aborted) throw _cancelled;
        _headers.forEach(request.headers.set);
        request.headers.set(HttpHeaders.authorizationHeader, 'Bearer $token');
        request.headers.contentType = ContentType.json;
        request.write(body);
        final response = await request.close();
        final result = await utf8.decoder.bind(response).join();
        if (response.statusCode < 200 || response.statusCode >= 300) {
          throw _refusal(what, response, result);
        }
        return result;
      } finally {
        client.close(force: true);
      }
    });
    try {
      return await Future.any([sending, stopped.future]);
    } finally {
      http = null;
      // Settle the losing future so a completed answer is not retained until
      // the connection eventually ends. The cancellation callback has no IO.
      if (!stopped.isCompleted) stopped.complete('');
    }
  }

  /// Why a non-2xx [response] failed: an admission refusal, a 401, or the
  /// status alone.
  static Exception _refusal(
    String what,
    HttpClientResponse response,
    String body,
  ) {
    if (response.headers.value(_admissionHeader) == 'refused') {
      return RefusedResponse(what, response.statusCode, body);
    }
    if (response.statusCode == 401) return const AuthenticationExpired();
    return HttpFailure(what, response.statusCode, body);
  }

  /// Open the WebSocket on [http] by hand, so a refused upgrade's status,
  /// headers and body are read like any other answer's: a 101 whose accept
  /// key matches becomes the socket; anything else fails as [_refusal] says.
  Future<WebSocket> _upgrade(HttpClient http, String token) async {
    final random = Random.secure();
    final key = base64.encode([
      for (var i = 0; i < 16; i++) random.nextInt(256),
    ]);
    final request = await http.openUrl('GET', _endpoint('live', false));
    request.followRedirects = false;
    _headers.forEach(request.headers.set);
    request.headers
      ..set(HttpHeaders.authorizationHeader, 'Bearer $token')
      ..set(HttpHeaders.connectionHeader, 'Upgrade')
      ..set(HttpHeaders.upgradeHeader, 'websocket')
      ..set('sec-websocket-key', key)
      ..set('sec-websocket-version', '13');
    final response = await request.close();
    if (response.statusCode != HttpStatus.switchingProtocols) {
      final body = BytesBuilder(copy: false);
      await for (final chunk in response) {
        body.add(chunk);
        if (body.length >= refusalBytes) break;
      }
      throw _refusal(
        'live',
        response,
        utf8.decode(body.takeBytes(), allowMalformed: true),
      );
    }
    final accept = base64.encode(_sha1(ascii.encode('$key$_handshakeGuid')));
    if (response.headers.value('sec-websocket-accept') != accept ||
        response.headers.value(HttpHeaders.upgradeHeader)?.toLowerCase() !=
            'websocket') {
      throw const WebSocketException('invalid WebSocket handshake');
    }
    return WebSocket.fromUpgradedSocket(
      await response.detachSocket(),
      serverSide: false,
      compression: CompressionOptions.compressionOff,
    );
  }

  Uri _endpoint(String path, bool websocket) => _base.replace(
    scheme: websocket
        ? (_base.scheme == 'https' || _base.scheme == 'wss' ? 'wss' : 'ws')
        : (_base.scheme == 'https' || _base.scheme == 'wss' ? 'https' : 'http'),
    path: '${_base.path.replaceFirst(RegExp(r'/$'), '')}/sync/$path',
  );

  /// Open `/sync/live`, send [subscribe] once open and deliver every frame to
  /// [on] in order until [cancellation] completes or the socket ends. Frames
  /// that arrive while one is being delivered wait in a bounded buffer; past
  /// the bound the buffer is dropped and [SocketEvents.overflow] is reported.
  void open(String subscribe, Future<void> cancellation, SocketEvents on) {
    WebSocket? socket;
    StreamSubscription<dynamic>? subscription;
    final http = HttpClient();
    bool ended = false;
    void finish([Object? error, StackTrace? stack]) {
      if (ended) return;
      ended = true;
      http.close(force: true);
      unawaited(subscription?.cancel());
      unawaited(socket?.close());
      if (error != null) on.closed(error, stack);
    }

    unawaited(
      cancellation.then(
        (_) => finish(),
        onError: (Object error) => finish(error),
      ),
    );
    unawaited(
      Future<void>(() async {
        final token = await _token();
        if (ended) return;
        final opened = await _upgrade(http, token);
        socket = opened;
        if (ended) {
          unawaited(opened.close());
          return;
        }
        opened.add(subscribe);
        final pending = <String>[];
        int pendingBytes = 0;
        bool draining = false;
        bool overflowed = false;
        Future<void> drain() async {
          if (draining || ended) return;
          draining = true;
          try {
            while (!ended && (overflowed || pending.isNotEmpty)) {
              if (overflowed) {
                overflowed = false;
                await on.overflow();
              } else {
                final frame = pending.removeAt(0);
                pendingBytes -= frame.length;
                await on.message(frame);
              }
            }
          } finally {
            draining = false;
          }
        }

        subscription = opened.listen(
          (dynamic raw) {
            if (ended) return;
            try {
              final text = raw is String ? raw : utf8.decode(raw as List<int>);
              if (text.length > maxFrameLength) {
                throw const FormatException('live frame too large');
              }
              if (pending.length >= bufferedFrames ||
                  pendingBytes + text.length > bufferedBytes) {
                // Keep the socket and the in-flight HTTP request: the session
                // recovers from the durable cursor instead of starting over.
                pending.clear();
                pendingBytes = 0;
                overflowed = true;
              }
              pending.add(text);
              pendingBytes += text.length;
              unawaited(
                drain().catchError((Object e, StackTrace s) => finish(e, s)),
              );
            } catch (e, s) {
              finish(e, s);
            }
          },
          onError: (Object error, StackTrace stack) => finish(error, stack),
          onDone: () => finish(
            StateError(
              'live disconnected: ${opened.closeCode} ${opened.closeReason}',
            ),
          ),
        );
      }).catchError((Object error, StackTrace stack) => finish(error, stack)),
    );
  }

  /// What an HTTP request its effect's cancellation abandoned fails with.
  static StateError get _cancelled => StateError('cancelled');

  /// Host resource bounds; not protocol rules.
  static const int refusalBytes = 64 * 1024;
  static const int maxFrameLength = 8 * 1024 * 1024;
  static const int bufferedFrames = 128;
  static const int bufferedBytes = 8 * 1024 * 1024;
}

/// How the `socket` effect hears from one socket.
class SocketEvents {
  final Future<void> Function(String text) message;
  final Future<void> Function() overflow;

  /// The socket ended on its own; not called for a cancelled socket.
  final void Function(Object error, StackTrace? stack) closed;
  const SocketEvents({
    required this.message,
    required this.overflow,
    required this.closed,
  });
}

/// SHA-1 (FIPS 180-4) of [message], for the WebSocket accept key only; the
/// handshake needs no other digest, and `dart:io` exposes none.
Uint8List _sha1(List<int> message) {
  final length = message.length;
  final padded = Uint8List(((length + 8) ~/ 64 + 1) * 64)
    ..setAll(0, message)
    ..[length] = 0x80;
  final bits = ByteData.sublistView(padded);
  bits.setUint32(padded.length - 8, (length * 8) ~/ 0x100000000);
  bits.setUint32(padded.length - 4, (length * 8) & 0xffffffff);
  final h = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
  final w = Uint32List(80);
  int rotate(int x, int n) => ((x << n) | (x >> (32 - n))) & 0xffffffff;
  for (var chunk = 0; chunk < padded.length; chunk += 64) {
    for (var i = 0; i < 16; i++) {
      w[i] = bits.getUint32(chunk + i * 4);
    }
    for (var i = 16; i < 80; i++) {
      w[i] = rotate(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }
    var [a, b, c, d, e] = h;
    for (var i = 0; i < 80; i++) {
      final (f, k) = i < 20
          ? ((b & c) | (~b & d), 0x5A827999)
          : i < 40
          ? (b ^ c ^ d, 0x6ED9EBA1)
          : i < 60
          ? ((b & c) | (b & d) | (c & d), 0x8F1BBCDC)
          : (b ^ c ^ d, 0xCA62C1D6);
      final t = (rotate(a, 5) + (f & 0xffffffff) + e + k + w[i]) & 0xffffffff;
      e = d;
      d = c;
      c = rotate(b, 30);
      b = a;
      a = t;
    }
    for (final (i, v) in [a, b, c, d, e].indexed) {
      h[i] = (h[i] + v) & 0xffffffff;
    }
  }
  final digest = ByteData(20);
  for (final (i, v) in h.indexed) {
    digest.setUint32(i * 4, v);
  }
  return digest.buffer.asUint8List();
}
