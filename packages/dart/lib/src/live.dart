import 'dart:async';
import 'dart:convert';
import 'dart:io';
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

/// Immutable configuration reusable across independent client connections.
class SyncServer {
  final String url;
  final FutureOr<String> Function() token;
  const SyncServer({required this.url, required this.token});
}

/// Internal per-client network session: the platform side of the runtime's
/// `http` and `socket` effects. Every request and socket owns its
/// cancellation, so aborting one never touches another.
class ServerSession {
  final Uri _base;
  final FutureOr<String> Function() _token;
  ServerSession(SyncServer server)
    : _base = Uri.parse(server.url),
      _token = server.token;

  /// `POST /sync/mutations`: one frozen push batch.
  Future<String> push(String body, Future<void> cancellation) =>
      _post('mutations', 'push', body, cancellation);

  /// `POST /sync/actions`: one direct attempt. Its cancellation closes the
  /// socket even while the response is stalled.
  Future<String> action(String body, Future<void> cancellation) =>
      _post('actions', 'action', body, cancellation);

  /// `POST /sync/pull`: an ordinary catch-up or a Bootstrap page.
  Future<String> pull(String body, Future<void> cancellation) =>
      _post('pull', 'pull', body, cancellation);

  /// One request on its own HTTP client. A 401 is [AuthenticationExpired],
  /// any other non-2xx answer an [HttpFailure] with its status; once
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
        request.headers.set(HttpHeaders.authorizationHeader, 'Bearer $token');
        request.headers.contentType = ContentType.json;
        request.write(body);
        final response = await request.close();
        final result = await utf8.decoder.bind(response).join();
        if (response.statusCode == 401) throw const AuthenticationExpired();
        if (response.statusCode < 200 || response.statusCode >= 300) {
          throw HttpFailure(what, response.statusCode, result);
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
        WebSocket opened;
        try {
          opened = await WebSocket.connect(
            _endpoint('live', true).toString(),
            headers: {HttpHeaders.authorizationHeader: 'Bearer $token'},
            customClient: http,
          );
        } on WebSocketException catch (error) {
          if (error.httpStatusCode == 401) throw const AuthenticationExpired();
          rethrow;
        }
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
