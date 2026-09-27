/// The connection as an effect executor
/// ([#134](https://github.com/zanminwang/axton/issues/134);
/// [Controller](../../../../docs/engineering/architecture/client/connection/controller/README.md)).
///
/// Rust owns the push lane, the Downlink lane, direct calls, credential
/// refresh coordination, timeouts and every retry. This file only executes the
/// effects the runtime asks for - HTTP, the live socket, timers and the
/// application's `refreshAuth` - with the platform network adapter, and aborts
/// each one when the runtime cancels it. It decides nothing.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'bridge.dart';
import 'live.dart';

/// A direct call the runtime could not complete: `action.unavailable`,
/// `action.execution_unknown` or `action.observation_failed`. Its execution is
/// unknown.
class ActionTransportException implements Exception {
  final String code;
  final String execution = 'unknown';
  final Object? cause;
  ActionTransportException(this.code, [this.cause]);
  @override
  String toString() => code;
}

/// The server answered 401.
class AuthenticationExpired implements Exception {
  const AuthenticationExpired();
}

/// The HTTP status a transport error carried: what tells the runtime a
/// refusal the server decided from a transport failure, and a 401 from both.
int? statusOf(Object error) => switch (error) {
  HttpFailure failure => failure.statusCode,
  AuthenticationExpired _ => 401,
  _ => null,
};

/// The text an effect failure carries.
String _message(Object error) =>
    error is HttpException ? error.message : error.toString();

/// Hand one `report` diagnostic to [onError]: one [AxtonReport] per record a
/// delivery could not apply, a [StateError] with the message for a lane
/// failure or a protocol violation. A throwing [onError] is reported to the
/// current zone and never stops the others.
void deliverDiagnostic(
  Map<String, dynamic> diagnostic,
  void Function(Object error) onError,
) {
  void deliver(Object error) {
    try {
      onError(error);
    } catch (thrown, stack) {
      Zone.current.handleUncaughtError(thrown, stack);
    }
  }

  switch (diagnostic['kind']) {
    case 'records':
      for (final report in diagnostic['reports'] as List<dynamic>) {
        deliver(AxtonReport.fromJson(report as Map<String, dynamic>));
      }
    case 'error' || 'protocol':
      deliver(StateError(diagnostic['message'] as String));
  }
}

/// The `prerequisite` effect handler for [handlers]: run the application's
/// handler with the task's arguments, in the zone that asked, and answer ok
/// or a failure whose message is the reason the task keeps.
EffectHandler prerequisiteHandler(
  Map<String, Future<void> Function(Map<String, dynamic>)> handlers,
) {
  final zone = Zone.current;
  return (effect) => zone.run(() {
    final handler = handlers[effect.operation['name']];
    if (handler == null) {
      effect.fail('missing prerequisite handler');
      return;
    }
    Future<void>.sync(
      () => handler(effect.operation['arguments'] as Map<String, dynamic>),
    ).then(
      (_) => effect.succeed(),
      onError: (Object thrown) => effect.fail(thrown.toString()),
    );
  });
}

/// The client is closing: stop [connection] without a task. The runtime's
/// `close` cancels every effect and ends the lanes, and a `connection stop`
/// would wait behind a callback that holds the transaction.
void abandonConnection(RuntimeConnection connection) => connection._abandon();

/// One runtime-owned connection: its controls submit `connection` tasks and
/// its effect handlers run the platform I/O. Application callbacks - the
/// token, `refreshAuth`, `onError` - run in the zone that connected.
class RuntimeConnection {
  final RuntimeHost _host;
  final ServerSession _network;
  final Future<void> Function()? _refreshAuth;
  final void Function(Object)? _onError;
  final Zone _zone;
  final void Function(RuntimeConnection connection)? _onClosed;
  late final Map<String, EffectHandler> _handlers = {
    'http': _http,
    'socket': _socket,
    'timer': _timer,
    if (_refreshAuth != null) 'refreshAuth': _refresh,
  };
  bool _stopped = false;
  Future<void>? _closing;

  RuntimeConnection._(
    this._host,
    this._network,
    this._refreshAuth,
    this._onError,
    this._onClosed,
  ) : _zone = Zone.current;

  /// Submit `connect`; the effect handlers are installed while its completion
  /// is dispatched, before the first lane effect, and [onConnected] runs
  /// there too. A refused connect - the runtime refuses a second active
  /// connection - installs nothing and throws the runtime's reason.
  static Future<RuntimeConnection> connect({
    required RuntimeHost host,
    required ServerSession network,
    void Function(Object)? onError,
    Future<void> Function()? refreshAuth,
    Duration directTimeout = const Duration(seconds: 30),
    void Function(RuntimeConnection connection)? onConnected,
    void Function(RuntimeConnection connection)? onClosed,
  }) async {
    // Typed encoding: a Duration the wire's positive milliseconds cannot say.
    if (directTimeout.inMicroseconds <= 0) {
      throw ArgumentError.value(directTimeout, 'directTimeout');
    }
    final connection = RuntimeConnection._(
      host,
      network,
      refreshAuth,
      onError,
      onClosed,
    );
    await host.task(
      {
        'kind': 'connect',
        'directTimeoutMs': _millis(directTimeout),
        'refreshAuth': refreshAuth != null,
      },
      onValue: (_) {
        connection._install();
        onConnected?.call(connection);
      },
    );
    return connection;
  }

  /// Whole milliseconds, rounded up, within the runtime's range.
  static int _millis(Duration timeout) =>
      ((timeout.inMicroseconds + 999) ~/ 1000).clamp(1, 2147483647);

  /// Hand one `report` diagnostic to `onError`, in the zone that connected.
  /// A closed handle hears nothing: a later connection's reports are not its.
  void report(Map<String, dynamic> diagnostic) {
    final onError = _onError;
    if (onError == null || _stopped) return;
    _zone.run(() => deliverDiagnostic(diagnostic, onError));
  }

  Future<void> pause() => _control('pause');
  Future<void> resume() => _control('resume');
  Future<void> wake() => _control('wake');

  /// Stop the lanes; the runtime cancels every effect they hold, and whatever
  /// is still held here is aborted. Idempotent.
  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    if (_stopped) return;
    _stopped = true;
    try {
      await _host.task({'kind': 'connection', 'event': 'stop'});
    } on StateError catch (error) {
      // A closed runtime already stopped everything.
      if (error.message != 'client_closed') rethrow;
    } finally {
      _uninstall();
      _onClosed?.call(this);
    }
  }

  /// Stop here only: controls end, the handlers go and every effect they hold
  /// is aborted. A `close` in flight still settles from its task.
  void _abandon() {
    _stopped = true;
    _closing ??= Future<void>.value();
    _uninstall();
    _onClosed?.call(this);
  }

  /// Handle identity: the runtime has no connection id, so a closed handle's
  /// controls stop here instead of altering a later connection's lanes.
  Future<void> _control(String event) async {
    if (_stopped) return;
    await _host.task({'kind': 'connection', 'event': event});
  }

  void _install() {
    for (final MapEntry(key: kind, value: handler) in _handlers.entries) {
      _host.handleEffects(kind, handler);
    }
  }

  void _uninstall() {
    for (final MapEntry(key: kind, value: handler) in _handlers.entries) {
      _host.stopHandling(kind, handler);
    }
  }

  /// Run [body] in the zone that connected, so the application's token and
  /// callbacks see it.
  void _run(void Function() body) => _zone.run(body);

  /// `http {route, body}`: POST to the route's endpoint. The answer is the
  /// response text; a failure carries the HTTP status when there was one.
  void _http(Effect effect) => _run(() {
    final body = effect.operation['body'] as String;
    final Future<String> sent = switch (effect.operation['route']) {
      'push' => _network.push(body, effect.cancelled),
      'pull' => _network.pull(body, effect.cancelled),
      'action' => _network.action(body, effect.cancelled),
      final route => Future.error(StateError('unknown route $route')),
    };
    sent.then(
      effect.succeed,
      onError: (Object error) =>
          effect.fail(_message(error), status: statusOf(error)),
    );
  });

  /// `socket {subscribe}`: open the live socket and stream its frames under
  /// this effect until it ends or is cancelled. A cancelled socket is not
  /// reported.
  void _socket(Effect effect) => _run(
    () => _network.open(
      effect.operation['subscribe'] as String,
      effect.cancelled,
      SocketEvents(
        message: (text) async =>
            effect.emit({'event': 'message', 'body': text}),
        overflow: () async => effect.emit({'event': 'overflow'}),
        closed: (error, _) =>
            effect.fail(_message(error), status: statusOf(error)),
      ),
    ),
  );

  /// `timer {millis}`: answer when it fires; a cancellation clears it.
  void _timer(Effect effect) {
    final timer = Timer(
      Duration(milliseconds: effect.operation['millis'] as int),
      effect.succeed,
    );
    unawaited(effect.cancelled.then((_) => timer.cancel()));
  }

  /// `refreshAuth`: run the application's refresh once.
  void _refresh(Effect effect) => _run(() {
    Future<void>.sync(_refreshAuth!).then(
      (_) => effect.succeed(),
      onError: (Object error) => effect.fail(_message(error)),
    );
  });
}

/// A delivery the client could not apply, handed to `onError`. The client
/// stays consistent: a `readFailed` or `skipped` record keeps its local
/// content and stamp, a `conflict` keeps the local content, a `diverged`
/// mutation shows the server's row and is still sent.
class AxtonReport implements Exception {
  AxtonReport({
    required this.kind,
    required this.model,
    required this.identity,
    required this.stamp,
    this.code,
    this.ordinal,
    this.detail,
  });
  factory AxtonReport.fromJson(Map<String, dynamic> json) => AxtonReport(
    kind: json['kind'] as String,
    model: json['model'] as String,
    identity: Map<String, dynamic>.from(json['identity'] as Map),
    stamp: json['stamp'] as int,
    code: json['code'] as String?,
    ordinal: json['ordinal'] as int?,
    detail: json['detail'],
  );

  /// `readFailed`, `skipped`, `conflict` or `diverged`.
  final String kind;
  final String model;
  final Map<String, dynamic> identity;
  final int stamp;

  /// `readFailed`: the server's code (`loader.failed`, or the refusal code).
  final String? code;

  /// `diverged`: the queued mutation whose replay failed; it is still sent.
  final int? ordinal;
  final Object? detail;

  @override
  String toString() =>
      'AxtonReport($kind: $model ${jsonEncode(identity)} at stamp $stamp'
      '${code == null ? '' : ' ($code)'}'
      '${ordinal == null ? '' : ' (mutation $ordinal)'})';
}
