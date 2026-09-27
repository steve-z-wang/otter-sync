/// The Dart SDK Bridge over the Rust-owned client runtime
/// ([#134](https://github.com/zanminwang/axton/issues/134)).
///
/// A [Bridge] submits complete tasks through the C ABI
/// (`axton_runtime_open/submit/drain/detach`), answers the effects the
/// runtime asks for, delivers each `taskCompleted` to the waiter that
/// submitted it and each `observerChanged` snapshot to the observer claimed
/// at its task's completion. It holds maps and platform resources only: task
/// progression, observer projection, database scheduling and retry stay in
/// Rust, whose actor thread owns SQLite, so admission and drain never block on
/// the database and everything runs on the calling isolate.
///
/// The actor wakes the bridge through one process-wide
/// `NativeCallable.listener`: Rust calls it from its own thread under the
/// actor's sink lock, the trampoline only posts to this isolate, and the drain
/// runs on this isolate's event loop. The VM deletes that callable when the
/// isolate shuts down, so every bridge of the C ABI carries a native finalizer
/// that detaches its runtime then: no wake reaches a deleted callable, and the
/// runtime closes and releases its database. See the
/// [bridge contract](../../../../crates/client/src/runtime/protocol.rs).
library;

import 'dart:async';
import 'dart:convert';
import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';

typedef _WakeNative = Void Function(Uint64 runtime, Pointer<Void> context);
typedef _OpenNative =
    Uint64 Function(
      Pointer<Utf8> request,
      Pointer<NativeFunction<_WakeNative>> wake,
      Pointer<Void> context,
      Pointer<Pointer<Utf8>> errorOut,
    );
typedef _Open =
    int Function(
      Pointer<Utf8> request,
      Pointer<NativeFunction<_WakeNative>> wake,
      Pointer<Void> context,
      Pointer<Pointer<Utf8>> errorOut,
    );
typedef _SubmitNative =
    Int32 Function(
      Uint64 runtime,
      Pointer<Utf8> message,
      Pointer<Pointer<Utf8>> errorOut,
    );
typedef _Submit =
    int Function(
      int runtime,
      Pointer<Utf8> message,
      Pointer<Pointer<Utf8>> errorOut,
    );
typedef _DrainNative = Pointer<Utf8> Function(Uint64 runtime);
typedef _Drain = Pointer<Utf8> Function(int runtime);
typedef _DetachNative = Void Function(Uint64 runtime);
typedef _Detach = void Function(int runtime);
typedef _FreeNative = Void Function(Pointer<Utf8> output);
typedef _Free = void Function(Pointer<Utf8> output);

/// What a [Bridge] drives: open, admission, drain and detach of one runtime.
/// The C ABI below is the carrier the package ships; a test substitutes a
/// fake to publish an exact event sequence.
abstract interface class Carrier {
  /// Open a runtime; its id, or 0 with the reason. Whenever events are ready
  /// the carrier runs [wake] with that id on this isolate (the C ABI reaches
  /// it through the process-wide wake listener).
  (int, String?) open(String request, void Function(int runtime) wake);

  /// Admit one envelope; null, or why it was refused.
  String? submit(int runtime, String message);

  /// The events published so far, in order.
  List<dynamic> drain(int runtime);

  /// Stop every wake for [runtime]; nothing is published after it.
  void detach(int runtime);
}

/// The runtime functions of one loaded library. Every `char*` the library
/// returns is copied into Dart and freed here exactly once.
class _Abi implements Carrier {
  _Abi(DynamicLibrary library)
    : _open = library.lookupFunction<_OpenNative, _Open>('axton_runtime_open'),
      _submit = library.lookupFunction<_SubmitNative, _Submit>(
        'axton_runtime_submit',
      ),
      _drain = library.lookupFunction<_DrainNative, _Drain>(
        'axton_runtime_drain',
      ),
      _detach = library.lookupFunction<_DetachNative, _Detach>(
        'axton_runtime_detach',
      ),
      _free = library.lookupFunction<_FreeNative, _Free>('axton_free'),
      finalizer = NativeFinalizer(
        library.lookup<NativeFinalizerFunction>('axton_runtime_finalize'),
      );

  final _Open _open;
  final _Submit _submit;
  final _Drain _drain;
  final _Detach _detach;
  final _Free _free;

  /// `axton_runtime_finalize`: detaches the runtime whose id is the token's
  /// address. It runs when a bridge is collected, or its isolate shuts down,
  /// while the runtime is attached. Detach is safe from any thread and
  /// idempotent; once it returns no wake runs for that id.
  final NativeFinalizer finalizer;

  /// Attach [bridge]'s finalizer, unless a pointer cannot carry its id.
  void attach(Bridge bridge) {
    final id = bridge.runtimeId;
    if (sizeOf<IntPtr>() < 8 && id >= 1 << 32) return;
    finalizer.attach(bridge, Pointer<Void>.fromAddress(id), detach: bridge);
  }

  static final _loaded = <String?, _Abi>{};

  /// The library at [libraryPath], or the process's own symbols on iOS,
  /// loaded once per isolate.
  static _Abi load(String? libraryPath) {
    final loaded = _loaded[libraryPath];
    if (loaded != null) return loaded;
    if (libraryPath == null && !Platform.isIOS) {
      throw StateError('libraryPath is required outside iOS');
    }
    try {
      return _loaded[libraryPath] = _Abi(
        libraryPath != null
            ? DynamicLibrary.open(libraryPath)
            : DynamicLibrary.process(),
      );
    } catch (error) {
      throw StateError('$error');
    }
  }

  /// Answer the owned text at [output] and free it.
  String _take(Pointer<Utf8> output) {
    try {
      return output.toDartString();
    } finally {
      _free(output);
    }
  }

  /// Run [call] with an error slot; answer its result and the error text the
  /// callee wrote, if any.
  (int, String?) _withError(int Function(Pointer<Pointer<Utf8>>) call) {
    final slot = calloc<Pointer<Utf8>>();
    try {
      final result = call(slot);
      return (result, slot.value == nullptr ? null : _take(slot.value));
    } finally {
      calloc.free(slot);
    }
  }

  /// The wake runs through [Bridge]'s one native listener, whose target is
  /// the same dispatch [wake] names.
  @override
  (int, String?) open(String request, void Function(int runtime) wake) {
    final text = request.toNativeUtf8();
    try {
      return _withError(
        (error) => _open(text, Bridge._wakePointer, nullptr, error),
      );
    } finally {
      malloc.free(text);
    }
  }

  @override
  String? submit(int runtime, String message) {
    final text = message.toNativeUtf8();
    try {
      final (code, error) = _withError(
        (error) => _submit(runtime, text, error),
      );
      return code == 0 ? null : error ?? 'client_closed';
    } finally {
      malloc.free(text);
    }
  }

  @override
  List<dynamic> drain(int runtime) =>
      jsonDecode(_take(_drain(runtime))) as List<dynamic>;

  @override
  void detach(int runtime) => _detach(runtime);
}

/// One submitted input awaiting its `taskCompleted`.
class _Route {
  _Route({this.run, this.onValue}) : zone = Zone.current;
  final completer = Completer<dynamic>();

  /// The transaction callback of a `transaction` task.
  final Future<void> Function(String transactionId)? run;

  /// Runs with the value of a successful completion while it is dispatched,
  /// before any later event: where an observer is claimed.
  final void Function(dynamic value)? onValue;

  /// The zone the task was submitted from, where [run] and [onValue] run.
  final Zone zone;

  /// What the callback threw, rethrown as it was when the task fails.
  Object? thrown;
  StackTrace? stack;
}

/// Runs one kind of host effect. It answers through the [Effect] and aborts
/// its platform resource when [Effect.cancelled] completes.
typedef EffectHandler = void Function(Effect effect);

/// One effect the runtime asked the host for: its operation, the per-effect
/// cancellation a `cancelEffect` completes, and its answers. Answers after a
/// cancellation are dropped here, and the runtime fences them anyway.
class Effect {
  Effect(this.id, this.operation, this._answer, [this._done]);

  /// The runtime's `effectId`.
  final String id;

  /// The `{kind, ...}` operation to execute.
  final Map<String, dynamic> operation;
  final void Function(Map<String, dynamic> outcome) _answer;
  final void Function()? _done;
  final _cancelled = Completer<void>();
  bool _finished = false;

  /// Completes once the runtime cancelled this effect or its handler was
  /// removed: the platform resource is aborted, and nothing more is answered.
  Future<void> get cancelled => _cancelled.future;
  bool get isCancelled => _cancelled.isCompleted;

  /// The single answer of an HTTP, timer, credential or prerequisite effect.
  void succeed([Object? value]) {
    _send({'ok': true, if (value != null) 'value': value});
    _finish();
  }

  /// One result of a socket stream; the stream goes on.
  void emit(Object value) => _send({'ok': true, 'value': value});

  /// The effect failed, with the HTTP [status] the failure carried, if any.
  /// It ends a socket stream too.
  void fail(String message, {int? status}) {
    _send({
      'ok': false,
      'error': {'message': message, if (status != null) 'status': status},
    });
    _finish();
  }

  /// Abort the platform resource; later answers are dropped.
  void cancel() {
    if (!_cancelled.isCompleted) _cancelled.complete();
    _finish();
  }

  void _send(Map<String, dynamic> outcome) {
    if (!_finished && !isCancelled) _answer(outcome);
  }

  void _finish() {
    if (_finished) return;
    _finished = true;
    _done?.call();
  }
}

/// What the connection and the prerequisite runner need from a runtime: tasks
/// and effect handlers by operation kind. The [Bridge] is one; tests drive the
/// handlers through a fake.
abstract interface class RuntimeHost {
  /// Submit [command]; [onValue] runs with a successful value while its
  /// completion is dispatched, before any later event of the same batch.
  Future<dynamic> task(
    Map<String, dynamic> command, {
    void Function(dynamic value)? onValue,
  });

  /// Run every effect of [kind] with [handler] until [stopHandling].
  void handleEffects(String kind, EffectHandler handler);

  /// Remove [handler] if it still handles [kind], aborting every effect of
  /// that kind it still holds.
  void stopHandling(String kind, EffectHandler handler);
}

/// A task failure the runtime gave a machine-readable reason: `details` is
/// the `taskCompleted` object whose `code` callers map to their public error
/// instead of matching the message. It is a [StateError] carrying the
/// engine's message, like every other task failure.
class TaskFailure extends StateError {
  TaskFailure(super.message, this.details);
  final Map<String, dynamic> details;
}

/// What observer handles need from a runtime: a task whose completion claims
/// the observer it names, and the snapshots of claimed observers. The
/// [Bridge] is one; tests drive handles through a fake.
abstract interface class ObserverHost {
  /// Submit [command]; [onValue] runs with a successful value while its
  /// completion is dispatched, before any later event of the same batch.
  Future<dynamic> task(
    Map<String, dynamic> command, {
    void Function(dynamic value)? onValue,
  });

  /// Deliver every `observerChanged` snapshot of [observerId] to [listener],
  /// in the zone that listened, until a terminal (`closed`) snapshot, which is
  /// delivered and then ends the listener, or [unlisten].
  void listen(
    String observerId,
    void Function(Map<String, dynamic> snapshot) listener,
  );
  void unlisten(String observerId);
}

/// The SDK side of one Rust-owned client runtime.
class Bridge implements RuntimeHost, ObserverHost, Finalizable {
  Bridge._(this._carrier, this.runtimeId) {
    final carrier = _carrier;
    if (carrier is _Abi) carrier.attach(this);
  }

  final Carrier _carrier;

  /// The runtime's id: fresh per open, never reused.
  final int runtimeId;

  /// What the open answered: `clientId` and the schema check's `schema`.
  late final Map<String, dynamic> opened;

  /// Request ids: decimal strings from one increasing counter, never reused.
  /// The open task is `1`.
  int _requests = 1;
  final _routes = <String, _Route>{};
  bool _draining = false;
  bool _detached = false;
  Future<void>? _closing;
  final _terminated = Completer<void>();
  final _reports = StreamController<Map<String, dynamic>>.broadcast(sync: true);

  /// What the runtime reports that is not a task outcome: the `diagnostic` of
  /// every `report` event. Listener errors go to the listener's zone.
  Stream<Map<String, dynamic>> get reports => _reports.stream;

  /// `callCompleted`: a durable, direct or abandoned call's final outcome,
  /// after the commit that decided it.
  void Function(String callId, dynamic outcome)? onCallCompleted;

  /// Claimed observers by id, with the zone each listened from. An observer
  /// is claimed while the completion of the task that named it is dispatched
  /// ([ObserverHost.task]'s `onValue`), and the runtime publishes its first
  /// snapshot after that completion, so no snapshot has to be buffered: one
  /// for an observer nobody claims (a cancelled watch) is dropped.
  final _observers =
      <String, (Zone, void Function(Map<String, dynamic> snapshot))>{};

  /// Effect handlers by operation kind, and the effects they hold by id.
  final _handlers = <String, EffectHandler>{};
  final _effects = <String, Effect>{};

  /// Callback effects asked for and neither started nor cancelled yet.
  final _callbacks = <String>{};

  /// Attached bridges by runtime id: what a wake names.
  static final _bridges = <int, Bridge>{};

  /// Runtime ids of this isolate's attached bridges.
  static Iterable<int> get attached => _bridges.keys;

  /// The one wake callback of this isolate, shared by every runtime and never
  /// closed: a process-wide listener costs one port, and never closing it
  /// means no runtime can ever hold a pointer to a closed callable, however
  /// its detach and a close raced. It does not keep the isolate alive on its
  /// own: while anything waits on a runtime (an open, a task, a close) it
  /// does, so an awaited outcome is always delivered, and a forgotten client
  /// with nothing outstanding pins nothing.
  static NativeCallable<_WakeNative>? _wake;
  static int _held = 0;

  static Pointer<NativeFunction<_WakeNative>> get _wakePointer =>
      (_wake ??= NativeCallable<_WakeNative>.listener(
        _woken,
      )..keepIsolateAlive = _held > 0).nativeFunction;

  static void _woken(int runtime, Pointer<Void> _) => _wakeRuntime(runtime);

  static void _wakeRuntime(int runtime) => _bridges[runtime]?._drain();

  static void _hold() {
    if (_held++ == 0) _wake?.keepIsolateAlive = true;
  }

  static void _release() {
    if (--_held == 0) _wake?.keepIsolateAlive = false;
  }

  /// Open a runtime for the database at [path] and answer once Rust opened
  /// it. A failed open throws its reason as a [StateError], after the runtime
  /// announced its end and was detached. [migration] is accepted for API
  /// compatibility; the row-based client keeps none. [carrier] is a test
  /// seam; the C ABI of [libraryPath] otherwise.
  static Future<Bridge> open({
    required String path,
    required Map<String, dynamic> schema,
    String? libraryPath,
    Map<String, dynamic>? migration,
    bool discardPending = false,
    Carrier? carrier,
  }) async {
    final opener = carrier ?? _Abi.load(libraryPath);
    final request = jsonEncode({
      'type': 'open',
      'requestId': '1',
      'path': path,
      'schema': schema,
      'discardPending': discardPending,
    });
    final (runtime, refused) = opener.open(request, _wakeRuntime);
    if (runtime == 0) throw StateError(refused ?? 'runtime open failed');
    // Registered before any wake can be delivered: the listener only posts to
    // this isolate, which runs it after this synchronous section.
    final bridge = Bridge._(opener, runtime);
    final route = _Route();
    bridge._routes['1'] = route;
    _hold();
    _bridges[runtime] = bridge;
    try {
      bridge.opened = (await route.completer.future) as Map<String, dynamic>;
    } catch (_) {
      // A failed open closed its runtime; wait for its end and detach.
      _hold();
      try {
        await bridge._terminated.future;
      } finally {
        _release();
      }
      rethrow;
    }
    return bridge;
  }

  /// Submit one task and answer its value, or throw its error as a
  /// [StateError] carrying the engine's message (`client_closed` once the
  /// runtime is gone) - a [TaskFailure] when the runtime gave a code.
  /// [onValue] runs with a successful value while the completion is
  /// dispatched, before any later event; if it throws, the task fails with
  /// what it threw.
  @override
  Future<dynamic> task(
    Map<String, dynamic> command, {
    void Function(dynamic value)? onValue,
  }) => _route(_Route(onValue: onValue), (id) => taskEnvelope(id, command));

  @override
  void listen(
    String observerId,
    void Function(Map<String, dynamic> snapshot) listener,
  ) {
    if (!_detached) _observers[observerId] = (Zone.current, listener);
  }

  @override
  void unlisten(String observerId) => _observers.remove(observerId);

  @override
  void handleEffects(String kind, EffectHandler handler) =>
      _handlers[kind] = handler;

  @override
  void stopHandling(String kind, EffectHandler handler) {
    if (!identical(_handlers[kind], handler)) return;
    _handlers.remove(kind);
    for (final effect in _effects.values.toList()) {
      if (effect.operation['kind'] == kind) effect.cancel();
    }
  }

  /// Run [run] as the callback of one local transaction the runtime owns.
  /// Ordinary tasks wait while it runs; its own commands go through
  /// [transactionCommand] with the id it is given. Completes once Rust
  /// committed; otherwise throws what [run] threw, or the runtime's reason.
  Future<void> transaction(Future<void> Function(String transactionId) run) =>
      _route(
        _Route(run: run),
        (id) => taskEnvelope(id, const {'kind': 'transaction'}),
      );

  /// One command of the open callback transaction [transactionId], in the
  /// savepoint [scope] (null at the top level).
  Future<dynamic> transactionCommand(
    String transactionId,
    String? scope,
    Map<String, dynamic> command,
  ) => _route(
    _Route(),
    (id) => transactionCommandEnvelope(id, transactionId, scope, command),
  );

  /// Answer one effect. A refusal means the runtime is gone, which fences the
  /// effect anyway.
  void effectResult(
    String effectId, {
    required bool ok,
    Object? value,
    String? error,
    int? status,
  }) => _submitQuietly(
    effectResultEnvelope(
      effectId,
      ok: ok,
      value: value,
      error: error,
      status: status,
    ),
  );

  /// Test seam: admit a raw envelope and answer the refusal, if any.
  String? submitRaw(Map<String, dynamic> envelope) => _detached
      ? 'client_closed'
      : _carrier.submit(runtimeId, jsonEncode(envelope));

  /// Close the runtime: every waiter settles (`client_closed` for what did not
  /// complete), the runtime detaches, and this completes. Idempotent.
  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    if (_terminated.isCompleted) return;
    _hold();
    try {
      // A refusal means the runtime already closed itself: its
      // `runtimeClosed` is dispatched or on its way.
      _submitQuietly(closeEnvelope);
      await _terminated.future;
    } finally {
      _release();
    }
  }

  /// Register the route before admission; an admission refusal removes and
  /// fails it.
  Future<dynamic> _route(
    _Route route,
    Map<String, dynamic> Function(String requestId) envelope,
  ) {
    if (_detached) return Future.error(StateError('client_closed'));
    final requestId = '${++_requests}';
    final String message;
    try {
      message = jsonEncode(envelope(requestId));
    } catch (error, stack) {
      return Future.error(error, stack);
    }
    _routes[requestId] = route;
    _hold();
    final refused = _carrier.submit(runtimeId, message);
    if (refused != null && identical(_routes.remove(requestId), route)) {
      _release();
      route.completer.completeError(StateError(refused));
    }
    return route.completer.future;
  }

  void _submitQuietly(Map<String, dynamic> envelope) {
    if (!_detached) _carrier.submit(runtimeId, jsonEncode(envelope));
  }

  /// Drain until empty, dispatching every event in order. Never re-entrant: a
  /// wake that arrives while draining is covered by the loop.
  void _drain() {
    if (_draining) return;
    _draining = true;
    try {
      while (!_detached) {
        final batch = _carrier.drain(runtimeId);
        if (batch.isEmpty) return;
        for (final event in batch) {
          if (_detached) return;
          try {
            _dispatch(event as Map<String, dynamic>);
          } catch (error, stack) {
            Zone.current.handleUncaughtError(error, stack);
          }
        }
      }
    } finally {
      _draining = false;
    }
  }

  void _dispatch(Map<String, dynamic> event) {
    switch (event['type']) {
      case 'taskCompleted':
        _complete(event);
      case 'effect':
        _effect(
          event['effectId'] as String,
          event['operation'] as Map<String, dynamic>,
        );
      case 'cancelEffect':
        _effects[event['effectId']]?.cancel();
        _callbacks.remove(event['effectId']);
      case 'report':
        _reports.add(event['diagnostic'] as Map<String, dynamic>);
      case 'callCompleted':
        onCallCompleted?.call(event['callId'] as String, event['outcome']);
      case 'observerChanged':
        _observe(
          event['observerId'] as String,
          event['snapshot'] as Map<String, dynamic>,
        );
      case 'runtimeClosed':
        _terminate();
    }
  }

  /// Remove the route, then settle it exactly once.
  void _complete(Map<String, dynamic> event) {
    final route = _routes.remove(event['requestId']);
    if (route == null) return;
    _release();
    if (event['ok'] == true) {
      final value = event['value'];
      final onValue = route.onValue;
      if (onValue != null) {
        try {
          route.zone.runUnary(onValue, value);
        } catch (error, stack) {
          route.completer.completeError(error, stack);
          return;
        }
      }
      route.completer.complete(value);
    } else if (route.thrown != null) {
      route.completer.completeError(route.thrown!, route.stack);
    } else {
      final message = event['error'] as String? ?? 'task failed';
      final details = event['details'];
      route.completer.completeError(
        details is Map<String, dynamic>
            ? TaskFailure(message, details)
            : StateError(message),
      );
    }
  }

  /// One `observerChanged`: to the observer's listener, in its zone; an
  /// exception there reaches that zone and changes nothing else. Nothing
  /// follows a terminal snapshot, so its listener is removed.
  void _observe(String observerId, Map<String, dynamic> snapshot) {
    final observer = snapshot['closed'] == true
        ? _observers.remove(observerId)
        : _observers[observerId];
    if (observer == null) return;
    final (zone, listener) = observer;
    zone.runUnaryGuarded(listener, snapshot);
  }

  void _effect(String effectId, Map<String, dynamic> operation) {
    if (operation['kind'] != 'callback') {
      final effect = Effect(
        effectId,
        operation,
        (outcome) => _submitQuietly(effectOutcomeEnvelope(effectId, outcome)),
        () => _effects.remove(effectId),
      );
      final handler = _handlers[operation['kind']];
      if (handler == null) {
        effect.fail('unsupported effect');
        return;
      }
      _effects[effectId] = effect;
      try {
        handler(effect);
      } catch (error) {
        effect.fail('$error');
      }
      return;
    }
    final transactionId = operation['transactionId'] as String;
    final route = _routes[operation['requestId']];
    final run = route?.run;
    if (route == null || run == null) {
      _submitQuietly(
        callbackResultEnvelope(
          effectId,
          transactionId,
          ok: false,
          error: 'no transaction callback',
        ),
      );
      return;
    }
    // Application code runs after this batch is dispatched, in the zone the
    // transaction was submitted from; the task completes only from Rust. A
    // later event of the batch may have cancelled the effect or settled the
    // task (a close refuses it), and then the callback never starts.
    _callbacks.add(effectId);
    route.zone.scheduleMicrotask(() {
      final pending = identical(_routes[operation['requestId']], route);
      if (!_callbacks.remove(effectId) || !pending) return;
      Future<void>.sync(() => run(transactionId)).then(
        (_) => _submitQuietly(
          callbackResultEnvelope(effectId, transactionId, ok: true),
        ),
        onError: (Object error, StackTrace stack) {
          route
            ..thrown = error
            ..stack = stack;
          _submitQuietly(
            callbackResultEnvelope(
              effectId,
              transactionId,
              ok: false,
              error: error.toString(),
            ),
          );
        },
      );
    });
  }

  /// `runtimeClosed`: fail what never completed, detach (after which no wake
  /// runs for this id), forget the id and end the report stream.
  void _terminate() {
    if (_detached) return;
    _detached = true;
    _carrier.detach(runtimeId);
    final carrier = _carrier;
    if (carrier is _Abi) carrier.finalizer.detach(this);
    _bridges.remove(runtimeId);
    _callbacks.clear();
    for (final effect in _effects.values.toList()) {
      effect.cancel();
    }
    _handlers.clear();
    // Every observer's terminal snapshot came before `runtimeClosed`.
    _observers.clear();
    final remaining = _routes.values.toList();
    _routes.clear();
    for (final route in remaining) {
      _release();
      route.completer.completeError(StateError('client_closed'));
    }
    unawaited(_reports.close());
    _terminated.complete();
  }

  static Map<String, dynamic> taskEnvelope(
    String requestId,
    Map<String, dynamic> command,
  ) => {'type': 'task', 'requestId': requestId, 'command': command};

  static Map<String, dynamic> transactionCommandEnvelope(
    String requestId,
    String transactionId,
    String? scope,
    Map<String, dynamic> command,
  ) => {
    'type': 'transactionCommand',
    'requestId': requestId,
    'transactionId': transactionId,
    if (scope != null) 'scope': scope,
    'command': command,
  };

  static Map<String, dynamic> callbackResultEnvelope(
    String effectId,
    String transactionId, {
    required bool ok,
    String? error,
  }) => {
    'type': 'callbackResult',
    'effectId': effectId,
    'transactionId': transactionId,
    'ok': ok,
    if (error != null) 'error': error,
  };

  static Map<String, dynamic> effectResultEnvelope(
    String effectId, {
    required bool ok,
    Object? value,
    String? error,
    int? status,
  }) => {
    'type': 'effectResult',
    'effectId': effectId,
    'outcome': {
      'ok': ok,
      if (value != null) 'value': value,
      if (error != null)
        'error': {'message': error, if (status != null) 'status': status},
    },
  };

  static Map<String, dynamic> effectOutcomeEnvelope(
    String effectId,
    Map<String, dynamic> outcome,
  ) => {'type': 'effectResult', 'effectId': effectId, 'outcome': outcome};

  static const Map<String, dynamic> closeEnvelope = {'type': 'close'};
}
