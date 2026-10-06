import 'dart:async';

enum CallStatus { pending, succeeded, failed }

/// A failed execution or an inability to observe its final result.
final class CallError implements Exception {
  final String code;
  final String execution;
  final Object? cause;
  const CallError(this.code, {this.execution = 'unknown', this.cause});

  @override
  String toString() => 'CallError($code, execution: $execution)';
}

sealed class CallOutcome<T> {
  const CallOutcome();
}

final class CallSuccess<T> extends CallOutcome<T> {
  final T result;
  const CallSuccess(this.result);
}

final class CallFailure<T> extends CallOutcome<T> {
  final CallError error;
  const CallFailure(this.error);
}

abstract interface class Call<T> {
  CallStatus get status;
  Future<CallOutcome<T>> wait();
}

abstract interface class ActionWeakState {
  Object? get target;
}

final class _WeakState implements ActionWeakState {
  final WeakReference<Object> _reference;
  _WeakState(Object state) : _reference = WeakReference(state);
  @override
  Object? get target => _reference.target;
}

/// Where a Call stands against the local transaction that submitted it. A
/// standalone submission is committed when its handle exists; one submitted
/// in a transaction is provisional until the runtime announces the commit or
/// the rollback that decides it.
enum _Lifecycle { provisional, committed, rolledBack }

abstract class _PendingState {
  bool get provisional;
  void commit();
  void rollBack();
  void complete(Map<String, dynamic> outcome);
  void fail(CallError error);
}

final class _CallState<T> implements _PendingState {
  final T Function(dynamic) decode;
  final void Function(_PendingState) retain;
  final void Function()? admit;
  final Completer<CallOutcome<T>> _done = Completer<CallOutcome<T>>();
  CallStatus status = CallStatus.pending;
  _Lifecycle _lifecycle;

  _CallState(
    this.decode,
    this.retain, [
    this._lifecycle = _Lifecycle.committed,
    this.admit,
  ]);

  @override
  bool get provisional => _lifecycle == _Lifecycle.provisional;

  /// Local observation errors, never backend outcomes: before the commit a
  /// wait is refused without settling or retaining the Call; after a rollback
  /// every wait fails.
  Future<CallOutcome<T>> wait() {
    switch (_lifecycle) {
      case _Lifecycle.rolledBack:
        return Future.error(
          const CallError('transaction_rolled_back', execution: 'rejected'),
        );
      case _Lifecycle.provisional:
        return Future.error(const CallError('transaction_uncommitted'));
      case _Lifecycle.committed:
        if (!_done.isCompleted) {
          try {
            admit?.call();
          } catch (error, stack) {
            return Future.error(error, stack);
          }
          retain(this);
        }
        return _done.future;
    }
  }

  @override
  void commit() {
    if (provisional) _lifecycle = _Lifecycle.committed;
  }

  /// The transaction or savepoint rolled back: the call never becomes
  /// sendable.
  @override
  void rollBack() {
    if (!provisional) return;
    _lifecycle = _Lifecycle.rolledBack;
    status = CallStatus.failed;
  }

  @override
  void complete(Map<String, dynamic> outcome) {
    if (_done.isCompleted) return;
    commit();
    if (outcome['status'] == 'succeeded') {
      try {
        final value = decode(outcome['result']);
        status = CallStatus.succeeded;
        _done.complete(CallSuccess<T>(value));
      } catch (error) {
        fail(CallError('action.observation_failed', cause: error));
      }
      return;
    }
    fail(
      CallError(
        outcome['code'] as String? ?? 'action.failed',
        execution: outcome['execution'] as String? ?? 'rejected',
      ),
    );
  }

  @override
  void fail(CallError error) {
    if (_done.isCompleted) return;
    status = CallStatus.failed;
    _done.complete(CallFailure<T>(error));
  }
}

final class _ActionHandle<T> implements Call<T> {
  final _CallState<T> _state;
  _ActionHandle(this._state);
  @override
  CallStatus get status => _state.status;
  @override
  Future<CallOutcome<T>> wait() => _state.wait();
}

/// Per-client completion routing. Weak slots do not retain abandoned handles.
final class ActionObservers {
  final Map<String, ActionWeakState> _routes = {};
  final Map<String, _PendingState> _active = {};
  final ActionWeakState Function(Object) _weak;
  bool _closed = false;
  bool _ended = false;

  final Future<Map<String, dynamic>?> Function(String)? lookup;
  final Set<String> _looking = {};
  final void Function()? admit;
  ActionObservers({
    ActionWeakState Function(Object)? weak,
    this.lookup,
    this.admit,
  }) : _weak = weak ?? _WeakState.new;

  int get routingCount {
    _sweep();
    return _routes.length;
  }

  void _sweep() => _routes.removeWhere((_, ref) => ref.target == null);

  /// Route [callId]'s completion to a new handle. A [provisional] handle was
  /// submitted in an open transaction: it waits for [transition].
  Call<T> register<T>(
    String callId,
    T Function(dynamic) decode, {
    bool provisional = false,
  }) {
    // Closed, a committed call can no longer be observed; a provisional one
    // still hears its transaction's fate, until the runtime ended.
    if (provisional ? _ended : _closed) {
      final state = _CallState<T>(
        decode,
        (_) {},
        provisional ? _Lifecycle.provisional : _Lifecycle.committed,
      );
      if (provisional) {
        state.rollBack();
      } else {
        state.fail(const CallError('client.closed'));
      }
      return _ActionHandle<T>(state);
    }
    _sweep();
    final state = _CallState<T>(
      decode,
      (state) {
        _active[callId] = state;
        if (lookup != null && _looking.add(callId)) {
          lookup!(callId)
              .then(
                (completion) {
                  if (completion != null) complete(completion);
                },
                onError: (Object error, StackTrace stack) {
                  if (!_active.containsKey(callId)) return;
                  state.fail(
                    CallError('action.observation_failed', cause: error),
                  );
                  _active.remove(callId);
                  _routes.remove(callId);
                },
              )
              .whenComplete(() => _looking.remove(callId));
        }
      },
      provisional ? _Lifecycle.provisional : _Lifecycle.committed,
      admit,
    );
    _routes[callId] = _weak(state);
    return _ActionHandle<T>(state);
  }

  /// The runtime's `transactionCallState`: the local commit made the call
  /// durable (`committed`), or a rollback discarded it (`rolledBack`).
  /// Nobody needs to be waiting.
  void transition(String callId, String state) {
    _sweep();
    final call = _routes[callId]?.target;
    if (call is! _PendingState || !call.provisional) return;
    if (state == 'committed') {
      call.commit();
      if (!_closed) return;
      call.fail(const CallError('client.closed'));
    } else {
      call.rollBack();
    }
    _routes.remove(callId);
  }

  void complete(Map<String, dynamic> event) {
    _sweep();
    final callId = event['callId'] as String;
    final state =
        _active.remove(callId) ??
        (_routes.remove(callId)?.target as _PendingState?);
    _routes.remove(callId);
    if (state == null) return;
    state.complete((event['outcome'] as Map).cast<String, dynamic>());
  }

  /// The client closes: every committed handle fails with `client.closed`.
  /// A provisional one stays routed until the runtime announces its fate.
  void close() {
    if (_closed) return;
    _closed = true;
    final states = <_PendingState>{..._active.values};
    for (final ref in _routes.values) {
      final state = ref.target;
      if (state is _PendingState && !state.provisional) states.add(state);
    }
    _routes.removeWhere((_, ref) {
      final state = ref.target;
      return state is! _PendingState || !state.provisional;
    });
    _active.clear();
    for (final state in states) {
      state.fail(const CallError('client.closed'));
    }
  }

  /// The runtime ended: no transaction can commit any more.
  void ended() {
    close();
    _ended = true;
    for (final ref in _routes.values) {
      final state = ref.target;
      if (state is _PendingState) state.rollBack();
    }
    _routes.clear();
  }
}
