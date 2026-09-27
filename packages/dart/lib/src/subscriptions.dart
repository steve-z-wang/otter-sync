/// Subscription handles: the identity a registration keeps, the status the
/// runtime publishes for it and the observers watching it
/// ([#150](https://github.com/zanminwang/axton/issues/150),
/// [#134](https://github.com/zanminwang/axton/issues/134)). A handle decides
/// nothing: Rust projects the status, parks every `bootstrap()` on its run and
/// ends the observer; this file keeps the language objects - one handle per
/// identity, its last snapshot and its streams - and maps the runtime's codes
/// to the public errors.
library;

import 'dart:async';

import 'bridge.dart';

/// Whether a durable starting boundary exists for a subscription. It says
/// nothing about the connection.
enum SubscriptionInitialization { pending, ready }

/// What the lane is doing for a subscription. `live` means the session
/// delivers normally, not that all history is loaded.
enum SubscriptionConnection { offline, connecting, catchingUp, live, stopped }

/// What the durable load of a Scope's published history is doing
/// ([#151](https://github.com/zanminwang/axton/issues/151)).
/// `waitingForInitialization` is a requested run with no starting boundary to
/// bound its interval yet, `catchingUp` a loaded interval whose completion
/// barrier ordinary delivery has not reached, and `complete` says the initial
/// publication coverage was processed - not that a snapshot was taken, nor that
/// the Scope is currently fresh.
enum BootstrapPhase {
  notRequested,
  waitingForInitialization,
  loading,
  catchingUp,
  complete,
  failed,
}

/// The stored failure of a load, as its status publishes it. The failed page's
/// record reports stay in the ledger and are not part of this.
class BootstrapError {
  final String code;
  final String message;
  const BootstrapError({required this.code, required this.message});
  @override
  bool operator ==(Object other) =>
      other is BootstrapError && other.code == code && other.message == message;
  @override
  int get hashCode => Object.hash(code, message);
  @override
  String toString() => 'BootstrapError($code: $message)';
}

/// One immutable snapshot of a subscription's durable load.
class BootstrapStatus {
  final BootstrapPhase phase;
  final BootstrapError? error;
  const BootstrapStatus({required this.phase, this.error});
  @override
  bool operator ==(Object other) =>
      other is BootstrapStatus && other.phase == phase && other.error == error;
  @override
  int get hashCode => Object.hash(phase, error);
  @override
  String toString() => 'BootstrapStatus(${phase.name}, error: $error)';
}

/// One immutable subscription status snapshot.
class SubscriptionStatus {
  /// Whether this handle still names a live registration.
  final bool active;
  final SubscriptionInitialization initialization;
  final SubscriptionConnection connection;

  /// The durable load of this Scope's published history, as it was last
  /// committed.
  final BootstrapStatus bootstrap;
  const SubscriptionStatus({
    required this.active,
    required this.initialization,
    required this.connection,
    this.bootstrap = const BootstrapStatus(phase: BootstrapPhase.notRequested),
  });
  @override
  bool operator ==(Object other) =>
      other is SubscriptionStatus &&
      other.active == active &&
      other.initialization == initialization &&
      other.connection == connection &&
      other.bootstrap == bootstrap;
  @override
  int get hashCode =>
      Object.hash(active, initialization, connection, bootstrap);
  @override
  String toString() =>
      'SubscriptionStatus(active: $active, '
      'initialization: ${initialization.name}, '
      'connection: ${connection.name}, '
      'bootstrap: $bootstrap)';

  /// The runtime's `status` object: the same fields, in their wire
  /// spelling.
  factory SubscriptionStatus._fromJson(Map<String, dynamic> json) {
    final bootstrap = json['bootstrap'] as Map<String, dynamic>;
    final error = bootstrap['error'] as Map<String, dynamic>?;
    return SubscriptionStatus(
      active: json['active'] as bool,
      initialization: SubscriptionInitialization.values.byName(
        json['initialization'] as String,
      ),
      connection: switch (json['connection']) {
        'catching-up' => SubscriptionConnection.catchingUp,
        final name as String => SubscriptionConnection.values.byName(name),
      },
      bootstrap: BootstrapStatus(
        phase: switch (bootstrap['phase']) {
          'not-requested' => BootstrapPhase.notRequested,
          'waiting-for-initialization' =>
            BootstrapPhase.waitingForInitialization,
          'catching-up' => BootstrapPhase.catchingUp,
          final name as String => BootstrapPhase.values.byName(name),
        },
        error: error == null
            ? null
            : BootstrapError(
                code: error['code'] as String,
                message: error['message'] as String,
              ),
      ),
    );
  }

  /// This status as its handle stopped with a client whose runtime was
  /// already gone: what the runtime's terminal snapshot would have said.
  SubscriptionStatus _stopped() => SubscriptionStatus(
    active: false,
    initialization: initialization,
    connection: SubscriptionConnection.stopped,
    bootstrap: bootstrap,
  );
}

/// Work attempted through a handle that is closed: unsubscribed, or stopped
/// with its client.
class SubscriptionClosedException implements Exception {
  final String code = 'subscription.closed';
  const SubscriptionClosedException();
  @override
  String toString() => 'subscription.closed';
}

/// This process stopped waiting because its client closed. The durable task is
/// untouched: a reopened client resumes it without a new call.
class ClientClosedException implements Exception {
  final String code = 'client_closed';
  const ClientClosedException();
  @override
  String toString() => 'client_closed';
}

/// A load run failed, and this is the failure the ledger stored for it. A later
/// explicit call retries that run; this one stays failed.
class BootstrapFailedException implements Exception {
  final String code;
  final String message;
  const BootstrapFailedException(this.code, this.message);
  @override
  String toString() => '$code: $message';
}

/// A later run of the same registration was observed than the one this call is
/// attached to: its own outcome can no longer be observed, and a waiter never
/// completes from another run's, so a rapid retry cannot turn an earlier failed
/// call into a success ([#151](https://github.com/zanminwang/axton/issues/151)).
class BootstrapSupersededException implements Exception {
  final String code = 'bootstrap.superseded';
  final String scope;
  const BootstrapSupersededException(this.scope);
  @override
  String toString() =>
      'bootstrap.superseded: the bootstrap run of $scope this call waited for '
      'was superseded';
}

/// One stored subscription, as the native Scope commands answer it. A boundary
/// that is not committed yet is `null`; zero is a delivery position.
class SubscriptionState {
  final String scope;
  final int subscriptionId;
  final int? startingCursor;
  final int? cursor;
  const SubscriptionState({
    required this.scope,
    required this.subscriptionId,
    this.startingCursor,
    this.cursor,
  });
  factory SubscriptionState.fromRecord(Map<String, dynamic> record) =>
      SubscriptionState(
        scope: record['scope'] as String,
        subscriptionId: record['subscriptionId'] as int,
        startingCursor: record['startingCursor'] as int?,
        cursor: record['cursor'] as int?,
      );
}

/// The public error of a failed `scopeBootstrap` task. The runtime names the
/// reason in `details.code`; a task still queued when the runtime closed has
/// none and fails with `client_closed`; anything else is the caller's to see
/// unchanged.
Object _bootstrapError(Object error, String scope) {
  if (error is TaskFailure) {
    final code = error.details['code'];
    if (code is String) {
      return switch (code) {
        'subscription.closed' => const SubscriptionClosedException(),
        'client_closed' => const ClientClosedException(),
        'bootstrap.superseded' => BootstrapSupersededException(scope),
        _ => BootstrapFailedException(
          code,
          error.details['message'] as String? ?? error.message,
        ),
      };
    }
  }
  if (error is StateError && error.message == 'client_closed') {
    return const ClientClosedException();
  }
  return error;
}

/// One durable subscription as the application holds it. It is a handle, not
/// the owner of a socket or of the subscription's lifetime.
class Subscription {
  final String scope;
  final int subscriptionId;
  final String _observerId;
  final Subscriptions _registry;

  /// The runtime's last snapshot. The first one is published in the batch of
  /// the `scopeSubscribe` completion that created this handle, so it replaces
  /// this placeholder before the handle is returned.
  SubscriptionStatus _snapshot = const SubscriptionStatus(
    active: true,
    initialization: SubscriptionInitialization.pending,
    connection: SubscriptionConnection.offline,
  );

  /// The runtime ended this handle's observer, or its client stopped it.
  bool _closed = false;

  /// It ended because its client closed: its status is readable, its work is
  /// not.
  bool _stopped = false;
  final _sinks = <MultiStreamController<SubscriptionStatus>>[];
  Subscription._(SubscriptionState state, this._observerId, this._registry)
    : scope = state.scope,
      subscriptionId = state.subscriptionId;
  SubscriptionStatus get status => _snapshot;

  /// One `observerChanged` snapshot. A terminal one - the registration was
  /// removed or rebuilt away, or the client closed - is the last.
  void _apply(Map<String, dynamic> snapshot) {
    if (_closed) return;
    _publish(
      SubscriptionStatus._fromJson(snapshot['status'] as Map<String, dynamic>),
    );
    if (snapshot['closed'] == true) _end(stopped: _registry._stopping);
  }

  void _publish(SubscriptionStatus next) {
    _snapshot = next;
    for (final sink in _sinks.toList()) {
      sink.add(next);
    }
  }

  /// A closed handle has no changes left: its streams end after its last
  /// snapshot, and the registry forgets its identity.
  void _end({required bool stopped}) {
    _closed = true;
    _stopped = stopped;
    _registry._forget(this);
    for (final sink in _sinks.toList()) {
      sink.close();
    }
    _sinks.clear();
  }

  /// Prepare this Scope's published history. The registration is submitted
  /// when the call is made, whether or not the returned Future is awaited; the
  /// runtime completes it only after the completion transaction commits. Calls
  /// during one active run share it, a call after a valid completion completes
  /// locally - offline too - and a call after a terminal failure explicitly
  /// retries the saved run.
  Future<void> bootstrap() {
    // Handle lifetime: a closed handle's registration is gone, and a stopped
    // one has no runtime to ask; the runtime answers the same code otherwise.
    if (_closed) return Future.error(const SubscriptionClosedException());
    return _registry._host
        .task({
          'kind': 'scopeBootstrap',
          'scope': scope,
          'subscriptionId': subscriptionId,
        })
        .then<void>(
          (_) {},
          onError: (Object error, StackTrace stack) =>
              Error.throwWithStackTrace(_bootstrapError(error, scope), stack),
        );
  }

  /// The current snapshot, then every change. Dart's stream convention
  /// applies: the current snapshot arrives on the microtask after `listen`.
  /// Cancelling removes the observer, not the persistent subscription; a
  /// closed handle delivers its stopped snapshot and completes.
  Stream<SubscriptionStatus> watch() =>
      Stream<SubscriptionStatus>.multi((sink) {
        sink.add(_snapshot);
        if (_closed) {
          sink.close();
          return;
        }
        _sinks.add(sink);
        sink.onCancel = () {
          _sinks.remove(sink);
        };
      });

  /// Commit the local removal of this registration and stop the handle: the
  /// runtime publishes the terminal snapshot before the removal completes.
  /// Repeating it on a closed handle is a no-op; a handle its client stopped
  /// cannot commit work at all.
  Future<void> unsubscribe() async {
    // Handle lifetime: nothing is left to remove, and a stopped handle has no
    // runtime to ask.
    if (_stopped) throw const SubscriptionClosedException();
    if (_closed) return;
    await _registry._host.task({
      'kind': 'scopeUnsubscribe',
      'scope': scope,
      'subscriptionId': subscriptionId,
    });
  }
}

/// The registry: one handle per persistent subscription identity, claimed
/// when the runtime names its observer.
class Subscriptions {
  final ObserverHost _host;
  final _handles = <int, Subscription>{};

  /// The client is closing: a terminal snapshot from here on stops a handle
  /// with its client rather than removing it.
  bool _stopping = false;
  Subscriptions(this._host);

  /// Register durable intent and answer with the handle of the identity that
  /// commit belongs to. The handle is claimed while the completion is
  /// dispatched, so the runtime's first snapshot, which follows it in the same
  /// batch, is already this handle's; concurrent calls read the same identity
  /// and share one cached handle.
  Future<Subscription> subscribe(String scope) async {
    late Subscription handle;
    await _host.task({
      'kind': 'scopeSubscribe',
      'scope': scope,
    }, onValue: (value) => handle = _claim(value as Map<String, dynamic>));
    return handle;
  }

  Subscription _claim(Map<String, dynamic> value) {
    final state = SubscriptionState.fromRecord(
      value['state'] as Map<String, dynamic>,
    );
    final existing = _handles[state.subscriptionId];
    if (existing != null) return existing;
    final observerId = value['observerId'] as String;
    final handle = Subscription._(state, observerId, this);
    _handles[state.subscriptionId] = handle;
    _host.listen(observerId, handle._apply);
    return handle;
  }

  void _forget(Subscription handle) {
    if (identical(_handles[handle.subscriptionId], handle)) {
      _handles.remove(handle.subscriptionId);
    }
    _host.unlisten(handle._observerId);
  }

  /// Remove whatever registration a Scope name has - the Scope-named form the
  /// generated `channels` facade keeps. The runtime ends the handle it had
  /// before the removal completes.
  Future<void> unsubscribeScope(String scope) async {
    await _host.task({
      'kind': 'channel',
      'channel': scope,
      'subscribed': false,
    });
  }

  /// The client began closing: the runtime's terminal snapshots from here on
  /// stop handles with it.
  void closing() => _stopping = true;

  /// The runtime is gone. Its terminal snapshots ended every handle it knew;
  /// any handle still open stops here, with the status it last had. The
  /// bridge contract makes this a fallback: it only keeps a Dart stream from
  /// waiting forever on a runtime that ended without its terminal snapshot.
  void close() {
    _stopping = true;
    for (final handle in _handles.values.toList()) {
      handle._publish(handle._snapshot._stopped());
      handle._end(stopped: true);
    }
  }
}
