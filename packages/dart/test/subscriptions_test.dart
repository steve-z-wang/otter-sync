// Subscription handles: identity, the committed status they publish, their
// observers, and what closing one means
// ([#150](https://github.com/zanminwang/axton/issues/150)).
import 'dart:async';
import 'package:axton/axton.dart';
// The registry and the runtime seam it observes through, for the orderings
// only an exact event sequence can produce.
import 'package:axton/src/bridge.dart' show ObserverHost, TaskFailure;
import 'package:axton/src/subscriptions.dart'
    show Subscriptions, Subscription, SubscriptionStatus;
import 'package:test/test.dart';

/// One `observerChanged` snapshot of the subscription observer, as the
/// runtime publishes it.
Map<String, dynamic> snapshot({
  bool active = true,
  String initialization = 'ready',
  String connection = 'offline',
  String phase = 'not-requested',
  Map<String, dynamic>? error,
  bool closed = false,
}) => {
  'kind': 'subscription',
  'stream': 'scope',
  'subscriptionId': 1,
  'status': {
    'active': active,
    'initialization': initialization,
    'connection': connection,
    'bootstrap': {'phase': phase, 'error': error},
  },
  if (closed) 'closed': true,
};

/// A terminal snapshot: removed, rebuilt or stopped with the runtime.
Map<String, dynamic> terminal() =>
    snapshot(active: false, connection: 'stopped', closed: true);

/// One task the fake runtime was given, answered by the test.
class Submitted {
  Submitted(this.command, this.onValue);
  final Map<String, dynamic> command;
  final void Function(dynamic value)? onValue;
  final completer = Completer<dynamic>();
}

/// The runtime seam without a runtime: it records every task, answers it
/// when the test says so, and delivers snapshots to the observers claimed at
/// a completion - synchronously, as the Bridge dispatches one batch.
class FakeHost implements ObserverHost {
  final submitted = <Submitted>[];
  final listeners = <String, void Function(Map<String, dynamic>)>{};

  @override
  Future<dynamic> task(
    Map<String, dynamic> command, {
    void Function(dynamic value)? onValue,
  }) {
    final task = Submitted(command, onValue);
    submitted.add(task);
    return task.completer.future;
  }

  @override
  void listen(
    String observerId,
    void Function(Map<String, dynamic> snapshot) listener,
  ) => listeners[observerId] = listener;

  @override
  void unlisten(String observerId) => listeners.remove(observerId);

  /// The oldest unanswered task of [kind].
  Submitted pending(String kind) => submitted.firstWhere(
    (task) => task.command['kind'] == kind && !task.completer.isCompleted,
  );

  /// Complete the oldest [kind] task: its claim runs first, as the Bridge
  /// runs it while dispatching the completion.
  void succeed(String kind, [Object? value]) {
    final task = pending(kind);
    task.onValue?.call(value);
    task.completer.complete(value);
  }

  void fail(String kind, Object error) =>
      pending(kind).completer.completeError(error);

  /// Deliver one snapshot the way the Bridge does: to the observer's
  /// listener, which a terminal snapshot removes.
  void emit(String observerId, Map<String, dynamic> snapshot) {
    final listener = snapshot['closed'] == true
        ? listeners.remove(observerId)
        : listeners[observerId];
    listener?.call(snapshot);
  }
}

/// The value of `streamSubscribe` for the one identity these tests use.
Map<String, dynamic> subscribed([String observerId = '7']) => {
  'state': {
    'stream': 'scope',
    'subscriptionId': 1,
    'startingCursor': 0,
    'cursor': 0,
  },
  'observerId': observerId,
};

/// A registry over [host] and its handle of `scope`, whose first snapshot
/// arrives in the batch of the subscribe completion, before the caller's
/// continuation runs.
Future<(Subscriptions, Subscription)> handle(
  FakeHost host, [
  Map<String, dynamic>? first,
]) async {
  final registry = Subscriptions(host);
  final subscribing = registry.subscribe('scope');
  host.succeed('streamSubscribe', subscribed());
  host.emit('7', first ?? snapshot());
  return (registry, await subscribing);
}

void main() {
  // The public multi-Stream manager is retired. Current bound single-Stream
  // bootstrap/catch-up/reconnect behavior is covered by protocol4 transport
  // fixtures and the generated real-backend SDK suite. These tests retain the
  // internal observer registry's error/lifetime routes for saved legacy tasks.
  test(
    'a registration the engine refuses as closed rejects with subscription.closed',
    () async {
      // The removal committed between this command and the handle's own close,
      // so the engine - not the handle - is what knows it is gone; the runtime
      // names that with a code, whatever the engine's text says.
      final host = FakeHost();
      final (_, subscription) = await handle(host);
      final refused = subscription.bootstrap();
      host.fail(
        'streamBootstrap',
        TaskFailure(
          'subscription.closed: subscription 1 for scope is closed; '
          'it has no bootstrap state',
          const {'code': 'subscription.closed'},
        ),
      );
      await expectLater(
        refused,
        throwsA(isA<SubscriptionClosedException>()),
        reason: "the engine's text must not reach the caller",
      );
      // An unrelated engine failure is still the caller's to see, unchanged.
      final other = StateError('the database is locked');
      final locked = subscription.bootstrap();
      host.fail('streamBootstrap', other);
      await expectLater(locked, throwsA(same(other)));
    },
  );

  test(
    'a handle closed while its registration commits settles as closed',
    () async {
      final host = FakeHost();
      final (registry, subscription) = await handle(host);
      final pending = subscription.bootstrap().then<Object?>(
        (_) => null,
        onError: (Object error) => error,
      );
      // The client closes: the runtime stops the observer, then fails the
      // waiter it parked.
      registry.closing();
      host.emit('7', terminal());
      host.fail(
        'streamBootstrap',
        TaskFailure('client_closed', const {'code': 'client_closed'}),
      );
      expect(await pending, isA<ClientClosedException>());
      expect(subscription.status.connection, SubscriptionConnection.stopped);
      await expectLater(
        subscription.unsubscribe(),
        throwsA(isA<SubscriptionClosedException>()),
        reason: 'a handle stopped with its client commits no work',
      );
      // A task still queued when the runtime closed carries no code: its
      // message is the one the client closed with.
      final host2 = FakeHost();
      final (_, queued) = await handle(host2);
      final late = queued.bootstrap();
      host2.fail('streamBootstrap', StateError('client_closed'));
      await expectLater(late, throwsA(isA<ClientClosedException>()));
    },
  );

  test(
    'a waiter whose run was superseded is rejected, never completed by the newer one',
    () async {
      final host = FakeHost();
      final (_, subscription) = await handle(host);
      final first = subscription.bootstrap().then<Object?>(
        (_) => null,
        onError: (Object error) => error,
      );
      host.emit('7', snapshot(phase: 'loading'));
      final second = subscription.bootstrap().then<Object?>(
        (_) => 'resolved',
        onError: (Object error) => error,
      );
      // The runtime observed run 2 and failed the waiter of run 1 with its
      // code; run 1 must not complete from run 2.
      host.fail(
        'streamBootstrap',
        TaskFailure('bootstrap.superseded', const {
          'code': 'bootstrap.superseded',
        }),
      );
      final superseded = await first;
      expect(superseded, isA<BootstrapSupersededException>());
      expect((superseded as BootstrapSupersededException).stream, 'scope');
      // The newest run still settles the call that belongs to it.
      host.emit('7', snapshot(phase: 'complete'));
      host.succeed('streamBootstrap');
      expect(await second, 'resolved');
      expect(
        subscription.status.bootstrap,
        const BootstrapStatus(phase: BootstrapPhase.complete),
      );
    },
  );

  test('bootstrap rejects with the public error its code names', () async {
    final host = FakeHost();
    final (_, subscription) = await handle(host);
    Future<Object?> outcome(Object failure) {
      final call = subscription.bootstrap().then<Object?>(
        (_) => 'resolved',
        onError: (Object error) => error,
      );
      final task = host.pending('streamBootstrap');
      expect(task.command, {
        'kind': 'streamBootstrap',
        'stream': 'scope',
        'subscriptionId': 1,
      }, reason: 'submitted when the call is made');
      host.fail('streamBootstrap', failure);
      return call;
    }

    expect(
      await outcome(
        TaskFailure('subscription.closed', const {
          'code': 'subscription.closed',
        }),
      ),
      isA<SubscriptionClosedException>(),
    );
    expect(
      await outcome(
        TaskFailure('client_closed', const {'code': 'client_closed'}),
      ),
      isA<ClientClosedException>(),
    );
    expect(
      await outcome(
        TaskFailure('bootstrap.superseded', const {
          'code': 'bootstrap.superseded',
        }),
      ),
      isA<BootstrapSupersededException>().having(
        (e) => e.stream,
        'scope',
        'scope',
      ),
    );
    final failed = await outcome(
      TaskFailure('the server refuses this interval', const {
        'code': 'bootstrap.request_rejected',
        'message': 'the server refuses this interval',
      }),
    );
    expect(
      failed,
      isA<BootstrapFailedException>()
          .having((e) => e.code, 'code', 'bootstrap.request_rejected')
          .having(
            (e) => e.message,
            'message',
            'the server refuses this interval',
          ),
    );
    // Success is `null`, once the completion committed.
    final done = subscription.bootstrap();
    host.succeed('streamBootstrap');
    await done;
  });

  test(
    'a snapshot in the batch of the subscribe completion is the status a listener starts with',
    () async {
      final host = FakeHost();
      final (_, subscription) = await handle(
        host,
        snapshot(connection: 'connecting', phase: 'waiting-for-initialization'),
      );
      const current = SubscriptionStatus(
        active: true,
        initialization: SubscriptionInitialization.ready,
        connection: SubscriptionConnection.connecting,
        bootstrap: BootstrapStatus(
          phase: BootstrapPhase.waitingForInitialization,
        ),
      );
      expect(subscription.status, current);
      final seen = <SubscriptionStatus>[];
      final observer = subscription.watch().listen(seen.add);
      await pumpEventQueue();
      expect(seen, [current], reason: 'the current snapshot, exactly once');
      host.emit(
        '7',
        snapshot(
          connection: 'catching-up',
          phase: 'failed',
          error: {'code': 'bootstrap.request_rejected', 'message': 'HTTP 403'},
        ),
      );
      await pumpEventQueue();
      expect(
        seen.last,
        const SubscriptionStatus(
          active: true,
          initialization: SubscriptionInitialization.ready,
          connection: SubscriptionConnection.catchingUp,
          bootstrap: BootstrapStatus(
            phase: BootstrapPhase.failed,
            error: BootstrapError(
              code: 'bootstrap.request_rejected',
              message: 'HTTP 403',
            ),
          ),
        ),
      );
      expect(seen, hasLength(2));
      await observer.cancel();
    },
  );

  test(
    'repeated subscribes of one identity share the claimed handle',
    () async {
      final host = FakeHost();
      final (registry, subscription) = await handle(host);
      final repeated = registry.subscribe('scope');
      host.succeed('streamSubscribe', subscribed());
      expect(identical(await repeated, subscription), isTrue);
      expect(host.listeners.keys, ['7'], reason: 'one observer per identity');
    },
  );

  test(
    'a terminal snapshot ends the handle: status, streams and listeners',
    () async {
      final host = FakeHost();
      final (registry, subscription) = await handle(host);
      final seen = <SubscriptionStatus>[];
      final done = Completer<void>();
      subscription.watch().listen(seen.add, onDone: done.complete);
      await pumpEventQueue();
      host.emit('7', terminal());
      await done.future.timeout(
        const Duration(seconds: 1),
        onTimeout: () => throw StateError('the stream of a closed handle ends'),
      );
      const stopped = SubscriptionStatus(
        active: false,
        initialization: SubscriptionInitialization.ready,
        connection: SubscriptionConnection.stopped,
      );
      expect(seen, [
        const SubscriptionStatus(
          active: true,
          initialization: SubscriptionInitialization.ready,
          connection: SubscriptionConnection.offline,
        ),
        stopped,
      ]);
      expect(subscription.status, stopped);
      expect(host.listeners, isEmpty, reason: 'nothing follows a terminal one');
      // A closed handle takes nothing further and submits nothing.
      final submitted = host.submitted.length;
      await subscription.unsubscribe();
      await expectLater(
        subscription.bootstrap(),
        throwsA(isA<SubscriptionClosedException>()),
      );
      expect(host.submitted, hasLength(submitted));
      expect(await subscription.watch().toList(), [stopped]);
      // The identity is forgotten: a later registration is another handle.
      final next = registry.subscribe('scope');
      host.succeed('streamSubscribe', {
        ...subscribed('9'),
        'state': <String, dynamic>{
          ...subscribed()['state'] as Map<String, dynamic>,
          'subscriptionId': 2,
        },
      });
      expect(identical(await next, subscription), isFalse);
    },
  );

  test(
    'unsubscribe resolves after the terminal snapshot closed the handle',
    () async {
      final host = FakeHost();
      final (_, subscription) = await handle(host);
      final removing = subscription.unsubscribe();
      expect(host.pending('streamUnsubscribe').command, {
        'kind': 'streamUnsubscribe',
        'stream': 'scope',
        'subscriptionId': 1,
      });
      expect(subscription.status.active, isTrue);
      host.emit('7', terminal());
      host.succeed('streamUnsubscribe', {'removed': true});
      await removing;
      expect(subscription.status.active, isFalse);
      await subscription.unsubscribe();
      expect(
        host.submitted.where((t) => t.command['kind'] == 'streamUnsubscribe'),
        hasLength(1),
        reason: 'repeating it on a closed handle is a no-op',
      );
    },
  );

  test(
    'a client whose runtime is already gone stops its handles locally',
    () async {
      final host = FakeHost();
      final (registry, subscription) = await handle(host);
      var completed = false;
      subscription.watch().listen((_) {}, onDone: () => completed = true);
      registry.closing();
      registry.close();
      await pumpEventQueue();
      expect(completed, isTrue);
      expect(
        subscription.status,
        const SubscriptionStatus(
          active: false,
          initialization: SubscriptionInitialization.ready,
          connection: SubscriptionConnection.stopped,
        ),
      );
      expect(host.listeners, isEmpty);
      await expectLater(
        subscription.unsubscribe(),
        throwsA(isA<SubscriptionClosedException>()),
      );
    },
  );
}
