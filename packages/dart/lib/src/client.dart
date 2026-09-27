import 'actions.dart';
import 'bridge.dart';
import 'connection.dart';
import 'live.dart';
import 'loads.dart';
import 'port.dart';
import 'subscriptions.dart';
import 'dart:async';

/// A raw Model store hook; generated clients decode changes before calling it.
typedef StoreHook =
    FutureOr<void> Function(Transaction tx, List<Map<String, dynamic>> changes);

/// A hook body keeps no decoded payload after handing it to user code.
class _StoreHookInvocation {
  _StoreHookInvocation(this.hook, this.changes);
  StoreHook? hook;
  List<Map<String, dynamic>>? changes;

  FutureOr<void> run(Transaction tx) {
    final callback = hook!;
    final delivered = changes!;
    hook = null;
    changes = null;
    return callback(tx, delivered);
  }
}

/// Typed generated model APIs delegate to this generic native client.
class Client implements WritePort, MutatePort {
  /// The Rust-owned runtime: it orders every task and owns the database.
  final Bridge _bridge;
  final String clientId;
  Future<void>? _tasks;
  bool _closed = false;
  RuntimeConnection? _connection;

  /// Connects not settled yet: close waits for them, so a connection set up
  /// while it closes is stopped with it.
  final _connecting = <Future<void>>{};
  Future<void>? _closing;
  final _completions = StreamController<Map<String, dynamic>>.broadcast(
    sync: true,
  );

  /// Every `callCompleted` as `{callId, outcome}`, after its [Call] handle
  /// settled.
  Stream<Map<String, dynamic>> get actionCompletions => _completions.stream;
  late final ActionObservers _actionObservers = ActionObservers();

  /// One `callCompleted`: the handle's waiter first, then the stream.
  void _callCompleted(String callId, dynamic outcome) {
    final event = {'callId': callId, 'outcome': outcome};
    _actionObservers.complete(event);
    if (!_completions.isClosed) _completions.add(event);
  }

  /// The runtime's direct-call codes whose execution is unknown.
  static const _unknownExecution = {
    'action.unavailable',
    'action.execution_unknown',
    'action.observation_failed',
  };

  CallError _publicActionError(Object error) {
    if (error is CallError) return error;
    if (error is ActionTransportException) {
      return CallError(
        error.code,
        execution: error.execution,
        cause: error.cause,
      );
    }
    final message = error is StateError ? error.message : null;
    if (error is TaskFailure && error.details['code'] == 'store_hook_failed') {
      return CallError('store_hook_failed', cause: error.cause ?? error);
    }
    if (message == 'action.invalid_options') {
      return CallError(message!, execution: 'rejected', cause: error);
    }
    final transactionActive = message == 'transaction_active';
    return CallError(
      transactionActive ? 'transaction_active' : 'action.transport_failed',
      execution: transactionActive ? 'rejected' : 'unknown',
      cause: error,
    );
  }

  /// Subscription handles by persistent identity, and the status the runtime
  /// publishes for them.
  late final Subscriptions _subscriptions = Subscriptions(_bridge);

  /// The Scope surface the generated `scopes` facade delegates to, with no
  /// logic of its own.
  late final ClientScopes scopes = ClientScopes(this);

  final Object _txZoneKey = Object();
  Object? _activeTxToken;

  /// Whether this runs inside this client's own transaction callback. An
  /// ordinary task issued there would wait behind the transaction that waits
  /// for the callback, so every one is refused with `transaction_active`; the
  /// [Transaction]'s own commands are not tasks.
  bool get _inTransaction =>
      _activeTxToken != null &&
      identical(Zone.current[_txZoneKey], _activeTxToken);

  /// One ordinary task, refused inside this client's transaction callback.
  Future<dynamic> _task(
    Map<String, dynamic> command, {
    void Function(dynamic value)? onValue,
  }) => _inTransaction
      ? Future.error(StateError('transaction_active'))
      : _bridge.task(command, onValue: onValue);
  Client._(this._bridge, this.clientId) {
    // What the runtime reports goes to the connection's `onError`.
    _bridge.reports.listen((diagnostic) => _connection?.report(diagnostic));
    // Durable, direct and abandoned calls, after the commit that decided
    // them - including those a drop, a receipt or a page settled.
    _bridge.onCallCompleted = _callCompleted;
    // A transaction's Calls become durable at its commit or end with its
    // rollback, whether or not anybody observes them.
    _bridge.onCallState = _actionObservers.transition;
  }
  static Future<Client> open({
    required String path,
    required Map<String, dynamic> schema,
    String? libraryPath,
    Map<String, dynamic>? migration,

    /// Rebuild at once when the schema is incompatible, leaving unsent work in the old file.
    bool discardPending = false,

    /// Test seam: the carrier to drive instead of the library's C ABI.
    Carrier? carrier,
    Map<String, StoreHook>? onStore,
  }) async {
    final hooks = Map<String, StoreHook>.of(onStore ?? const {});
    late Client client;
    final bridge = await Bridge.open(
      path: path,
      schema: schema,
      libraryPath: libraryPath,
      migration: migration,
      discardPending: discardPending,
      carrier: carrier,
      onStore: {
        for (final entry in hooks.entries)
          entry.key:
              (
                String transactionId,
                List<Map<String, dynamic>> changes,
                StoreCancellation cancellation,
              ) => client._runTransactionBody(
                transactionId,
                _StoreHookInvocation(entry.value, changes).run,
                cancellation,
              ),
      },
    );
    client = Client._(bridge, bridge.opened['clientId'] as String);
    return client;
  }

  /// Rust runs [body] as the callback of a local transaction it owns: ordinary
  /// reads and writes wait until it commits or rolls back, and the result is
  /// returned only once the commit is confirmed.
  Future<T> transaction<T>(Future<T> Function(Transaction tx) body) async {
    if (_inTransaction) throw StateError('transaction_active');
    late T result;
    await _bridge.transaction((transactionId) async {
      result = await _runTransactionBody(transactionId, body);
    });
    return result;
  }

  Future<T> _runTransactionBody<T>(
    String transactionId,
    FutureOr<T> Function(Transaction tx) body, [
    StoreCancellation? cancellation,
  ]) async {
    final tx = Transaction._(this, transactionId);
    cancellation?.onCancel(tx._cancel);
    final token = Object();
    _activeTxToken = token;
    try {
      final result = await runZoned(
        () => Future<T>.sync(() => body(tx)),
        zoneValues: {_txZoneKey: token},
      );
      await tx._finish();
      return result;
    } catch (error, stack) {
      try {
        await tx._finish();
      } catch (_) {}
      Error.throwWithStackTrace(error, stack);
    } finally {
      if (identical(_activeTxToken, token)) _activeTxToken = null;
    }
  }

  Future<Map<String, dynamic>?> read(
    String model,
    Map<String, dynamic> identity,
  ) async =>
      (await _task({
            'kind': 'read',
            'key': {'model': model, 'identity': identity},
          }))
          as Map<String, dynamic>?;
  Future<List<Map<String, dynamic>>> query(
    String model, {
    Map<String, dynamic> where = const {},
  }) async =>
      (await _task({'kind': 'query', 'model': model, 'filter': where}) as List)
          .cast<Map<String, dynamic>>();
  Future<List<Map<String, dynamic>>> readSql(
    String sql, {
    List<dynamic> parameters = const [],
  }) async =>
      (await _task({'kind': 'sql', 'sql': sql, 'parameters': parameters})
              as List)
          .cast<Map<String, dynamic>>();
  Future<List<Map<String, dynamic>>> querySpec(
    String model,
    Map<String, dynamic> query,
  ) async =>
      (await _task({'kind': 'querySpec', 'model': model, 'query': query})
              as List)
          .cast<Map<String, dynamic>>();
  Future<Map<String, dynamic>?> related(
    String model,
    Map<String, dynamic> identity,
    String relation,
  ) async =>
      await _task({
            'kind': 'related',
            'key': {'model': model, 'identity': identity},
            'relation': relation,
          })
          as Map<String, dynamic>?;
  Future<List<Map<String, dynamic>>> referencing(
    String model,
    Map<String, dynamic> identity,
    String source,
    String relation,
  ) async =>
      (await _task({
                'kind': 'referencing',
                'key': {'model': model, 'identity': identity},
                'source': source,
                'relation': relation,
              })
              as List)
          .cast<Map<String, dynamic>>();
  Future<int> mutate(Map<String, dynamic> mutation) =>
      _submitMutation(mutation);

  /// One framework-owned local transaction; no backend work is queued.
  Future<void> direct(Map<String, dynamic> operation) =>
      transaction((tx) => tx.direct(operation));

  Future<Call<T>> invokeAction<T>(
    String name,
    int version,
    Map<String, dynamic> args,
    T Function(dynamic) decode, {
    CallStore? store,
  }) async {
    Call<T>? call;
    final closedBefore = _closed;
    try {
      await submitAction(
        name,
        version,
        args,
        onCommitted: (callId, _) {
          call = _actionObservers.register(callId, decode);
        },
        store: store,
      );
    } catch (error) {
      // Close is priority control: a submission still queued when the client
      // began closing never runs. Its caller gets the handle close gives every
      // call it can no longer observe, as when the submission ran first.
      // Platform-specific: only this client object knows the call began
      // before its own close; the runtime answers `client_closed` either way.
      if (!closedBefore &&
          _closing != null &&
          error is StateError &&
          error.message == 'client_closed') {
        return _actionObservers.register<T>('', decode);
      }
      throw _publicActionError(error);
    }
    return call!;
  }

  Future<T> invokeDirectAction<T>(
    String name,
    int version,
    Map<String, dynamic> args,
    T Function(dynamic) decode, {
    CallStore? store,
  }) async {
    late final Map<String, dynamic> invoked;
    try {
      invoked = await callAction(name, version, args, store: store);
    } catch (error) {
      throw _publicActionError(error);
    }
    return _decodeOutcome(invoked['outcome'] as Map, decode);
  }

  /// Execute a direct Query. Without [once] it is exactly
  /// [invokeDirectAction]: a fresh request that reads and writes no
  /// snapshot. With [once], Rust decides: a saved result is decoded without
  /// any request or Model write, an active request is joined, or a new one
  /// is executed and its successful result saved with its authority.
  /// [refresh] (only with [once]) always requests and replaces on success.
  Future<T> invokeQuery<T>(
    String name,
    int version,
    Map<String, dynamic> args,
    T Function(dynamic) decode, {
    CallStore? store,
    bool once = false,
    bool refresh = false,
  }) async {
    final closedBefore = _closing != null;
    late final Map<String, dynamic> invoked;
    try {
      invoked = await _invoke(name, version, args, store, once, refresh);
    } catch (error) {
      // A once caller the closing client left waiting hears that it closed,
      // as every call close can no longer observe does. Platform-specific: the
      // public error depends on this object's close, not on the runtime.
      if (once &&
          !closedBefore &&
          _closing != null &&
          error is ActionTransportException &&
          error.code == 'action.unavailable') {
        throw CallError('client.closed', cause: error);
      }
      throw _publicActionError(error);
    }
    return _decodeOutcome(invoked['outcome'] as Map, decode);
  }

  /// Fetch one Model by identity through its existing Loader
  /// ([#153](https://github.com/zanminwang/axton/issues/153)). Rust validates
  /// the identity and [store], joins an identical request in flight or sends
  /// a new one, and by default stores the reply before answering; this
  /// submits the task and decodes this caller's own copy of the snapshot.
  /// `null` when the Loader has no readable record. With `store: false` the
  /// snapshot is returned without local storage or onStore.
  Future<T?> fetchModel<T>(
    String model,
    int version,
    Map<String, dynamic> identity,
    T Function(Map<String, dynamic> row) decode, {
    bool store = true,
  }) async {
    late final Map<String, dynamic> fetched;
    try {
      fetched =
          await _task({
                'kind': 'fetch',
                'model': model,
                'version': version,
                'identity': identity,
                if (!store) 'store': false,
              })
              as Map<String, dynamic>;
    } catch (error) {
      throw _fetchError(error);
    }
    return _decodeOutcome(
      fetched['outcome'] as Map,
      (result) => result == null
          ? null
          : decode((result as Map).cast<String, dynamic>()),
    );
  }

  /// Fetch failures refused before any request was sent.
  static const _fetchRejected = {
    'fetch.invalid_options',
    'fetch.schema_pending',
  };

  /// A `fetch` task's failure as a [CallError]: a `fetch.*` code the runtime
  /// decided keeps its cause - the refusing onStore callback's error or the
  /// transport failure with its status. A closed client's admission error
  /// and any other engine error stay as they are.
  Object _fetchError(Object error) {
    if (error is TaskFailure) {
      final code = error.details['code'];
      if (code is String && code.startsWith('fetch.')) {
        return CallError(
          code,
          execution: _fetchRejected.contains(code) ? 'rejected' : 'unknown',
          cause: error.cause ?? _directCause(error.details) ?? error,
        );
      }
    }
    if (error is StateError && error.message == 'transaction_active') {
      return _publicActionError(error);
    }
    return error;
  }

  /// Discard the saved once results of one Query argument set, every store
  /// variant, in a local transaction. Needs no network; an older request
  /// still in flight cannot save its result afterwards.
  Future<void> invalidateQuery(
    String name,
    int version,
    Map<String, dynamic> args,
  ) async {
    try {
      await _task({
        'kind': 'invalidateQueryOnce',
        'name': name,
        'version': version,
        'args': args,
      });
    } catch (error) {
      throw _publicActionError(error);
    }
  }

  T _decodeOutcome<T>(Map outcome, T Function(dynamic) decode) {
    if (outcome['status'] != 'succeeded') {
      throw CallError(
        outcome['code'] as String? ?? 'action.failed',
        execution: outcome['execution'] as String? ?? 'rejected',
      );
    }
    try {
      return decode(outcome['result']);
    } catch (error) {
      throw CallError('action.observation_failed', cause: error);
    }
  }

  /// One task: Rust enqueues the mutation in its own local transaction.
  Future<int> _submitMutation(Map<String, dynamic> mutation) async {
    return await _task({'kind': 'enqueue', 'mutation': mutation}) as int;
  }

  /// Internal Action seam: [onCommitted] runs while the submission's
  /// completion is dispatched, so a `callCompleted` later in the same batch
  /// always finds the handle it registers.
  Future<Map<String, dynamic>> submitAction(
    String name,
    int version,
    Map<String, dynamic> args, {
    void Function(String callId, int ordinal)? onCommitted,
    CallStore? store,
  }) async {
    final wire = store?.toWire();
    return await _task(
          {
            'kind': 'submitAction',
            'name': name,
            'version': version,
            'args': args,
            if (wire != null) 'store': wire,
          },
          onValue: onCommitted == null
              ? null
              : (value) => onCommitted(
                  (value as Map)['callId'] as String,
                  value['ordinal'] as int,
                ),
        )
        as Map<String, dynamic>;
  }

  /// One direct call: the runtime prepares the request, sends it, bounds it
  /// and applies the response; the value is `{outcome}`. A call the runtime
  /// could not complete throws [ActionTransportException] with its code.
  Future<Map<String, dynamic>> callAction(
    String name,
    int version,
    Map<String, dynamic> args, {
    CallStore? store,
  }) => _invoke(name, version, args, store, false, false);

  Future<Map<String, dynamic>> _invoke(
    String name,
    int version,
    Map<String, dynamic> args,
    CallStore? store,
    bool once,
    bool refresh,
  ) async {
    final wire = store?.toWire();
    try {
      return (await _task({
            'kind': 'invoke',
            'name': name,
            'version': version,
            'args': args,
            if (wire != null) 'store': wire,
            if (once) 'once': true,
            if (refresh) 'refresh': true,
          }))
          as Map<String, dynamic>;
    } on StateError catch (error) {
      final details = error is TaskFailure ? error.details : null;
      final code = details?['code'] as String? ?? error.message;
      if (_unknownExecution.contains(code)) {
        throw ActionTransportException(code, _directCause(details));
      }
      if (code == 'store_hook_failed') {
        throw ActionTransportException(
          code,
          error is TaskFailure ? error.cause ?? error : error,
        );
      }
      // The runtime is gone: no call can be made.
      if (error.message == 'client_closed') {
        throw ActionTransportException('action.unavailable', error);
      }
      rethrow;
    }
  }

  /// The cause the runtime gave a direct failure in its `details`: the
  /// transport's message, as an [HttpFailure] when it carried a status.
  static Object? _directCause(Map<String, dynamic>? details) {
    final message = details?['message'];
    if (message is! String) return null;
    final status = details!['status'];
    return status is int
        ? HttpFailure.reported(message, status)
        : StateError(message);
  }

  /// Load handles; the runtime owns every job and publishes its status.
  late final Loads _loads = Loads(_bridge, () => _inTransaction);

  /// Accept a native Load durably
  /// ([#173](https://github.com/zanminwang/axton/issues/173)) and answer its
  /// handle after the local commit; it needs no connection. Rust persists,
  /// schedules and applies every page; [once] and [refresh] are call-site
  /// controls, never sent to the backend.
  Future<Load> startLoad(
    String name,
    int version,
    Map<String, dynamic> args, {
    bool once = false,
    bool refresh = false,
  }) => _loads.start(name, version, args, once: once, refresh: refresh);

  /// Reattach to a job of this replica: a fresh handle, or `null`.
  Future<Load?> getLoad(String id) => _loads.get(id);

  /// The most recently started jobs, newest first; [limit] is 1..100.
  Future<List<LoadStatus>> listLoads({int limit = 50}) =>
      _loads.list(limit: limit);

  /// Remove the once mappings of one Load argument set, offline, in a local
  /// commit.
  Future<void> invalidateLoad(String name, Map<String, dynamic> args) =>
      _loads.invalidate(name, args);

  /// Register durable intent to follow [scope] and answer with its handle. It
  /// resolves when the local transaction commits: it awaits no
  /// authentication, connection or acknowledgement, and the same Scope answers
  /// with the same handle while its registration lives. The socket is never
  /// cancelled here; the Downlink worker sees the committed change and
  /// reconciles its own session.
  Future<Subscription> subscribeScope(String scope) => _inTransaction
      ? Future.error(StateError('transaction_active'))
      : _subscriptions.subscribe(scope);
  Future<Subscription> subscribe(String channel) => subscribeScope(channel);

  /// Remove whatever registration this Scope name has; its handle stops.
  Future<void> unsubscribe(String channel) => _inTransaction
      ? Future.error(StateError('transaction_active'))
      : _subscriptions.unsubscribeScope(channel);

  /// Connect to [server]: the runtime runs both lanes and every direct call
  /// from here on, and this client only executes the effects it asks for.
  /// The runtime refuses a second active connection
  /// (`connection already active`).
  Future<RuntimeConnection> connect(
    SyncServer server, {
    void Function(Object)? onError,
    Future<void> Function()? refreshAuth,
    Duration directTimeout = const Duration(seconds: 30),
  }) async {
    if (_inTransaction) throw StateError('transaction_active');
    final live = ServerSession(server);
    // Close has begun: a connect admitted now would outlive it.
    if (_closing != null) throw StateError('client_closed');
    final connecting = RuntimeConnection.connect(
      host: _bridge,
      network: live,
      onError: onError,
      refreshAuth: refreshAuth,
      directTimeout: directTimeout,
      // While the completion is dispatched: a report later in the same batch
      // already reaches this connection's onError.
      onConnected: (connection) => _connection = connection,
      onClosed: (connection) {
        if (identical(_connection, connection)) _connection = null;
      },
    );
    final settled = connecting.then<void>((_) {}, onError: (Object _) {});
    _connecting.add(settled);
    unawaited(settled.whenComplete(() => _connecting.remove(settled)));
    return await connecting;
  }

  /// Run every pending prerequisite task this client has a handler for. Rust
  /// picks each task and records its outcome; the handler runs as a
  /// `prerequisite` effect.
  Future<void> runPrerequisites(
    Map<String, Future<void> Function(Map<String, dynamic>)> handlers,
  ) => _inTransaction
      ? Future.error(StateError('transaction_active'))
      : _tasks ??= _runPrerequisites(handlers).whenComplete(() {
          _tasks = null;
        });
  Future<void> _runPrerequisites(
    Map<String, Future<void> Function(Map<String, dynamic>)> handlers,
  ) async {
    final handler = prerequisiteHandler(handlers);
    _bridge.handleEffects('prerequisite', handler);
    try {
      await _task({
        'kind': 'runPrerequisites',
        'handlers': handlers.keys.toList(),
      });
    } finally {
      _bridge.stopHandling('prerequisite', handler);
    }
  }

  /// Test seams over the legacy commands: freeze the next push batch, settle
  /// it with a receipt, apply one page. The connection never uses them.
  Future<String?> freeze() async => await _task({'kind': 'freeze'}) as String?;

  /// The runtime announces every completion the receipt settled as
  /// `callCompleted`.
  Future<void> acknowledge(int sequence, Map<String, dynamic> receipt) async {
    await _task({'kind': 'ack', 'sequence': sequence, 'receipt': receipt});
  }

  Future<Map<String, dynamic>> applyPull(Map<String, dynamic> page) async =>
      (await _task({'kind': 'pull', 'page': page})) as Map<String, dynamic>;

  /// One record's sync state: its pending mutations and retained rejections.
  Future<Map<String, dynamic>> recordSyncState(
    String model,
    Map<String, dynamic> identity,
  ) async =>
      await _task({
            'kind': 'recordStatus',
            'key': {'model': model, 'identity': identity},
          })
          as Map<String, dynamic>;

  /// The client's sync state: a local snapshot, not a network probe.
  Future<Map<String, dynamic>> syncState() async =>
      (await _task({'kind': 'status'})) as Map<String, dynamic>;

  /// Leave an incompatible database behind and open a fresh file for the
  /// schema this client asked for. Refused while unsent mutations remain
  /// unless [discardPending]; the report says what the old file keeps:
  /// `oldFile`, `newFile`, `reason`, `leftPending`, `leftDirect`,
  /// `abandonedCalls` and `abandonedLoads`, the IDs of the Load jobs left
  /// behind, whose handles and waiters ended with `load.schema_changed`.
  Future<Map<String, dynamic>> rebuild({bool discardPending = false}) async {
    final report =
        (await _task({'kind': 'rebuild', 'discardPending': discardPending}))
            as Map<String, dynamic>;
    // The runtime ended every handle of the replica left behind and completed
    // every abandoned call before this completion.
    return report;
  }

  Future<List<Map<String, dynamic>>> pendingTasks() async =>
      (await _task({'kind': 'tasks'}) as List).cast<Map<String, dynamic>>();
  Future<void> setReadiness(String key, String state) async {
    await _task({'kind': 'readiness', 'key': key, 'state': state});
  }

  /// The runtime announces the dropped call's completion as `callCompleted`.
  Future<void> drop(int ordinal) async {
    await _task({'kind': 'drop', 'ordinal': ordinal});
  }

  Future<void> dismissRejection(int ordinal) async {
    await _task({'kind': 'dismiss', 'ordinal': ordinal});
  }

  /// The rows of [model] matching [where]: the committed result when the
  /// stream is listened to, then every different result after a commit. The
  /// runtime runs, re-runs and compares the query; this stream only delivers
  /// what it publishes. A query that fails ends the stream with its error; a
  /// later re-run that fails is reported to the connection's `onError` and the
  /// watch stays. Cancelling unwatches; closing the client completes it.
  Stream<List<Map<String, dynamic>>> watch(
    String model, {
    Map<String, dynamic> where = const {},
  }) => Stream<List<Map<String, dynamic>>>.multi((sink) {
    // The watch task is submitted when the stream is listened to.
    if (_inTransaction) {
      sink
        ..addError(StateError('transaction_active'))
        ..close();
      return;
    }
    String? observer;
    var cancelled = false;
    void deliver(Map<String, dynamic> snapshot) {
      // The terminal snapshot carries the rows already delivered.
      if (snapshot['closed'] == true) {
        observer = null;
        sink.close();
        return;
      }
      sink.add((snapshot['rows'] as List).cast<Map<String, dynamic>>());
    }

    _bridge
        .task(
          {
            'kind': 'watch',
            'model': model,
            'spec': {'filter': where},
          },
          onValue: (value) {
            final id = (value as Map)['observerId'] as String;
            // Cancelled before the runtime named the observer.
            if (cancelled) {
              unawaited(_unwatch(id));
              return;
            }
            observer = id;
            _bridge.listen(id, deliver);
          },
        )
        .then<void>(
          (_) {},
          onError: (Object error, StackTrace stack) {
            if (cancelled) return;
            sink.addError(error, stack);
            sink.close();
          },
        );
    sink.onCancel = () {
      cancelled = true;
      final id = observer;
      observer = null;
      if (id == null) return null;
      _bridge.unlisten(id);
      return _unwatch(id);
    };
  });

  /// Stop the runtime publishing [observerId]; a closed runtime already did.
  Future<void> _unwatch(String observerId) async {
    try {
      await _bridge.task({'kind': 'unwatch', 'observerId': observerId});
    } on StateError catch (error) {
      if (error.message != 'client_closed') rethrow;
    }
  }

  Future<void> close() => _closing ??= _finishClose();

  void _abandonConnection() {
    final connection = _connection;
    if (connection != null) abandonConnection(connection);
  }

  /// Close is priority control: the runtime's `close` is submitted before
  /// anything is awaited, so it never waits behind a task - a callback that
  /// holds the transaction, or the connection's `stop` parked behind it. The
  /// runtime cancels every effect and ends the lanes; the connection is only
  /// stopped here, and a connect in flight settles before this completes.
  Future<void> _finishClose() async {
    _actionObservers.close();
    // The runtime stops every handle and watch with a terminal snapshot before
    // it announces its end.
    _subscriptions.closing();
    final closing = _bridge.close();
    try {
      _abandonConnection();
      await Future.wait(_connecting.toList());
      _abandonConnection();
      await closing;
    } finally {
      _closed = true;
      _subscriptions.close();
      _actionObservers.ended();
      await _completions.close();
    }
  }
}

/// The Scope surface of one client: what the generated `scopes` facade
/// delegates to.
class ClientScopes {
  final Client _client;
  const ClientScopes(this._client);
  Future<Subscription> subscribe(String scope) => _client.subscribeScope(scope);
}

/// The runtime's refusal of an outer transaction command issued while a
/// `local` callback runs. A [Transaction] gives it to every command issued
/// while a submission with a callback is unfinished, so it never depends on
/// when the runtime received the command.
const _capability = 'invalid transaction capability';

/// The application callback's handle on the local transaction Rust owns.
/// Its commands carry the runtime's transaction id and the savepoint scope of
/// the zone they are issued from; Rust runs them in submission order and
/// decides commit or rollback: scope checks, failure accounting and the
/// refusal of a poisoned unit are its own. What stays here is what only the
/// language sees - which zone issued a command, whether the callback awaited
/// what it started, and which submissions still run a `local` callback.
class Transaction implements WritePort, SubmitMutationPort {
  final Client _client;
  final String _transactionId;
  bool _open = true;

  /// Submissions with a `local` callback that have not completed.
  int _locals = 0;
  Transaction._(this._client, this._transactionId);
  void _cancel() => _open = false;

  late final channels = TransactionChannels._(this);

  /// Settles once every command submitted so far has settled.
  Future<void> _tail = Future<void>.value();
  int _pending = 0;

  /// The first command failure not undone by a savepoint rollback: what a
  /// savepoint compares to decide it rolls back.
  Object? _failure;

  /// Zone misuse the runtime cannot see: overlapping or unawaited savepoints.
  Object? _structural;
  final Object _zoneKey = Object();
  Object? _active;

  /// The scope Rust answered for each open savepoint's zone token.
  final Map<Object, String> _scopeTokens = {};
  final Set<Future<dynamic>> _scopes = {};

  /// The savepoint scope of [zone]: that of its innermost savepoint, or null
  /// at the top level.
  String? _scopeOf(Zone zone) {
    final token = zone[_zoneKey];
    return token == null ? null : _scopeTokens[token];
  }

  Future<dynamic> _queue(Map<String, dynamic> command, String? scope) => _track(
    _client._bridge.transactionCommand(_transactionId, scope, command),
  );

  Future<dynamic> _track(Future<dynamic> work) {
    _pending++;
    final settled = work.then<void>(
      (_) {
        _pending--;
      },
      onError: (Object error, StackTrace stack) {
        _pending--;
        _failure ??= error;
      },
    );
    _tail = _tail.then((_) => settled);
    return work;
  }

  Future<dynamic> _send(Map<String, dynamic> command) =>
      _refusal() ?? _queue(command, _scopeOf(Zone.current));

  /// Why an outer command is refused before it is submitted, if it is.
  Future<Never>? _refusal() {
    // The callback's Future ended: a late command must not reach the runtime
    // before the callback's result does.
    if (!_open) return Future.error(StateError('transaction_closed'));
    // A `local` callback owns the transaction until its submission completes:
    // a captured or pipelined parent command is refused as the runtime would.
    if (_locals > 0) {
      _structural ??= StateError(_capability);
      return Future.error(StateError(_capability));
    }
    if (_active != null && Zone.current[_zoneKey] != _active) {
      _structural = StateError('overlapping savepoint work');
      return Future.error(_structural!);
    }
    return null;
  }

  /// Queue a named Mutation in this transaction. It completes after the
  /// Mutation's optimism and its [local] callback ran, with a [Call] that
  /// stays provisional until the transaction commits: its `wait` fails with
  /// `transaction_uncommitted` before, and with `transaction_rolled_back` once
  /// a rollback discarded it. [local] runs in this zone; its writes are the
  /// call's local companions. Outer commands are refused until the
  /// submission completed.
  @override
  Future<Call<T>> submitMutation<T>(
    String name,
    int version,
    Map<String, dynamic> args,
    T Function(dynamic) decode, {
    CallStore? store,
    Future<void> Function(WritePort local)? local,
  }) {
    final refused = _refusal();
    if (refused != null) return refused;
    final wire = store?.toWire();
    late final Call<T> call;
    if (local != null) _locals++;
    final work = _track(
      _client._bridge.submitMutation(
        _transactionId,
        _scopeOf(Zone.current),
        {
          'kind': 'submitMutation',
          'name': name,
          'version': version,
          'args': args,
          if (wire != null) 'store': wire,
          if (local != null) 'local': true,
        },
        local: local == null ? null : LocalTransaction._run(local),
        // Routed while the answer is dispatched: the transaction's rollback
        // may follow it in the same batch.
        onValue: (value) => call = _client._actionObservers.register<T>(
          (value as Map)['callId'] as String,
          decode,
          provisional: true,
        ),
      ),
    );
    if (local != null) {
      work.then<void>(
        (_) => _locals--,
        onError: (Object _, StackTrace _) => _locals--,
      );
    }
    return work.then((_) => call);
  }

  /// The callback returned. A command it did not await fails the unit even
  /// if the runtime already ran it: only the language knows it was not
  /// awaited. A failed command it caught is the runtime's to refuse at commit.
  Future<void> _finish() async {
    final outstanding = _pending > 0 || _scopes.isNotEmpty;
    _open = false;
    await _tail;
    if (_structural != null) throw _structural!;
    if (outstanding) throw StateError('unawaited transaction operation');
  }

  Future<Map<String, dynamic>?> read(
    String model,
    Map<String, dynamic> identity,
  ) async =>
      (await _send({
            'kind': 'read',
            'key': {'model': model, 'identity': identity},
          }))
          as Map<String, dynamic>?;
  Future<List<Map<String, dynamic>>> query(
    String model, {
    Map<String, dynamic> where = const {},
  }) async =>
      (await _send({'kind': 'query', 'model': model, 'filter': where}) as List)
          .cast<Map<String, dynamic>>();
  Future<List<Map<String, dynamic>>> readSql(
    String sql, {
    List<dynamic> parameters = const [],
  }) async =>
      (await _send({'kind': 'sql', 'sql': sql, 'parameters': parameters})
              as List)
          .cast<Map<String, dynamic>>();
  Future<List<Map<String, dynamic>>> querySpec(
    String model,
    Map<String, dynamic> query,
  ) async =>
      (await _send({'kind': 'querySpec', 'model': model, 'query': query})
              as List)
          .cast<Map<String, dynamic>>();
  Future<Map<String, dynamic>?> related(
    String model,
    Map<String, dynamic> identity,
    String relation,
  ) async =>
      await _send({
            'kind': 'related',
            'key': {'model': model, 'identity': identity},
            'relation': relation,
          })
          as Map<String, dynamic>?;
  Future<List<Map<String, dynamic>>> referencing(
    String model,
    Map<String, dynamic> identity,
    String source,
    String relation,
  ) async =>
      (await _send({
                'kind': 'referencing',
                'key': {'model': model, 'identity': identity},
                'source': source,
                'relation': relation,
              })
              as List)
          .cast<Map<String, dynamic>>();
  Future<void> direct(Map<String, dynamic> operation) async {
    await _send({'kind': 'direct', 'operation': operation});
  }

  Future<T> savepoint<T>(Future<T> Function() body) {
    if (!_open) return Future.error(StateError('transaction_closed'));
    if (_locals > 0) {
      _structural ??= StateError(_capability);
      return Future.error(StateError(_capability));
    }
    if (_active != null && Zone.current[_zoneKey] != _active) {
      _structural = StateError('overlapping savepoints');
      return Future.error(_structural!);
    }
    final parent = _active;
    final parentScope = _scopeOf(Zone.current);
    final token = Object();
    _active = token;
    final failure = _failure;
    final run = runZoned(() async {
      // The savepoint opens in its parent's scope; Rust answers the scope its
      // own commands, release and rollback carry. A token keeps its scope after
      // it closes, so a late command from its zone is refused by Rust.
      final opened =
          await _queue({'kind': 'savepoint'}, parentScope)
              as Map<String, dynamic>;
      final scope = opened['scope'] as String;
      _scopeTokens[token] = scope;
      try {
        if (!_open) throw StateError('transaction_closed');
        final result = await body();
        await _tail;
        if (!_open) throw StateError('transaction_closed');
        if (_active != token) {
          _structural = StateError('unawaited nested savepoint');
          throw _structural!;
        }
        // A command that failed in this savepoint's body rolls it back even
        // when caught; Rust's `release` does not refuse a poisoned scope, so
        // this choice stays here until it does.
        if (_failure != failure) throw _failure!;
        if (_structural != null) throw _structural!;
        await _queue({'kind': 'release', 'scope': scope}, scope);
        return result;
      } catch (error, stack) {
        await _tail;
        if (_open && _structural == null) {
          if (_active != token) {
            _structural = StateError('unawaited nested savepoint');
            throw _structural!;
          }
          await _queue({'kind': 'rollbackSavepoint', 'scope': scope}, scope);
          _failure = failure;
        }
        Error.throwWithStackTrace(error, stack);
      } finally {
        if (_active == token) _active = parent;
      }
    }, zoneValues: {_zoneKey: token});
    _scopes.add(run);
    unawaited(
      run.then<void>(
        (_) {
          _scopes.remove(run);
        },
        onError: (Object _, StackTrace __) {
          _scopes.remove(run);
        },
      ),
    );
    return run;
  }
}

/// The handle a Mutation's `local` callback receives: local Model reads and
/// direct writes through the callback's own capability, nothing else - no
/// Mutation, Channel, watch or savepoint. Its writes are the submitting call's
/// local companions. It expires when the callback returns; the unawaited-work
/// rule is the transaction's, and a failed command it caught is the
/// runtime's to refuse with the submission.
class LocalTransaction implements WritePort {
  LocalTransaction._(this._submit);
  final Future<dynamic> Function(Map<String, dynamic> command) _submit;
  bool _open = true;
  Future<void> _tail = Future<void>.value();
  int _pending = 0;

  /// Run [callback] as a `local` callback, then apply the transaction's
  /// checks to it.
  static LocalRun _run(Future<void> Function(WritePort local) callback) =>
      (send) async {
        final local = LocalTransaction._(send);
        try {
          await callback(local);
        } catch (error, stack) {
          try {
            await local._finish();
          } catch (_) {}
          Error.throwWithStackTrace(error, stack);
        }
        await local._finish();
      };

  Future<dynamic> _send(Map<String, dynamic> command) {
    if (!_open) return Future.error(StateError('transaction_closed'));
    _pending++;
    final work = _submit(command);
    final settled = work.then<void>(
      (_) {
        _pending--;
      },
      onError: (Object _, StackTrace __) {
        _pending--;
      },
    );
    _tail = _tail.then((_) => settled);
    return work;
  }

  Future<void> _finish() async {
    final outstanding = _pending > 0;
    _open = false;
    await _tail;
    if (outstanding) throw StateError('unawaited transaction operation');
  }

  @override
  Future<Map<String, dynamic>?> read(
    String model,
    Map<String, dynamic> identity,
  ) async =>
      (await _send({
            'kind': 'read',
            'key': {'model': model, 'identity': identity},
          }))
          as Map<String, dynamic>?;
  Future<List<Map<String, dynamic>>> query(
    String model, {
    Map<String, dynamic> where = const {},
  }) async =>
      (await _send({'kind': 'query', 'model': model, 'filter': where}) as List)
          .cast<Map<String, dynamic>>();
  Future<List<Map<String, dynamic>>> readSql(
    String sql, {
    List<dynamic> parameters = const [],
  }) async =>
      (await _send({'kind': 'sql', 'sql': sql, 'parameters': parameters})
              as List)
          .cast<Map<String, dynamic>>();
  @override
  Future<List<Map<String, dynamic>>> querySpec(
    String model,
    Map<String, dynamic> query,
  ) async =>
      (await _send({'kind': 'querySpec', 'model': model, 'query': query})
              as List)
          .cast<Map<String, dynamic>>();
  @override
  Future<Map<String, dynamic>?> related(
    String model,
    Map<String, dynamic> identity,
    String relation,
  ) async =>
      await _send({
            'kind': 'related',
            'key': {'model': model, 'identity': identity},
            'relation': relation,
          })
          as Map<String, dynamic>?;
  @override
  Future<List<Map<String, dynamic>>> referencing(
    String model,
    Map<String, dynamic> identity,
    String source,
    String relation,
  ) async =>
      (await _send({
                'kind': 'referencing',
                'key': {'model': model, 'identity': identity},
                'source': source,
                'relation': relation,
              })
              as List)
          .cast<Map<String, dynamic>>();

  /// A Model write recorded as a local companion of the submitting call.
  @override
  Future<void> direct(Map<String, dynamic> operation) async {
    await _send({'kind': 'direct', 'operation': operation});
  }
}

/// Local Channel intent in a transaction; no live Subscription handle.
class TransactionChannels {
  final Transaction _tx;
  const TransactionChannels._(this._tx);
  Future<void> subscribe(String channel) async {
    await _tx._send({
      'kind': 'channel',
      'channel': channel,
      'subscribed': true,
    });
  }

  Future<void> unsubscribe(String channel) async {
    await _tx._send({
      'kind': 'channel',
      'channel': channel,
      'subscribed': false,
    });
  }
}
