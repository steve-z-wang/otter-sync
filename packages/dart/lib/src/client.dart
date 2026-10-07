import 'actions.dart';
import 'bridge.dart';
import 'connection.dart';
import 'live.dart';
import 'port.dart';
import 'subscriptions.dart';
import 'dart:async';

part 'unsent.dart';

final Object _callbackOwnerKey = Object();

/// Typed generated model APIs delegate to this generic native client.
class Client implements WritePort, SubmitMutationPort {
  /// Initialize the process-wide durable application container before opening
  /// any Store. Android hosts must provide their stable files/support directory.
  /// Reusing the same directory is safe; changing it in a running process is refused.
  static void configureApplicationData(String path, {String? libraryPath}) =>
      Bridge.configureApplicationData(path, libraryPath: libraryPath);

  /// The Rust-owned runtime: it orders every task and owns the database.
  final Bridge _bridge;
  final String clientId;
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
  late final ActionObservers _actionObservers = ActionObservers(
    admit: () {
      if (_inTransaction) throw StateError('transaction_active');
    },
    lookup: (id) async =>
        (await _bridge.task({'kind': 'callCompletion', 'callId': id}))
            as Map<String, dynamic>?,
  );

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

  late final ClientRejections rejections = ClientRejections._(this);

  /// The unsent acts blocked on a terminally failed prerequisite task.
  late final ClientFailures failures = ClientFailures._(this);

  /// The queue of unsettled acts.
  late final ClientOutbound outbound = ClientOutbound._(this);

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
  late final String _stream;
  RuntimeConnection? get connection => _connection;
  static Future<Client> open({
    required String path,
    required Map<String, dynamic> schema,
    required String stream,
    StoreConnection? connection,
    String? libraryPath,
    Carrier? carrier,
    Map<String, PrerequisiteHandler>? prerequisites,
  }) async {
    final required = Map<String, PrerequisiteHandler>.of(
      prerequisites ?? const {},
    );
    final bridge = await Bridge.open(
      path: path,
      schema: schema,
      stream: stream,
      projectionGeneration: connection?.projectionGeneration ?? '1',
      libraryPath: libraryPath,
      carrier: carrier,
      prerequisiteHandlers: required.keys.toList(),
      effects: {
        'timer': timerHandler(),
        if (required.isNotEmpty) 'prerequisite': prerequisiteHandler(required),
      },
    );
    final client = Client._(bridge, bridge.opened['clientId'] as String)
      .._stream = stream;
    try {
      if (connection != null)
        await client.connect(
          connection,
          onError: connection.onError,
          refreshAuth: connection.refreshAuth,
          directTimeout: connection.directTimeout,
        );
    } catch (_) {
      await client.close();
      rethrow;
    }
    return client;
  }

  Future<void> bootstrap() async {
    if (_inTransaction) throw StateError('transaction_active');
    final handle = await _subscriptions.subscribe(_stream);
    await handle.bootstrap();
  }

  Future<void> resetStore({bool discardPending = false}) async {
    await _task({'kind': 'resetStore', 'discardPending': discardPending});
  }

  /// Rust runs [body] as the callback of a local transaction it owns: ordinary
  /// reads and writes wait until it commits or rolls back, and the result is
  /// returned only once the commit is confirmed.
  @override
  Future<Call<T>> submitMutation<T>(
    String name,
    int version,
    Map<String, dynamic>? args,
    T Function(dynamic) decode, {
    Future<Map<String, dynamic>> Function(WritePort tx)? input,
  }) => transaction(
    (tx) => tx.submitMutation(name, version, args, decode, input: input),
  );

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
    FutureOr<T> Function(Transaction tx) body,
  ) async {
    final tx = Transaction._(this, transactionId);
    final token = Object();
    _activeTxToken = token;
    try {
      final result = await runZoned(
        () => Future<T>.sync(() => body(tx)),
        zoneValues: {_txZoneKey: token, _callbackOwnerKey: tx},
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
  Future<void> direct(Map<String, dynamic> operation) =>
      transaction((tx) => tx.direct(operation));

  /// Execute a fresh Query and decode its committed invocation snapshot.
  Future<T> invokeQuery<T>(
    String name,
    int version,
    Map<String, dynamic> args,
    T Function(dynamic) decode, {
    bool? store,
  }) async {
    late final Map<String, dynamic> invoked;
    try {
      invoked = await _invoke(name, version, args, store);
    } catch (error) {
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
  /// snapshot is returned without local cache writes.
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

  /// Internal Action seam: [onCommitted] runs while the submission's
  /// completion is dispatched, so a `callCompleted` later in the same batch
  /// always finds the handle it registers.
  Future<Map<String, dynamic>> _invoke(
    String name,
    int version,
    Map<String, dynamic> args,
    bool? store,
  ) async {
    final wire = store;
    try {
      return (await _task({
            'kind': 'invoke',
            'name': name,
            'version': version,
            'args': args,
            if (wire != null) 'store': wire,
          }))
          as Map<String, dynamic>;
    } on StateError catch (error) {
      final details = error is TaskFailure ? error.details : null;
      final code = details?['code'] as String? ?? error.message;
      if (_unknownExecution.contains(code)) {
        throw ActionTransportException(code, _directCause(details));
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
  }) => _observeRows({
    'kind': 'watch',
    'model': model,
    'spec': {'filter': where},
  });

  /// The rows of read-only [sql] over several Models, with bound
  /// [parameters] ([#184](https://github.com/zanminwang/axton/issues/184)):
  /// the committed result when the stream is listened to, then every
  /// different result after a commit that writes a table the statement
  /// reads. SQLite names those tables; nothing is listed here. Only one
  /// read-only `SELECT` (or `WITH … SELECT`) over Model tables is accepted:
  /// a write or an engine table (`axton_*`) ends the stream with its error,
  /// like any first failure. Errors, cancelling and closing are [watch]'s.
  Stream<List<Map<String, dynamic>>> watchSql(
    String sql, {
    List<dynamic> parameters = const [],
  }) =>
      _observeRows({'kind': 'watchSql', 'sql': sql, 'parameters': parameters});

  /// Register a row observer when listened to and deliver what the runtime
  /// publishes for it.
  Stream<List<Map<String, dynamic>>> _observeRows(
    Map<String, dynamic> command,
  ) => _observe(
    command,
    (snapshot) => (snapshot['rows'] as List).cast<Map<String, dynamic>>(),
  );

  /// Register an observer when listened to and deliver [pick] of each
  /// snapshot the runtime publishes for it: rows for a watch, the items or
  /// count of an unsent-work observer.
  Stream<T> _observe<T>(
    Map<String, dynamic> command,
    T Function(Map<String, dynamic> snapshot) pick,
  ) => Stream<T>.multi((sink) {
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
      // The terminal snapshot carries the result already delivered.
      if (snapshot['closed'] == true) {
        observer = null;
        sink.close();
        return;
      }
      sink.add(pick(snapshot));
    }

    _bridge
        .task(
          command,
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
      _subscriptions.close();
      _actionObservers.ended();
      await _completions.close();
    }
  }
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

  /// Dismiss a refusal as part of this transaction.
  late final TransactionRejections rejections = TransactionRejections._(this);

  /// Retry failed tasks or drop a failed act as part of this transaction:
  /// later commands see the effect, and it commits or rolls back with it.
  late final TransactionFailures failures = TransactionFailures._(this);

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
  Future<Never>? _foreignRefusal() {
    final owner = Zone.current[_callbackOwnerKey];
    if (owner != null && !identical(owner, this)) {
      _structural ??= StateError('foreign transaction scope');
      return Future.error(_structural!);
    }
    return null;
  }

  Future<Never>? _refusal() {
    final foreign = _foreignRefusal();
    if (foreign != null) return foreign;
    // The callback's Future ended: a late command must not reach the runtime
    // before the callback's result does.
    if (!_open) return Future.error(StateError('transaction_closed'));
    // A `local` callback owns the transaction until its submission completes:
    // a captured or pipelined parent command is refused as the runtime would.
    if (_active != null && Zone.current[_zoneKey] != _active) {
      _structural = StateError('overlapping savepoint work');
      return Future.error(_structural!);
    }
    return null;
  }

  /// The refusal of a parent command while a submission with a `local`
  /// callback is unfinished, recorded as structural so the transaction fails
  /// whatever the callback catches.
  Future<Never>? _callbackRefusal() {
    if (_locals == 0) return null;
    _structural ??= StateError(_capability);
    return Future.error(StateError(_capability));
  }

  /// The refusal of a command sent through an expired `local` handle. While
  /// this transaction is open it is the runtime's answer to a stale
  /// capability and poisons the transaction; afterwards the handle is simply
  /// closed.
  StateError _expired() {
    if (!_open) return StateError('transaction_closed');
    _structural ??= StateError(_capability);
    return StateError(_capability);
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
    Map<String, dynamic>? args,
    T Function(dynamic) decode, {
    Future<Map<String, dynamic>> Function(WritePort tx)? input,
  }) {
    final refused = _refusal();
    if (refused != null) return refused;
    late final Call<T> call;
    if (input != null) _locals++;
    final work = _track(
      _client._bridge.submitMutation(
        _transactionId,
        _scopeOf(Zone.current),
        {
          'kind': 'submitMutation',
          'name': name,
          'version': version,
          if (input == null) 'args': args,
          if (input != null) 'local': true,
        },
        local: input == null ? null : LocalTransaction._run(input, _expired),
        // Routed while the answer is dispatched: the transaction's rollback
        // may follow it in the same batch.
        onValue: (value) => call = _client._actionObservers.register<T>(
          (value as Map)['callId'] as String,
          decode,
          provisional: true,
        ),
      ),
    );
    if (input != null) {
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
    final foreign = _foreignRefusal();
    if (foreign != null) return foreign;
    if (!_open) return Future.error(StateError('transaction_closed'));
    final refused = _callbackRefusal();
    if (refused != null) return refused;
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
/// Mutation, Stream, watch or savepoint. Its writes are the submitting call's
/// local companions. It expires when the callback returns: a later command is
/// refused, and poisons the transaction while it is still open. The
/// unawaited-work rule is the transaction's, and a failed command it caught
/// is the runtime's to refuse with the submission.
class LocalTransaction implements WritePort {
  LocalTransaction._(this._submit, this._expired)
    : _owner = Zone.current[_callbackOwnerKey];
  final Object? _owner;
  final Future<dynamic> Function(Map<String, dynamic> command) _submit;

  /// The refusal of a command sent after the callback returned.
  final StateError Function() _expired;
  bool _open = true;
  Future<void> _tail = Future<void>.value();
  int _pending = 0;

  /// Run [callback] as a `local` callback, then apply the transaction's
  /// checks to it. [expired] answers a command sent through its handle after
  /// it returned.
  static LocalRun _run(
    Future<Map<String, dynamic>> Function(WritePort tx) callback,
    StateError Function() expired,
  ) => (send) async {
    final local = LocalTransaction._(send, expired);
    try {
      final input = await callback(local);
      await local._finish();
      return input;
    } catch (error, stack) {
      try {
        await local._finish();
      } catch (_) {}
      Error.throwWithStackTrace(error, stack);
    }
  };

  Future<dynamic> _send(Map<String, dynamic> command) {
    if (!_open) return Future.error(_expired());
    if (Zone.current[_callbackOwnerKey] != null &&
        !identical(Zone.current[_callbackOwnerKey], _owner)) {
      _expired();
      return Future.error(StateError('foreign transaction scope'));
    }
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

/// Local Stream intent in a transaction; no live Subscription handle.
