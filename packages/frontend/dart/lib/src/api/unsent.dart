part of 'client.dart';

// Unsent work, account-wide: the acts the server refused, with the act as
// submitted, the queued acts blocked on a terminally failed prerequisite
// task, and the pending count, each as a stream, with their resolutions on
// the client and inside `client.transaction` (#186, #205, #204). The runtime
// reads, re-reads after every commit and compares; these streams only
// deliver what it publishes, with the lifecycle of `Client.watch`.

Map<String, dynamic>? _record(Object? value) =>
    value == null ? null : (value as Map).cast<String, dynamic>();

/// One Model operation of an act, with its values.
class ActOperation {
  final String model;

  /// `create`, `update` or `delete`.
  final String op;
  final Map<String, dynamic> identity;
  final Map<String, dynamic>? values;
  const ActOperation({
    required this.model,
    required this.op,
    required this.identity,
    this.values,
  });
  factory ActOperation.fromRecord(Map<String, dynamic> record) => ActOperation(
    model: record['model'] as String,
    op: record['op'] as String,
    identity: _record(record['identity'])!,
    values: _record(record['values']),
  );
}

/// A named act as submitted: normalized arguments with fresh-create defaults
/// already folded in, and its declared Model operations.
/// Local companions and cascade effects are not part of it.
class SubmittedAct {
  final Map<String, dynamic>? args;
  final List<ActOperation> operations;
  const SubmittedAct({required this.args, required this.operations});
  factory SubmittedAct.fromRecord(Map<String, dynamic> record) => SubmittedAct(
    args: _record(record['args']),
    operations: [
      for (final op in (record['operations'] as List? ?? const []))
        ActOperation.fromRecord(_record(op)!),
    ],
  );
}

/// A retained refusal. [id] is the act's ordinal.
class RefusedAct {
  final int id;
  final String name;
  final int version;
  final String code;
  final SubmittedAct act;
  const RefusedAct({
    required this.id,
    required this.name,
    required this.version,
    required this.code,
    required this.act,
  });
  factory RefusedAct.fromRecord(Map<String, dynamic> record) => RefusedAct(
    id: record['id'] as int,
    name: record['name'] as String,
    version: record['version'] as int,
    code: record['code'] as String,
    act: SubmittedAct.fromRecord(_record(record['act'])!),
  );
}

/// A prerequisite task that failed terminally. [name] and [arguments] are
/// those of a schema-derived key, `null` for an opaque one.
class FailedTask {
  final String key;
  final String? name;
  final Map<String, dynamic>? arguments;
  final String error;
  const FailedTask({
    required this.key,
    required this.name,
    required this.arguments,
    required this.error,
  });
  factory FailedTask.fromRecord(Map<String, dynamic> record) => FailedTask(
    key: record['key'] as String,
    name: record['name'] as String?,
    arguments: _record(record['arguments']),
    error: record['error'] as String,
  );
}

/// A queued act blocked on at least one failed task.
class FailedAct {
  final int ordinal;
  final String name;
  final int version;
  final SubmittedAct act;
  final List<FailedTask> tasks;
  const FailedAct({
    required this.ordinal,
    required this.name,
    required this.version,
    required this.act,
    required this.tasks,
  });
  factory FailedAct.fromRecord(Map<String, dynamic> record) => FailedAct(
    ordinal: record['ordinal'] as int,
    name: record['name'] as String,
    version: record['version'] as int,
    act: SubmittedAct.fromRecord(_record(record['act'])!),
    tasks: [
      for (final task in (record['tasks'] as List? ?? const []))
        FailedTask.fromRecord(_record(task)!),
    ],
  );
}

/// `client.rejections`: the refusals retained until dismissed.
class ClientRejections {
  final Client _client;
  ClientRejections._(this._client);

  /// The retained refusals, oldest first: the current list when the stream
  /// is listened to, then every different list after a commit.
  Stream<List<RefusedAct>> watch() => _client._observe(
    {'kind': 'unsentWatch', 'view': 'rejections'},
    (snapshot) => [
      for (final item in snapshot['items'] as List)
        RefusedAct.fromRecord(_record(item)!),
    ],
  );

  /// One retained refusal, or `null`.
  Future<RefusedAct?> get(int id) async {
    final value = await _client._task({'kind': 'rejectionGet', 'id': id});
    return value == null ? null : RefusedAct.fromRecord(_record(value)!);
  }

  /// Remove a retained refusal; it is not retried.
  Future<void> dismiss(int id) async {
    await _client._task({'kind': 'dismiss', 'ordinal': id});
  }
}

/// `client.failures`: the acts blocked on a failed prerequisite task.
class ClientFailures {
  final Client _client;
  ClientFailures._(this._client);

  /// The failed acts, oldest first: the current list, then every different
  /// list after a commit.
  Stream<List<FailedAct>> watch() => _client._observe(
    {'kind': 'unsentWatch', 'view': 'failures'},
    (snapshot) => [
      for (final item in snapshot['items'] as List)
        FailedAct.fromRecord(_record(item)!),
    ],
  );

  /// Make the tasks pending again, for every act waiting on them; the
  /// handlers registered at open run them.
  Future<void> retry(List<String> taskKeys) async {
    await _client._task({'kind': 'retryTasks', 'keys': taskKeys});
  }

  /// Remove an unsent act and its optimism, recording no refusal for it.
  /// Acts that depended on its records are refused and appear in
  /// `rejections`. Its [Call] completes as `dropped`.
  Future<void> drop(int ordinal) async {
    await _client._task({'kind': 'discard', 'ordinal': ordinal});
  }
}

/// `client.outbound`: the queue of unsettled acts.
class ClientOutbound {
  final Client _client;
  ClientOutbound._(this._client);

  /// The number of queued, unsettled acts: now, then every different count.
  Stream<int> watchPending() => _client._observe({
    'kind': 'unsentWatch',
    'view': 'pending',
  }, (snapshot) => snapshot['count'] as int);
}

/// `tx.rejections`: dismiss a refusal as part of the transaction.
class TransactionRejections {
  final Transaction _tx;
  TransactionRejections._(this._tx);
  Future<void> dismiss(int id) async {
    await _tx._send({'kind': 'dismiss', 'ordinal': id});
  }
}

/// `tx.failures`: resolve a failed act as part of the transaction. Each
/// resolution takes effect for the rest of the callback and commits or rolls
/// back with it; a dropped act's [Call] completes, and a retried task's
/// handler runs, only once the transaction commits.
class TransactionFailures {
  final Transaction _tx;
  TransactionFailures._(this._tx);
  Future<void> retry(List<String> taskKeys) async {
    await _tx._send({'kind': 'retryTasks', 'keys': taskKeys});
  }

  Future<void> drop(int ordinal) async {
    await _tx._send({'kind': 'discard', 'ordinal': ordinal});
  }
}
