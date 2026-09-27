/// Native Load handles
/// ([#173](https://github.com/zanminwang/axton/issues/173)). A handle decides
/// nothing: the Rust runtime persists every job, schedules and batches its
/// pages, projects its status and parks every `wait()` on its run. This file
/// keeps the language objects - a handle's last published status, its streams
/// and its observer route - and maps the runtime's codes to [LoadException].
library;

import 'dart:async';

import 'bridge.dart';

/// Where a job is. `loading` is a page in flight or being applied; `waiting`
/// is offline, paused, backing off or behind a pending rebuild.
enum LoadPhase { pending, loading, waiting, complete, failed, cancelled }

/// A Load operation or `wait()` the runtime refused or failed, and the error a
/// failed or cancelled [LoadStatus] carries.
class LoadException implements Exception {
  final String code;
  final String message;
  const LoadException(this.code, [String? message]) : message = message ?? code;
  @override
  bool operator ==(Object other) =>
      other is LoadException && other.code == code && other.message == message;
  @override
  int get hashCode => Object.hash(code, message);
  @override
  String toString() => message == code ? code : '$code: $message';
}

/// One immutable snapshot of a job, as the runtime projected it. [name] is
/// the schema operation name; [pages] counts committed pages, not rows or a
/// percent; [error] is set only for a failed or cancelled job.
class LoadStatus {
  final String id;
  final String name;
  final int version;
  final LoadPhase phase;
  final int pages;
  final LoadException? error;
  const LoadStatus({
    required this.id,
    required this.name,
    required this.version,
    required this.phase,
    required this.pages,
    this.error,
  });

  /// The runtime's `status` object, in its wire spelling.
  factory LoadStatus.fromJson(Map<String, dynamic> json) {
    final error = json['error'] as Map?;
    return LoadStatus(
      id: json['id'] as String,
      name: json['name'] as String,
      version: json['version'] as int,
      phase: LoadPhase.values.byName(json['phase'] as String),
      pages: json['pages'] as int,
      error: error == null
          ? null
          : LoadException(error['code'] as String, error['message'] as String),
    );
  }
  @override
  bool operator ==(Object other) =>
      other is LoadStatus &&
      other.id == id &&
      other.name == name &&
      other.version == version &&
      other.phase == phase &&
      other.pages == pages &&
      other.error == error;
  @override
  int get hashCode => Object.hash(id, name, version, phase, pages, error);
  @override
  String toString() =>
      'LoadStatus($name v$version $id: ${phase.name}, pages: $pages'
      '${error == null ? '' : ', error: $error'})';
}

/// A runtime failure as the Load API names it: the code the runtime decided
/// (`details.code`), a closed client, or a call from a transaction callback.
/// Anything else stays the engine's error.
Object _loadError(Object error) {
  if (error is TaskFailure) {
    final code = error.details['code'];
    if (code is String) {
      final message = error.details['message'];
      return LoadException(code, message is String ? message : code);
    }
  }
  if (error is StateError &&
      (error.message == 'client_closed' ||
          error.message == 'transaction_active')) {
    return LoadException(error.message);
  }
  return error;
}

/// One process-local handle of a durable job. Several handles may name the
/// same job; each has its own observer, and [dispose] releases only that one.
class Load {
  final String id;
  final Loads _loads;

  /// Unset once disposed or ended by the runtime: nothing more is delivered.
  String? _observerId;
  LoadStatus _snapshot;
  final _sinks = <MultiStreamController<LoadStatus>>[];
  Load._(this.id, this._observerId, this._snapshot, this._loads);

  /// The runtime's last published status. It stays readable after [dispose]
  /// or the client's close. A schema rebuild ends the handle with a `failed`
  /// status whose error is `load.schema_changed`.
  LoadStatus get status => _snapshot;

  /// One `observerChanged` snapshot. A terminal one - the client closed or a
  /// rebuild replaced the replica - is the last; the runtime decides what it
  /// carries: the last status on close, a failed status with the rebuild's
  /// `load.schema_changed` error on rebuild.
  void _apply(Map<String, dynamic> snapshot) {
    if (_observerId == null) return;
    final status = LoadStatus.fromJson(
      (snapshot['status'] as Map).cast<String, dynamic>(),
    );
    // The runtime publishes only changes; the status the answer carried can
    // equal its first snapshot, which is then not delivered again.
    if (status != _snapshot) {
      _snapshot = status;
      for (final sink in _sinks.toList()) {
        sink.add(status);
      }
    }
    if (snapshot['closed'] == true) _end();
  }

  void _end() {
    _observerId = null;
    for (final sink in _sinks.toList()) {
      sink.close();
    }
    _sinks.clear();
  }

  /// The current status, then every distinct change. Dart's stream convention
  /// applies: the current status arrives on the microtask after `listen`. A
  /// disposed or ended handle delivers its last status and completes.
  Stream<LoadStatus> watch() => Stream<LoadStatus>.multi((sink) {
    sink.add(_snapshot);
    if (_observerId == null) {
      sink.close();
      return;
    }
    _sinks.add(sink);
    sink.onCancel = () {
      _sinks.remove(sink);
    };
  });

  /// Completes after the current run's final page committed; throws its
  /// recorded [LoadException] on failure or cancellation.
  Future<void> wait() => _manage('loadWait');

  /// Stop the job; committed pages stay. A no-op for a complete job.
  Future<void> cancel() => _manage('loadCancel');

  /// Read a failed job again from its last committed continuation.
  Future<void> retry() => _manage('loadRetry');

  /// Delete a terminal job; later management calls fail `load.not_found`.
  /// Every management call on a job a schema rebuild abandoned fails
  /// `load.schema_changed`.
  Future<void> forget() => _manage('loadForget');

  Future<void> _manage(String kind) =>
      _loads._task({'kind': kind, 'loadId': id}).then<void>((_) {});

  /// Release this handle's observer. The job continues.
  void dispose() {
    final observerId = _observerId;
    if (observerId == null) return;
    _end();
    _loads._release(observerId);
  }
}

/// The `client.loads` surface: start, reattach, list and invalidate. Every
/// handle is a fresh object with its own observer; none is cached here, so a
/// handle the application drops or disposes is not retained.
class Loads {
  final ObserverHost _host;
  final bool Function() _inTransaction;
  Loads(this._host, this._inTransaction);

  Future<dynamic> _task(
    Map<String, dynamic> command, {
    void Function(dynamic value)? onValue,
  }) async {
    try {
      if (_inTransaction()) throw StateError('transaction_active');
      return await _host.task(command, onValue: onValue);
    } catch (error, stack) {
      Error.throwWithStackTrace(_loadError(error), stack);
    }
  }

  void _release(String observerId) {
    _host.unlisten(observerId);
    unawaited(
      _host
          .task({'kind': 'loadDispose', 'observerId': observerId})
          .then<void>((_) {}, onError: (Object _) {}),
    );
  }

  /// Accept a job durably and answer its handle: after the local commit, with
  /// no connection needed and no data promised yet. [once] and [refresh] are
  /// call-site controls, never sent to the backend.
  Future<Load> start(
    String name,
    int version,
    Map<String, dynamic> args, {
    bool once = false,
    bool refresh = false,
  }) async => (await _open({
    'kind': 'loadStart',
    'name': name,
    'version': version,
    'args': args,
    if (once) 'once': true,
    if (refresh) 'refresh': true,
  }))!;

  /// Reattach to a job of this replica by ID: `null` when there is none.
  Future<Load?> get(String id) => _open({'kind': 'loadGet', 'loadId': id});

  /// The most recently started jobs, newest first; [limit] is 1..100.
  Future<List<LoadStatus>> list({int limit = 50}) async => [
    for (final status
        in await _task({'kind': 'loadList', 'limit': limit}) as List)
      LoadStatus.fromJson((status as Map).cast<String, dynamic>()),
  ];

  /// Remove the once mappings of one operation and business arguments across
  /// its retained versions, in a local commit. No job is cancelled and no
  /// Model deleted; a later once start creates fresh work.
  Future<void> invalidate(String name, Map<String, dynamic> args) async {
    await _task({'kind': 'loadInvalidate', 'name': name, 'args': args});
  }

  /// The handle is claimed while the answer is dispatched, so the observer's
  /// first snapshot, which follows it in the same batch, is already its own.
  Future<Load?> _open(Map<String, dynamic> command) async {
    Load? handle;
    await _task(
      command,
      onValue: (value) {
        if (value == null) return;
        final opened = (value as Map).cast<String, dynamic>();
        final observerId = opened['observerId'] as String;
        final load = Load._(
          opened['loadId'] as String,
          observerId,
          LoadStatus.fromJson(
            (opened['status'] as Map).cast<String, dynamic>(),
          ),
          this,
        );
        _host.listen(observerId, load._apply);
        handle = load;
      },
    );
    return handle;
  }
}
