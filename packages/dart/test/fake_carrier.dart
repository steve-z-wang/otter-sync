// A carrier without a runtime: it records every envelope a Bridge admits and
// publishes the events the test decides, one drained batch at a time.
import 'dart:async';
import 'dart:convert';

import 'package:axton/src/bridge.dart';

class FakeCarrier implements Carrier {
  /// The events each admitted envelope publishes as one batch; `close` ends
  /// the runtime unless this says otherwise. The open answers
  /// `{clientId: 'fake'}`.
  FakeCarrier([this.answer]);
  final List<Map<String, dynamic>>? Function(Map<String, dynamic> envelope)?
  answer;

  /// Every envelope admitted after the open request, in order.
  final admitted = <Map<String, dynamic>>[];
  final _events = <Map<String, dynamic>>[];
  late final void Function(int runtime) _wake;
  static int _runtimes = 1 << 40;
  final int runtime = ++_runtimes;
  bool _detached = false;

  @override
  (int, String?) open(String request, void Function(int runtime) wake) {
    _wake = wake;
    final envelope = jsonDecode(request) as Map<String, dynamic>;
    publish([
      completed(envelope['requestId'] as String, {'clientId': 'fake'}),
    ]);
    return (runtime, null);
  }

  @override
  String? submit(int runtime, String message) {
    if (_detached) return 'client_closed';
    final envelope = jsonDecode(message) as Map<String, dynamic>;
    admitted.add(envelope);
    final events = answer?.call(envelope);
    if (events != null) {
      publish(events);
    } else if (envelope['type'] == 'close') {
      publish([
        {'type': 'runtimeClosed'},
      ]);
    }
    return null;
  }

  /// Publish [events] as one batch and wake the bridge, as the actor does.
  void publish(List<Map<String, dynamic>> events) {
    _events.addAll(events);
    scheduleMicrotask(() {
      if (!_detached) _wake(runtime);
    });
  }

  @override
  List<dynamic> drain(int runtime) {
    final batch = jsonDecode(jsonEncode(_events)) as List<dynamic>;
    _events.clear();
    return batch;
  }

  @override
  void detach(int runtime) => _detached = true;

  /// The commands of the admitted tasks and transaction commands, in order.
  List<Map<String, dynamic>> get commands => [
    for (final envelope in admitted)
      if (envelope['command'] case final Map<String, dynamic> command) command,
  ];
}

/// A successful `taskCompleted` of [requestId].
Map<String, dynamic> completed(String requestId, [Object? value]) => {
  'type': 'taskCompleted',
  'requestId': requestId,
  'ok': true,
  'value': value,
};
