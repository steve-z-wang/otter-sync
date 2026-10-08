// Authored empty protocol carriers for transport tests. No Model authority,
// publication, Loader or settlement decision is implemented by this fixture.
import 'dart:convert';
import 'dart:io';
import 'package:crypto/crypto.dart';

Map<String, Object?> context05(Map body) => {
  for (final key in ['protocol', 'storeId', 'stream', 'materialization'])
    key: body[key],
};
String _hash05(String domain, Object value) => sha256.convert([
  ...utf8.encode(domain),
  0,
  ...utf8.encode(_canonical(value)),
]).toString();
Map<String, Object?> emptyHandshake(Map body) => {
  'protocol': 5,
  'storeId': body['storeId'],
  'stream': body['stream'],
  'head': 0,
};
Map<String, Object?> delivery05(Map body, [List<Object?> changes = const []]) {
  final unit = {'index': 0, 'through': body['through'], 'changes': changes};
  final manifest = {
    'through': body['through'],
    'minimumCursor': changes.isEmpty
        ? null
        : changes
              .map((c) => (c as Map)['cursor'] as int)
              .reduce((a, b) => a < b ? a : b),
    'digest': _hash05('axton:delivery-unit:5', unit),
    'parts': [
      _hash05('axton:delivery-part:5', {
        'unit': 0,
        'part': 0,
        'changes': changes,
      }),
    ],
  };
  final header = <String, Object?>{
    ...context05(body),
    'planId': 'transport-${body['storeId']}',
    'bootstrap': body['bootstrap'],
    'owner': null,
    'after': body['after'],
    'through': body['through'],
    'observedHead': body['through'],
    'expiresAt': DateTime.now().millisecondsSinceEpoch + 300000,
    'units': [manifest],
  };
  header['digest'] = _hash05('axton:delivery-plan:5', header);
  return {
    'header': header,
    'parts': [
      {
        'planId': header['planId'],
        'planDigest': header['digest'],
        'unit': 0,
        'part': 0,
        'changes': changes,
      },
    ],
  };
}

Map<String, Object?> read05(Map body, Object? result, List<Object?> records) =>
    {
      ...context05(body),
      'requestId': body['requestId'],
      'outcome': {'kind': 'succeeded', 'result': result},
      'records': records,
    };

Map<String, Object?> emptyPull(Map body) => body.containsKey('materialization')
    ? delivery05(body)
    : emptyHandshake(body);

Map<String, Object?> emptyRead(Map body) => read05(
  body,
  null,
  body['invocation']['kind'] == 'fetch'
      ? [
          {'key': body['invocation']['key'], 'cursor': null, 'state': null},
        ]
      : [],
);

String _canonical(Object? value) {
  if (value is Map) {
    final keys = value.keys.cast<String>().toList()..sort();
    return '{${keys.map((key) => '${jsonEncode(key)}:${_canonical(value[key])}').join(',')}}';
  }
  if (value is List) return '[${value.map(_canonical).join(',')}]';
  return jsonEncode(value);
}

Map<String, Object?> emptyMutation(Map body) => {
  ...context05(body),
  'batchId': body['batchId'],
  'digest': body['digest'],
  'results': [
    for (final mutation in body['mutations'])
      {
        'mutationId': mutation['id'],
        'outcome': {
          'kind': 'accepted',
          'syncCursor': 0,
          'result': null,
          'targets': <Object>[],
        },
      },
  ],
};

Future<bool> answerEmptyBackground(HttpRequest request) async {
  if (request.uri.path == '/sync/pull' ||
      request.uri.path == '/sync/handshake') {
    final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
    request.response.write(jsonEncode(emptyPull(body)));
    await request.response.close();
    return true;
  }
  if (request.uri.path == '/sync/live') {
    final socket = await WebSocketTransformer.upgrade(request);
    socket.listen((message) {
      final body = jsonDecode(message as String) as Map;
      socket.add(jsonEncode(emptyHandshake(body)));
    }, onError: (Object _) {});
    return true;
  }
  return false;
}
