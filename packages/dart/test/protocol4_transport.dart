// Authored empty protocol carriers for transport tests. No Model authority,
// publication, Loader or settlement decision is implemented by this fixture.
import 'dart:convert';
import 'dart:io';
import 'package:crypto/crypto.dart';

Map<String, Object?> emptyPull(Map body, {int total = 0}) =>
    switch (body['kind']) {
      'start' => {
        'context': body['context'],
        'manifestId': 'transport-${body['callId']}',
        'start': 0,
        'total': total,
      },
      'tail' => {
        'context': body['context'],
        'manifestId': body['manifestId'],
        'head': 0,
      },
      _ => {
        'context': body['context'],
        'pageId': 'empty-${body['callId']}',
        'from': body['after'],
        'to': body['after'],
        'head': body['after'],
        'units': <Object>[],
      },
    };

Map<String, Object?> emptyRead(Map body) => {
  'context': body['context'],
  'completion': {
    'callId': body['callId'],
    'outcome': {'status': 'succeeded', 'result': null},
  },
  'records': <Object>[],
};

String _canonical(Object? value) {
  if (value is Map) {
    final keys = value.keys.cast<String>().toList()..sort();
    return '{${keys.map((key) => '${jsonEncode(key)}:${_canonical(value[key])}').join(',')}}';
  }
  if (value is List) return '[${value.map(_canonical).join(',')}]';
  return jsonEncode(value);
}

Map<String, Object?> emptyMutation(Map body) => {
  'context': body['context'],
  'intentDigest': sha256.convert([
    ...utf8.encode('axton:protocol4:sha256:mutation-intent'),
    0,
    ...utf8.encode(_canonical(body)),
  ]).toString(),
  'completion': {
    'callId': body['callId'],
    'outcome': {'status': 'succeeded', 'result': null},
  },
  'targets': <Object>[],
};

Future<bool> answerEmptyBackground(HttpRequest request) async {
  if (request.uri.path == '/sync/pull') {
    final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
    request.response.write(jsonEncode(emptyPull(body)));
    await request.response.close();
    return true;
  }
  if (request.uri.path == '/sync/live') {
    final socket = await WebSocketTransformer.upgrade(request);
    socket.listen((message) {
      final body = jsonDecode(message as String) as Map;
      socket.add(
        jsonEncode({
          'context': body['context'],
          'cursor': body['cursor'],
          'head': body['cursor'],
        }),
      );
    }, onError: (Object _) {});
    return true;
  }
  return false;
}
