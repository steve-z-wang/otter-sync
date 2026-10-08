import 'dart:convert';
import 'dart:io';
import 'package:axton/src/connection.dart';
import 'package:axton/src/live.dart';
import 'package:test/test.dart';
import 'connection_test.dart' show FakeHost;

void main() {
  test(
    'protocol5 Handshake and Materialize effects preserve route and body',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final seen = <List<String>>[];
      server.listen((request) async {
        seen.add([
          request.uri.path,
          await utf8.decoder.bind(request).join(),
          request.headers.value('authorization')!,
        ]);
        request.response.write('native answer');
        await request.response.close();
      });
      final host = FakeHost();
      final connection = await RuntimeConnection.connect(
        host: host,
        network: ServerSession(
          SyncServer(
            url: 'http://127.0.0.1:${server.port}',
            token: () => 'credential',
          ),
        ),
      );
      try {
        for (final route in ['handshake', 'materialize']) {
          final id = host.effect({
            'kind': 'http',
            'route': route,
            'body': 'frozen',
          });
          final outcome = await host.answerOf(id);
          expect(outcome['ok'], true, reason: outcome.toString());
          expect(outcome['value'], 'native answer');
        }
        expect(seen, [
          ['/sync/handshake', 'frozen', 'Bearer credential'],
          ['/sync/materialize', 'frozen', 'Bearer credential'],
        ]);
      } finally {
        await connection.close();
        await server.close(force: true);
      }
    },
  );
}
