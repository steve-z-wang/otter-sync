// Retained Bootstrap lifecycle tests use finite protocol 5 ranges.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'package:axton/axton.dart';
import 'package:test/test.dart';
import 'store_fixture.dart';
import 'protocol4_transport.dart';

void main() {
  late Directory directory;
  late HttpServer server;
  late StreamSubscription<HttpRequest> served;
  late Client client;
  final requests = <Map>[];
  Completer<void>? tailGate;
  late Completer<void> tailEntered;
  const schema = {
    'models': <Object>[],
    'enums': <Object>[],
    'actions': [
      {
        'name': 'Ping',
        'version': 1,
        'kind': 'query',
        'inputs': <Object>[],
        'outputs': <Object>[],
      },
    ],
  };
  Future<Client> open() => Client.open(
    path: '${directory.path}/db',
    schema: schema,
    stream: 'User:viewer',
    connection: offlineStoreConnection(),
    libraryPath: Platform.environment['AXTON_LIBRARY']!,
  );
  Future<void> online(Client value) async {
    await value.connection?.close();
    await value.connect(
      SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'viewer'),
    );
  }

  setUp(() async {
    requests.clear();
    tailGate = null;
    tailEntered = Completer<void>();
    directory = await Directory.systemTemp.createTemp('axton-bootstrap-');
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    served = server.listen((request) async {
      if (request.uri.path != '/sync/pull' &&
          request.uri.path != '/sync/handshake') {
        await answerEmptyBackground(request);
        return;
      }
      final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
      requests.add({...body, 'route': request.uri.path});
      if (body['bootstrap'] == true) {
        if (!tailEntered.isCompleted) tailEntered.complete();
        await tailGate?.future;
      }
      request.response.write(jsonEncode(emptyPull(body)));
      await request.response.close();
    });
    client = await open();
  });
  tearDown(() async {
    if (tailGate case final gate?) {
      if (!gate.isCompleted) gate.complete();
    }
    await client.close();
    await served.cancel();
    await server.close(force: true);
    await directory.delete(recursive: true);
  });
  test(
    'Bootstrap await includes durable start and complete local range',
    () async {
      tailGate = Completer<void>();
      await online(client);
      var complete = false;
      final pending = client.bootstrap().then((_) => complete = true);
      await tailEntered.future.timeout(const Duration(seconds: 5));
      expect(complete, isFalse);
      expect(
        requests.where((x) => x['route'] == '/sync/handshake'),
        hasLength(1),
      );
      tailGate!.complete();
      await pending.timeout(const Duration(seconds: 5));
      expect(complete, isTrue);
      expect(requests.where((x) => x['bootstrap'] == true), hasLength(1));
    },
  );
  test(
    'offline Bootstrap persists registration and resumes after connection',
    () async {
      await client.connection?.close();
      var complete = false;
      final pending = client.bootstrap().then((_) => complete = true);
      await pumpEventQueue();
      expect(complete, isFalse);
      expect(requests, isEmpty);
      await online(client);
      await pending.timeout(const Duration(seconds: 5));
      expect(
        requests.where((x) => x['route'] == '/sync/handshake'),
        hasLength(1),
      );
    },
  );
  test('close ends waiter but reopen resumes exact saved range', () async {
    tailGate = Completer<void>();
    await online(client);
    final outcome = client.bootstrap().then<Object?>(
      (_) => null,
      onError: (Object e) => e,
    );
    await tailEntered.future.timeout(const Duration(seconds: 5));
    final start = requests.singleWhere((x) => x['route'] == '/sync/handshake');
    final tail = requests.singleWhere((x) => x['bootstrap'] == true);
    await client.close();
    expect(await outcome, isA<ClientClosedException>());
    tailGate!.complete();
    tailGate = null;
    client = await open();
    await online(client);
    await client.bootstrap().timeout(const Duration(seconds: 5));
    expect(
      requests.where((x) => x['route'] == '/sync/handshake'),
      hasLength(2),
    );
    final resumed = requests.where((x) => x['bootstrap'] == true).last;
    expect(resumed, tail);
    expect(resumed['storeId'], start['storeId']);
    expect((resumed['after'], resumed['through']), (0, 0));
  });
  test(
    'Bootstrap rejects inside local transaction without registering work',
    () async {
      // A live connection automatically starts Bootstrap before any public
      // registration. Keep this refusal window offline so only this call
      // could register work, rather than racing that background Start.
      await client.connection?.close();
      await client.transaction((_) async {
        await expectLater(
          client.bootstrap().timeout(const Duration(seconds: 5)),
          throwsA(
            isA<StateError>().having(
              (error) => error.message,
              'message',
              'transaction_active',
            ),
          ),
        );
      });
      expect(requests.where((x) => x['route'] == '/sync/handshake'), isEmpty);
      await online(client);
      await client.bootstrap().timeout(const Duration(seconds: 5));
      expect(
        requests
            .where((x) => x['route'] == '/sync/handshake')
            .map((x) => x['callId'])
            .toSet(),
        hasLength(1),
      );
    },
  );
}
