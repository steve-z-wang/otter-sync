import 'package:axton/axton.dart';
import 'package:test/test.dart';
import 'fake_carrier.dart';

void main() {
  test(
    'offline open sends protocol 5 without identity or connection task',
    () async {
      final carrier = FakeCarrier();
      final client = await Client.open(
        path: 'unused',
        schema: {},
        stream: 'User:u',
        carrier: carrier,
      );
      expect(carrier.openedRequest['protocol'], 5);
      expect(carrier.openedRequest['stream'], 'User:u');
      expect(carrier.openedRequest['projectionGeneration'], '1');
      expect(carrier.openedRequest.containsKey('binding'), false);
      expect(carrier.commands, isEmpty);
      expect(client.connection, isNull);
      await client.close();
    },
  );
  test(
    'identical Query invocations independently submit store-only tasks',
    () async {
      final carrier = FakeCarrier((input) {
        if (input['type'] == 'close') return null;
        return [
          completed(input['requestId'] as String, {
            'outcome': {
              'status': 'succeeded',
              'result': {'value': 1},
            },
          }),
        ];
      });
      final client = await Client.open(
        path: 'unused',
        schema: {},
        stream: 'User:u',
        carrier: carrier,
      );
      for (final store in [null, true, false]) {
        final result = await client.invokeQuery(
          'Read',
          1,
          {'once': true, 'refresh': false},
          (x) => x,
          store: store,
        );
        expect(result, {'value': 1});
      }
      expect(carrier.commands.length, 3);
      expect(carrier.commands.map((x) => x['store']), [null, true, false]);
      expect(
        carrier.commands.every(
          (x) => !x.containsKey('once') && !x.containsKey('refresh'),
        ),
        true,
      );
      await client.close();
    },
  );
}
