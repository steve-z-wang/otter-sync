import 'dart:io';
import 'generated.dart';

void check(bool value, String message) { if (!value) throw StateError(message); }
Future<void> main(List<String> args) async {
  final directory = await Directory.systemTemp.createTemp('axton-dart-date-host-');
  final library = Platform.environment['AXTON_DART_LIBRARY'] ?? File('../../target/debug/libaxton_dart.dylib').absolute.path;
  final connection = StoreConnection(url: args.single, token: () => 'dates');
  var client = await GeneratedClient.open(path: '${directory.path}/db', stream: 'User:dates', connection: connection, libraryPath: library);
  try {
    await client.bootstrap();
    final at = DateTime.utc(2026, 9, 23, 12, 34, 56, 123);
    final call = await client.mutations.echo(EchoInput(at: at, moods: [Mood.calm, Mood.loud], maybe: null));
    final outcome = await call.wait();
    check(outcome is CallSuccess<EchoOutput>, 'Echo did not settle');
    final result = (outcome as CallSuccess<EchoOutput>).result;
    check(result.result == at && result.moods.length == 2 && result.maybe == null, 'scalar result codec mismatch');
    final query = await client.queries.now(at: at, store: false);
    check(query.at == at, 'Query DateTime decode mismatch');
    await client.close();
    client = await GeneratedClient.open(path: '${directory.path}/db', stream: 'User:dates', connection: connection, libraryPath: library);
    check((await client.syncState())['pending'] == 0, 'settled call requeued');
    check((await client.queries.now(at: at, store: false)).at == at, 'fresh Query codec changed after reopen');
    stdout.writeln('Dart generated DateTime/enum real host: PASS');
  } finally { await client.close(); await directory.delete(recursive: true); }
}
