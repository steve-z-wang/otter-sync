import 'dart:io';
import 'generated.dart';

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Future<void> main(List<String> args) async {
  final dir = await Directory.systemTemp.createTemp('axton-read-dart-');
  Future<GeneratedClient> open() => GeneratedClient.open(
    path: '${dir.path}/db',
    stream: 'User:dart',
    connection: StoreConnection(
      url: args[0],
      token: () => 'dart',
      identity: const StoreIdentity(
        backend: 'read-e2e',
        viewer: 'dart',
        contract: 'read-v04',
      ),
    ),
    libraryPath:
        Platform.environment['AXTON_LIBRARY'] ??
        Platform.environment['AXTON_DART_LIBRARY'],
  );
  var client = await open();
  stderr.writeln('phase:open');
  try {
    stderr.writeln('phase:read');
    final first = await client.queries.projectItems(
      project: 'dart',
      store: false,
      once: true,
    );
    check(first.items.length == 2, 'Query output');
    check((await client.models.tag.query()).isEmpty, 'store=false');
    stderr.writeln('phase:reopen');
    await client.close();
    client = await open();
    stderr.writeln('phase:pause');
    await client.connection!.pause();
    stderr.writeln('phase:offline-once');
    final saved = await client.queries.projectItems(
      project: 'dart',
      store: false,
      once: true,
    );
    check(saved.items.first.title == first.items.first.title, 'durable once');
    stderr.writeln('phase:resume');
    await client.connection!.resume();
    stderr.writeln('phase:bootstrap');
    await client.bootstrap();
    check((await client.models.item.query()).length == 2, 'Bootstrap');
    stderr.writeln('phase:mutation');
    final call = await client.mutations.renameItem(
      const RenameItemInput(
        item: RenameItemItemUpdate(
          id: 'dart-1',
          title: Present('dart renamed'),
        ),
      ),
    );
    check(await call.wait() is CallSuccess<RenameItemOutput>, 'named Mutation');
    stdout.writeln('Dart bound reads: PASS');
  } finally {
    await client.close();
    await dir.delete(recursive: true);
  }
}
