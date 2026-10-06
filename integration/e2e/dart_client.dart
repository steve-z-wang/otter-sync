import 'dart:io';
import 'fixtures/round-trip/generated/generated.dart';

Future<void> main(List<String> args) async {
  final client = await GeneratedClient.open(
    path: '${args[1]}/dart.sqlite',
    stream: 'User:demo-user',
    connection: StoreConnection(
      url: args[0],
      token: () => 'demo-user',
      identity: const StoreIdentity(
        backend: 'round-trip',
        viewer: 'demo-user',
        contract: 'round-trip-v04',
      ),
    ),
    libraryPath: Platform.environment['AXTON_LIBRARY'],
  );
  try {
    await client.bootstrap();
    final initial = await client.models.entry.get(
      const EntryIdentity(id: 'entry-1'),
    );
    if (initial == null) throw StateError('marked Bootstrap missing');
    final call = await client.mutations.editEntry(
      const EditEntryInput(
        entry: EditEntryEntryUpdate(
          id: 'entry-1',
          text: Present(' from Dart '),
        ),
      ),
    );
    final result = await call.wait();
    if (result is! CallSuccess<EditEntryOutput> ||
        result.result.entry.text != 'from Dart')
      throw StateError('canonical receipt');
    if ((await client.models.entry.get(
          const EntryIdentity(id: 'entry-1'),
        ))?.text !=
        'from Dart')
      throw StateError('SQLite settlement');
    print('Dart -> Rust -> HTTP -> Rust -> Prisma -> SQLite: passed');
  } finally {
    await client.close();
  }
}
