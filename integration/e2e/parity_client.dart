import 'dart:convert';
import 'dart:io';
import 'fixtures/round-trip/generated/generated.dart';

Future<void> main(List<String> args) async {
  final client = await GeneratedClient.open(
    path: '${args[1]}/dart-parity',
    stream: 'User:demo-user',
    connection: StoreConnection(
      url: args[0],
      token: () => 'demo-user',
    ),
    libraryPath: Platform.environment['AXTON_LIBRARY'],
  );
  try {
    await client.bootstrap();
    final ok = await client.mutations.editEntry(
      const EditEntryInput(
        entry: EditEntryEntryUpdate(id: 'entry-1', text: Present(' parity ')),
      ),
    );
    if (await ok.wait() is! CallSuccess<EditEntryOutput>)
      throw StateError('acceptance');
    final no = await client.mutations.editEntry(
      const EditEntryInput(
        entry: EditEntryEntryUpdate(id: 'entry-1', text: Present('reject')),
      ),
    );
    if (await no.wait() is! CallFailure<EditEntryOutput>)
      throw StateError('rejection');
    await client.transaction((tx) async {
      await tx.models.entry.create(
        const EntryCreate(id: 'local', text: 'device-only', note: null),
      );
    });
    final rows =
        (await client.models.entry.query())
            .map((row) => {'id': row.id, 'text': row.text, 'note': row.note})
            .toList()
          ..sort((a, b) => (a['id'] as String).compareTo(b['id'] as String));
    stdout.writeln('PARITY ${jsonEncode(rows)}');
  } finally {
    await client.close();
  }
}
