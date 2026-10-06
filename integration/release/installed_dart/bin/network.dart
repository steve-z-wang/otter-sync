// The installed CLI emits generated.dart into this scratch application.
// Neither that file nor this consumer imports a repository SDK or native path.
import 'dart:io';
import 'package:axton/axton.dart' show Client;
import 'generated.dart';

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Future<void> main(List<String> args) async {
  final directory = await Directory.systemTemp.createTemp(
    'axton-installed-net-',
  );
  Client.configureApplicationData(directory.path);
  final connection = StoreConnection(
    url: args.single,
    token: () => 'installed',
    identity: const StoreIdentity(
      backend: 'installed',
      viewer: 'installed',
      contract: 'installed-v04',
    ),
  );
  final path = '${directory.path}/client.sqlite';
  var client = await GeneratedClient.open(
    path: path,
    stream: 'User:installed',
    connection: connection,
  );
  try {
    await client.bootstrap();
    check(
      (await client.models.note.get(const NoteIdentity(id: 'note-1')))?.text ==
          'written offline',
      'Bootstrap did not install historical Stream content',
    );
    final call = await client.mutations.addNote(
      const AddNoteInput(
        note: NoteCreate(id: 'dart-installed', text: ' canonical Dart '),
      ),
    );
    check(
      await call.wait() is CallSuccess<void>,
      'Mutation did not complete after local settlement',
    );
    check(
      (await client.models.note.get(
            const NoteIdentity(id: 'dart-installed'),
          ))?.text ==
          'canonical Dart',
      'Call.wait exposed optimistic rather than canonical content',
    );
    check(
      await client.fetch.note(
            const NoteIdentity(id: 'missing'),
            store: false,
          ) ==
          null,
      'snapshot-only Fetch did not report absence',
    );
    check(
      (await client.fetch.note(const NoteIdentity(id: 'note-2')))?.text ==
          'durable',
      'Fetch did not return canonical content',
    );
    await client.close();
    client = await GeneratedClient.open(
      path: path,
      stream: 'User:installed',
      connection: connection,
    );
    await client.bootstrap();
    check(
      (await client.models.note.get(
            const NoteIdentity(id: 'dart-installed'),
          ))?.text ==
          'canonical Dart',
      'same-file reopen lost committed authority',
    );
    stdout.writeln('installed generated Dart real backend: PASS');
  } finally {
    await client.close();
    await directory.delete(recursive: true);
  }
}
