// The retired onStore hook obligation is now an owned typed companion callback.
import 'dart:io';
import 'dart:isolate';
import 'package:axton/axton.dart' as sdk;
import '../action-e2e/generated.dart' as app;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Future<void> main(List<String> args) async {
  final [url, path, library] = args;
  final keepAlive = ReceivePort();
  final client = await app.GeneratedClient.open(
    path: path,
    stream: 'User:alice',
    connection: sdk.StoreConnection(
      url: url,
      token: () => 'alice',
      identity: const sdk.StoreIdentity(
        backend: 'action-e2e',
        viewer: 'alice',
        contract: 'action-v04',
      ),
    ),
    libraryPath: library,
  );
  try {
    await client.bootstrap();
    await (await client.mutations.addTodo(
      const app.AddTodoInput(
        todo: app.Todo(id: 'dart-callback-a', title: 'A'),
      ),
    )).wait();
    await (await client.mutations.addTodo(
      const app.AddTodoInput(
        todo: app.Todo(id: 'dart-callback-b', title: 'B'),
      ),
    )).wait();
    final observed = <List<String>>[];
    final watcher = client.models.composition.watch().listen(
      (rows) => observed.add(rows.map((row) => row.id).toList()),
    );
    final call = await client.mutations.editAndShow.withTransaction((tx) async {
      await tx.models.composition.create(
        const app.Composition(
          id: 'dart-derived',
          title: 'device-only',
          body: 'companion',
        ),
      );
      return const app.EditAndShowInput(
        todo: app.EditAndShowTodoUpdate(
          id: 'dart-callback-a',
          title: app.Present(' A1 '),
        ),
        shown: 'dart-callback-b',
      );
    });
    check(
      await client.models.composition.get(
            const app.CompositionIdentity(id: 'dart-derived'),
          ) !=
          null,
      'companion committed before Call',
    );
    final outcome = await call.wait();
    check(
      outcome is sdk.CallSuccess<app.EditAndShowOutput> &&
          outcome.result.todo.title == 'B',
      'B business snapshot',
    );
    check(
      (await client.models.todo.get(
            const app.TodoIdentity(id: 'dart-callback-a'),
          ))?.title ==
          'A1',
      'canonical A',
    );
    final end = DateTime.now().add(const Duration(seconds: 5));
    while (!observed.any((rows) => rows.contains('dart-derived')) &&
        DateTime.now().isBefore(end)) {
      await Future<void>.delayed(Duration.zero);
    }
    check(
      observed.any((rows) => rows.contains('dart-derived')),
      'observer saw committed companion',
    );
    await watcher.cancel();
    stdout.writeln('Dart typed callback companion: passed');
  } finally {
    await client.close();
    keepAlive.close();
  }
}
