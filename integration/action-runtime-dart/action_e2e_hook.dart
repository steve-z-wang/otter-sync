import 'dart:io';
import 'package:axton/axton.dart' as sdk;

import '../action-e2e/generated.dart' as app;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

Future<void> main(List<String> args) async {
  final [url, path, libraryPath] = args;
  final server = sdk.SyncServer(url: url, token: () => 'alice');
  var client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath, server: server);
  try {
    await client.mutations.call.addTodo(todo: const app.Todo(id: 'dart-hook-a', title: 'A'));
    await client.mutations.call.addTodo(todo: const app.Todo(id: 'dart-hook-b', title: 'B'));
  } finally {
    await client.close();
  }

  final observed = <String>[];
  final observedIdentities = <String>[];
  client = await app.GeneratedClient.open(
    path: path,
    libraryPath: libraryPath,
    server: server,
    onStore: app.StoreHooks(todo: (tx, changes) async {
      for (final change in changes) {
        observedIdentities.add(change.identity.id);
        if (change.identity.id != 'dart-hook-a') continue;
        if (change is! app.StoreUpsert<app.TodoIdentity, app.Todo>) throw StateError('expected full incoming upsert');
        final before = await tx.models.todo.get(change.identity);
        observed.add('${change.row.title}:${before?.title}');
        await tx.models.todo.update(const app.TodoIdentity(id: 'dart-hook-b'), const app.TodoPatch(title: app.Present('derived B')));
        await tx.scopes.subscribe('dart:derived');
      }
    }),
  );
  final watched = <List<String>>[];
  final watcher = client.models.todo.watch().listen((rows) {
    watched.add(rows.map((row) => '${row.id}:${row.title}').toList());
  });
  try {
    final result = await client.mutations.call.editAndShow(
      todo: const app.EditAndShowTodoUpdate(id: 'dart-hook-a', title: app.Present(' A1 ')),
      shown: 'dart-hook-b',
      store: const app.EditAndShowStore.none(),
    );
    check(result.todo.title == 'B', 'result B must remain the Loader snapshot');
    check(observed.length == 1 && observed.single == 'A1:A', 'mandatory A hook sees incoming row and pre-store view: $observed');
    check(observedIdentities.length == 1 && observedIdentities.single == 'dart-hook-a', 'store:false invoked only A, not output B: $observedIdentities');
    check((await client.models.todo.get(const app.TodoIdentity(id: 'dart-hook-a')))?.title == 'A1', 'input A authority committed');
    check((await client.models.todo.get(const app.TodoIdentity(id: 'dart-hook-b')))?.title == 'derived B', 'derived B committed before success');
    final rows = await client.readSql('SELECT scope FROM axton_subscription WHERE scope = ?', parameters: ['dart:derived']);
    check(rows.length == 1, 'hook subscription intent committed');
    for (var attempt = 0; attempt < 100 && !watched.any((rows) => rows.contains('dart-hook-a:A1') && rows.contains('dart-hook-b:derived B')); attempt++) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    check(watched.any((rows) => rows.contains('dart-hook-a:A1') && rows.contains('dart-hook-b:derived B')), 'watcher observed committed A and derived B');
    check(!watched.any((rows) => rows.contains('dart-hook-a:A1') != rows.contains('dart-hook-b:derived B')), 'watcher observed no partial A/B commit');
    stdout.writeln('Dart generated A/B store hook: passed');
  } finally {
    await watcher.cancel();
    await client.close();
  }
}
