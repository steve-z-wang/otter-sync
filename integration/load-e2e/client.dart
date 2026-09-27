// The generated Dart client against the Load end-to-end backend:
// `dart run client.dart URL PATH LIBRARY`. The backend holds Items dart-1..3.
import 'dart:io';
import 'package:axton/axton.dart' as sdk;

import 'generated.dart' as app;

void check(bool condition, String message) {
  if (!condition) throw StateError(message);
}

/// One Seen row per stored Item, counting committed hook runs.
Future<void> countSeen(app.GeneratedTransaction tx, List<app.StoreChange<app.ItemIdentity, app.Item>> changes) async {
  for (final change in changes) {
    if (change is! app.StoreUpsert<app.ItemIdentity, app.Item>) continue;
    final identity = app.SeenIdentity(id: change.identity.id);
    final seen = await tx.models.seen.get(identity);
    if (seen == null) {
      await tx.models.seen.create(app.Seen(id: change.identity.id, hits: 1));
    } else {
      await tx.models.seen.update(identity, app.SeenPatch(hits: app.Present(seen.hits + 1)));
    }
  }
}

Future<Map<String, int>> seen(app.GeneratedClient client) async =>
    {for (final row in await client.models.seen.query()) row.id: row.hits};

Future<sdk.LoadStatus?> statusOf(app.GeneratedClient client, String id) async {
  for (final status in await client.loads.list(limit: 100)) {
    if (status.id == id) return status;
  }
  return null;
}

Future<void> main(List<String> args) async {
  final [url, path, libraryPath] = args;
  final server = sdk.SyncServer(url: url, token: () => 'alice');
  var hookRuns = 0;
  Future<void> hook(app.GeneratedTransaction tx, List<app.StoreChange<app.ItemIdentity, app.Item>> changes) async {
    hookRuns++;
    await countSeen(tx, changes);
  }

  var client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath, server: server, onStore: app.StoreHooks(item: hook));
  late final String onceId;
  late final String ordinaryId;
  try {
    // An ordinary multi-page Load: three Items in pages of two, then an empty page.
    final load = await client.loads.projectItems(project: 'dart');
    ordinaryId = load.id;
    final statuses = <sdk.LoadStatus>[];
    final watching = load.watch().listen(statuses.add);
    await load.wait();
    final done = await statusOf(client, load.id);
    check(done?.phase == sdk.LoadPhase.complete && done?.pages == 3 && done?.error == null, 'ordinary Load completed in three pages: $done');
    check(done?.name == 'ProjectItems' && done?.version == 1, 'status names the operation: $done');
    for (var attempt = 0; attempt < 200 && statuses.lastOrNull?.phase != sdk.LoadPhase.complete; attempt++) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    await watching.cancel();
    final pages = statuses.map((status) => status.pages).toList();
    for (var n = 1; n < pages.length; n++) {
      check(pages[n] >= pages[n - 1], 'pages never go back: $statuses');
    }
    check(statuses.last.phase == sdk.LoadPhase.complete && statuses.last.pages == 3, 'watch ended complete: $statuses');
    final items = await client.models.item.query(where: const app.ItemFilter(project: app.Present('dart')));
    check(items.length == 3, 'three Items stored: $items');
    check((await seen(client)).values.every((hits) => hits == 1), 'onStore ran once per Item');

    // A once start is a new job (ordinary runs register nothing); a second once start joins it.
    final once = await client.loads.projectItems(project: 'dart', once: true);
    onceId = once.id;
    check(once.id != load.id, 'once did not reuse the ordinary job');
    final joined = await client.loads.projectItems(project: 'dart', once: true);
    check(joined.id == once.id, 'a second once start shares the job');
    await once.wait();
    await joined.wait();
    joined.dispose();
    load.dispose();
  } finally {
    await client.close();
  }

  // Offline: the completed once job is returned without a request or hook run.
  client = await app.GeneratedClient.open(path: path, libraryPath: libraryPath, onStore: app.StoreHooks(item: hook));
  try {
    final hooksBefore = hookRuns;
    final seenBefore = await seen(client);
    final hit = await client.loads.projectItems(project: 'dart', once: true);
    check(hit.id == onceId, 'offline once hit returns the completed job');
    check(hit.status.phase == sdk.LoadPhase.complete, 'the hit is complete: ${hit.status}');
    await hit.wait();
    check(hookRuns == hooksBefore, 'a complete hit runs no onStore');
    check((await seen(client)).toString() == seenBefore.toString(), 'and writes nothing');
    // Invalidation is local; the next once start is new work, waiting offline.
    await client.loads.invalidate.projectItems(project: 'dart');
    final fresh = await client.loads.projectItems(project: 'dart', once: true);
    check(fresh.id != onceId, 'invalidation made the next once start new');
    check(fresh.status.phase == sdk.LoadPhase.waiting, 'new work waits offline: ${fresh.status}');
    try {
      await client.loads.projectItems(project: 'dart', refresh: true);
      check(false, 'refresh without once must be refused');
    } on sdk.LoadException catch (error) {
      check(error.code == 'load.invalid_options', 'refresh without once: ${error.code}');
    }
    await fresh.cancel();
    final cancelled = await statusOf(client, fresh.id);
    check(cancelled?.phase == sdk.LoadPhase.cancelled, 'cancelled: $cancelled');
    // Reattach and clean up a terminal job by ID.
    final reattached = await client.loads.get(ordinaryId);
    check(reattached?.status.phase == sdk.LoadPhase.complete, 'reattached ordinary job is complete');
    await reattached!.forget();
    check(await client.loads.get(ordinaryId) == null, 'a forgotten job is gone');
    try {
      await reattached.wait();
      check(false, 'a forgotten handle must fail');
    } on sdk.LoadException catch (error) {
      check(error.code == 'load.not_found', 'forgotten handle: ${error.code}');
    }
    stdout.writeln('Dart generated Loads: passed');
  } finally {
    await client.close();
  }
}
