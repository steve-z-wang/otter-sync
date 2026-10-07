// Generated facade acceptance. Transport fixtures author finite protocol-5 carriers;
// real-host canonical Loader/settlement semantics are checked by action-e2e.
import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:test/test.dart';
import 'generated.dart';
import 'package:axton/axton.dart' show WritePort, SubmitMutationPort;
import '../../packages/dart/test/store_fixture.dart';
import '../../packages/dart/test/protocol4_transport.dart';

const id = '123e4567-e89b-42d3-a456-426614174000';
final row = Entry(
  id: id,
  title: 'hello',
  note: null,
  at: DateTime.utc(2026),
  tags: const ['x'],
  status: Status.active,
);
const tricky = 'q \'single\' "double" \'\'\' """ \$dollar \${x} \\ back\nline';
Future<GeneratedClient> _open(
  Directory directory, {
  StoreConnection? connection,
  String stream = 'User:viewer',
}) => GeneratedClient.open(
  path: '${directory.path}/state.sqlite',
  stream: stream,
  connection: connection ?? offlineStoreConnection(),
  libraryPath: Platform.environment['AXTON_DART_LIBRARY']!,
);

void main() {
  test(
    'concrete records implement inherited fields without inheriting input or identity types',
    () {
      final EntryFields fields = Entry(
        id: '123e4567-e89b-42d3-a456-426614174001',
        title: 'hello',
        note: null,
        at: DateTime.utc(2026),
        tags: const ['x'],
        status: Status.active,
      );
      final IdentifiedFields identified = fields;
      expect(identified.id, fields.id);
      expect(fields.at, DateTime.utc(2026));
      final DraftFields draft = Draft(
        id: '123e4567-e89b-42d3-a456-426614174002',
        body: 'draft',
        mood: Mood.busy,
        created: DateTime.utc(2026),
        note: null,
        memo: null,
      );
      expect(draft.body, 'draft');
    },
  );
  test(
    'create defaults fill omitted fields and round-trip escaped strings',
    () async {
      final draft = (schema['models'] as List)
          .cast<Map<String, dynamic>>()
          .singleWhere((m) => m['name'] == 'Draft');
      final body = (draft['fields'] as List)
          .cast<Map<String, dynamic>>()
          .singleWhere((f) => f['name'] == 'body');
      expect(body['createDefault'], {'kind': 'literal', 'value': tricky});
      expect(
        const DraftCreate(memo: null).toCreateRecord(),
        {'memo': null},
        reason: 'omission is not encoded',
      );
      expect(
        const DraftCreate(memo: null, note: Present(null)).toCreateRecord(),
        {'note': null, 'memo': null},
      );
      final temp = await Directory.systemTemp.createTemp(
        'generated-api-defaults-',
      );
      final client = await _open(temp);
      try {
        final before = DateTime.now().toUtc().subtract(
          const Duration(seconds: 5),
        );
        await client.transaction((tx) async {
          await tx.models.draft.create(const DraftCreate(memo: null));
          await tx.models.draft.create(
            const DraftCreate(
              memo: 'explicit',
              body: 'mine',
              note: Present(null),
            ),
          );
        });
        await client.models.draft.create(const DraftCreate(memo: 'direct'));
        final rows = await client.models.draft.query();
        expect(rows.length, 3);
        final uuid = RegExp(
          r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
        );
        expect(
          rows.map((r) => r.id).toSet().length,
          3,
          reason: 'each create generates its own id',
        );
        for (final row in rows) {
          expect(uuid.hasMatch(row.id), isTrue, reason: row.id);
          expect(row.created.isAfter(before), isTrue);
          expect(row.mood, Mood.busy);
        }
        final defaulted = rows.singleWhere((r) => r.memo == null);
        expect(defaulted.body, tricky);
        expect(defaulted.note, 'n');
        final explicit = rows.singleWhere((r) => r.memo == 'explicit');
        expect(explicit.body, 'mine');
        expect(
          explicit.note,
          isNull,
          reason: 'an explicit null is kept, not defaulted',
        );
        // A complete record remains a valid create input.
        final copy = Draft(
          id: '123e4567-e89b-42d3-a456-426614174001',
          body: 'full',
          mood: Mood.calm,
          created: DateTime.utc(2020),
          note: null,
          memo: null,
        );
        await client.transaction((tx) => tx.models.draft.create(copy));
        expect(
          (await client.models.draft.get(DraftIdentity(id: copy.id)))?.body,
          'full',
        );
      } finally {
        await client.close();
        await temp.delete(recursive: true);
      }
    },
  );
  test(
    'a microsecond DateTime reads back as its UTC millisecond truncation',
    () async {
      final micro = DateTime.utc(2026, 9, 28, 12, 34, 56, 789, 123);
      final milli = DateTime.utc(2026, 9, 28, 12, 34, 56, 789);
      final local = DateTime(2026, 9, 28, 14, 0, 0, 0, 456);
      expect(
        micro.toAxtonPrecision(),
        milli,
        reason: 'the helper is re-exported by generated code',
      );
      expect(
        Placement(shelf: 's', at: micro, label: 'x').toRecord()['at'],
        '2026-09-28T12:34:56.789Z',
      );
      expect(
        PlacementIdentity(shelf: 's', at: micro).toRecord()['at'],
        '2026-09-28T12:34:56.789Z',
      );
      expect(
        EntryPatch(at: Present(micro)).toRecord()['at'],
        '2026-09-28T12:34:56.789Z',
      );
      expect(
        EntryFilter(at: Present(micro)).toRecord()['at'],
        '2026-09-28T12:34:56.789Z',
      );
      final temp = await Directory.systemTemp.createTemp(
        'generated-api-precision-',
      );
      final client = await _open(temp);
      try {
        await client.transaction((tx) async {
          await tx.models.placement.create(
            Placement(shelf: 's', at: micro, label: 'utc'),
          );
          await tx.models.entry.create(
            Entry(
              id: id,
              title: 't',
              note: null,
              at: local,
              tags: const [],
              status: Status.active,
            ),
          );
        });
        // A DateTime identity: the microsecond value and its truncation name one record.
        final placed = await client.models.placement.get(
          PlacementIdentity(shelf: 's', at: micro),
        );
        expect(placed?.at, milli);
        expect(placed!.at.isUtc, isTrue);
        expect(
          await client.models.placement.get(
            PlacementIdentity(shelf: 's', at: milli),
          ),
          isNotNull,
        );
        expect(
          (await client.models.placement.query(
            where: PlacementFilter(at: Present(micro)),
          )).single.label,
          'utc',
        );
        await client.models.placement.update(
          PlacementIdentity(shelf: 's', at: micro),
          const PlacementPatch(label: Present('moved')),
        );
        expect(
          (await client.models.placement.query()).single.label,
          'moved',
          reason: 'the update found the same record',
        );
        // A local DateTime reads back as the same instant in UTC.
        final entry = await client.models.entry.get(
          const EntryIdentity(id: id),
        );
        expect(entry!.at, local.toAxtonPrecision());
        expect(entry.at, DateTime(2026, 9, 28, 14).toUtc());
        expect(entry.at.isUtc, isTrue);
        expect(
          entry.at == local,
          isFalse,
          reason: 'Dart == compares isUtc and microseconds too',
        );
        await client.models.entry.update(
          const EntryIdentity(id: id),
          EntryPatch(at: Present(micro)),
        );
        expect(
          (await client.models.entry.get(const EntryIdentity(id: id)))!.at,
          milli,
        );
        final watched = await client.models.entry
            .watch(where: EntryFilter(at: Present(micro)))
            .firstWhere((rows) => rows.isNotEmpty)
            .timeout(const Duration(seconds: 2));
        expect(watched.single.at, milli);
      } finally {
        await client.close();
        await temp.delete(recursive: true);
      }
    },
  );
  test(
    'typed Mutation input callback reads and writes through its companion port',
    () async {
      final port = _ScriptedTransaction();
      final mutations = TransactionMutations(port);
      CompanionContext? context;
      final call = await mutations.publishEntry.withTransaction((tx) async {
        context = tx;
        final composition = await tx.models.composition.get(
          const CompositionIdentity(id: id),
        );
        expect(composition?.title, 'draft');
        await tx.models.composition.delete(const CompositionIdentity(id: id));
        return PublishEntryInput(entry: row, composition: id);
      });
      expect(context, isA<CompanionContext>());
      expect(port.writes, [
        {
          'model': 'Composition',
          'op': 'delete',
          'identity': {'id': id},
        },
      ]);
      expect(port.submitted.single, {
        'name': 'PublishEntry',
        'version': 1,
        'args': {'entry': row.toRecord(), 'composition': id},
        'callback': true,
      });
      final result =
          (await call.wait() as CallSuccess<PublishEntryOutput>).result;
      expect(result.published.at, DateTime.utc(2026));
      final renamed = await mutations.rename(
        const RenameInput(id: id, title: 'x'),
      );
      expect(port.submitted.last['callback'], false);
      expect(port.submitted.last['args'], {'id': id, 'title': 'x'});
      expect(renamed.status, CallStatus.pending);
    },
  );
  test('source codecs distinguish absent patch fields from explicit null', () {
    expect(Entry.fromRecord(row.toRecord()).at, row.at);
    expect(const EntryPatch(note: Present(null)).toRecord(), {'note': null});
    expect(const EntryPatch().toRecord(), isEmpty);
    expect(
      PublishEntryInput(entry: row, composition: id).toRecord()['entry'],
      row.toRecord(),
    );
  });
  test(
    'named optimistic work, local relations and read-only SQL use the real native Store',
    () async {
      final temp = await Directory.systemTemp.createTemp('generated-api04-');
      final client = await _open(temp);
      try {
        await client.connection?.pause();
        final call = await client.mutations.publishEntry(
          PublishEntryInput(entry: row, composition: id),
        );
        expect(call.status, CallStatus.pending);
        expect(
          (await client.models.entry.get(const EntryIdentity(id: id)))?.title,
          'hello',
        );
        await client.models.entry.update(
          const EntryIdentity(id: id),
          const EntryPatch(note: Present('changed')),
        );
        await client.models.entry.update(
          const EntryIdentity(id: id),
          const EntryPatch(note: Present(null)),
        );
        expect(
          (await client.models.entry.query(
            where: EntryFilter(
              at: Present(DateTime.parse('2026-01-01T01:00:00+01:00')),
              note: const Present(null),
            ),
            orderBy: const [
              EntryOrder(EntryOrderField.byTitle, descending: true),
            ],
            limit: 1,
          )),
          hasLength(1),
        );
        await client.transaction((tx) async {
          await tx.models.book.create(const Book(id: 'b', title: 'Book'));
          await tx.models.comment.create(
            const Comment(id: 'c', bookId: 'b', text: 'Comment'),
          );
        });
        expect(
          (await client.models.comment.book(
            const CommentIdentity(id: 'c'),
          ))?.id,
          'b',
        );
        expect(
          await client.models.book.comments(const BookIdentity(id: 'b')),
          hasLength(1),
        );
        final joined = <List<String>>[];
        final watching = client
            .watchSql(
              'SELECT b.title AS title, c.text AS text FROM Book b JOIN Comment c ON c.bookId = b.id WHERE b.id = ? ORDER BY c.id',
              parameters: const ['b'],
            )
            .listen(
              (rows) => joined.add([
                for (final r in rows) '${r['title']}:${r['text']}',
              ]),
            );
        await _until(() => joined.length == 1, 'initial SQL observer');
        await client.models.comment.create(
          const Comment(id: 'c2', bookId: 'b', text: 'Second'),
        );
        await _until(() => joined.length == 2, 'comment commit');
        await client.transaction(
          (tx) => tx.models.book.update(
            const BookIdentity(id: 'b'),
            const BookPatch(title: Present('Renamed')),
          ),
        );
        await _until(() => joined.length == 3, 'book commit');
        await watching.cancel();
        expect(joined, [
          ['Book:Comment'],
          ['Book:Comment', 'Book:Second'],
          ['Renamed:Comment', 'Renamed:Second'],
        ]);
        final state = await client.models.entry.syncState(
          const EntryIdentity(id: id),
        );
        expect(state.pending, isNotEmpty);
        expect(state.pending.every((p) => p.phase == 'queued'), isTrue);
        expect(await client.rejections.watch().first, isEmpty);
        expect(await client.failures.watch().first, isEmpty);
        expect(await client.outbound.watchPending().first, greaterThan(0));
        expect(await client.rejections.get(99), isNull);
        await client.transaction((tx) async {
          await tx.rejections.dismiss(99);
          await tx.failures.retry(const ['none']);
        });
        expect(client.clientId, isNotEmpty);
      } finally {
        await client.close();
        await temp.delete(recursive: true);
      }
    },
  );
  test(
    'one file refuses a different bound Stream and failed open releases its runtime',
    () async {
      final temp = await Directory.systemTemp.createTemp(
        'generated-binding04-',
      );
      final client = await _open(temp);
      await client.models.book.create(const Book(id: 'b', title: 'kept'));
      await client.close();
      try {
        await expectLater(
          _open(temp, stream: 'other'),
          throwsA(isA<StateError>()),
        );
        final reopened = await _open(temp);
        try {
          expect(
            (await reopened.models.book.get(
              const BookIdentity(id: 'b'),
            ))?.title,
            'kept',
          );
        } finally {
          await reopened.close();
        }
        final child = await Process.start(Platform.resolvedExecutable, [
          'failed_open.dart',
          '${temp.path}/failed.sqlite',
        ]);
        final code = await child.exitCode.timeout(const Duration(seconds: 5));
        expect(
          code,
          0,
          reason: await child.stderr.transform(utf8.decoder).join(),
        );
      } finally {
        await temp.delete(recursive: true);
      }
    },
  );
  test(
    'Bootstrap await joins one finite run and waits for its committed tail',
    () async {
      final temp = await Directory.systemTemp.createTemp(
        'generated-bootstrap04-',
      );
      final entered = Completer<void>(), gate = Completer<void>();
      final network = await _Network.start(
        onRequest: (request, body) async {
          if (body['bootstrap'] == true) {
            if (!entered.isCompleted) entered.complete();
            await gate.future;
          }
          return emptyPull(body);
        },
      );
      final client = await _open(temp, connection: network.connection);
      try {
        var done = false;
        final both = Future.wait([
          client.bootstrap(),
          client.bootstrap(),
        ]).then((_) => done = true);
        await entered.future.timeout(const Duration(seconds: 5));
        expect(done, false);
        expect(
          network.requests.where(
            (b) => b['protocol'] == 5 && !b.containsKey('materialization'),
          ),
          hasLength(1),
        );
        gate.complete();
        await both.timeout(const Duration(seconds: 5));
        expect(done, true);
        await client.bootstrap();
        expect(
          network.requests.where((b) => b['bootstrap'] == true),
          hasLength(1),
        );
      } finally {
        if (!gate.isCompleted) gate.complete();
        await client.close();
        await network.close();
        await temp.delete(recursive: true);
      }
    },
  );
  test(
    'Stream authority decodes date, enum and composite identity and observes canonical deletion',
    () async {
      final temp = await Directory.systemTemp.createTemp('generated-stream04-');
      final network = await _Network.start();
      final client = await _open(temp, connection: network.connection);
      try {
        await client.bootstrap();
        await _until(() => network.context != null, 'subscribed context');
        final seen = <List<Entry>>[];
        final observer = client.models.entry.watch().listen(seen.add);
        await _until(() => seen.isNotEmpty, 'initial observer');
        final identity = {'shelf': 's', 'at': '2026-03-04T05:06:07.000Z'};
        network.send(0, 2, [
          {
            'kind': 'upsert',
            'record': {
              'model': 'Entry',
              'identity': {'id': id},
              'cursor': 1,
              'state': {
                'title': 'server',
                'note': null,
                'at': '2026-01-02T00:00:00.000Z',
                'tags': ['server'],
                'status': 'archived',
              },
            },
          },
          {
            'kind': 'upsert',
            'record': {
              'model': 'Placement',
              'identity': identity,
              'cursor': 2,
              'state': {'label': 'placed'},
            },
          },
        ]);
        await _until(
          () => seen.any((rows) => rows.isNotEmpty),
          'typed authority observer',
        );
        expect(seen.last.single.at, DateTime.utc(2026, 1, 2));
        expect(seen.last.single.status, Status.archived);
        expect(
          (await client.models.placement.get(
            PlacementIdentity(
              shelf: 's',
              at: DateTime.utc(2026, 3, 4, 5, 6, 7),
            ),
          ))?.label,
          'placed',
        );
        network.send(2, 3, [
          {
            'kind': 'upsert',
            'record': {
              'model': 'Entry',
              'identity': {'id': id},
              'cursor': 3,
              'state': null,
            },
          },
        ]);
        await _until(
          () => seen.length >= 3 && seen.last.isEmpty,
          'canonical absence observer',
        );
        expect(
          await client.models.entry.get(const EntryIdentity(id: id)),
          isNull,
        );
        await observer.cancel();
      } finally {
        await client.close();
        await network.close();
        await temp.delete(recursive: true);
      }
    },
  );
  test(
    'generated Fetch stores nullable snapshots by default and preserves typed independent results',
    () async {
      final temp = await Directory.systemTemp.createTemp('generated-fetch04-');
      final entered = Completer<void>(), gate = Completer<void>();
      final network = await _Network.start(
        onRequest: (request, body) async {
          if (request.uri.path != '/sync/fetch') return emptyPull(body);
          final invocation = body['invocation'] as Map;
          final key = invocation['key'] as Map;
          final identity = (key['identity'] as Map).cast<String, dynamic>();
          if (key['model'] == 'Book') {
            if (!entered.isCompleted) entered.complete();
            await gate.future;
          }
          final Map<String, dynamic>? state = switch (key['model']) {
            'Placement' => {'label': 'placed'},
            'Book' => {'title': 'remote book'},
            'Entry'
                when identity['id'] != '00000000-0000-4000-8000-000000000000' =>
              {
                'title': 'remote',
                'note': null,
                'at': '2026-02-03T04:05:06.000Z',
                'tags': ['x'],
                'status': 'archived',
              },
            _ => null,
          };
          final failed = key['model'] == 'Counter';
          return failed
              ? {
                  ...context05(body),
                  'requestId': body['requestId'],
                  'outcome': {
                    'kind': 'failed',
                    'code': 'loader.failed',
                    'message': null,
                  },
                  'records': <Object>[],
                }
              : read05(body, state == null ? null : {...identity, ...state}, [
                  {'key': key, 'cursor': null, 'state': state},
                ]);
        },
      );
      final client = await _open(temp, connection: network.connection);
      try {
        final entry = await client.fetch.entry(const EntryIdentity(id: id));
        expect(entry!.at, DateTime.utc(2026, 2, 3, 4, 5, 6));
        expect(entry.status, Status.archived);
        expect(entry.tags, ['x']);
        expect(
          (await client.models.entry.get(const EntryIdentity(id: id)))?.title,
          'remote',
        );
        const other = '123e4567-e89b-42d3-a456-426614174999';
        expect(
          (await client.fetch.entry(
            const EntryIdentity(id: other),
            store: false,
          ))?.title,
          'remote',
        );
        expect(
          await client.models.entry.get(const EntryIdentity(id: other)),
          isNull,
        );
        final at = DateTime.utc(2026, 3, 4, 5, 6, 7);
        expect(
          (await client.fetch.placement(
            PlacementIdentity(shelf: 's', at: at),
          ))?.at,
          at,
        );
        expect(
          (await client.models.placement.get(
            PlacementIdentity(shelf: 's', at: at),
          ))?.label,
          'placed',
        );
        expect(
          await client.fetch.entry(
            const EntryIdentity(id: '00000000-0000-4000-8000-000000000000'),
          ),
          isNull,
        );
        await expectLater(
          client.fetch.counter(const CounterIdentity(id: 'n')),
          throwsA(
            isA<CallError>()
                .having((e) => e.code, 'code', 'loader.failed')
                .having((e) => e.execution, 'execution', 'rejected'),
          ),
        );
        final joined = [
          client.fetch.book(const BookIdentity(id: 'b')),
          client.fetch.book(const BookIdentity(id: 'b')),
        ];
        await entered.future.timeout(const Duration(seconds: 5));
        gate.complete();
        final books = await Future.wait(joined);
        expect(
          network.requests.where(
            (b) => (b['invocation'] as Map?)?['key']?['model'] == 'Book',
          ),
          hasLength(2),
        );
        expect(books[0]!.title, books[1]!.title);
        expect(identical(books[0], books[1]), false);
        final requests = network.requests
            .where((b) => b.containsKey('invocation'))
            .toList();
        expect((requests.first['invocation'] as Map)['version'], 2);
        expect(requests.first['store'], true);
        expect(requests[1]['store'], false);
        expect(network.authorization.every((v) => v == 'Bearer secret'), true);
      } finally {
        if (!gate.isCompleted) gate.complete();
        await client.close();
        await network.close();
        await temp.delete(recursive: true);
      }
    },
  );
}

Future<void> _until(bool Function() predicate, String what) async {
  final end = DateTime.now().add(const Duration(seconds: 5));
  while (DateTime.now().isBefore(end)) {
    if (predicate()) return;
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
  throw StateError('$what timed out');
}

final class _ScriptedTransaction implements SubmitMutationPort {
  final submitted = <Map<String, Object?>>[], writes = <Map<String, dynamic>>[];
  @override
  Future<Call<T>> submitMutation<T>(
    String name,
    int version,
    Map<String, dynamic>? args,
    T Function(dynamic) decode, {
    Future<Map<String, dynamic>> Function(WritePort tx)? input,
  }) async {
    final value = input == null ? args! : await input(_CompanionPort(writes));
    submitted.add({
      'name': name,
      'version': version,
      'args': value,
      'callback': input != null,
    });
    return _ScriptedCall(
      decode(name == 'PublishEntry' ? {'published': row.toRecord()} : null),
    );
  }
}

final class _CompanionPort implements WritePort {
  final List<Map<String, dynamic>> writes;
  _CompanionPort(this.writes);
  @override
  Future<void> direct(Map<String, dynamic> operation) async =>
      writes.add(operation);
  @override
  Future<Map<String, dynamic>?> read(
    String model,
    Map<String, dynamic> identity,
  ) async => {'id': identity['id'], 'title': 'draft', 'body': 'text'};
  @override
  Future<List<Map<String, dynamic>>> querySpec(
    String model,
    Map<String, dynamic> query,
  ) async => const [];
  @override
  Future<Map<String, dynamic>?> related(
    String model,
    Map<String, dynamic> identity,
    String relation,
  ) async => null;
  @override
  Future<List<Map<String, dynamic>>> referencing(
    String model,
    Map<String, dynamic> identity,
    String source,
    String relation,
  ) async => const [];
}

final class _ScriptedCall<T> implements Call<T> {
  final T result;
  _ScriptedCall(this.result);
  @override
  CallStatus get status => CallStatus.pending;
  @override
  Future<CallOutcome<T>> wait() async => CallSuccess<T>(result);
}

final class _Network {
  final HttpServer server;
  final Future<Map<String, Object?>> Function(HttpRequest, Map)? onRequest;
  final requests = <Map>[],
      authorization = <String?>[],
      errors = <Object>[],
      sockets = <WebSocket>[];
  Map? context;
  _Network(this.server, this.onRequest);
  static Future<_Network> start({
    Future<Map<String, Object?>> Function(HttpRequest, Map)? onRequest,
  }) async {
    final network = _Network(
      await HttpServer.bind(InternetAddress.loopbackIPv4, 0),
      onRequest,
    );
    network.server.listen(network._serve);
    return network;
  }

  StoreConnection get connection => StoreConnection(
    url: 'http://127.0.0.1:${server.port}',
    token: () => 'secret',
    onError: (error) {
      stderr.writeln('generated fixture runtime: $error');
    },
  );
  Future<void> _serve(HttpRequest request) async {
    try {
      authorization.add(request.headers.value('authorization'));
      if (request.uri.path == '/sync/live') {
        final socket = await WebSocketTransformer.upgrade(request);
        sockets.add(socket);
        socket.listen((message) {
          final body = jsonDecode(message as String) as Map;
          socket.add(jsonEncode(emptyHandshake(body)));
        }, onError: (Object _) {});
        return;
      }
      final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
      requests.add(body);
      if (body.containsKey('materialization')) context = context05(body);
      request.response.write(
        jsonEncode(await onRequest?.call(request, body) ?? emptyPull(body)),
      );
      await request.response.close();
    } catch (error) {
      errors.add(error);
      request.response.statusCode = 500;
      await request.response.close();
    }
  }

  void send(int from, int to, List<Map<String, Object?>> changes) =>
      sockets.last.add(
        jsonEncode(
          delivery05(
            {...context!, 'bootstrap': false, 'after': from, 'through': to},
            changes.map((change) {
              final record = change['record'] as Map;
              return <String, Object?>{
                'kind': 'record',
                'cursor': record['cursor'],
                'key': {
                  'model': record['model'],
                  'identity': record['identity'],
                },
                'state': record['state'],
              };
            }).toList(),
          ),
        ),
      );
  Future<void> close() async {
    for (final socket in sockets) {
      await socket.close();
    }
    await server.close(force: true);
    expect(errors, isEmpty);
  }
}

// Compile-only public shape fixture; native lifecycle is verified after the join.
Future<void> offlineReadShape(String path, String id) async {
  final client = await GeneratedClient.open(path: path, stream: 'User:viewer');
  final Entry? stored = await client.fetch.entry(EntryIdentity(id: id));
  final Entry? transient = await client.fetch.entry(
    EntryIdentity(id: id),
    store: false,
  );
  final ReadEntryOutput result = await client.queries.readEntry(
    id: id,
    store: false,
  );
  if (stored == transient && result.entry == null) await client.close();
}
