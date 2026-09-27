import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';
import 'package:axton/axton.dart' as sdk;
import 'generated.dart';
import 'model_only/generated.dart' as model_only;
import 'model_free/generated.dart' as model_free;

void main() {
  late Directory directory;
  late GeneratedClient client;
  setUp(() async {
    directory = await Directory.systemTemp.createTemp(
      'axton-generated-action-',
    );
    client = await GeneratedClient.open(
      path: '${directory.path}/state.sqlite',
      libraryPath: Platform.environment['AXTON_DART_LIBRARY']!,
    );
  });
  tearDown(() async {
    await client.close();
    await directory.delete(recursive: true);
  });

  test(
    'standalone and transaction models write locally and notify watch',
    () async {
      final date = DateTime.utc(2026, 9, 23);
      final watched = client.models.note.watch().firstWhere(
        (rows) => rows.isNotEmpty,
      );
      await client.models.note.create(
        Note(id: 'n', at: date, mood: Mood.calm, label: null),
      );
      expect(
        (await watched.timeout(const Duration(seconds: 2))).single.at,
        date,
      );
      expect(
        (await client.models.note.get(const NoteIdentity(id: 'n')))?.mood,
        Mood.calm,
      );
      await client.models.note.update(
        const NoteIdentity(id: 'n'),
        const NotePatch(label: Present('updated')),
      );
      await client.transaction((tx) async {
        await tx.models.note.update(
          const NoteIdentity(id: 'n'),
          NotePatch(at: Present(date.add(const Duration(days: 1)))),
        );
        expect(
          (await tx.models.note.get(const NoteIdentity(id: 'n')))?.label,
          'updated',
        );
      });
      final record = (await client.models.note.query()).single;
      expect(record.at, date.add(const Duration(days: 1)));
      expect(record.label, 'updated');
      expect((await client.syncState())['pending'], 0);
      await client.models.note.delete(const NoteIdentity(id: 'n'));
      expect(await client.models.note.get(const NoteIdentity(id: 'n')), isNull);
    },
  );

  test(
    'generated durable and direct routes of both kinds decode shared SDK outcomes',
    () async {
      final at = DateTime.utc(2026, 9, 23, 12);
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var pumps = 0;
      var directs = 0;
      final served = server.listen((request) async {
        final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
        final result = {
          'result': at.toIso8601String(),
          'moods': ['calm', 'loud'],
          'maybe': null,
        };
        if (request.uri.path == '/sync/mutations') {
          pumps++;
          final mutation = (body['mutations'] as List).single as Map;
          request.response.write(
            jsonEncode({
              'clientId': body['clientId'],
              'batchSequence': body['batchSequence'],
              'rejections': [],
              'records': [],
              'completions': [
                {
                  'callId': mutation['callId'],
                  'outcome': {
                    'status': 'succeeded',
                    'result': switch (mutation['name']) {
                      'Ping' => null,
                      'Now' => {'at': at.toIso8601String()},
                      _ => result,
                    },
                  },
                },
              ],
            }),
          );
        } else if (request.uri.path == '/sync/actions') {
          directs++;
          final call = body['call'] as Map;
          if (call['name'] == 'Echo') {
            expect((call['args'] as Map)['at'], at.toIso8601String());
            expect((call['args'] as Map)['moods'], ['calm']);
          }
          request.response.write(
            jsonEncode({
              'completion': {
                'callId': call['callId'],
                'outcome': {
                  'status': 'succeeded',
                  'result': switch (call['name']) {
                    'Ping' => null,
                    'Now' => {'at': at.toIso8601String()},
                    _ => result,
                  },
                },
              },
              'records': [],
            }),
          );
        } else {
          request.response.statusCode = 404;
        }
        await request.response.close();
      });
      final connection = await client.connect(
        SyncServer(
          url: 'http://127.0.0.1:${server.port}',
          token: () => 'alice',
        ),
        directTimeout: const Duration(seconds: 2),
      );
      try {
        final Call<EchoOutput> call = await client.mutations.echo(
          at: at,
          moods: [Mood.calm],
          maybe: null,
        );
        final sdk.Call<EchoOutput> sdkCall = call;
        expect(sdkCall.status, sdk.CallStatus.pending);
        final CallOutcome<EchoOutput> outcome = await call.wait().timeout(
          const Duration(seconds: 3),
        );
        final sdk.CallOutcome<EchoOutput> sdkOutcome = outcome;
        expect(sdkOutcome, isA<sdk.CallSuccess<EchoOutput>>());
        expect(outcome, isA<CallSuccess<EchoOutput>>());
        expect((outcome as CallSuccess<EchoOutput>).result.result, at);
        expect(outcome.result.moods, [Mood.calm, Mood.loud]);
        final Call<void> pingCall = await client.mutations.ping();
        final sdk.Call<void> sdkPingCall = pingCall;
        final CallOutcome<void> pingOutcome = await sdkPingCall.wait().timeout(
          const Duration(seconds: 3),
        );
        expect(pingOutcome, isA<CallSuccess<void>>());
        expect(pingCall.status, CallStatus.succeeded);
        final direct = await client.mutations.call.echo(
          at: at,
          moods: [Mood.calm],
          maybe: null,
        );
        expect(direct.result, at);
        expect(direct.maybe, isNull);
        await client.mutations.call.ping();
        expect(pumps, 2);
        expect(directs, 2);
        // A default Query is direct: a final result and no queue row.
        final NowOutput now = await client.queries.now(at: at);
        expect(now.at, at);
        expect(directs, 3);
        expect((await client.syncState())['pending'], 0);
        // Under enqueue it is durable and settles through the pump.
        final Call<NowOutput> queued = await client.queries.enqueue.now(at: at);
        final CallOutcome<NowOutput> queuedOutcome = await queued
            .wait()
            .timeout(const Duration(seconds: 3));
        expect((queuedOutcome as CallSuccess<NowOutput>).result.at, at);
        expect(pumps, 3);
        expect((await client.syncState())['pending'], 0);
        // `once` saves the complete result; equal args hit it with no request.
        final NowOutput first = await client.queries.now(at: at, once: true);
        expect(directs, 4);
        final NowOutput hit = await client.queries.now(
          at: at.toLocal(),
          once: true,
        );
        expect(hit.at, first.at);
        expect(directs, 4);
        await client.queries.now(at: at, once: true, refresh: true);
        expect(directs, 5);
        await client.queries.invalidate.now(at: at);
        await client.queries.now(at: at, once: true);
        expect(directs, 6);
        expect((await client.syncState())['pending'], 0);
      } finally {
        await connection.close();
        await served.cancel();
        await server.close(force: true);
      }
    },
  );

  test(
    'model-only and model-free generated clients execute through the SDK',
    () async {
      final only = await model_only.GeneratedClient.open(
        path: '${directory.path}/only.sqlite',
        libraryPath: Platform.environment['AXTON_DART_LIBRARY']!,
      );
      try {
        await only.models.item.create(
          const model_only.Item(id: 'i', label: 'Local'),
        );
        expect(
          (await only.models.item.get(
            const model_only.ItemIdentity(id: 'i'),
          ))?.label,
          'Local',
        );
        expect((await only.syncState())['pending'], 0);
      } finally {
        await only.close();
      }
      final free = await model_free.GeneratedClient.open(
        path: '${directory.path}/free.sqlite',
        libraryPath: Platform.environment['AXTON_DART_LIBRARY']!,
        onStore: const model_free.StoreHooks(),
      );
      try {
        final model_free.Call<void> call = await free.mutations.ping();
        expect(call.status, model_free.CallStatus.pending);
        expect((await free.syncState())['pending'], 1);
        final model_free.Call<model_free.ClockOutput> clock = await free
            .queries
            .enqueue
            .clock(at: DateTime.utc(2026));
        expect((await free.syncState())['pending'], 2);
        // Without a connection a direct Query fails instead of enqueueing.
        await expectLater(
          free.queries.clock(at: DateTime.utc(2026)),
          throwsA(
            isA<model_free.CallError>().having(
              (error) => error.code,
              'code',
              'action.unavailable',
            ),
          ),
        );
        expect((await free.syncState())['pending'], 2);
        await free.close();
        expect(await call.wait(), isA<model_free.CallFailure<void>>());
        expect(
          await clock.wait(),
          isA<model_free.CallFailure<model_free.ClockOutput>>(),
        );
      } finally {
        await free.close();
      }
    },
  );

  test('generated store selector is persisted beside durable args', () async {
    await client.mutations.ping(store: const PingStore.none());
    await client.mutations.ping(store: const PingStore.all());
    final at = DateTime.utc(2026);
    await client.queries.enqueue.now(at: at, store: const NowStore.none());
    final frozen = jsonDecode((await client.client.freeze())!) as Map;
    final mutations = (frozen['mutations'] as List).cast<Map>();
    expect(mutations[0]['store'], false);
    expect(mutations[0]['args'], isEmpty);
    expect(mutations[1].containsKey('store'), isFalse);
    expect(mutations[2]['name'], 'Now');
    expect(mutations[2]['store'], false);
    expect(mutations[2]['args'], {'at': at.toIso8601String()});
  });

  test('generated open forwards the direct timeout', () async {
    await expectLater(
      GeneratedClient.open(
        path: '${directory.path}/invalid-timeout.sqlite',
        libraryPath: Platform.environment['AXTON_DART_LIBRARY']!,
        server: SyncServer(url: 'http://127.0.0.1:1', token: () => 'alice'),
        directTimeout: Duration.zero,
      ),
      throwsArgumentError,
    );
  });

  test(
    'model operand and omitted update fields use generated wire codecs',
    () async {
      final at = DateTime.utc(2026, 9, 23);
      await client.mutations.touch(
        note: Note(id: 'n', at: at, mood: Mood.loud, label: null),
        changed: const TouchChangedUpdate(id: 'n'),
      );
      await client.mutations.touch(
        note: Note(id: 'other', at: at, mood: Mood.calm, label: null),
      );
      final frozen = jsonDecode((await client.client.freeze())!) as Map;
      final mutations = frozen['mutations'] as List;
      final args = (mutations.first as Map)['args'] as Map;
      final omitted = (mutations.last as Map)['args'] as Map;
      expect((args['note'] as Map)['at'], at.toIso8601String());
      expect((args['note'] as Map)['mood'], 'loud');
      expect(args['changed'], {'id': 'n'});
      expect(omitted['changed'], isNull);
    },
  );

  test(
    'generated Loads encode typed args, share once jobs and apply pages over /sync/loads',
    () async {
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final bodies = <Map<String, dynamic>>[];
      final at = DateTime.utc(2026, 9, 27, 8);
      server.listen((request) async {
        final body =
            jsonDecode(await utf8.decoder.bind(request).join())
                as Map<String, dynamic>;
        bodies.add({'path': request.uri.path, ...body});
        final pages = [
          for (final intent in body['loads'] as List)
            {
              'loadId': intent['loadId'],
              'callId': intent['callId'],
              'outcome': {
                'status': 'succeeded',
                'data': {
                  'notes': [
                    {'id': 'a'},
                    {'id': 'b'},
                  ],
                  'pinned': [
                    {'id': 'a'},
                  ],
                },
                'next': null,
              },
              'records': [
                for (final id in ['a', 'b'])
                  {
                    'model': 'Note',
                    'identity': {'id': id},
                    'stamp': 1,
                    'state': {
                      'at': at.toIso8601String(),
                      'mood': 'loud',
                      'label': id,
                    },
                  },
              ],
            },
        ];
        request.response.headers.contentType = ContentType.json;
        request.response.write(jsonEncode({'loads': pages}));
        await request.response.close();
      });
      try {
        // Accepted offline: a handle before any connection.
        final job = await client.loads.notesSince(
          since: DateTime.utc(2026, 1, 2),
          moods: [Mood.calm, Mood.loud],
          once: true,
        );
        expect(job.status.name, 'NotesSince');
        expect(job.status.phase, LoadPhase.waiting);
        final joined = await client.loads.notesSince(
          since: DateTime.utc(2026, 1, 2),
          moods: [Mood.calm, Mood.loud],
          once: true,
        );
        expect(joined.id, job.id, reason: 'an active once job is shared');
        expect(identical(joined, job), isFalse);
        final ordinary = await client.loads.notesSince(since: null, moods: []);
        expect(ordinary.id, isNot(job.id));
        final phases = <LoadPhase>[];
        final observer = job.watch().listen(
          (status) => phases.add(status.phase),
        );
        await client.connect(
          SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'a'),
        );
        await job.wait();
        await ordinary.wait();
        expect(job.status.phase, LoadPhase.complete);
        expect(job.status.pages, 1);
        expect(phases.last, LoadPhase.complete);
        await observer.cancel();
        final notes = await client.models.note.query();
        expect(notes.map((n) => n.label), unorderedEquals(['a', 'b']));
        expect(bodies.first['path'], '/sync/loads');
        final intents = [for (final b in bodies) ...(b['loads'] as List)];
        final typed = intents.firstWhere((i) => i['loadId'] == job.id) as Map;
        expect(typed['name'], 'NotesSince');
        expect(typed['args'], {
          'since': DateTime.utc(2026, 1, 2).toIso8601String(),
          'moods': ['calm', 'loud'],
        });
        expect(typed.containsKey('once'), isFalse);
        final requests = bodies.length;
        // A complete once job is reused offline; invalidation needs no network.
        final hit = await client.loads.notesSince(
          since: DateTime.utc(2026, 1, 2),
          moods: [Mood.calm, Mood.loud],
          once: true,
        );
        expect(hit.id, job.id);
        await hit.wait();
        await client.loads.invalidate.notesSince(
          since: DateTime.utc(2026, 1, 2),
          moods: [Mood.calm, Mood.loud],
        );
        expect(bodies.length, requests);
        final restored = await client.loads.get(job.id);
        expect(restored?.status, job.status);
        final listed = await client.loads.list(limit: 10);
        expect(listed.map((s) => s.id), containsAll([job.id, ordinary.id]));
        await expectLater(
          client.loads.notesSince(since: null, moods: [], refresh: true),
          throwsA(
            isA<LoadException>().having(
              (e) => e.code,
              'code',
              'load.invalid_options',
            ),
          ),
        );
        for (final handle in [job, joined, ordinary, hit, restored!]) {
          handle.dispose();
        }
      } finally {
        await server.close(force: true);
      }
    },
  );
}
