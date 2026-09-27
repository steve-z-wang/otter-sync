import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:axton/axton.dart';
import 'package:axton/src/actions.dart' show ActionObservers, ActionWeakState;
import 'package:test/test.dart';

import 'fake_carrier.dart';

void main() {
  test(
    'a call completed in the batch of its submission is registered first',
    () async {
      // The runtime publishes the submission's completion and the call's
      // `callCompleted` in one drained batch: the handle is registered while
      // the completion is dispatched, never in a later continuation.
      final carrier = FakeCarrier((envelope) {
        final command = envelope['command'] as Map<String, dynamic>?;
        if (command?['kind'] != 'submitAction') return null;
        return [
          completed(envelope['requestId'] as String, {
            'callId': 'call-1',
            'ordinal': 1,
          }),
          {
            'type': 'callCompleted',
            'callId': 'call-1',
            'outcome': {'status': 'succeeded', 'result': 'pong'},
          },
        ];
      });
      final client = await Client.open(
        path: 'unused',
        schema: const {},
        carrier: carrier,
      );
      try {
        final completions = <Map<String, dynamic>>[];
        client.actionCompletions.listen(completions.add);
        final call = await client.invokeAction(
          'Ping',
          1,
          const {},
          (value) => value as String,
        );
        final outcome = await call.wait().timeout(const Duration(seconds: 1));
        expect((outcome as CallSuccess<String>).result, 'pong');
        expect(call.status, CallStatus.succeeded);
        expect(completions.single['callId'], 'call-1');
      } finally {
        await client.close();
      }
    },
  );

  test('weak routes sweep without retaining an abandoned handle', () {
    final refs = <_TestWeak>[];
    final observers = ActionObservers(
      weak: (state) {
        final ref = _TestWeak(state);
        refs.add(ref);
        return ref;
      },
    );
    observers.register<void>('gone', (_) {});
    refs.single.value = null;
    expect(observers.routingCount, 0);
    observers.complete({
      'callId': 'gone',
      'outcome': {'status': 'succeeded', 'result': null},
    });
  });

  test('active wait survives loss of its weak routing target', () async {
    final refs = <_TestWeak>[];
    final observers = ActionObservers(
      weak: (state) {
        final ref = _TestWeak(state);
        refs.add(ref);
        return ref;
      },
    );
    final call = observers.register<String>('held', (value) => value as String);
    final waiting = call.wait();
    refs.single.value = null;
    observers.complete({
      'callId': 'held',
      'outcome': {'status': 'succeeded', 'result': 'ready'},
    });
    final result =
        await waiting.timeout(const Duration(milliseconds: 100))
            as CallSuccess<String>;
    expect(result.result, 'ready');
  });

  late Directory directory;
  late Client client;
  setUp(() async {
    directory = await Directory.systemTemp.createTemp('axton-actions-');
    client = await Client.open(
      path: '${directory.path}/db',
      schema: {
        'enums': [],
        'models': [],
        'actions': [
          {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
        ],
      },
      libraryPath: Platform.environment['AXTON_LIBRARY']!,
    );
  });
  tearDown(() async {
    await client.close();
    await directory.delete(recursive: true);
  });

  test('real native acknowledgement settles a cached typed handle', () async {
    final call = await client.invokeAction<void>('Ping', 1, {}, (_) {});
    expect(call.status, CallStatus.pending);
    final frozen = await client.freeze();
    expect(frozen, isNotNull);
    final request = jsonDecode(frozen!) as Map<String, dynamic>;
    final mutation = (request['mutations'] as List).single as Map;
    final receipt = {
      'clientId': request['clientId'],
      'batchSequence': request['batchSequence'],
      'rejections': [],
      'completions': [
        {
          'callId': mutation['callId'],
          'outcome': {'status': 'succeeded', 'result': null},
        },
      ],
      'records': [],
    };
    final waiting = call.wait();
    await client.acknowledge(request['batchSequence'] as int, receipt);
    expect(await waiting, isA<CallSuccess<void>>());
    expect(identical(await waiting, await call.wait()), isTrue);
    expect(call.status, CallStatus.succeeded);
  });

  test('store selector travels beside args on both routes', () async {
    await expectLater(
      client.invokeAction<void>(
        'Ping',
        1,
        {},
        (_) {},
        store: const _Store({'missing': false}),
      ),
      throwsA(isA<CallError>()),
    );
    expect(await client.freeze(), isNull, reason: 'nothing was enqueued');
    await client.invokeAction<void>(
      'Ping',
      1,
      {},
      (_) {},
      store: const _Store(false),
    );
    await client.invokeAction<void>(
      'Ping',
      1,
      {},
      (_) {},
      store: const _Store(null),
    );
    final frozen = jsonDecode((await client.freeze())!) as Map;
    final mutations = (frozen['mutations'] as List).cast<Map>();
    expect(mutations[0]['store'], false);
    expect(mutations[0]['args'], isEmpty);
    expect(mutations[1].containsKey('store'), isFalse);
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    final bodies = <Map>[];
    final served = server.listen((request) async {
      if (request.uri.path != '/sync/actions') {
        request.response.statusCode = 404;
        await request.response.close();
        return;
      }
      final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
      bodies.add(body);
      request.response.write(
        jsonEncode({
          'completion': {
            'callId': (body['call'] as Map)['callId'],
            'outcome': {'status': 'succeeded', 'result': null},
          },
          'records': [],
        }),
      );
      await request.response.close();
    });
    try {
      final connection = await client.connect(
        SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'a'),
      );
      try {
        await client.invokeDirectAction<void>(
          'Ping',
          1,
          {},
          (_) {},
          store: const _Store(false),
        );
      } finally {
        await connection.close();
      }
      final direct = bodies.single;
      expect((direct['call'] as Map)['store'], false);
      expect((direct['call'] as Map)['args'], isEmpty);
    } finally {
      await served.cancel();
      await server.close(force: true);
    }
  });

  test('close settles a handle even without a prior wait', () async {
    final call = await client.invokeAction<void>('Ping', 1, {}, (_) {});
    await client.close();
    final outcome = await call.wait();
    expect(outcome, isA<CallFailure<void>>());
    expect((outcome as CallFailure<void>).error.code, 'client.closed');
    expect(call.status, CallStatus.failed);
  });

  test('close racing a queued submit returns a failed handle', () async {
    final entered = Completer<void>();
    final release = Completer<void>();
    final blocker = client.transaction((_) async {
      entered.complete();
      await release.future;
    });
    await entered.future;
    final submitting = client.invokeAction<void>('Ping', 1, {}, (_) {});
    final closing = client.close();
    release.complete();
    // Close is priority control: it may roll the blocker back before its
    // callback result arrives.
    await blocker.then<void>(
      (_) {},
      onError: (Object error) => expect(
        error,
        isA<StateError>().having((e) => e.message, 'message', 'client_closed'),
      ),
    );
    final call = await submitting.timeout(const Duration(seconds: 1));
    final failure = await call.wait() as CallFailure<void>;
    expect(failure.error.code, 'client.closed');
    await closing;
  });

  test('drop settles a pending handle through native completion', () async {
    final call = await client.invokeAction<void>('Ping', 1, {}, (_) {});
    await client.drop(1);
    final outcome = await call.wait() as CallFailure<void>;
    expect(outcome.error.code, 'dropped');
  });

  test('rebuild discard settles live unsent and frozen handles', () async {
    final schema =
        jsonDecode(
              await File('../../fixtures/schemas/entry.json').readAsString(),
            )
            as Map<String, dynamic>;
    schema['actions'] = [
      {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
    ];
    final breaking = jsonDecode(jsonEncode(schema)) as Map<String, dynamic>;
    ((breaking['models'] as List).first['fields'] as List).add({
      'name': 'due',
      'nullable': false,
      'type': {'kind': 'scalar', 'name': 'string'},
    });
    for (final frozen in [false, true]) {
      final path = '${directory.path}/${frozen ? 'frozen' : 'unsent'}';
      final old = await Client.open(
        path: path,
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      await old.invokeAction<void>('Ping', 1, {}, (_) {});
      await old.close();
      final reopened = await Client.open(
        path: path,
        schema: breaking,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      try {
        final call = await reopened.invokeAction<void>('Ping', 1, {}, (_) {});
        final waiting = call.wait();
        if (frozen) await reopened.freeze();
        final report = await reopened.rebuild(discardPending: true);
        expect(
          (report['abandonedCalls'] as List).any(
            (entry) => entry['frozen'] == frozen,
          ),
          isTrue,
        );
        final outcome = await waiting as CallFailure<void>;
        expect(outcome.error.code, 'abandoned');
        expect(outcome.error.execution, frozen ? 'unknown' : 'rejected');
        expect(call.status, CallStatus.failed);
      } finally {
        await reopened.close();
      }
    }
  });

  test(
    'dropping an unsent Action settles its live lifecycle dependent',
    () async {
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      schema['actions'] = [
        {
          'name': 'Add',
          'version': 1,
          'inputs': [
            {
              'kind': 'model',
              'name': 'entry',
              'model': 'Entry',
              'operation': 'create',
              'cardinality': 'single',
            },
          ],
          'outputs': [],
        },
        {
          'name': 'Edit',
          'version': 1,
          'inputs': [
            {
              'kind': 'model',
              'name': 'entry',
              'model': 'Entry',
              'operation': 'update',
              'cardinality': 'single',
            },
          ],
          'outputs': [],
        },
      ];
      final local = await Client.open(
        path: '${directory.path}/dependent',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      try {
        final created = await local.invokeAction<void>('Add', 1, {
          'entry': {'id': 'e', 'text': 'A'},
        }, (_) {});
        final dependent = await local.invokeAction<void>('Edit', 1, {
          'entry': {'id': 'e', 'text': 'B'},
        }, (_) {});
        final waiting = dependent.wait();
        final state = await local.recordSyncState('Entry', {'id': 'e'});
        await local.drop((state['pending'] as List).first['ordinal'] as int);
        expect(
          (await created.wait() as CallFailure<void>).error.code,
          'dropped',
        );
        expect(
          (await waiting as CallFailure<void>).error.code,
          'dependency.rejected',
        );
        expect(dependent.status, CallStatus.failed);
        expect((await local.syncState())['pending'], 0);
      } finally {
        await local.close();
      }
    },
  );

  test(
    'typed actions and standalone direct reject inside a transaction',
    () async {
      await client.transaction((tx) async {
        for (final pending in <Future<dynamic>>[
          client.invokeAction<void>('Ping', 1, {}, (_) {}),
          client.invokeDirectAction<void>('Ping', 1, {}, (_) {}),
        ]) {
          await expectLater(
            pending.timeout(const Duration(milliseconds: 300)),
            throwsA(
              isA<CallError>()
                  .having((e) => e.code, 'code', 'transaction_active')
                  .having((e) => e.execution, 'execution', 'rejected'),
            ),
          );
        }
        await expectLater(
          client
              .direct({
                'model': 'Entry',
                'op': 'delete',
                'identity': {'id': 'e'},
              })
              .timeout(const Duration(milliseconds: 300)),
          throwsA(
            isA<StateError>().having(
              (e) => e.message,
              'message',
              'transaction_active',
            ),
          ),
        );
      });
    },
  );

  test(
    'invalid result decoder settles the handle as observation failure',
    () async {
      final call = await client.invokeAction<String>('Ping', 1, {}, (_) {
        throw const FormatException('bad result');
      });
      final request = jsonDecode((await client.freeze())!) as Map;
      final mutation = (request['mutations'] as List).single as Map;
      await client.acknowledge(request['batchSequence'] as int, {
        'clientId': request['clientId'],
        'batchSequence': request['batchSequence'],
        'rejections': [],
        'completions': [
          {
            'callId': mutation['callId'],
            'outcome': {'status': 'succeeded', 'result': null},
          },
        ],
        'records': [],
      });
      final outcome = await call.wait() as CallFailure<String>;
      expect(outcome.error.code, 'action.observation_failed');
      expect(outcome.error.cause, isA<FormatException>());
      expect(call.status, CallStatus.failed);
    },
  );

  test(
    'direct action returns decoded result and maps unavailable transport',
    () async {
      await expectLater(
        client.invokeDirectAction<void>('Ping', 1, {}, (_) {}),
        throwsA(
          isA<CallError>().having((e) => e.code, 'code', 'action.unavailable'),
        ),
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var succeed = true;
      final sub = server.listen((request) async {
        if (request.uri.path != '/sync/actions') {
          request.response.statusCode = 404;
          await request.response.close();
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
        request.response.write(
          jsonEncode({
            'completion': {
              'callId': (body['call'] as Map)['callId'],
              'outcome': succeed
                  ? {'status': 'succeeded', 'result': null}
                  : {
                      'status': 'failed',
                      'code': 'handler.failed',
                      'execution': 'rejected',
                    },
            },
            'records': [],
          }),
        );
        await request.response.close();
      });
      final connection = await client.connect(
        SyncServer(
          url: 'http://127.0.0.1:${server.port}',
          token: () => 'alice',
        ),
      );
      try {
        expect(
          await client.invokeDirectAction<String>(
            'Ping',
            1,
            {},
            (_) => 'decoded',
          ),
          'decoded',
        );
        await expectLater(
          client.invokeDirectAction<String>('Ping', 1, {}, (_) {
            throw const FormatException('bad direct result');
          }),
          throwsA(
            isA<CallError>()
                .having((e) => e.code, 'code', 'action.observation_failed')
                .having((e) => e.cause, 'cause', isA<FormatException>()),
          ),
        );
        succeed = false;
        await expectLater(
          client.invokeDirectAction<void>('Ping', 1, {}, (_) {}),
          throwsA(
            isA<CallError>()
                .having((e) => e.code, 'code', 'handler.failed')
                .having((e) => e.execution, 'execution', 'rejected'),
          ),
        );
      } finally {
        await connection.close();
        await sub.cancel();
        await server.close(force: true);
      }
    },
  );

  test('connected native push pump resolves a live typed handle', () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    var executions = 0;
    final served = server.listen((request) async {
      if (request.uri.path != '/sync/mutations') {
        request.response.statusCode = 404;
        await request.response.close();
        return;
      }
      final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
      final mutation = (body['mutations'] as List).single as Map;
      executions++;
      request.response.write(
        jsonEncode({
          'clientId': body['clientId'],
          'batchSequence': body['batchSequence'],
          'rejections': [],
          'completions': [
            {
              'callId': mutation['callId'],
              'outcome': {'status': 'succeeded', 'result': null},
            },
          ],
          'records': [],
        }),
      );
      await request.response.close();
    });
    final connection = await client.connect(
      SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'alice'),
    );
    try {
      final call = await client.invokeAction<void>('Ping', 1, {}, (_) {});
      final deadline = DateTime.now().add(const Duration(seconds: 2));
      while (call.status == CallStatus.pending &&
          DateTime.now().isBefore(deadline)) {
        await Future<void>.delayed(const Duration(milliseconds: 5));
      }
      expect(
        call.status,
        CallStatus.succeeded,
        reason: 'the pump runs without an active wait',
      );
      expect(
        await call.wait().timeout(const Duration(seconds: 2)),
        isA<CallSuccess<void>>(),
      );
      expect(executions, 1);
      expect((await client.syncState())['pending'], 0);
    } finally {
      await connection.close();
      await served.cancel();
      await server.close(force: true);
    }
  });

  test(
    'durable reports isolate throwing onError and keep the pump alive',
    () async {
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      schema['actions'] = [
        {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
      ];
      final local = await Client.open(
        path: '${directory.path}/durable-diagnostic',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var requests = 0;
      final served = server.listen((request) async {
        if (request.uri.path != '/sync/mutations') {
          request.response.statusCode = 404;
          await request.response.close();
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
        requests++;
        request.response.write(
          jsonEncode({
            'clientId': body['clientId'],
            'batchSequence': body['batchSequence'],
            'rejections': [],
            'completions': [
              for (final mutation in body['mutations'] as List)
                {
                  'callId': (mutation as Map)['callId'],
                  'outcome': {'status': 'succeeded', 'result': null},
                },
            ],
            'records': requests == 3
                ? []
                : [
                    for (final id in ['a', 'b'])
                      {
                        'model': 'Entry',
                        'identity': {'id': id},
                        'stamp': 1,
                        'state': {
                          'text': requests == 1 ? 'first' : 'second',
                          'note': null,
                        },
                      },
                  ],
          }),
        );
        await request.response.close();
      });
      final diagnostic = StateError('application diagnostic failed');
      final reports = <Object>[];
      final observed = <Object>[];
      final finished = Completer<void>();
      try {
        runZonedGuarded(() {
          () async {
            final connection = await local.connect(
              SyncServer(
                url: 'http://127.0.0.1:${server.port}',
                token: () => 'alice',
              ),
              onError: (report) {
                reports.add(report);
                throw diagnostic;
              },
            );
            try {
              for (var i = 0; i < 3; i++) {
                final call = await local.invokeAction<void>(
                  'Ping',
                  1,
                  {},
                  (_) {},
                );
                expect(
                  await call.wait().timeout(const Duration(seconds: 2)),
                  isA<CallSuccess<void>>(),
                );
              }
            } finally {
              await connection.close();
            }
          }().then(finished.complete, onError: finished.completeError);
        }, (error, stack) => observed.add(error));
        await finished.future.timeout(const Duration(seconds: 3));
        expect(requests, 3);
        expect(reports, hasLength(2));
        expect(
          reports.every((report) => report.toString().contains('conflict')),
          isTrue,
        );
        expect(observed, [same(diagnostic), same(diagnostic)]);
        expect((await local.syncState())['pending'], 0);
      } finally {
        await local.close();
        await served.cancel();
        await server.close(force: true);
      }
    },
  );

  // A direct response's records are a runtime `report`, not part of the
  // call: a throwing `onError` reaches the zone that connected.
  test(
    'throwing direct diagnostic preserves applied result and reaches the connecting Zone',
    () async {
      final schema =
          jsonDecode(
                await File('../../fixtures/schemas/entry.json').readAsString(),
              )
              as Map<String, dynamic>;
      schema['actions'] = [
        {'name': 'Ping', 'version': 1, 'inputs': [], 'outputs': []},
      ];
      final local = await Client.open(
        path: '${directory.path}/direct-diagnostic',
        schema: schema,
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      var requests = 0;
      final served = server.listen((request) async {
        if (request.uri.path != '/sync/actions') {
          request.response.statusCode = 404;
          await request.response.close();
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
        requests++;
        request.response.write(
          jsonEncode({
            'completion': {
              'callId': (body['call'] as Map)['callId'],
              'outcome': {'status': 'succeeded', 'result': null},
            },
            'records': [
              {
                'model': 'Entry',
                'identity': {'id': 'e'},
                'stamp': 1,
                'state': {
                  'text': requests == 1 ? 'first' : 'second',
                  'note': null,
                },
              },
            ],
          }),
        );
        await request.response.close();
      });
      final diagnostic = StateError('diagnostic failed');
      final observed = <Object>[];
      try {
        final connected = Completer<RuntimeConnection>();
        runZonedGuarded(() {
          local
              .connect(
                SyncServer(
                  url: 'http://127.0.0.1:${server.port}',
                  token: () => 'alice',
                ),
                onError: (_) => throw diagnostic,
              )
              .then(connected.complete, onError: connected.completeError);
        }, (error, stack) => observed.add(error));
        final connection = await connected.future;
        try {
          expect(
            await local.invokeDirectAction<String>(
              'Ping',
              1,
              {},
              (_) => 'first',
            ),
            'first',
          );
          expect(
            observed,
            isEmpty,
            reason: 'the first response applies cleanly',
          );
          expect(
            await local
                .invokeDirectAction<String>('Ping', 1, {}, (_) => 'decoded')
                .timeout(const Duration(seconds: 2)),
            'decoded',
          );
        } finally {
          await connection.close();
        }
        expect(observed, [same(diagnostic)]);
      } finally {
        await local.close();
        await served.cancel();
        await server.close(force: true);
      }
    },
  );

  test('standalone direct write commits locally and wakes watch', () async {
    final schema =
        jsonDecode(
              await File('../../fixtures/schemas/entry.json').readAsString(),
            )
            as Map<String, dynamic>;
    final local = await Client.open(
      path: '${directory.path}/local',
      schema: schema,
      libraryPath: Platform.environment['AXTON_LIBRARY']!,
    );
    try {
      final seen = <List<Map<String, dynamic>>>[];
      final first = Completer<void>();
      final second = Completer<void>();
      final sub = local.watch('Entry').listen((rows) {
        seen.add(rows);
        if (seen.length == 1) first.complete();
        if (seen.length == 2) second.complete();
      });
      await first.future;
      await local.direct({
        'model': 'Entry',
        'op': 'create',
        'identity': {'id': 'e'},
        'values': {'text': 'local'},
      });
      await second.future.timeout(const Duration(seconds: 1));
      expect((await local.read('Entry', {'id': 'e'}))?['text'], 'local');
      expect((seen.last.single)['text'], 'local');
      expect(await local.pendingTasks(), isEmpty);
      await sub.cancel();
    } finally {
      await local.close();
    }
  });

  // Creation defaults (#27): the Dart SDK passes omitted fields through and
  // the native client generates them once for both routes.
  test(
    'create defaults are generated natively for durable and direct calls',
    () async {
      final dir = await Directory.systemTemp.createTemp(
        'axton-actions-defaults-',
      );
      final note = {
        'name': 'Note',
        'version': 1,
        'identity': ['id'],
        'fields': [
          {
            'name': 'id',
            'type': {'kind': 'scalar', 'name': 'uuid'},
            'nullable': false,
            'createDefault': {'kind': 'uuid'},
          },
          {
            'name': 'body',
            'type': {'kind': 'scalar', 'name': 'string'},
            'nullable': false,
            'createDefault': {'kind': 'literal', 'value': 'b'},
          },
          {
            'name': 'createdAt',
            'type': {'kind': 'scalar', 'name': 'dateTime'},
            'nullable': false,
            'createDefault': {'kind': 'now'},
          },
        ],
      };
      final local = await Client.open(
        path: '${dir.path}/db',
        schema: {
          'enums': [],
          'models': [note],
          'actions': [
            {
              'name': 'AddNote',
              'version': 1,
              'inputs': [
                {
                  'kind': 'model',
                  'name': 'note',
                  'model': 'Note',
                  'operation': 'create',
                  'cardinality': 'single',
                },
              ],
              'outputs': [],
            },
          ],
        },
        libraryPath: Platform.environment['AXTON_LIBRARY']!,
      );
      final uuid = RegExp(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$',
      );
      final millis = RegExp(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$');
      final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
      final bodies = <Map>[];
      final served = server.listen((request) async {
        if (request.uri.path != '/sync/actions') {
          request.response.statusCode = 404;
          await request.response.close();
          return;
        }
        final body = jsonDecode(await utf8.decoder.bind(request).join()) as Map;
        bodies.add(body);
        final call = body['call'] as Map;
        final args = (call['args'] as Map)['note'] as Map;
        request.response.write(
          jsonEncode({
            'completion': {
              'callId': call['callId'],
              'outcome': {'status': 'succeeded', 'result': null},
            },
            'records': [
              {
                'model': 'Note',
                'identity': {'id': args['id']},
                'stamp': 1,
                'state': {'body': args['body'], 'createdAt': args['createdAt']},
              },
            ],
          }),
        );
        await request.response.close();
      });
      try {
        await local.invokeAction<void>('AddNote', 1, {
          'note': {'body': 'queued'},
        }, (_) {});
        final frozen = jsonDecode((await local.freeze())!) as Map;
        final queued =
            ((frozen['mutations'] as List).single as Map)['args']['note']
                as Map;
        expect(
          uuid.hasMatch(queued['id'] as String),
          isTrue,
          reason: '$queued',
        );
        expect(
          millis.hasMatch(queued['createdAt'] as String),
          isTrue,
          reason: '$queued',
        );
        expect(queued['body'], 'queued');
        final optimistic = await local.read('Note', {'id': queued['id']});
        expect(optimistic, {
          'id': queued['id'],
          'body': 'queued',
          'createdAt': queued['createdAt'],
        });
        final connection = await local.connect(
          SyncServer(url: 'http://127.0.0.1:${server.port}', token: () => 'a'),
        );
        try {
          await local.invokeDirectAction<void>('AddNote', 1, {
            'note': {},
          }, (_) {});
        } finally {
          await connection.close();
        }
        final sent =
            ((bodies.last['call'] as Map)['args'] as Map)['note'] as Map;
        expect(uuid.hasMatch(sent['id'] as String), isTrue);
        expect(sent['id'], isNot(queued['id']));
        expect(sent['body'], 'b');
        expect(millis.hasMatch(sent['createdAt'] as String), isTrue);
        expect(
          await local.read('Note', {'id': sent['id']}),
          sent,
          reason: 'direct authority applied locally',
        );
      } finally {
        await served.cancel();
        await server.close(force: true);
        await local.close();
        await dir.delete(recursive: true);
      }
    },
  );
}

final class _TestWeak implements ActionWeakState {
  Object? value;
  _TestWeak(this.value);
  @override
  Object? get target => value;
}

final class _Store extends CallStore {
  const _Store(this.wire);
  final Object? wire;
  @override
  Object? toWire() => wire;
}
