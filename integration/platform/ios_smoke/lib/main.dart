import 'dart:async';
import 'dart:io';

import 'package:flutter/material.dart';
import 'package:axton/axton.dart';

const _schema = {
  'enums': <Object>[],
  'models': [
    {
      'name': 'Entry',
      'identity': ['id'],
      'fields': [
        {
          'name': 'id',
          'nullable': false,
          'type': {'kind': 'scalar', 'name': 'string'},
        },
        {
          'name': 'text',
          'nullable': false,
          'type': {'kind': 'scalar', 'name': 'string'},
        },
        {
          'name': 'note',
          'nullable': true,
          'type': {'kind': 'scalar', 'name': 'string'},
        },
      ],
    },
  ],
};

Future<void> main() async {
  final storage = Directory.systemTemp;
  final stageFile = File('${storage.path}/axton-smoke-stage.txt');
  final resultFile = File('${storage.path}/axton-smoke-result.txt');
  final expectedFile = File('${storage.path}/axton-smoke-freeze.txt');
  final databasePath = '${storage.path}/axton-smoke.sqlite';

  void stage(String value) {
    print('AXTON_SMOKE_STAGE $value temp=${storage.path}');
    stageFile.writeAsStringSync(value, flush: true);
  }

  Future<T> bounded<T>(String name, Future<T> work) async {
    stage('${name}_BEFORE');
    try {
      final value = await work.timeout(const Duration(seconds: 10));
      stage('${name}_AFTER');
      return value;
    } catch (error) {
      stage('${name}_ERROR: $error');
      rethrow;
    }
  }

  stage('MAIN_BEFORE_ENSURE');
  WidgetsFlutterBinding.ensureInitialized();
  stage('MAIN_AFTER_ENSURE');

  var result = 'FAIL: smoke did not run';
  Client? client;
  try {
    client = await bounded(
      'OPEN',
      Client.open(path: databasePath, schema: _schema),
    );
    if (expectedFile.existsSync()) {
      final row = await bounded(
        'RESTART_READ',
        client!.read('Entry', {'id': 'ios'}),
      );
      final frozen = await bounded('RESTART_FREEZE', client!.freeze());
      if (row?['text'] != 'queued' ||
          frozen == null ||
          frozen != expectedFile.readAsStringSync()) {
        throw StateError('restart state mismatch');
      }
      result = 'AXTON_SMOKE_RESTART_OK';
    } else {
      await bounded(
        'DIRECT_TX',
        client!.transaction((tx) async {
          await tx.direct({
            'model': 'Entry',
            'op': 'create',
            'identity': {'id': 'ios'},
            'values': {'text': 'direct'},
          });
        }),
      );
      final direct = await bounded(
        'DIRECT_READ',
        client!.read('Entry', {'id': 'ios'}),
      );
      if (direct?['text'] != 'direct') {
        throw StateError('direct write was not readable');
      }
      await bounded(
        'ENQUEUE',
        client!.mutate({
          'name': 'Edit',
          'operations': [
            {
              'model': 'Entry',
              'op': 'update',
              'identity': {'id': 'ios'},
              'values': {'text': 'queued'},
            },
          ],
        }),
      );
      final queued = await bounded(
        'ENQUEUE_READ',
        client!.read('Entry', {'id': 'ios'}),
      );
      final frozen = await bounded('FREEZE', client!.freeze());
      if (queued?['text'] != 'queued' || frozen == null || frozen.isEmpty) {
        throw StateError('queued write or freeze failed');
      }
      await bounded('MID_CLOSE', client!.close());
      client = null;
      client = await bounded(
        'REOPEN',
        Client.open(path: databasePath, schema: _schema),
      );
      final reopened = await bounded(
        'REOPEN_READ',
        client!.read('Entry', {'id': 'ios'}),
      );
      final reopenedFreeze = await bounded('REOPEN_FREEZE', client!.freeze());
      if (reopened?['text'] != 'queued' || reopenedFreeze != frozen) {
        throw StateError('close and reopen state mismatch');
      }
      expectedFile.writeAsStringSync(frozen, flush: true);
      result = 'AXTON_SMOKE_PHASE1_OK';
    }
  } catch (error, stack) {
    result = 'FAIL: $error\n$stack';
    resultFile.writeAsStringSync(result, flush: true);
  } finally {
    if (client != null) {
      try {
        await bounded('FINAL_CLOSE', client!.close());
      } catch (error, stack) {
        result = 'FAIL: close: $error\n$stack\n$result';
      }
    }
    resultFile.writeAsStringSync(result, flush: true);
    stage('RESULT_WRITTEN');
  }
  runApp(
    MaterialApp(
      home: Scaffold(body: Center(child: Text(result))),
    ),
  );
}
