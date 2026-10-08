// Runs in a scratch application that depends on an installed axton package:
// the package's build hook bundled this host's library, so the client opens
// without a libraryPath, writes, closes and reopens its database.
import 'dart:io';

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
      ],
    },
  ],
};

Future<void> main() async {
  final directory = await Directory.systemTemp.createTemp('axton-installed-');
  final path = '${directory.path}/client.sqlite';
  final connection = StoreConnection(
    url: 'http://127.0.0.1:1',
    token: () => 'offline',
  );
  Client.configureApplicationData(directory.path);
  try {
    final client = await Client.open(
      path: path,
      schema: _schema,
      stream: 'User:viewer',
      connection: connection,
    );
    await client.transaction((tx) async {
      await tx.direct({
        'model': 'Entry',
        'op': 'create',
        'identity': {'id': 'installed'},
        'values': {'text': 'kept'},
      });
    });
    await client.close();
    final reopened = await Client.open(
      path: path,
      schema: _schema,
      stream: 'User:viewer',
      connection: connection,
    );
    final row = await reopened.read('Entry', {'id': 'installed'});
    await reopened.close();
    if (row?['text'] != 'kept') {
      throw StateError('the reopened database lost the write: $row');
    }
    print('axton: the bundled library wrote and reopened a local database');
  } finally {
    await directory.delete(recursive: true);
  }
}
