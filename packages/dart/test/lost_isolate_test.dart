// A lost isolate detaches its runtimes (#134): the VM deletes an isolate's
// wake callback when the isolate shuts down, so its bridges detach through a
// native finalizer. The scenario runs in its own process, because the failure
// it guards against aborts the process.
import 'dart:convert';
import 'dart:io';

import 'package:test/test.dart';

void main() {
  test(
    'an isolate that exits with a transaction open releases its database',
    () async {
      final dir = await Directory.systemTemp.createTemp('axton-lost-isolate-');
      final child = await Process.start(Platform.resolvedExecutable, [
        'test/lost_isolate.dart',
        '${dir.path}/db',
        '../../fixtures/schemas/entry.json',
      ]);
      try {
        final output = child.stdout.transform(utf8.decoder).join();
        final errors = child.stderr.transform(utf8.decoder).join();
        final code = await child.exitCode.timeout(const Duration(seconds: 20));
        expect(code, 0, reason: '${await output}${await errors}');
      } finally {
        child.kill();
        await dir.delete(recursive: true);
      }
    },
  );
}
