/// Writes `hook/native_manifest.dart` for a release: the package version and
/// the SHA-256 of every Dart target's library, read from a directory that
/// holds them under their release file names, `libaxton_dart-<V>-<target>.<ext>`.
///
/// ```sh
/// dart run tool/write_native_manifest.dart --artifacts DIR [--package DIR] [--targets FILE]
/// ```
///
/// `--package` is the package to write into (default: this one) and supplies
/// V from its `pubspec.yaml`. `--targets` is the release target table
/// (default: the repository's `scripts/release/targets.json`); every host
/// target with a Dart library and every mobile target must be present.
library;

import 'dart:convert';
import 'dart:io';

import 'package:crypto/crypto.dart';

void main(List<String> args) {
  final options = <String, String>{};
  for (var i = 0; i + 1 < args.length; i += 2) {
    options[args[i]] = args[i + 1];
  }
  final artifacts = options['--artifacts'];
  if (artifacts == null || args.length.isOdd) {
    stderr.writeln(
      'usage: dart run tool/write_native_manifest.dart --artifacts DIR '
      '[--package DIR] [--targets FILE]',
    );
    exit(2);
  }
  final here = Platform.script.resolve('..');
  final package = Directory(options['--package'] ?? here.toFilePath());
  final targets = File(
    options['--targets'] ??
        here.resolve('../../scripts/release/targets.json').toFilePath(),
  );

  final version = RegExp(r'^version:\s*(\S+)', multiLine: true)
      .firstMatch(File('${package.path}/pubspec.yaml').readAsStringSync())
      ?.group(1);
  if (version == null) fail('${package.path}/pubspec.yaml has no version');

  final table = jsonDecode(targets.readAsStringSync()) as Map<String, dynamic>;
  final libraries = {
    for (final target in table['host'] as List)
      if (target['dartLibrary'] != null)
        target['name'] as String: target['dartLibrary'] as String,
    for (final target in table['mobile'] as List)
      target['name'] as String: target['dynamicLibrary'] as String,
  };

  final entries = StringBuffer();
  for (final MapEntry(key: target, value: built) in libraries.entries) {
    final extension = built.substring(built.lastIndexOf('.'));
    final name = 'libaxton_dart-$version-$target$extension';
    final file = File('$artifacts/$name');
    if (!file.existsSync()) fail('$artifacts has no $name');
    final digest = sha256.convert(file.readAsBytesSync());
    entries.writeln("  '$target': (file: '$name', sha256: '$digest'),");
  }

  final out = File('${package.path}/hook/native_manifest.dart');
  out.writeAsStringSync('''
// The native libraries of this package's release, written by
// tool/write_native_manifest.dart when the release is staged. A checkout
// lists none: its clients open with an explicit `libraryPath`.

const nativeVersion = '$version'; // x-release-please-version

/// Release file name and SHA-256 of each target's `libaxton_dart`, by target.
const nativeLibraries = <String, ({String file, String sha256})>{
$entries};
''');
  final format = Process.runSync(Platform.resolvedExecutable, [
    'format',
    out.path,
  ]);
  if (format.exitCode != 0) fail('dart format ${out.path}: ${format.stderr}');
  stdout.writeln(
    'wrote ${out.path}: axton $version, ${libraries.length} targets',
  );
}

Never fail(String message) {
  stderr.writeln('write_native_manifest: $message');
  exit(1);
}
