/// Bundles this release's prebuilt `libaxton_dart` as the code asset
/// `package:axton/axton_dart`, which the bridge's `@Native` functions resolve.
///
/// The library for the build's target comes from the AXTON GitHub release, or
/// from the directory named by the `local_artifacts` user-define. Either way
/// its SHA-256 must equal the one [nativeLibraries] records, and it is cached
/// under that hash, so a warm cache reads neither source.
library;

import 'dart:io';
import 'dart:typed_data';

import 'package:axton/src/bindings/native_manifest.dart';
import 'package:code_assets/code_assets.dart';
import 'package:crypto/crypto.dart';
import 'package:hooks/hooks.dart';

void main(List<String> args) async {
  await build(args, (input, output) async {
    if (!input.config.buildCodeAssets) return;
    // A checkout lists no libraries; clients there pass `libraryPath`.
    if (nativeLibraries.isEmpty) return;
    final code = input.config.code;
    final target = _target(code);
    final library = nativeLibraries[target];
    if (library == null) {
      throw BuildError(
        message:
            'axton $nativeVersion has no native library for '
            '${target ?? '${code.targetOS.name}-${code.targetArchitecture.name}'}. '
            'Supported targets: ${nativeLibraries.keys.join(', ')}.',
      );
    }
    // One file name per platform: Flutter names the iOS framework and the
    // Android library after it.
    final bundled = library.file.endsWith('.so')
        ? 'libaxton_dart.so'
        : 'libaxton_dart.dylib';
    final cached = File.fromUri(
      input.outputDirectoryShared.resolve(
        'axton-$nativeVersion/${library.sha256}/$bundled',
      ),
    );
    if (!cached.existsSync() ||
        sha256.convert(cached.readAsBytesSync()).toString() != library.sha256) {
      final local = input.userDefines.path('local_artifacts');
      final Uint8List bytes;
      final String source;
      if (local != null) {
        final file = File.fromUri(
          Directory.fromUri(local).uri.resolve(library.file),
        );
        output.dependencies.add(file.uri);
        if (!file.existsSync()) {
          throw BuildError(
            message:
                'axton: hooks.user_defines.axton.local_artifacts is '
                '${local.toFilePath()}, which has no ${library.file}.',
          );
        }
        bytes = file.readAsBytesSync();
        source = file.path;
      } else {
        final url = _releaseUrl(library.file);
        bytes = await _download(url, library.file);
        source = '$url';
      }
      final actual = sha256.convert(bytes).toString();
      if (actual != library.sha256) {
        throw BuildError(
          message:
              'axton: $source has SHA-256 $actual, but axton $nativeVersion '
              'expects ${library.sha256}. It is not the library this package '
              'was released with; refusing to bundle it.',
        );
      }
      cached.parent.createSync(recursive: true);
      final partial = File('${cached.path}.partial')..writeAsBytesSync(bytes);
      partial.renameSync(cached.path);
    }
    output.assets.code.add(
      CodeAsset(
        package: input.packageName,
        name: 'axton_dart',
        linkMode: DynamicLoadingBundled(),
        file: cached.uri,
      ),
    );
  });
}

/// The release target name (`scripts/release/targets.json`) of a build, or
/// null when AXTON has none for it.
String? _target(CodeConfig code) => switch ((
  code.targetOS,
  code.targetArchitecture,
)) {
  (OS.macOS, Architecture.arm64) => 'darwin-arm64',
  (OS.linux, Architecture.x64) => 'linux-x64-gnu',
  (OS.iOS, Architecture.arm64) =>
    code.iOS.targetSdk == IOSSdk.iPhoneOS ? 'ios-arm64' : 'ios-arm64-simulator',
  (OS.iOS, Architecture.x64) =>
    code.iOS.targetSdk == IOSSdk.iPhoneSimulator ? 'ios-x64-simulator' : null,
  (OS.android, Architecture.arm64) => 'android-arm64-v8a',
  (OS.android, Architecture.arm) => 'android-armeabi-v7a',
  (OS.android, Architecture.x64) => 'android-x86_64',
  _ => null,
};

/// Where the AXTON release of this version publishes [file].
Uri _releaseUrl(String file) => Uri.https(
  'github.com',
  '/zanminwang/axton/releases/download/v$nativeVersion/$file',
);

Future<Uint8List> _download(Uri url, String file) async {
  final client = HttpClient()..findProxy = HttpClient.findProxyFromEnvironment;
  try {
    final response = await (await client.getUrl(url)).close();
    if (response.statusCode != HttpStatus.ok) {
      throw InfraError(
        message:
            'axton: GET $url answered HTTP ${response.statusCode}. To build '
            'without it, set hooks.user_defines.axton.local_artifacts to a '
            'directory holding $file.',
      );
    }
    final body = await response.fold(
      BytesBuilder(copy: false),
      (builder, chunk) => builder..add(chunk),
    );
    return body.takeBytes();
  } on IOException catch (error, trace) {
    throw InfraError(
      message: 'axton: downloading $url failed: $error',
      wrappedException: error,
      wrappedTrace: trace,
    );
  } finally {
    client.close();
  }
}
