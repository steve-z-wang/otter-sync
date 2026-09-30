// The native libraries of this package's release, written by
// tool/write_native_manifest.dart when the release is staged. A checkout
// lists none: its clients open with an explicit `libraryPath`.

const nativeVersion = '0.1.2'; // x-release-please-version

/// Release file name and SHA-256 of each target's `libaxton_dart`, by target.
const nativeLibraries = <String, ({String file, String sha256})>{};
