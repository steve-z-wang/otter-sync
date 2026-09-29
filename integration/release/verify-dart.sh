#!/usr/bin/env bash
# Builds a Dart application outside the repository against an installed axton
# package and runs it: the package's build hook selects this host's library,
# checks its SHA-256 and bundles it, and the client opens without a
# libraryPath, writes, closes and reopens a local database.
#
#   bash integration/release/verify-dart.sh RELEASE_DIR    the staged axton-dart-<V>.tar.gz in RELEASE_DIR,
#                                                          with the libraries beside it
#   bash integration/release/verify-dart.sh --registry V   axton V from pub.dev, with the libraries
#                                                          from its GitHub release
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/axton-installed-dart.XXXXXX")"
trap 'rm -rf -- "$work"' EXIT
fail() { echo "verify-dart: $*" >&2; exit 1; }

project="$work/project"
cp -R "$root/integration/release/installed_dart" "$project"
if [[ "${1:-}" == --registry ]]; then
  [[ -n "${2:-}" ]] || fail "--registry needs a version"
  printf 'dependencies:\n  axton: %s\n' "$2" >>"$project/pubspec.yaml"
else
  [[ -d "${1:-}" ]] || fail "usage: $0 RELEASE_DIR | --registry V"
  release="$(cd "$1" && pwd)"
  archives=("$release"/axton-dart-*.tar.gz)
  [[ ${#archives[@]} -eq 1 && -f "${archives[0]}" ]] || fail "$release holds no single axton-dart-*.tar.gz"
  mkdir "$work/axton"
  tar -xzf "${archives[0]}" -C "$work/axton"
  printf 'dependencies:\n  axton:\n    path: %s\nhooks:\n  user_defines:\n    axton:\n      local_artifacts: %s\n' \
    "$work/axton" "$release" >>"$project/pubspec.yaml"
fi
cd "$project"
dart pub get
dart run bin/check.dart
