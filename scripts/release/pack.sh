#!/usr/bin/env bash
# Packs AXTON's npm packages at the release version with the standard tools:
# `tsc -b` compiles the JavaScript SDKs, `napi artifacts` and `napi pre-publish`
# place each Node addon in its platform package and pin those packages as
# optional dependencies of @axtonjs/native, `npm pkg set` does the same for the
# compiler packages of @axtonjs/cli, and `npm pack` packs every package.
#
#   bash scripts/release/pack.sh OUT ARTIFACTS [TARGET...]
#
# ARTIFACTS holds `axton-node.<target>.node` (from `napi build --platform`) and
# `<target>/axton` (the compiler) for each TARGET; without TARGET arguments every
# host target in scripts/release/targets.json is packed. The two selector
# packages are staged as copies so the source tree stays unchanged. OUT
# receives the archives and `packed.json`, npm's report of every archive, plus
# the Dart SDK staged under OUT/dart/axton and archived as
# OUT/axton-dart-<V>.tar.gz, the tree `dart pub publish` uploads.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[[ $# -ge 2 ]] || { echo "usage: $0 OUT ARTIFACTS [TARGET...]" >&2; exit 2; }
mkdir -p "$1"
out="$(cd "$1" && pwd)"
artifacts="$(cd "$2" && pwd)"
shift 2
targets_file="$root/scripts/release/targets.json"
targets=("$@")
dart_targets="$targets_file"
if [[ ${#targets[@]} -eq 0 ]]; then
  read -r -a targets <<<"$(node -p 'require(process.argv[1]).host.map((t) => t.name).join(" ")' "$targets_file")"
fi
cd "$root"
node scripts/release/version.mjs check
node_modules/.bin/tsc -b scripts/release/tsconfig.json
[[ -f packages/native/index.js ]] || { echo "build packages/native first (scripts/build.sh)" >&2; exit 1; }
version="$(node -p 'require("./package.json").version')"
stage="$out/stage"
rm -rf "$stage"
mkdir -p "$stage"
if [[ $# -gt 0 ]]; then
  # A host verification pack has only the selected host's artifacts. Full
  # release packing keeps the inventory, including every mobile library.
  dart_targets="$stage/dart-targets.json"
  node -e '
    const [, inventory, out, ...names] = process.argv;
    const fs = require("node:fs");
    const table = JSON.parse(fs.readFileSync(inventory));
    fs.writeFileSync(out, JSON.stringify({ ...table,
      host: table.host.filter((target) => names.includes(target.name)), mobile: [] }));
  ' "$targets_file" "$dart_targets" "${targets[@]}"
fi
cp -R packages/native packages/cli "$stage/"
rm -f "$stage"/native/*.node "$stage"/native/npm/*/*.node "$stage"/cli/npm/*/bin/axton

# The addon, with napi's own config narrowed to the selected targets: both napi
# commands require an artifact for every configured target.
node -e '
  const [, targetsFile, packageFile, out, ...names] = process.argv;
  const fs = require("node:fs");
  const host = JSON.parse(fs.readFileSync(targetsFile)).host;
  const triples = names.map((name) => host.find((t) => t.name === name).rust);
  fs.writeFileSync(out, JSON.stringify({ ...JSON.parse(fs.readFileSync(packageFile)).napi, targets: triples }));
' "$targets_file" packages/native/package.json "$stage/napi.json" "${targets[@]}"
napi="$root/node_modules/.bin/napi"
"$napi" artifacts --cwd "$stage/native" --config-path "$stage/napi.json" --output-dir "$artifacts" --npm-dir npm
"$napi" pre-publish --cwd "$stage/native" --config-path "$stage/napi.json" --npm-dir npm --skip-optional-publish --no-gh-release
rm -f "$stage"/native/*.node

# The compiler: one executable per platform package, pinned by the launcher package.
directories=("$stage/native" ./packages/backend/server ./packages/frontend/client-js ./packages/backend/postgres "$stage/cli")
for target in "${targets[@]}"; do
  mkdir -p "$stage/cli/npm/$target/bin"
  install -m 755 "$artifacts/$target/axton" "$stage/cli/npm/$target/bin/axton"
  (cd "$stage/cli" && npm pkg set "optionalDependencies.@axtonjs/cli-$target=$version")
  directories+=("$stage/native/npm/$target" "$stage/cli/npm/$target")
done
npm pack "${directories[@]}" --pack-destination "$out" --json --loglevel=error >"$out/packed.json"

# The Dart SDK: its tracked files, the LICENSE pub requires at the package root,
# the release notes, and, when ARTIFACTS holds the Dart libraries
# (`libaxton_dart-<V>-<target>.<ext>`), the native manifest their hashes fill.
dart_package="$out/dart/axton"
rm -rf "$dart_package"
mkdir -p "$dart_package"
git ls-files packages/frontend/dart | tar -cf - -T - | tar -xf - -C "$dart_package" --strip-components=3
rm "$dart_package/pubspec.lock" # pub never uploads it
cp LICENSE "$dart_package/LICENSE"
cp CHANGELOG.md "$dart_package/CHANGELOG.md"
if compgen -G "$artifacts/libaxton_dart-*" >/dev/null; then
  (cd packages/frontend/dart && dart run tool/write_native_manifest.dart --artifacts "$artifacts" --package "$dart_package" --targets "$dart_targets")
else
  echo "no libaxton_dart artifacts in $artifacts: the staged Dart package lists no native libraries" >&2
fi
# pub.dev accepts no hook/**/*.dart other than hook/build.dart and hook/link.dart.
stray="$(cd "$dart_package" && find hook -name '*.dart' ! -path hook/build.dart ! -path hook/link.dart)"
[[ -z "$stray" ]] || { echo "pub.dev rejects hook Dart files other than hook/build.dart and hook/link.dart: $stray" >&2; exit 1; }
COPYFILE_DISABLE=1 tar -czf "$out/axton-dart-$version.tar.gz" -C "$dart_package" .
node -e 'for (const p of require(process.argv[1])) console.log(`${p.filename} (${p.files.length} files)`)' "$out/packed.json"
