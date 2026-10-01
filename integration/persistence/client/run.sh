#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$root/scripts/env.sh"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/axton-scope-reopen.XXXXXX")"
trap 'rm -rf -- "$fixture"' EXIT
cargo run --manifest-path "$root/Cargo.toml" -p axton-compiler --locked -- compile "$root/integration/persistence/client" "$fixture" --client-runtime "$root/packages/client-js/index.mts" --backend-runtime "$root/packages/server/index.mts" --mutation-history "$fixture/mutations.json" --model-history "$fixture/models.json" --action-history "$fixture/actions.json" --load-history "$fixture/loads.json" --initialize-mutation-history --initialize-model-history --initialize-action-history --initialize-load-history
python3 "$root/integration/persistence/client/seed.py" "$fixture/js.sqlite"
node "$root/integration/persistence/client/reopen.mts" "$fixture" "$fixture/js.sqlite"
python3 "$root/integration/persistence/client/seed.py" "$fixture/dart.sqlite"
cp "$root/integration/persistence/client/reopen.dart" "$fixture/reopen.dart"
dart --packages="$root/integration/generated-api/.dart_tool/package_config.json" "$fixture/reopen.dart" "$fixture/dart.sqlite" "$root/crates/sqlite/tests/fixtures/frozen-push-logical.json"
