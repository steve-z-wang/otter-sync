#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$root/scripts/env.sh"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/axton-scope-reopen.XXXXXX")"
trap 'rm -rf -- "$fixture"' EXIT
cargo run --manifest-path "$root/Cargo.toml" -p axton-compiler --locked -- compile "$root/integration/persistence/client" "$fixture" --client-runtime "$root/packages/client-js/index.mts" --backend-runtime "$root/packages/server/index.mts" --mutation-history "$fixture/mutations.json" --model-history "$fixture/models.json" --action-history "$fixture/actions.json" --initialize-mutation-history --initialize-model-history --initialize-action-history
# Preserve explicit internal legacy migration/frozen-byte coverage separately
# from the protocol-4 public rejection boundary below.
cargo test --manifest-path "$root/Cargo.toml" -p axton-sqlite --test stream_upgrade original_v02_layout_reopens_without_parallel_empty_holds_or_queue_loss
snapshot() {
 python3 - "$1" "$2" <<'PYCODE'
import sqlite3, sys
con=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True)
with open(sys.argv[2],'w') as out: out.write('\n'.join(con.iterdump()))
con.close()
PYCODE
}
python3 "$root/integration/persistence/client/seed.py" "$fixture/js.sqlite"
snapshot "$fixture/js.sqlite" "$fixture/js.before"
node "$root/integration/persistence/client/reopen.mts" "$fixture" "$fixture/js.sqlite"
snapshot "$fixture/js.sqlite" "$fixture/js.after"
cmp "$fixture/js.before" "$fixture/js.after"
python3 "$root/integration/persistence/client/seed.py" "$fixture/dart.sqlite"
snapshot "$fixture/dart.sqlite" "$fixture/dart.before"
cp "$root/integration/persistence/client/reopen.dart" "$fixture/reopen.dart"
dart --packages="$root/integration/generated-api/.dart_tool/package_config.json" "$fixture/reopen.dart" "$fixture/dart.sqlite" "$root/crates/sqlite/tests/fixtures/frozen-push-logical.json"

snapshot "$fixture/dart.sqlite" "$fixture/dart.after"
cmp "$fixture/dart.before" "$fixture/dart.after"
