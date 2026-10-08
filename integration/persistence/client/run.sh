#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$root/scripts/env.sh"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/axton-scope-reopen.XXXXXX")"
trap 'rm -rf -- "$fixture"' EXIT
cargo run --manifest-path "$root/Cargo.toml" -p axton-compiler --locked -- compile "$root/integration/persistence/client" "$fixture" --client-runtime "$root/packages/client-js/index.mts" --backend-runtime "$root/packages/server/index.mts" --mutation-history "$fixture/mutations.json" --model-history "$fixture/models.json" --action-history "$fixture/actions.json" --initialize-mutation-history --initialize-model-history --initialize-action-history
# Refuse the original layout unchanged; a fresh file is independent.
cargo test --manifest-path "$root/Cargo.toml" -p axton-sqlite --test stream_upgrade original_v02_layout_is_refused_unchanged_and_fresh_file_is_independent
snapshot() {
 python3 - "$1" "$2" <<'PYCODE'
import sqlite3, sys
from pathlib import Path
path=Path(sys.argv[1]).resolve()
wal=Path(str(path)+'-wal')
if wal.exists() and wal.stat().st_size:
    raise RuntimeError('snapshot requires a closed, checkpointed fixture')
# Both subprocesses have ended; read the stable image without creating WAL sidecars.
con=sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)
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
dart --packages="$root/integration/generated-api/.dart_tool/package_config.json" "$fixture/reopen.dart" "$fixture/dart.sqlite"

snapshot "$fixture/dart.sqlite" "$fixture/dart.after"
cmp "$fixture/dart.before" "$fixture/dart.after"
