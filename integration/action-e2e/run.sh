#!/usr/bin/env bash
# Compile the public Action fixture, then exercise it through native Node,
# local SQLite, HTTP, the generated backend, and disposable PostgreSQL.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
case "$(uname -s)" in
 Darwin) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.dylib";;
 Linux) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.so";;
 *) echo 'Unsupported native test host' >&2; exit 1;;
esac
export AXTON_DART_LIBRARY="$AXTON_LIBRARY"
cd "$root"
cargo run -p axton-compiler --locked -- compile integration/action-e2e/source integration/action-e2e \
  --backend-runtime ../../packages/backend/server/index.mts \
  --client-runtime ../../packages/frontend/client-js/index.mts
cargo run -p axton-compiler --locked -- compile integration/action-e2e/evolved/source integration/action-e2e/evolved \
  --backend-runtime ../../../packages/backend/server/index.mts \
  --client-runtime ../../../packages/frontend/client-js/index.mts
"$root/node_modules/.bin/tsc" -p integration/action-e2e
dart pub get --directory integration/action-runtime-dart
dart analyze integration/action-runtime-dart/action_e2e_publish.dart integration/action-runtime-dart/action_e2e_datetime.dart integration/action-runtime-dart/action_e2e_hook.dart
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-action-e2e-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
node --experimental-strip-types --test integration/action-e2e/action.test.mts
