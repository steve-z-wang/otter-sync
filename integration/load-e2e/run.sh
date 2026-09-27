#!/usr/bin/env bash
# Compile the Load fixture into TypeScript and Dart from one schema and
# history, then exercise it through native Node and Dart clients, local SQLite,
# HTTP, the generated backend, and disposable PostgreSQL.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
cargo run -p axton-compiler --locked -- compile integration/load-e2e integration/load-e2e \
  --backend-runtime ../../packages/server/index.mts \
  --client-runtime ../../packages/client-js/index.mts
"$root/node_modules/.bin/tsc" -p integration/load-e2e
dart pub get --directory integration/load-e2e
dart analyze integration/load-e2e/client.dart
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-load-e2e-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
node --experimental-strip-types --test integration/load-e2e/load.test.mts
