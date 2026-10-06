#!/usr/bin/env bash
# Real PostgreSQL HTTP/WebSocket -> native actor child -> SQLite acceptance.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
node bindings/node/build.mjs
node_modules/.bin/tsc -b scripts/release/tsconfig.json
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-04-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
node --experimental-strip-types --test --test-reporter=tap --test-timeout=180000 integration/0.4/production.test.mjs
