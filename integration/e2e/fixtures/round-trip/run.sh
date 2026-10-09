#!/usr/bin/env bash
# Serves the Entry/Edit round-trip fixture backend on PORT (default 4242) against a
# disposable PostgreSQL cluster. Regression coverage lives in ../../round-trip.test.mjs.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
source "$root/scripts/env.sh"
bash "$root/scripts/build.sh"
(cd "$root" && cargo run -p axton-compiler --locked -- compile \
 integration/e2e/fixtures/round-trip/models integration/e2e/fixtures/round-trip/generated \
 --backend-runtime ../../../../../packages/backend/server/index.mts \
 --client-runtime ../../../../../packages/frontend/client-js/index.mts)
(cd "$root/integration/e2e/fixtures/round-trip" && npm ci && npm run generate)
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-fixture-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
node "$root/integration/e2e/fixtures/round-trip/server.mts"
