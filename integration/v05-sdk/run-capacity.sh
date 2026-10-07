#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
export CARGO_INCREMENTAL=0
if [[ "${AXTON_CAPACITY_PROFILE:-debug}" == release ]]; then
  node bindings/node/build.mjs --release
else
  node bindings/node/build.mjs
fi
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-task7-capacity-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
export AXTON_CAPACITY_EVIDENCE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/axton-task7-capacity-evidence.XXXXXX")"
printf 'Retained exact PostgreSQL carrier evidence: %s\n' "$AXTON_CAPACITY_EVIDENCE_DIR"
node --test --test-reporter=tap --test-timeout=600000 --test-name-pattern='real selected and unique-model capacity measurements' integration/persistence/server/protocol-v05-delivery.test.mjs
