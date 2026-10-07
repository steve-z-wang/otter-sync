#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-sdk04-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres" node --test --test-concurrency=1 integration/v04-sdk/host.test.mts integration/v04-sdk/http-shutdown.test.mts
