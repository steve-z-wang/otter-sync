#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$root/scripts/env.sh"
node "$root/bindings/node/build.mjs"
(cd "$root/integration/bindings/node" && npm ci && npm run generate)
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-server-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
# The declaration collector needs no database.
node --test "$root/integration/persistence/server/effects.test.mjs" "$root/integration/persistence/server/protocol-admission.test.mjs"
# Test files run in parallel by default; both apply migration.sql to one cluster, so keep them sequential.
# A test that waits on a transaction another one left open fails at the timeout, and the
# runner exits rather than waiting on a pool that cannot drain.
test=(node --test --test-timeout=300000 --test-force-exit)
"${test[@]}" "$root/integration/persistence/server/driver-conformance.test.mjs"
"${test[@]}" "$root/integration/persistence/server/runtime.test.mjs" "$root/integration/persistence/server/host-contract.test.mjs"
"${test[@]}" "$root/integration/persistence/server/actions.test.mjs"
"${test[@]}" "$root/integration/persistence/server/membership.test.mjs"
"${test[@]}" "$root/integration/persistence/server/loads.test.mjs"
"${test[@]}" "$root/integration/persistence/server/fetch.test.mjs"
"${test[@]}" "$root/integration/persistence/server/stream-tracking.test.mjs"
# Protocol 4 starts from an empty publication history. Legacy tests above may
# leave compacted positions without protocol-4 group evidence, so use a fresh
# database in the same disposable cluster for its vertical and adapter gates.
createdb -h 127.0.0.1 -p "$port" axton_protocol4
protocol4_database_url="${DATABASE_URL%/postgres}/axton_protocol4"
DATABASE_URL="$protocol4_database_url" "${test[@]}" "$root/integration/persistence/server/protocol-v04.test.mjs"
DATABASE_URL="$protocol4_database_url" "${test[@]}" "$root/integration/persistence/server/protocol-v04-drivers.test.mjs"
DATABASE_URL="$protocol4_database_url" "${test[@]}" "$root/integration/persistence/server/publication-closure.test.mjs"
