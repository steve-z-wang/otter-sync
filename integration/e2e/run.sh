#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
case "$(uname -s)" in
 Darwin) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.dylib";;
 Linux) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.so";;
 *) echo 'Unsupported native test host' >&2; exit 1;;
esac
export AXTON_DART_LIBRARY="$AXTON_LIBRARY"
bash "$root/scripts/build.sh"
(cd "$root/packages/dart" && dart pub get)
(cd "$root/integration/e2e/fixtures/round-trip" && npm ci && PRISMA_GENERATE_SKIP_AUTOINSTALL=true npm run generate)
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-e2e-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
node --test "$root/integration/e2e/round-trip.test.mjs"
node --test "$root/integration/e2e/subscriptions.test.mjs"
node --test "$root/integration/e2e/bootstrap.test.mjs"
node --test "$root/integration/e2e/parity.test.mjs"
