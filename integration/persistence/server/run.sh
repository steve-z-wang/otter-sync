#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$root/scripts/env.sh"
node "$root/bindings/node/build.mjs"
if [[ -z "${AXTON_PRISMA_CLIENT:-}" ]]; then
 (cd "$root/integration/bindings/node" && npm ci && npm run generate)
 export AXTON_PRISMA_CLIENT="$root/integration/bindings/node/generated/client"
fi
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-server-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
export DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres"
# Pure declaration, host codec, startup and retry gates need no database.
node --test "$root/integration/persistence/server/effects.test.mjs" "$root/integration/persistence/server/host-contract.test.mjs" "$root/integration/persistence/server/driver-runtime.test.mjs" "$root/integration/persistence/server/startup.test.mjs"
# Each file owns its fixtures; serialize files sharing the fresh namespace.
test=(node --test --test-timeout=300000 --test-force-exit)
for file in namespace-and-locks persistence-batching publication-savepoint publication-semantics retained-fetch read-liveness live-shutdown protocol-v05-batch protocol-v05-drivers protocol-v05-delivery; do
 "${test[@]}" "$root/integration/persistence/server/$file.test.mjs"
done
