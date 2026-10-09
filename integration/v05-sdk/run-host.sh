#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
# Build this checkout; a binary from another worktree is not acceptance evidence.
bash scripts/build.sh
cargo run -p axton-compiler --locked -- compile integration/v05-sdk/schema integration/v05-sdk --backend-runtime ../../packages/backend/server/index.mts --client-runtime ../../packages/frontend/client-js/index.mts
cargo run -p axton-compiler --locked -- compile integration/v05-sdk/rollover/schema integration/v05-sdk/rollover --backend-runtime ../../../packages/backend/server/index.mts --client-runtime ../../../packages/frontend/client-js/index.mts
cargo run -p axton-compiler --locked -- compile integration/v05-sdk/versioned/schema integration/v05-sdk/versioned --backend-runtime ../../../packages/backend/server/index.mts --client-runtime ../../../packages/frontend/client-js/index.mts
"$root/node_modules/.bin/tsc" --noEmit --strict --exactOptionalPropertyTypes --skipLibCheck --target ES2022 --module NodeNext --moduleResolution NodeNext --allowImportingTsExtensions integration/v05-sdk/backend-positive.ts integration/v05-sdk/versioned/backend-positive.ts
dart pub get --directory integration/v05-sdk
dart analyze integration/v05-sdk/generated.dart integration/v05-sdk/client.dart
case "$(uname -s)" in
 Darwin) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.dylib";;
 Linux) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.so";;
 *) exit 1;;
esac
export AXTON_DART_LIBRARY="$AXTON_LIBRARY"
export AXTON_DART="${AXTON_DART:-$(command -v dart)}"
cluster="$(mktemp -d "${TMPDIR:-/tmp}/axton-sdk05-pg.XXXXXX")"
cleanup(){ pg_ctl -D "$cluster/data" -m immediate stop >/dev/null 2>&1 || true; rm -rf -- "$cluster"; }
trap cleanup EXIT
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
initdb -D "$cluster/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$cluster/data" -l "$cluster/log" -o "-p $port -h 127.0.0.1 -k $cluster" start >/dev/null
DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres" node --test --test-reporter=tap --test-name-pattern="${AXTON_GATE_TESTS:-.*}" --test-timeout=120000 --test-force-exit integration/v05-sdk/client.mjs
