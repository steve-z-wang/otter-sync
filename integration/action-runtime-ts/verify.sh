#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
cargo run -p axton-compiler --locked -- compile integration/action-runtime-ts/source integration/action-runtime-ts \
  --backend-runtime ../../packages/backend/server/index.mts \
  --client-runtime ../../packages/frontend/client-js/index.mts
"$root/node_modules/.bin/tsc" -p integration/action-runtime-ts
node --experimental-strip-types --test integration/action-runtime-ts/*.test.mts
node --experimental-strip-types --test integration/bindings/client-react-native/actions.test.mjs
