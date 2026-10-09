#!/usr/bin/env bash
# Generates the Node and React Native clients and the backend types from one schema.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
cargo run -p axton-compiler --locked -- compile examples/todo/models examples/todo/generated/node \
  --backend-runtime ../../../../packages/backend/server/index.mts \
  --client-runtime ../../../../packages/frontend/client-js/index.mts
cargo run -p axton-compiler --locked -- compile examples/todo/models examples/todo/generated/mobile \
  --backend-runtime ../../../../packages/backend/server/index.mts \
  --client-runtime ../../../../packages/frontend/client-react-native/index.ts
