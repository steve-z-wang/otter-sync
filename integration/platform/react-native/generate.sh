#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$root"
cargo run -p axton-compiler --locked -- compile integration/platform/react-native/models integration/platform/react-native/generated --backend-runtime ../../../../packages/backend/server/index.mts --client-runtime ../../../../packages/frontend/client-react-native/index.ts
