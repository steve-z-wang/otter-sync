#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
cargo build --workspace --locked
node bindings/node/build.mjs
# @axtonjs/native: the addon and its generated napi-rs loader; then the compiled
# JavaScript SDKs, which workspace packages import by name.
node_modules/.bin/napi build --platform --cwd packages/native --manifest-path ../../bindings/node/Cargo.toml --output-dir . --js index.js --dts index.d.ts
node_modules/.bin/tsc -b scripts/release/tsconfig.json
