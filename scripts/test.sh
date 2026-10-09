#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$root/scripts/env.sh"
cd "$root"
npm ci
node scripts/release/version.mjs check
node --test integration/release/*.test.mjs
bash scripts/build.sh
cargo fmt --all --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
(cd integration/e2e/fixtures/round-trip && npm ci && npx prisma generate)
cargo run -p axton-compiler -- compile integration/e2e/fixtures/round-trip/models integration/e2e/fixtures/round-trip/generated --backend-runtime ../../../../../packages/backend/server/index.mts --client-runtime ../../../../../packages/frontend/client-js/index.mts
(cd examples/todo && npm ci && npx prisma generate)
bash examples/todo/generate.sh
npm run typecheck
cargo run -p axton-compiler --locked -- compile integration/action-contract integration/action-contract --backend-runtime ../../packages/backend/server/index.mts --client-runtime ../../packages/frontend/client-js/index.mts
"$root/node_modules/.bin/tsc" -p integration/action-contract
dart pub get --directory integration/action-contract
dart analyze integration/action-contract/generated.dart
dart analyze integration/action-contract/positive.dart
bash integration/action-contract/check-negative.sh
bash integration/action-runtime-ts/verify.sh
"$root/node_modules/.bin/prettier" --check packages/frontend/client-js/*.mts packages/frontend/client-js/api/*.mts packages/frontend/client-js/bindings/*.mts packages/backend/server/*.mts packages/backend/server/api/*.mts packages/backend/server/bindings/*.mts packages/backend/postgres/*.mts packages/backend/postgres/src/*.mts packages/frontend/client-react-native/*.mts packages/frontend/client-react-native/index.ts packages/frontend/client-react-native/api/*.mts packages/frontend/client-react-native/api/*.ts packages/frontend/client-react-native/bindings/*.mts
"$root/node_modules/.bin/tsc" -p packages/frontend/client-react-native
node --test packages/frontend/client-react-native/plugins/expo-path-spaces.test.cjs
node --test integration/bindings/client-js/*.test.mjs packages/frontend/client-js/*.test.mjs
node --test integration/bindings/client-react-native/*.test.mjs
bash integration/persistence/transaction-probe/run.sh
node --test packages/backend/server/bindings/*.test.mjs
bash integration/persistence/server/run.sh
bash integration/v05-sdk/run-host.sh
bash integration/v05-sdk/run-capacity.sh
case "$(uname -s)" in
 Darwin) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.dylib";;
 Linux) export AXTON_LIBRARY="$root/target/debug/libaxton_dart.so";;
 *) echo 'Use the documented platform-specific native library path on this host.' >&2; exit 1;;
esac
export AXTON_DART_LIBRARY="$AXTON_LIBRARY"
(cd packages/frontend/dart && dart pub get && dart analyze && dart test)
cargo run -p axton-compiler --locked -- compile integration/action-runtime-dart integration/action-runtime-dart --backend-runtime ../../packages/backend/server/index.mts --client-runtime ../../packages/frontend/client-js/index.mts
cargo run -p axton-compiler --locked -- compile integration/action-runtime-dart/model_only integration/action-runtime-dart/model_only --backend-runtime ../../../packages/backend/server/index.mts --client-runtime ../../../packages/frontend/client-js/index.mts
cargo run -p axton-compiler --locked -- compile integration/action-runtime-dart/model_free integration/action-runtime-dart/model_free --backend-runtime ../../../packages/backend/server/index.mts --client-runtime ../../../packages/frontend/client-js/index.mts
(cd integration/action-runtime-dart && dart pub get && dart analyze generated.dart generated_test.dart application_data.dart model_only/generated.dart model_free/generated.dart action_e2e_publish.dart action_e2e_datetime.dart && bash check-negative.sh && dart test generated_test.dart && dart run application_data.dart)
bash integration/generated-api/verify.sh
# Default-v5 e2e includes retained cascade, constraints/SIGKILL, authenticated
# Store context, whole-read atomicity and admitted HTTP shutdown assertions.
bash integration/e2e/run.sh
bash integration/action-e2e/run.sh
node --test integration/e2e/todo-ui.test.mjs
bash integration/e2e/todo-run.sh
python3 website/scripts/check_examples.py
bash integration/release/verify-installed.sh
