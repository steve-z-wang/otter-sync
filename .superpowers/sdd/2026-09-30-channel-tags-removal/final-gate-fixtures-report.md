# Final gate fixture verification

Owned changes: generated API JS/Dart bootstrap responses, JS binding live/subscription channel frames, and React Native binding network channel frame. Ordinary Action/Fetch/Load authority records and typed API negative assertions remain intact. No production changes.

## RED

Required generated API gate failed in `/tmp/axton-final-generated-api.log` at `test.ts:296` with `bootstrap.protocol_invalid`: `bootstrap page changes missing`. Its JS and Dart bootstrap fixtures emitted the retired `records` field.

`CI=true node --test integration/bindings/client-js/*.test.mjs integration/bindings/client-react-native/*.test.mjs` exposed outdated channel upserts without channel/cursor/kind. After adapting those frames, the test server also failed to answer automatic bootstrap reconciliation (`Cannot convert undefined or null to object` at `emptyPage`) and a reconnect assertion omitted SDK-owned capability metadata. Logs: `/tmp/axton-final-bindings-fixtures-red.log` and intermediate green log.

## GREEN

```sh
CI=true PATH=/opt/homebrew/share/flutter/bin/cache/dart-sdk/bin:$PATH AXTON_LIBRARY=$PWD/target/debug/libaxton_dart.dylib AXTON_DART_LIBRARY=$PWD/target/debug/libaxton_dart.dylib bash integration/generated-api/verify.sh
```

Exit 0. Compiler tests, generation, TypeScript typecheck and missing-handler negative check, JS/native generated clients, Dart analyzer, negative API assertions (50 expected analyzer errors), and 12 generated Dart tests pass. Evidence: `/tmp/axton-final-generated-api-rerun.log`. Loopback/network access required escalation; the first sandbox attempt exited at `http.listen` before exercising the fixtures.

```sh
CI=true node --test integration/bindings/client-js/*.test.mjs integration/bindings/client-react-native/*.test.mjs
```

Exit 0: 269 tests passed, 0 failed. Evidence: `/tmp/axton-final-bindings-fixtures-green.log`.

`git diff --check` passed. Native artifacts reused; no root dependency install or build performed. The unrelated React Native generated backend artifact was preserved.
