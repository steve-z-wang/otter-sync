#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
source "$root/scripts/env.sh"
cd "$root/integration/v04-sdk"
"$root/node_modules/.bin/tsc" --noEmit --strict --exactOptionalPropertyTypes --skipLibCheck --target ES2022 --module NodeNext --moduleResolution NodeNext --allowImportingTsExtensions positive.ts backend-positive.ts
dart pub get --offline
dart analyze positive.dart native.dart application-data.dart host.dart
negative="$(mktemp "$PWD/negative-XXXXXX.dart")"
output="$(mktemp)"
trap 'rm -f "$negative" "$output"' EXIT
cp negative.dart.txt "$negative"
if dart analyze --format machine "$negative" >"$output" 2>&1; then
  echo 'Invalid Dart named API unexpectedly analyzed cleanly.' >&2
  exit 1
fi
python3 - "$output" <<'PY'
import sys
from pathlib import Path
errors={int(fields[4]):fields[2] for line in Path(sys.argv[1]).read_text().splitlines() if (fields:=line.split('|'))[0]=='ERROR'}
expected={3:'RETURN_OF_INVALID_TYPE_FROM_CLOSURE',4:'ARGUMENT_TYPE_NOT_ASSIGNABLE',5:'UNDEFINED_GETTER',6:'ARGUMENT_TYPE_NOT_ASSIGNABLE',7:'UNDEFINED_GETTER',8:'UNDEFINED_GETTER',9:'UNDEFINED_GETTER'}
if errors!=expected:
    raise SystemExit(f'Unexpected typed API negative diagnostics: {errors}')
print('Dart named API rejects all seven specified misuses.')
PY
