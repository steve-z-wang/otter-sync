#!/usr/bin/env bash
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
set +e
output="$(cd "$dir" && "${AXTON_DART:-dart}" analyze negative.dart 2>&1)"
status=$?
set -e
[[ $status -ne 0 ]] || { echo 'Generated Dart misuse analyzed cleanly' >&2; exit 1; }
count=0
while IFS=: read -r line rest; do
 code="${rest##*// error: }"
 [[ "$code" != "$rest" ]] || continue
 if ! grep -E "negative\.dart:${line}:[0-9]+ - .* - ${code}$" <<<"$output" >/dev/null; then
  echo "Expected $code at line $line" >&2; echo "$output" >&2; exit 1
 fi
 count=$((count+1))
done < <(grep -n '// error:' "$dir/negative.dart")
echo "Generated Dart misuse refused: $count expected diagnostics."
