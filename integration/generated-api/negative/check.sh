#!/usr/bin/env bash
# Check every intentional misuse against its own analyzer location and code.
set -euo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output="$(mktemp)"
trap 'rm -f "$output"' EXIT
if dart analyze --format machine "$dir" > "$output" 2>&1; then
  echo 'Generated Dart misuse unexpectedly analyzed cleanly.' >&2
  exit 1
fi
python3 - "$dir/misuse.dart" "$output" <<'PY'
from pathlib import Path
import re,sys
source,report=map(Path,sys.argv[1:])
expected={i:m.group(1) for i,line in enumerate(source.read_text().splitlines(),1) if (m:=re.search(r'// reject: (\w+)',line))}
actual=set()
for line in report.read_text().splitlines():
    parts=line.split('|')
    if len(parts)>=8 and Path(parts[3]).resolve()==source.resolve():
        actual.add((int(parts[4]),parts[2]))
missing=[f'line {line}: {code}' for line,code in expected.items() if (line,code) not in actual]
unexpected=[f'line {line}: {code}' for line,code in actual if code not in {'UNUSED_LOCAL_VARIABLE'} and line not in expected]
if missing or unexpected or not expected:
    print('\n'.join(['Missing: '+m for m in missing]+['Unexpected: '+m for m in unexpected]),file=sys.stderr)
    print(report.read_text(),file=sys.stderr)
    sys.exit(1)
print(f'Generated Dart API refuses all {len(expected)} marked misuse cases.')
PY
