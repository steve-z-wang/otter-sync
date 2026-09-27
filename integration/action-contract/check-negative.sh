#!/usr/bin/env bash
# Every deliberate misuse in negative.dart must produce its expected diagnostic.
set -uo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output="$(cd "$dir" && dart analyze negative.dart 2>&1)"
status=$?
if [[ $status -eq 0 ]]; then
  echo 'Mutation/Query Dart API misuse unexpectedly analyzed cleanly.' >&2
  exit 1
fi
expected=(
  'missing_required_argument:8'
  'duplicate_named_argument:9'
  'argument_type_not_assignable:10'
  'undefined_named_parameter:11'
  'list_element_type_not_assignable:12'
  'undefined_getter:14'
  'undefined_method:15'
  'undefined_getter:17'
  'undefined_getter:18'
  'undefined_getter:19'
  'argument_type_not_assignable:20'
  'argument_type_not_assignable:21'
  'missing_required_argument:22'
  'list_element_type_not_assignable:23'
  'missing_required_argument:24'
  'argument_type_not_assignable:25'
  'argument_type_not_assignable:35'
  'missing_required_argument:40'
  'argument_type_not_assignable:43'
  'undefined_enum_constant:44'
  'argument_type_not_assignable:46'
  'undefined_enum_constant:47'
  'const_with_undefined_constructor:50'
  'argument_type_not_assignable:51'
  'undefined_named_parameter:52'
  'argument_type_not_assignable:53'
  'undefined_getter:57'
  'undefined_method:58'
  'undefined_method:59'
  'invalid_assignment:60'
  'invalid_assignment:61'
  'missing_required_argument:62'
  'undefined_getter:64'
  'undefined_named_parameter:71'
  'undefined_named_parameter:72'
  'undefined_named_parameter:73'
  'undefined_named_parameter:74'
  'undefined_named_parameter:75'
  'undefined_method:76'
  'use_of_void_result:77'
  'missing_required_argument:82'
  'argument_type_not_assignable:83'
  'argument_type_not_assignable:84'
  'missing_required_argument:89'
  'argument_type_not_assignable:90'
  'use_of_void_result:91'
  'use_of_void_result:92'
)
failed=0
for pair in "${expected[@]}"; do
  code="${pair%%:*}"
  line="${pair##*:}"
  if ! grep -E "negative\.dart:${line}:[0-9]+ - .* - ${code}$" <<<"$output" >/dev/null; then
    echo "Expected $code on negative.dart:$line was not reported." >&2
    failed=1
  fi
done
actual="$(grep -c ' error - negative.dart:' <<<"$output")"
if [[ "$actual" -ne "${#expected[@]}" ]]; then
  echo "Expected ${#expected[@]} analyzer errors, got $actual." >&2
  failed=1
fi
if [[ $failed -ne 0 ]]; then
  echo "$output" >&2
  exit 1
fi
echo "Mutation/Query Dart API refuses misuse: $actual expected analyzer errors."
