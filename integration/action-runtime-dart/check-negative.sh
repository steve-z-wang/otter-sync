#!/usr/bin/env bash
set -uo pipefail
cd "$(dirname "$0")"
output="$(dart analyze negative.dart 2>&1)"
status=$?
if [[ $status -eq 0 ]]; then
  echo 'Generated Dart misuse analyzed cleanly' >&2
  exit 1
fi
expected=(
  'argument_type_not_assignable:9'
  'list_element_type_not_assignable:12'
  'undefined_named_parameter:17'
  'undefined_method:20'
  'undefined_getter:22'
  'undefined_getter:23'
  'invalid_assignment:24'
  'undefined_getter:26'
  'undefined_getter:33'
  'undefined_getter:34'
  # `code:line:name` also requires the message to name that member, so a
  # missing facade cannot stand in for the missing member.
  'undefined_getter:41:mutations'
  'undefined_getter:42:clock'
)
for entry in "${expected[@]}"; do
  IFS=: read -r code line name <<<"$entry"
  message='.*'
  [[ -n "$name" ]] && message=".*'${name}'.*"
  if ! grep -E "negative\.dart:${line}:[0-9]+ - ${message} - ${code}$" <<<"$output" >/dev/null; then
    echo "Expected $code at line $line${name:+ naming '$name'}" >&2
    echo "$output" >&2
    exit 1
  fi
done
echo "Generated Dart misuse refused: ${#expected[@]} expected analyzer errors."
