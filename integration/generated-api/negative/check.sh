#!/usr/bin/env bash
# The generated Dart API must refuse misuse at analysis time. Every case in
# misuse.dart has to be reported; a fixture that analyzes cleanly, or that fails
# for an unrelated reason, fails this check.
set -uo pipefail
dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
output="$(dart analyze "$dir" 2>&1)"
status=$?
if [[ $status -eq 0 ]]; then
  echo 'Generated Dart API misuse unexpectedly analyzed cleanly.' >&2
  exit 1
fi
expected=(
  "The getter 'channels' isn't defined for the type 'GeneratedClient'"
  "The getter 'channels' isn't defined for the type 'GeneratedTransaction'"
  "The named parameter 'tags' isn't defined"          # list as a query predicate, and in a mutation patch
  "There's no constant named 'byStatus' in 'EntryOrderField'"
  "The argument type 'String' can't be assigned to the parameter type 'DateTime'"
  "The method 'watch' isn't defined for the type 'EntryTxModel'"
  "The getter 'mutate' isn't defined for the type 'GeneratedTransaction'"
  "The getter 'actions' isn't defined for the type 'GeneratedTransaction'"
  "The getter 'mutate' isn't defined for the type 'Transaction'"
  "The getter 'actions' isn't defined for the type 'Transaction'"
  "The named parameter 'id' isn't defined"
  "The argument type 'Null' can't be assigned to the parameter type 'String'"
  "There's no constant named 'typo' in 'Status'"
  "'archived' is deprecated and shouldn't be used. archive with RemoveEntries instead"
  "'index' is deprecated and shouldn't be used. counters are not indexed"
  "'maybe' is deprecated and shouldn't be used. use entries"
  "'active' can't be used as a setter because it's final"
  "'stream' can't be used as a setter because it's final"
  "The method 'get' isn't defined for the type 'Streams'"
  "'phase' can't be used as a setter because it's final"
  "The method 'cancel' isn't defined for the type 'Function'"
  "The method 'refresh' isn't defined for the type 'Subscription'"
  "The named parameter 'memo' is required, but there's no corresponding argument"
  "The argument type 'String' can't be assigned to the parameter type 'Present<String?>?'"
  "The named parameter 'id' is required, but there's no corresponding argument"
  "The getter 'missing' isn't defined for the type 'Entry'"
  "The getter 'row' isn't defined for the type 'StoreDelete<EntryIdentity, Entry>'"
  "The getter 'mutations' isn't defined for the type 'GeneratedTransaction'"
  "The named parameter 'unknown' isn't defined"
  # Model Fetch (#153)
  "The named parameter 'at' is required, but there's no corresponding argument"
  "The argument type 'int' can't be assigned to the parameter type 'String'"
  "The argument type 'Entry' can't be assigned to the parameter type 'EntryIdentity'"
  "The argument type 'Map<String, bool>' can't be assigned to the parameter type 'bool'"
  "The named parameter 'once' isn't defined"
  "The named parameter 'refresh' isn't defined"
  "The getter 'fetch' isn't defined for the type 'GeneratedTransaction'"
  "The getter 'fetchModel' isn't defined for the type 'Transaction'"
  # Transactional Mutation enqueue
  "The getter 'call' isn't defined for the type 'TransactionMutations'"
  "The getter 'queries' isn't defined for the type 'ApplicationTransaction'"
  "The getter 'fetch' isn't defined for the type 'ApplicationTransaction'"
  "The argument type 'Future<void> Function(GeneratedTransaction)' can't be assigned to the parameter type 'Future<void> Function(CompanionContext)?'"
  "A value of type 'Call<PublishEntryOutput>' can't be assigned to a variable of type 'Call<String>'"
  "The named parameter 'local' isn't defined"
  "The getter 'mutations' isn't defined for the type 'CompanionContext'"
  "The getter 'streams' isn't defined for the type 'CompanionContext'"
  "The getter 'transaction' isn't defined for the type 'CompanionContext'"
  "The method 'watch' isn't defined for the type 'CompositionTxModel'"
)
failed=0
for message in "${expected[@]}"; do
  if ! grep -Fq "$message" <<<"$output"; then
    echo "Expected analyzer error not reported: $message" >&2
    failed=1
  fi
done
if [[ "$(grep -Fc "The named parameter 'tags' isn't defined" <<<"$output")" -lt 2 ]]; then
  echo "Expected 'tags' to be refused both as a filter and as a patch field." >&2
  failed=1
fi
if [[ "$(grep -Fc "The named parameter 'local' isn't defined" <<<"$output")" -lt 2 ]]; then
  echo "Expected 'local' to be refused on both standalone and direct Mutations." >&2
  failed=1
fi
# Fetch repeats two refusals: a DateTime identity component, and Fetch in both
# the generated and the onStore transaction.
for message in "The argument type 'String' can't be assigned to the parameter type 'DateTime'" \
  "The getter 'fetch' isn't defined for the type 'GeneratedTransaction'"; do
  if [[ "$(grep -Fc "$message" <<<"$output")" -lt 2 ]]; then
    echo "Expected '$message' at least twice." >&2
    failed=1
  fi
done
if [[ $failed -ne 0 ]]; then
  echo "$output" >&2
  exit 1
fi
echo "Generated Dart API refuses misuse: $(grep -c ' error - ' <<<"$output") analyzer errors, all expected."
