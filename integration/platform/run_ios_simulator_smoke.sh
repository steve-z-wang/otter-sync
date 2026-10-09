#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/axton-ios-smoke.XXXXXX")"
app_dir="$work/ios_smoke"
bundle_id="dev.localfirststate.axtonIosSmoke"
device_name="Local First State Smoke $$"
device_id=""
launch_pid=""

cleanup() {
  if [[ -n "$launch_pid" ]]; then
    kill "$launch_pid" >/dev/null 2>&1 || true
    wait "$launch_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$device_id" ]]; then
    xcrun simctl shutdown "$device_id" >/dev/null 2>&1 || true
    xcrun simctl delete "$device_id" >/dev/null 2>&1 || true
  fi
  rm -rf -- "$work"
}
trap cleanup EXIT

source "$repo_root/scripts/env.sh"
cd "$repo_root"

# The app runs as a consumer would: a copy outside the repository depends on a
# staged axton package whose build hook bundles the simulator libraries, named
# and hashed as a release names them, from the app's local_artifacts directory.
cp -R "$script_dir/ios_smoke" "$app_dir"
rm -rf "$app_dir/build" "$app_dir/.dart_tool"
version="$(sed -n 's/^version: \([^ ]*\).*/\1/p' "$repo_root/packages/frontend/dart/pubspec.yaml")"
mkdir -p "$app_dir/build/axton-libraries" "$work/axton"
node -e '
  const table = require(process.argv[1]);
  console.log(JSON.stringify({ host: [], mobile: table.mobile.filter((t) => t.platform === "ios-simulator") }));
' "$repo_root/scripts/release/targets.json" >"$work/targets.json"
for target in $(node -p 'require(process.argv[1]).mobile.map((t) => `${t.name}:${t.rust}`).join(" ")' "$work/targets.json"); do
  name="${target%%:*}" triple="${target#*:}"
  rustup target add "$triple"
  IPHONEOS_DEPLOYMENT_TARGET=13.0 cargo build --locked -p axton-dart --target "$triple"
  cp "$repo_root/target/$triple/debug/libaxton_dart.dylib" "$app_dir/build/axton-libraries/libaxton_dart-$version-$name.dylib"
done
git ls-files packages/frontend/dart | tar -cf - -T - | tar -xf - -C "$work/axton" --strip-components=3
(cd "$repo_root/packages/frontend/dart" && dart run tool/write_native_manifest.dart \
  --artifacts "$app_dir/build/axton-libraries" --package "$work/axton" --targets "$work/targets.json")
printf 'dependency_overrides:\n  axton:\n    path: %s\n' "$work/axton" >"$app_dir/pubspec_overrides.yaml"

runtime_id="$(xcrun simctl list runtimes available | awk '/iOS 18[.]/ { gsub(/[()]/, "", $NF); print $NF; exit }')"
device_type="$(xcrun simctl list devicetypes | awk -F '[()]' '/iPhone 16/ { print $2; exit }')"
if [[ -z "$runtime_id" || -z "$device_type" ]]; then
  echo "No available iOS runtime or iPhone 16 simulator device type" >&2
  exit 1
fi

device_id="$(xcrun simctl create "$device_name" "$device_type" "$runtime_id")"
xcrun simctl boot "$device_id"
xcrun simctl bootstatus "$device_id" -b

(
  cd "$app_dir"
  flutter pub get
  flutter build ios --simulator --debug
)
app_bundle="$app_dir/build/ios/iphonesimulator/Runner.app"
xcrun simctl install "$device_id" "$app_bundle"
data_container="$(xcrun simctl get_app_container "$device_id" "$bundle_id" data)"
result_file="$data_container/tmp/axton-smoke-result.txt"
stage_file="$data_container/tmp/axton-smoke-stage.txt"
console_file="$data_container/tmp/axton-smoke-console.txt"
: >"$console_file"

wait_for_result() {
  local expected="$1"
  local attempt
  for attempt in $(seq 1 180); do
    if [[ -f "$result_file" ]] && [[ "$(head -n 1 "$result_file")" == "$expected" ]]; then
      echo "$expected"
      return 0
    fi
    if [[ -f "$result_file" ]] && grep -q '^FAIL:' "$result_file"; then
      [[ -f "$stage_file" ]] && echo "Last stage: $(cat "$stage_file")" >&2
      cat "$result_file" >&2
      cat "$console_file" >&2
      return 1
    fi
    sleep 1
  done
  echo "Timed out waiting for $expected" >&2
  [[ -f "$stage_file" ]] && echo "Last stage: $(cat "$stage_file")" >&2
  [[ -f "$result_file" ]] && cat "$result_file" >&2
  cat "$console_file" >&2
  return 1
}

launch_app() {
  xcrun simctl launch --console "$device_id" "$bundle_id" >>"$console_file" 2>&1 &
  launch_pid="$!"
}
finish_launch() {
  kill "$launch_pid" >/dev/null 2>&1 || true
  wait "$launch_pid" >/dev/null 2>&1 || true
  launch_pid=""
}

launch_app
wait_for_result AXTON_SMOKE_PHASE1_OK
finish_launch
# Ending the console launch usually ends the app too.
xcrun simctl terminate "$device_id" "$bundle_id" >/dev/null 2>&1 || true
launch_app
wait_for_result AXTON_SMOKE_RESTART_OK
finish_launch
cat "$console_file"
