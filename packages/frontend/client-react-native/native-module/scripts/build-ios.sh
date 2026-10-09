#!/usr/bin/env bash
set -euo pipefail

module_dir="$(cd "$(dirname "$0")/.." && pwd)"
workspace_dir="${AXTON_WORKSPACE_DIR:-}"
if [[ -z "$workspace_dir" ]]; then
  candidate="$module_dir"
  while [[ "$candidate" != "/" ]]; do
    if [[ -f "$candidate/bindings/mobile/Cargo.toml" ]]; then
      workspace_dir="$candidate"
      break
    fi
    candidate="$(dirname "$candidate")"
  done
fi
if [[ -z "$workspace_dir" || ! -f "$workspace_dir/bindings/mobile/Cargo.toml" ]]; then
  echo "AXTON workspace not found; set AXTON_WORKSPACE_DIR" >&2
  exit 1
fi
configuration="${CONFIGURATION:-Release}"
profile="release"
if [[ "$configuration" == "Debug" ]]; then
  profile="debug"
fi

case "${1:-simulator}" in
  simulator)
    rust_target="aarch64-apple-ios-sim"
    ;;
  device)
    rust_target="aarch64-apple-ios"
    ;;
  *)
    echo "usage: $0 [simulator|device]" >&2
    exit 2
    ;;
esac

build_args=(build -p axton-mobile --target "$rust_target" --locked)
if [[ "$profile" == "release" ]]; then
  build_args+=(--release)
fi
cargo "${build_args[@]}" --manifest-path "$workspace_dir/Cargo.toml"
mkdir -p "$module_dir/ios/lib"
cp "$workspace_dir/target/$rust_target/$profile/libaxton_mobile.a" "$module_dir/ios/lib/libaxton_mobile.a"
