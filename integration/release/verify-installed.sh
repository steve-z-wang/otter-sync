#!/usr/bin/env bash
# Installs the packed AXTON npm packages into a scratch project outside the
# repository and exercises them there: the installed `axton` CLI, the
# generated TypeScript, ESM imports and require(esm), a local client database
# that is written and reopened, and a round trip against a disposable
# PostgreSQL backend. Nothing in the project imports the repository.
#
#   bash integration/release/verify-installed.sh            pack this host's packages first
#   bash integration/release/verify-installed.sh PACK_DIR   use the *.tgz archives in PACK_DIR
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
source "$root/scripts/env.sh"
work="$(mktemp -d "${TMPDIR:-/tmp}/axton-installed.XXXXXX")"
cleanup() {
  [[ -f "$work/pg/data/postmaster.pid" ]] && pg_ctl -D "$work/pg/data" -m immediate stop >/dev/null 2>&1
  rm -rf -- "$work"
}
trap cleanup EXIT
fail() { echo "verify-installed: $*" >&2; exit 1; }

packs="${1:-}"
if [[ -z "$packs" ]]; then
  bash "$root/scripts/build.sh"
  host="$(node -p 'require(process.argv[1]).host.find((t) => t.rust === process.argv[2]).name' "$root/scripts/release/targets.json" "$(rustc -vV | sed -n 's/^host: //p')")"
  (cd "$root" && cargo build --release --locked -p axton-compiler)
  "$root/node_modules/.bin/napi" build --platform --release --cwd "$root/packages/native" \
    --manifest-path ../../bindings/node/Cargo.toml --output-dir "$work/artifacts" --no-js
  mkdir -p "$work/artifacts/$host"
  cp "$root/target/release/axton" "$work/artifacts/$host/axton"
  packs="$work/packs"
  bash "$root/scripts/release/pack.sh" "$packs" "$work/artifacts" "$host"
fi
packs="$(cd "$packs" && pwd)"
shopt -s nullglob
archives=("$packs"/*.tgz)
[[ ${#archives[@]} -gt 0 ]] || fail "no .tgz archives in $packs"

project="$work/project"
cp -R "$root/integration/release/installed" "$project"
cd "$project"
dev() { node -p 'require(process.argv[1]).devDependencies[process.argv[2]]' "$root/package.json" "$1"; }
npm install --no-audit --no-fund --loglevel=error "${archives[@]}" \
  "typescript@$(dev typescript)" "@types/node@$(dev @types/node)" "pg@$(dev pg)" "@types/pg@$(dev @types/pg)"

# The installed packages carry compiled JavaScript and nothing from the source tree.
sources="$(find node_modules/@axtonjs -name '*.mts' ! -name '*.d.mts')"
[[ -z "$sources" ]] || fail "TypeScript sources installed: $sources"
if grep -rlE --include='*.*js' --include='*.d.*ts' 'bindings/node|packages/(server|client-js|postgres)' node_modules/@axtonjs; then
  fail "installed packages reference the source tree"
fi

# The CLI: version, argument errors and code generation with public imports.
version="$(node -p 'require("@axtonjs/cli/package.json").version')"
[[ "$(node_modules/.bin/axton --version)" == "axton $version" ]] || fail "axton --version does not report $version"
set +e
node_modules/.bin/axton compile >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 1 ]] || fail "axton forwarded exit status $status for a usage error, expected 1"
node_modules/.bin/axton compile models generated
grep -q 'from "@axtonjs/server"' generated/backend.ts || fail "generated backend does not import @axtonjs/server"
grep -q 'from "@axtonjs/client"' generated/client.ts || fail "generated client does not import @axtonjs/client"
node_modules/.bin/tsc -p tsconfig.json

# ESM and CommonJS consumers; the drizzle entry point once its peer is present.
node imports.mjs
node require.cjs
npm install --no-audit --no-fund --loglevel=error "drizzle-orm@$(dev drizzle-orm)"
node drizzle.mjs

# A local database and a round trip against a disposable PostgreSQL cluster.
port="$(node -e 'const s=require("net").createServer().listen(0,"127.0.0.1",()=>{console.log(s.address().port);s.close()})')"
mkdir -p "$work/pg"
initdb -D "$work/pg/data" -A trust --no-locale -E UTF8 >/dev/null
pg_ctl -D "$work/pg/data" -l "$work/pg/log" -o "-p $port -h 127.0.0.1 -k $work/pg" start >/dev/null
DATABASE_URL="postgresql://$(id -un)@127.0.0.1:$port/postgres" node --test installed.test.mts
node --test loading.test.mjs
echo "verify-installed: ${#archives[@]} archives verified from $packs"
