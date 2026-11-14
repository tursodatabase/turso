# Shared helpers for the JS ORM suite runners. Source this from <suite>/run.sh.
set -euo pipefail

JS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$JS_DIR/../../.." && pwd)"
CACHE="$JS_DIR/.cache"
SHIM="$JS_DIR/shim/better-sqlite3"
NATIVE="$REPO_ROOT/bindings/javascript/packages/native"

# use_node <major>: each suite pins the Node major its better-sqlite3 version
# works with (better-sqlite3 13 prebuilds segfault on Node 22.12; 10 and 11 do
# not compile on Node 26). Switches with fnm when the current Node differs.
use_node() {
  local major
  major="$(node -p 'process.versions.node.split(".")[0]')"
  if [ "$major" != "$1" ] && command -v fnm > /dev/null; then
    PATH="$(dirname "$(fnm exec --using="$1" which node)"):$PATH"
    major="$(node -p 'process.versions.node.split(".")[0]')"
  fi
  if [ "$major" != "$1" ]; then
    echo "need Node $1 (have $(node --version))" >&2
    exit 1
  fi
}

# Builds the Turso native addon and its JS wrapper from this checkout.
# Set SKIP_TURSO_BUILD=1 to reuse the last build.
build_turso() {
  if [ -n "${SKIP_TURSO_BUILD:-}" ]; then return; fi
  (cd "$REPO_ROOT/bindings/javascript" && yarn install)
  (cd "$NATIVE" && CARGO_TARGET_DIR="$REPO_ROOT/target" npx napi build --platform \
    --esm --manifest-path ../../Cargo.toml --output-dir .)
  # napi rewrites the version strings in these tracked files.
  git -C "$NATIVE" checkout -- index.js index.d.ts
  (cd "$REPO_ROOT/bindings/javascript" &&
    npm run tsc-build --workspace=packages/common --workspace=packages/native)
}

# fetch_repo <dir-name> <git-url> <tag>: shallow clone of a pinned tag into the cache.
fetch_repo() {
  local dest="$CACHE/$1"
  if [ ! -d "$dest" ]; then
    git clone -q --depth 1 --branch "$3" "$2" "$dest"
  fi
  echo "$dest"
}

# use_driver <sqlite|turso> <node_modules dir>...: points each
# <dir>/better-sqlite3 at the stock package or at the Turso shim.
use_driver() {
  local driver="$1"; shift
  local nm
  for nm in "$@"; do
    if [ -L "$nm/.better-sqlite3.stock" ] || [ -e "$nm/.better-sqlite3.stock" ]; then
      rm "$nm/better-sqlite3"
      mv "$nm/.better-sqlite3.stock" "$nm/better-sqlite3"
    fi
    if [ "$driver" = turso ]; then
      mv "$nm/better-sqlite3" "$nm/.better-sqlite3.stock"
      ln -s "$SHIM" "$nm/better-sqlite3"
    fi
  done
}

# mocha_per_file <out.xml> <test file>... [-- <mocha args>]: runs test files in
# separate mocha processes, MOCHA_BATCH files at a time (default 1), so an
# engine panic only loses that batch. A crashed batch is rerun one file at a
# time; a crashed file is reported as one failing test "<file> (process crashed)".
# Per-batch XML and stdout/stderr logs stay in .cache/logs/ for triage.
mocha_per_file() {
  local out="$1"; shift
  local files=() args=() batch=() f
  while [ $# -gt 0 ] && [ "$1" != -- ]; do files+=("$1"); shift; done
  [ $# -gt 0 ] && shift && args=("$@")
  MOCHA_PARTS="$CACHE/logs/$(basename "$(dirname "$out")")-$(basename "$out" .xml)"
  rm -rf "$MOCHA_PARTS" && mkdir -p "$MOCHA_PARTS"
  for f in "${files[@]}"; do
    batch+=("$f")
    if [ "${#batch[@]}" -ge "${MOCHA_BATCH:-1}" ]; then
      mocha_batch "${batch[@]}"
      batch=()
    fi
  done
  [ "${#batch[@]}" -gt 0 ] && mocha_batch "${batch[@]}"
  { echo '<testsuites>'; cat "$MOCHA_PARTS"/*.xml | grep -v '^<?xml'; echo '</testsuites>'; } > "$out"
}

mocha_batch() {
  local part code=0 msg f
  part="$MOCHA_PARTS/$(echo "$1" | tr / _).$#.xml"
  npx mocha ${args[@]+"${args[@]}"} --reporter xunit --reporter-option "output=$part" "$@" \
    > "$part.log" 2>&1 || code=$?
  # mocha's xunit reporter puts the error message in the element text only;
  # copy its first line into a message attribute for the scorer. Also drop
  # control characters (from test names and errors) that XML does not allow.
  [ -f "$part" ] && perl -0pi -e 's/[\x00-\x08\x0B\x0C\x0E-\x1F]//g;
    s/<failure>([^\n<]*)/<failure message="$1">$1/g' "$part"
  if [ "$code" -lt 128 ] && grep -q '</testsuite>' "$part" 2>/dev/null; then return; fi
  msg="$(grep -m1 -E 'panicked|Error' "$part.log" | tr -d '<>&"' | cut -c1-200)"
  if [ $# -gt 1 ]; then
    rm "$part"
    for f in "$@"; do mocha_batch "$f"; done
    return
  fi
  echo "<testsuite><testcase classname=\"$1\" name=\"$1 (process crashed)\"><failure message=\"exit $code: $msg\"/></testcase></testsuite>" > "$part"
  echo "CRASH $1 (exit $code): $msg" >&2
}

results_dir() {
  local dir="$JS_DIR/../results/js-$1"
  mkdir -p "$dir"
  (cd "$dir" && pwd)
}
