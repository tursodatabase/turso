# Shared helpers for the Go ORM suite runners. Source this from <suite>/run.sh.
set -euo pipefail

GO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$GO_DIR/../../.." && pwd)"
CACHE="$GO_DIR/.cache"
RESULTS="$REPO/testing/orm-suites/results"
LIB_DIR="${CARGO_TARGET_DIR:-$REPO/target}/debug"

# Build the native library that the "turso" database/sql driver loads.
build_turso() {
  (cd "$REPO" && cargo build -q -p turso_sync_sdk_kit)
  (cd "$GO_DIR/junit" && go build -o "$CACHE/junit" .)
}

# fetch <git-url> <tag> <dir>: shallow-clone a pinned tag into the cache, once.
fetch() {
  local dir="$CACHE/$3"
  if [ ! -d "$dir" ]; then
    git clone -q --depth 1 -b "$2" -c advice.detachedHead=false "$1" "$dir"
  fi
  echo "$dir"
}

# add_turso <module-dir> <package-dir> <package-name>: make the "turso" driver
# available to the package's tests. The driver is built from bindings/go in this
# repo, and the native library is loaded from LIB_DIR instead of the prebuilt
# copy embedded in turso-go-platform-libs. The library is loaded from a
# package-level variable because those are set before any init() runs, and some
# suites open their database in init(). An optional 4th argument is Go code
# appended to the shim file.
add_turso() {
  local mod="$1" pkg="$2" name="$3" extra="${4:-}"
  cat >"$pkg/zz_turso_shim_test.go" <<EOF
package $name

import (
	libs "github.com/tursodatabase/turso-go-platform-libs"
	turso "turso.tech/database/tursogo"
)

var _ = func() bool {
	turso.InitLibrary(libs.LoadTursoLibraryConfig{LoadStrategy: libs.SystemLibraryLoadStrategy})
	return true
}()
$extra
EOF
  (cd "$mod" &&
    go mod edit -replace "turso.tech/database/tursogo=$REPO/bindings/go" \
      -require "turso.tech/database/tursogo@v0.0.0-00010101000000-000000000000" &&
    go mod tidy >/dev/null 2>&1 || go mod tidy)
}

# run_tests <variant> <suite> <dir> <go test args...>: run `go test` in <dir>
# and write JUnit XML to results/go-<suite>/<variant>.xml.
run_tests() {
  local variant="$1" suite="$2" dir="$3"
  shift 3
  local out="$RESULTS/go-$suite"
  mkdir -p "$out"
  export TMPDIR="$CACHE/tmp-$suite-$variant"
  rm -rf "$TMPDIR" && mkdir -p "$TMPDIR"
  (cd "$dir" && DYLD_LIBRARY_PATH="$LIB_DIR" LD_LIBRARY_PATH="$LIB_DIR" \
    go test -json -count=1 -p 1 -timeout "${GO_TEST_TIMEOUT:-30m}" "$@" 2>&1 || true) |
    tee "$CACHE/go-$suite-$variant.jsonl" | "$CACHE/junit" >"$out/$variant.xml"
  unset TMPDIR
  if ! grep -q '<testcase' "$out/$variant.xml"; then
    echo "no tests ran for $suite/$variant; see $CACHE/go-$suite-$variant.jsonl" >&2
    exit 1
  fi
  echo "wrote $out/$variant.xml"
}
