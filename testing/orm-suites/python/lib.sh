# Shared setup for the Python ORM suites. Source it from a suite's run.sh.
set -euo pipefail

PY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$PY_DIR/../../.." && pwd)"
CACHE="$PY_DIR/.cache"
VENV="$CACHE/venv"
RESULTS_ROOT="$REPO_ROOT/testing/orm-suites/results"
# Read by shim/turso_sqlite3.py. ATTACH is experimental in turso; failures it
# causes are not counted as findings.
export TURSO_EXPERIMENTAL_FEATURES="${TURSO_EXPERIMENTAL_FEATURES-attach}"

# Creates the venv and builds pyturso from this checkout into it.
# Set SKIP_TURSO_BUILD=1 to reuse the pyturso already installed there.
setup_venv() {
    mkdir -p "$CACHE"
    [ -x "$VENV/bin/python" ] || uv venv -q --python 3.13 "$VENV"
    export VIRTUAL_ENV="$VENV" PATH="$VENV/bin:$PATH"
    uv pip install -q maturin "$@"
    if [ -z "${SKIP_TURSO_BUILD:-}" ]; then
        CARGO_TARGET_DIR="$REPO_ROOT/target" maturin develop --uv -q -m "$REPO_ROOT/bindings/python/Cargo.toml"
    fi
    # Not $PY_DIR itself: its django/ directory would shadow the django package.
    export PYTHONPATH="$PY_DIR/shim${PYTHONPATH:+:$PYTHONPATH}"
}

# fetch <name> <git url> <tag>: shallow clone of the tag into $CACHE/<name>.
fetch() {
    local dir="$CACHE/$1"
    if [ "$(git -C "$dir" describe --tags 2>/dev/null)" != "$3" ]; then
        rm -rf "$dir"
        git clone -q -c advice.detachedHead=false --depth 1 --branch "$3" "$2" "$dir"
    fi
}
