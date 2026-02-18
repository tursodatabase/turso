#!/bin/bash
# Clone, patch, and build SQLRight for Turso fuzzing.
# Usage: ./setup.sh [--force]
set -e

# Check platform
if [ "$(uname)" = "Darwin" ]; then
    echo "❌ ERROR: SQLRight is not supported on macOS"
    echo ""
    echo "SQLRight requires Linux-specific AFL components that are incompatible with macOS."
    echo ""
    echo "Options:"
    echo "  1. Use Docker:"
    echo "     docker pull steveleungsly/sqlright_sqlite:version1.2"
    echo ""
    echo "  2. Use a Linux VM (UTM, Parallels, VMware, etc.)"
    echo ""
    echo "  3. Use WSL2 on Windows"
    echo ""
    echo "See README.md for more details."
    exit 1
fi

FORCE=false
[ "$1" = "--force" ] && FORCE=true

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build"
SQLRIGHT_REPO="https://github.com/PSU-Security-Universe/sqlright.git"
SQLRIGHT_COMMIT="9457f03"

# Quick check: already built?
if [ "$FORCE" = false ] && [ -x "$BUILD_DIR/afl-fuzz" ] && [ -d "$BUILD_DIR/init_lib" ]; then
    echo "SQLRight already built at $BUILD_DIR/afl-fuzz"
    echo "  Re-run with --force to rebuild."
    exit 0
fi

echo "=== SQLRight Setup for Turso ==="

# Check prerequisites
for cmd in g++ bison flex make git; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd not found. Install with:"
        echo "  sudo apt-get install -y build-essential bison flex"
        exit 1
    fi
done

if ! command -v cargo &>/dev/null; then
    echo "Error: cargo not found. Install Rust from https://rustup.rs/"
    exit 1
fi

if ! cargo afl --version &>/dev/null 2>&1; then
    echo "Error: cargo-afl not found. Install with:"
    echo "  cargo install cargo-afl && cargo afl config --build --force"
    exit 1
fi

# Clone SQLRight
if [ -d "$BUILD_DIR/sqlright" ]; then
    echo "Using existing clone at $BUILD_DIR/sqlright"
else
    echo "Cloning SQLRight..."
    mkdir -p "$BUILD_DIR"
    git clone --depth 1 "$SQLRIGHT_REPO" "$BUILD_DIR/sqlright"
fi

cd "$BUILD_DIR/sqlright"
git checkout "$SQLRIGHT_COMMIT" 2>/dev/null || git checkout -b turso "$SQLRIGHT_COMMIT"

# Apply patches (reset first so re-running is safe)
echo "Applying patches..."
git checkout -- . 2>/dev/null
git apply "$SCRIPT_DIR/patches/"*.patch
echo "  Patches applied."

# Build SQLRight's custom afl-fuzz
echo "Building SQLRight..."
cd SQLite/docker/src
make clean 2>/dev/null || true
make -j"$(nproc)"
echo "  Built: afl-fuzz, test-parser, query-minimizer"

# Copy artifacts to build directory for easy access
cp afl-fuzz "$BUILD_DIR/"
cp -r ../fuzz_root/init_lib "$BUILD_DIR/"
cp ../fuzz_root/pragma "$BUILD_DIR/"

echo ""
echo "=== Setup Complete ==="
echo "  afl-fuzz:  $BUILD_DIR/afl-fuzz"
echo "  init_lib:  $BUILD_DIR/init_lib/ ($(ls "$BUILD_DIR/init_lib/" | wc -l) files)"
echo "  pragma:    $BUILD_DIR/pragma"
echo ""
echo "Next steps:"
echo "  1. Build instrumented tursodb:  cargo afl build --bin tursodb"
echo "  2. Run fuzzer:                  $SCRIPT_DIR/run.sh"
