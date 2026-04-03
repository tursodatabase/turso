#!/usr/bin/env bash
#
# Build unmodified SQLite C extensions into Turso WASM modules.
#
# Usage:
#   ./build.sh                    # Build rot13.wasm (default)
#   ./build.sh myext.c myext_init # Build myext.wasm with entry point myext_init
#
# Requirements:
#   - wasi-sdk installed (set WASI_SDK or it defaults to /opt/wasi-sdk)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SHIM_DIR="$SCRIPT_DIR/../../c/sqlite3_wasm"

: "${WASI_SDK:=/opt/wasi-sdk}"
CC="${WASI_SDK}/bin/clang"

if [ ! -x "$CC" ]; then
    echo "Error: wasi-sdk not found at $WASI_SDK" >&2
    echo "Install wasi-sdk or set WASI_SDK=/path/to/wasi-sdk" >&2
    exit 1
fi

# Default: build rot13
SOURCE="${1:-rot13.c}"
ENTRY="${2:-sqlite3_rot_init}"
BASENAME="$(basename "$SOURCE" .c)"
OUTPUT="${SCRIPT_DIR}/${BASENAME}.wasm"

EXPORT_FLAGS="-Wl,--export=turso_malloc -Wl,--export=turso_ext_init -Wl,--export=__turso_call"

echo "Building ${BASENAME}.wasm (entry: ${ENTRY})..."

"$CC" --sysroot="${WASI_SDK}/share/wasi-sysroot" \
    -O2 \
    -mexec-model=reactor \
    -I "${SHIM_DIR}" \
    -DTURSO_SQLITE3_ENTRY="${ENTRY}" \
    "${SHIM_DIR}/sqlite3_wasm_shim.c" \
    "${SCRIPT_DIR}/${SOURCE}" \
    -o "${OUTPUT}" \
    $EXPORT_FLAGS

echo "Built: ${OUTPUT} ($(wc -c < "$OUTPUT") bytes)"
