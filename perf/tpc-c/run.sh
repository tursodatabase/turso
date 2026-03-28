#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export TURSO_ROOT="$(cd "$SCRIPT_DIR" && git rev-parse --show-toplevel)"
TPCC_DIR="$SCRIPT_DIR/tpcc-turso"
TPCC_REPO="https://github.com/PThorpe92/tpcc-turso.git"

if [ ! -d "$TPCC_DIR" ]; then
    echo "==> Cloning tpcc-turso benchmark..."
    git clone "$TPCC_REPO" "$TPCC_DIR"
fi

echo "==> Running TPC-C benchmark (turso root: $TURSO_ROOT)"
exec "$TPCC_DIR/run_bench.sh" "$@"
