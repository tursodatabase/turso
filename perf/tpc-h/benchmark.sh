#!/bin/bash
# TPC-H benchmark script

REPO_ROOT=$(git rev-parse --show-toplevel)
RELEASE_BUILD_DIR="$REPO_ROOT/target/release"
TPCH_DIR="$REPO_ROOT/perf/tpc-h"
DB_FILE="$TPCH_DIR/TPC-H.db"

# Install sqlite3 locally if needed
"$REPO_ROOT/scripts/install-sqlite3.sh"
SQLITE3_BIN="$REPO_ROOT/.sqlite3/sqlite3"

# Build Limbo in release mode if it's not already built
if [ ! -f "$RELEASE_BUILD_DIR/tursodb" ]; then
    echo "Building Limbo..."
    cargo build --bin tursodb --release
fi

# Download the TPC-H database if it doesn't exist
DB_URL="https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H.db"
if [ ! -f "$DB_FILE" ]; then
    echo "Downloading TPC-H database..."
    if command -v wget >/dev/null 2>&1; then
        wget -O "$DB_FILE" --no-verbose "$DB_URL"
    elif command -v curl >/dev/null 2>&1; then
        curl -sL -o "$DB_FILE" "$DB_URL"
    else
        echo "Error: Neither wget nor curl is available"
        exit 1
    fi
else
    echo "Using existing TPC-H.db file"
fi

# Run the benchmark
echo "Running TPC-H benchmark..."
"$TPCH_DIR/run.sh" 
