#!/bin/bash
# This script will run the TPC-H queries and compare timings.

REPO_ROOT=$(git rev-parse --show-toplevel)
RELEASE_BUILD_DIR="$REPO_ROOT/target/release"
TPCH_DIR="$REPO_ROOT/perf/tpc-h"
DB_FILE="$TPCH_DIR/TPC-H.db"
QUERIES_DIR="$TPCH_DIR/queries"
LIMBO_BIN="$RELEASE_BUILD_DIR/limbo"
SQLITE_BIN="sqlite3" # Assuming sqlite3 is in PATH

# Function to clear system caches based on OS
clear_caches() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sync
        sudo purge
    elif [[ "$OSTYPE" == "linux-gnu"* ]] || [[ "$OSTYPE" == "linux"* ]]; then
        # Linux
        sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    else
        echo "Warning: Cache clearing not supported on this OS ($OSTYPE)." >&2
    fi
}

# Ensure the Limbo binary exists
if [ ! -f "$LIMBO_BIN" ]; then
    echo "Error: Limbo binary not found at $LIMBO_BIN"
    echo "Please build Limbo first (e.g., by running benchmark.sh or 'cargo build --bin limbo --release')"
    exit 1
fi

# Ensure the SQLite binary exists
if ! command -v $SQLITE_BIN >/dev/null 2>&1; then
    echo "Error: sqlite3 command not found. Please install sqlite3."
    exit 1
fi

# Ensure the database file exists
if [ ! -f "$DB_FILE" ]; then
    echo "Error: TPC-H database not found at $DB_FILE"
    echo "Please ensure the database is downloaded (e.g., by running benchmark.sh)"
    exit 1
fi

echo "Starting TPC-H query timing comparison..."

# Initial cache clear
echo "The script might ask you to enter the password for sudo, in order to clear system caches."
clear_caches

for query_file in $(ls "$QUERIES_DIR"/*.sql | sort -V); do
    if [ -f "$query_file" ]; then
        query_name=$(basename "$query_file")

        # If the query file starts with "-- LIMBO_SKIP: ...", skip it and print the reason
        if head -n1 "$query_file" | grep -q "^-- LIMBO_SKIP: "; then
            skip_reason=$(head -n1 "$query_file" | sed 's/^-- LIMBO_SKIP: //')
            echo "Skipping $query_name, reason: $skip_reason"
            echo "-----------------------------------------------------------"
            continue
        fi

        echo "Running $query_name with Limbo..." >&2
        # Clear caches before Limbo run
        clear_caches
        # Run Limbo
        limbo_output=$( { time -p "$LIMBO_BIN" "$DB_FILE" --quiet --output-mode list "$(cat $query_file)" 2>&1; } 2>&1)
        limbo_non_time_lines=$(echo "$limbo_output" | grep -v -e "^real" -e "^user" -e "^sys")
        limbo_real_time=$(echo "$limbo_output" | grep "^real" | awk '{print $2}')
        echo "Running $query_name with SQLite3..." >&2
        # Clear caches before SQLite execution
        clear_caches
        sqlite_output=$( { time -p "$SQLITE_BIN" "$DB_FILE" "$(cat $query_file)" 2>&1; } 2>&1)
        sqlite_non_time_lines=$(echo "$sqlite_output" | grep -v -e "^real" -e "^user" -e "^sys")
        sqlite_real_time=$(echo "$sqlite_output" | grep "^real" | awk '{print $2}')
        echo "Limbo real time: $limbo_real_time"
        echo "SQLite3 real time: $sqlite_real_time"
        echo "Limbo output:"
        echo "$limbo_non_time_lines"
        echo "SQLite3 output:"
        echo "$sqlite_non_time_lines"
        output_diff=$(diff <(echo "$limbo_non_time_lines") <(echo "$sqlite_non_time_lines"))
        if [ -n "$output_diff" ]; then
            echo "Output difference:"
            echo "$output_diff"
        else
            echo "No output difference"
        fi
    else
        echo "Warning: Skipping non-file item $query_file"
    fi
    echo "-----------------------------------------------------------"
done

echo "-----------------------------------------------------------"
echo "TPC-H query timing comparison completed." 