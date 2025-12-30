#!/bin/bash
# This script will run the TPC-H queries and compare timings.

export RUST_LOG=off
REPO_ROOT=$(git rev-parse --show-toplevel)
RELEASE_BUILD_DIR="$REPO_ROOT/target/release"
TPCH_DIR="$REPO_ROOT/perf/tpc-h"
DB_FILE="$TPCH_DIR/TPC-H.db"
QUERIES_DIR="$TPCH_DIR/queries"
LIMBO_BIN="$RELEASE_BUILD_DIR/tursodb"
CURRENT_TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_FILE="$TPCH_DIR/results_${CURRENT_TIMESTAMP}.txt"

declare -A LIMBO_TIMES_WITHOUT_ANALYZE
declare -A SQLITE_TIMES_WITHOUT_ANALYZE
declare -A LIMBO_TIMES_WITH_ANALYZE
declare -A SQLITE_TIMES_WITH_ANALYZE

# Install sqlite3 locally if needed
"$REPO_ROOT/scripts/install-sqlite3.sh"
SQLITE_BIN="$REPO_ROOT/.sqlite3/sqlite3"

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

# Function to run all queries
run_queries() {
    local mode=$1
    echo "Running queries in $mode mode..."
    echo "MODE: $mode" >> "$RESULTS_FILE"
    echo "query,limbo_seconds,sqlite_seconds" >> "$RESULTS_FILE"

    local mode_exit_code=0
    
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
            limbo_output=$( { time -p RUST_LOG=off "$LIMBO_BIN" "$DB_FILE" --quiet --output-mode list "$(cat $query_file)" 2>&1; } 2>&1)
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
            echo "$query_name,$limbo_real_time,$sqlite_real_time" >> "$RESULTS_FILE"
            echo "Limbo output:"
            echo "$limbo_non_time_lines"
            echo "SQLite3 output:"
            echo "$sqlite_non_time_lines"
            output_diff=$(diff <(echo "$limbo_non_time_lines") <(echo "$sqlite_non_time_lines"))
            if [ -n "$output_diff" ]; then
                echo "Output difference:"
                echo "$output_diff"
                # Ignore differences for query 1 due to floating point precision incompatibility
                if [ "$query_file" = "$QUERIES_DIR/1.sql" ]; then
                    echo "Ignoring output difference for query 1 (known floating point precision incompatibility)"
                else
                    mode_exit_code=1
                fi
            else
                echo "No output difference"
            fi

            if [ "$mode" = "WITHOUT ANALYZE" ]; then
                LIMBO_TIMES_WITHOUT_ANALYZE["$query_name"]="$limbo_real_time"
                SQLITE_TIMES_WITHOUT_ANALYZE["$query_name"]="$sqlite_real_time"
            else
                LIMBO_TIMES_WITH_ANALYZE["$query_name"]="$limbo_real_time"
                SQLITE_TIMES_WITH_ANALYZE["$query_name"]="$sqlite_real_time"
            fi
        else
            echo "Warning: Skipping non-file item $query_file"
        fi
        echo "-----------------------------------------------------------"
    done
    echo "" >> "$RESULTS_FILE"
    return $mode_exit_code
}

# Ensure the Limbo binary exists
if [ ! -f "$LIMBO_BIN" ]; then
    echo "Error: Limbo binary not found at $LIMBO_BIN"
    echo "Please build Limbo first (e.g., by running benchmark.sh or 'cargo build --bin tursodb --release')"
    exit 1
fi

# Ensure the SQLite binary exists
if [ ! -x "$SQLITE_BIN" ]; then
    echo "Error: sqlite3 binary not found at $SQLITE_BIN"
    echo "Please run scripts/install-sqlite3.sh first."
    exit 1
fi

# Ensure the database file exists
if [ ! -f "$DB_FILE" ]; then
    echo "Error: TPC-H database not found at $DB_FILE"
    echo "Please ensure the database is downloaded (e.g., by running benchmark.sh)"
    exit 1
fi

echo "Starting TPC-H query timing comparison..."
echo "Writing timing results to $RESULTS_FILE"
echo "TPC-H timing results ($CURRENT_TIMESTAMP)" > "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# Initial cache clear
echo "The script might ask you to enter the password for sudo, in order to clear system caches."
clear_caches

exit_code=0

# Drop statistics tables
echo "Dropping statistics tables..."
"$SQLITE_BIN" "$DB_FILE" "DROP TABLE IF EXISTS sqlite_stat1; DROP TABLE IF EXISTS sqlite_stat4;"

# Run queries without ANALYZE
echo "==========================================================="
echo "Running queries WITHOUT ANALYZE"
echo "==========================================================="
run_queries "WITHOUT ANALYZE"
if [ $? -ne 0 ]; then
    exit_code=1
fi

# Run ANALYZE
echo "==========================================================="
echo "Running ANALYZE..."
echo "==========================================================="
"$SQLITE_BIN" "$DB_FILE" "ANALYZE;"

# Run queries with ANALYZE
echo "==========================================================="
echo "Running queries WITH ANALYZE"
echo "==========================================================="
run_queries "WITH ANALYZE"
if [ $? -ne 0 ]; then
    exit_code=1
fi

echo "DIFF: WITH ANALYZE - WITHOUT ANALYZE" >> "$RESULTS_FILE"
echo "query,limbo_delta_seconds,sqlite_delta_seconds" >> "$RESULTS_FILE"
for query_file in $(ls "$QUERIES_DIR"/*.sql | sort -V); do
    if [ -f "$query_file" ]; then
        query_name=$(basename "$query_file")
        if head -n1 "$query_file" | grep -q "^-- LIMBO_SKIP: "; then
            continue
        fi
        limbo_with=${LIMBO_TIMES_WITH_ANALYZE["$query_name"]}
        limbo_without=${LIMBO_TIMES_WITHOUT_ANALYZE["$query_name"]}
        sqlite_with=${SQLITE_TIMES_WITH_ANALYZE["$query_name"]}
        sqlite_without=${SQLITE_TIMES_WITHOUT_ANALYZE["$query_name"]}
        limbo_delta=$(awk -v w="$limbo_with" -v wo="$limbo_without" 'BEGIN{if(w==""||wo==""){print "NA"} else {printf "%.6f", w-wo}}')
        sqlite_delta=$(awk -v w="$sqlite_with" -v wo="$sqlite_without" 'BEGIN{if(w==""||wo==""){print "NA"} else {printf "%.6f", w-wo}}')
        echo "$query_name,$limbo_delta,$sqlite_delta" >> "$RESULTS_FILE"
    fi
done

echo "-----------------------------------------------------------"
echo "TPC-H query timing comparison completed." 

if [ $exit_code -ne 0 ]; then
    echo "Error: Output differences found"
    exit $exit_code
fi
