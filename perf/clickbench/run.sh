#!/bin/bash
# This is a modified version of the clickbench benchmark script from:
# https://github.com/ClickHouse/ClickBench/tree/main/sqlite
# It runs the queries in the queries.sql file, and prints the results for both Limbo and Sqlite.

TRIES=1

REPO_ROOT=$(git rev-parse --show-toplevel)
RELEASE_BUILD_DIR=$REPO_ROOT/target/release
CLICKBENCH_DIR=$REPO_ROOT/perf/clickbench

# Function to clear system caches based on OS
clear_caches() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sync
        sudo purge
    else
        # Linux
        sync
        echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    fi
}

# Clear caches once
echo "The script might ask you to enter the password for sudo, in order to clear system caches."
clear_caches
count=1;

# Run the queries, skipping any that are commented out
# Between each invocation to Limbo/Sqlite, purge the caches
grep -v '^--' "$CLICKBENCH_DIR/queries.sql" | while read -r query; do

    echo "$count $query"
    ((echo "$count $query") 2>&1) | tee -a clickbench-limbo.txt > /dev/null
    ((echo "$count $query") 2>&1) | tee -a clickbench-sqlite3.txt >/dev/null
    for _ in $(seq 1 $TRIES); do
        clear_caches
        echo "----limbo syscall IO ----"
        ((time "$RELEASE_BUILD_DIR/limbo" --vfs syscall --quiet "$CLICKBENCH_DIR/mydb" <<< "${query}") 2>&1) | tee -a clickbench-limbo.txt
        clear_caches
        echo "----limbo io_uring IO ----"
        clear_caches
        ((time "$RELEASE_BUILD_DIR/limbo" --quiet "$CLICKBENCH_DIR/mydb" <<< "${query}") 2>&1) | tee -a clickbench-limbo.txt
        echo "----sqlite----"
        clear_caches
        ((time sqlite3 "$CLICKBENCH_DIR/mydb" <<< "${query}") 2>&1) | tee -a clickbench-sqlite3.txt
    done;
    count=$(($count+1))
done;
