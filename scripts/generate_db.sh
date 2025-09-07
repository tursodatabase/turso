#!/usr/bin/env bash
set -e

REPO_ROOT=$(git rev-parse --show-toplevel)
TESTING_BASE_DIR="$REPO_ROOT/testing/tmp_db"

# Get job ID (from GNU parallel)
JOB_ID=$1
if [ -z "$JOB_ID" ]; then
    TESTING_DIR="$TESTING_BASE_DIR"
else
    TESTING_DIR="$TESTING_BASE_DIR/$JOB_ID"
    mkdir -p "$TESTING_DIR"
fi

TESTING_DUMP="$REPO_ROOT/testing/tmp_db/testing.dump"
DATABASE="$TESTING_DIR/testing.db"
ROWID_DATABASE="$TESTING_DIR/testing_norowidalias"
SMALL_DATABASE="$TESTING_DIR/testing_small"

if [ ! -f "$DATABASE" ]; then
    sqlite3 "$DATABASE" < "$TESTING_DUMP"
fi

if [ ! -f "$ROWID_DATABASE.db" ]; then
    sed -E 's/INTEGER PRIMARY KEY/INT PRIMARY KEY/g' "$TESTING_DUMP" > "$TESTING_DIR/testing_norowidalias.dump"
    sqlite3 "$ROWID_DATABASE" < "$ROWID_DATABASE.dump"
fi

if [ ! -f "$SMALL_DATABASE.db" ]; then
    sqlite3 "$SMALL_DATABASE.db" < "$SMALL_DATABASE.dump"
fi

sleep 1 # release file handles
echo "Databases generated in $TESTING_DIR"
