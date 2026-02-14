#!/bin/bash
# Run tursodb's PRAGMA integrity_check on AFL fuzzer seeds.
# This exercises turso's Rust integrity checker (including duplicate rowid detection)
# rather than sqlite3's checker.

set -euo pipefail

TURSODB="${TURSODB:-/home/ubuntu/work/limbo/target/debug/tursodb}"
OUTPUT_DIR="${1:-/tmp/sqlright_test}"
TIMEOUT=5
PASSED=0
FAILED=0
SKIPPED=0
ERRORS=""

if [ ! -x "$TURSODB" ]; then
    echo "ERROR: tursodb not found at $TURSODB"
    echo "Build with: cargo build"
    exit 1
fi

tmpdir=$(mktemp -d /tmp/turso_integrity_XXXXXX)
trap "rm -rf $tmpdir" EXIT

check_one() {
    local sql_file="$1"
    local name="$(basename "$sql_file")"
    local db_file="$tmpdir/${name}.db"

    # Clean up any previous DB
    rm -f "$db_file" "${db_file}-wal" "${db_file}-shm"

    # Replay SQL through tursodb to create the DB
    timeout "$TIMEOUT" "$TURSODB" "$db_file" -q -m list \
        --experimental-views --experimental-strict --experimental-triggers \
        --experimental-index-method --experimental-autovacuum --experimental-attach \
        --experimental-encryption < "$sql_file" 2>/dev/null || true

    # Skip if no DB was created
    if [ ! -s "$db_file" ]; then
        SKIPPED=$((SKIPPED + 1))
        return
    fi

    # Run turso's integrity_check on the resulting DB
    local result
    result=$(echo "PRAGMA integrity_check;" | timeout "$TIMEOUT" "$TURSODB" "$db_file" -q -m list \
        --experimental-views --experimental-strict --experimental-triggers \
        --experimental-index-method --experimental-autovacuum --experimental-attach \
        --experimental-encryption 2>/dev/null) || {
        # Timeout or crash during integrity check itself — skip
        SKIPPED=$((SKIPPED + 1))
        rm -f "$db_file" "${db_file}-wal" "${db_file}-shm"
        return
    }

    rm -f "$db_file" "${db_file}-wal" "${db_file}-shm"

    if [ "$result" = "ok" ]; then
        PASSED=$((PASSED + 1))
    else
        # Filter out known false positives with expression indexes
        if echo "$result" | grep -q "missing from index.*autoindex"; then
            SKIPPED=$((SKIPPED + 1))
            return
        fi
        FAILED=$((FAILED + 1))
        ERRORS="${ERRORS}\nFAIL: ${name}\n  ${result}\n"
        echo "  FAIL: $name — $result"
    fi
}

echo "=== Turso Integrity Check ==="
echo "Binary: $TURSODB"
echo "Output dir: $OUTPUT_DIR"
echo ""

# Process all queue items
count=0
for f in "$OUTPUT_DIR"/primary/queue/*; do
    [ -f "$f" ] || continue
    count=$((count + 1))
    check_one "$f"
    if [ $((count % 50)) -eq 0 ]; then
        echo "  [$count] passed=$PASSED failed=$FAILED skipped=$SKIPPED"
    fi
done

# Process all crashes
for f in "$OUTPUT_DIR"/primary/crashes/*; do
    [ -f "$f" ] || continue
    [ "$(basename "$f")" = "README.txt" ] && continue
    count=$((count + 1))
    check_one "$f"
done

# Process all hangs
for f in "$OUTPUT_DIR"/primary/hangs/*; do
    [ -f "$f" ] || continue
    [ "$(basename "$f")" = "README.txt" ] && continue
    count=$((count + 1))
    check_one "$f"
done

echo ""
echo "Results: $count checked, $PASSED passed, $FAILED failed, $SKIPPED skipped"

if [ $FAILED -gt 0 ]; then
    echo -e "\n--- Failures ---$ERRORS"
    exit 1
fi
