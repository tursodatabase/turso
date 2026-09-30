#!/bin/bash
# diff.sh - Quick comparison of sqlite3 vs tursodb output
#
# Usage:
#   scripts/diff.sh "SQL statements" [label]
#
# Examples:
#   scripts/diff.sh "SELECT 1 + 2;"
#   scripts/diff.sh "CREATE TABLE t(x); INSERT INTO t VALUES(1),(2); SELECT * FROM t;" "insert test"
#   scripts/diff.sh "SELECT typeof(1), typeof(1.0), typeof(NULL);" "types"
#
# Both engines run in :memory: mode with list output (pipe-separated columns).
# Prints PASS/FAIL with a diff on failure. Output is truncated to 20 lines per side.
#
# Quirks:
#   - Always exits 0, even on FAIL. Check stdout for PASS/FAIL.
#   - Error messages differ in format between sqlite3 and tursodb, so error
#     cases will almost always show FAIL. Use this for comparing *results*, not errors.
#   - sqlite3 gets .mode list via heredoc, which makes error messages say "line 2".
#   - Dot commands (.tables, .schema, etc.) are not comparable - sqlite3 handles
#     them but tursodb produces different output for these.
#   - Requires sqlite3 on PATH and cargo in the workspace root.

SQL="$1"
LABEL="${2:-test}"

S=$(sqlite3 :memory: <<< ".mode list
$SQL" 2>&1)
T=$(cargo run -q --bin tursodb -- :memory: "$SQL" --output-mode list 2>&1)

if [ "$S" = "$T" ]; then
    echo "PASS: $LABEL"
else
    echo "FAIL: $LABEL"
    echo "  sqlite3: $(echo "$S" | head -20)"
    echo "  tursodb: $(echo "$T" | head -20)"
    if [ $(echo "$S" | wc -l) -gt 20 ] || [ $(echo "$T" | wc -l) -gt 20 ]; then
        echo "  (truncated)"
    fi
fi
