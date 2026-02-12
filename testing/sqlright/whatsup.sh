#!/bin/bash
# SQLRight-compatible afl-whatsup replacement.
# Reads SQLRight's AFL 2.56b fuzzer_stats format.
# Usage: ./whatsup.sh [OUTPUT_DIR]
set -e

DIR="${1:-/tmp/sqlright_test}"

if [ ! -d "$DIR" ]; then
    echo "Error: directory not found: $DIR"
    exit 1
fi

CUR_TIME=$(date +%s)

ALIVE=0
DEAD=0
TOTAL_EXECS=0
TOTAL_EPS=0
TOTAL_CRASHES=0
TOTAL_HANGS=0
TOTAL_PFAV=0
TOTAL_PENDING=0
TOTAL_PATHS=0
MAX_COVERAGE="0.00"

echo "SQLRight Fuzzer Status"
echo "======================"
echo ""

for stats in "$DIR"/*/fuzzer_stats; do
    [ -f "$stats" ] || continue
    instance=$(basename "$(dirname "$stats")")

    pid=$(grep "^fuzzer_pid" "$stats" | awk '{print $3}')
    start=$(grep "^start_time" "$stats" | awk '{print $3}')
    execs=$(grep "^execs_done" "$stats" | awk '{print $3}')
    eps=$(grep "^execs_per_sec" "$stats" | awk '{print $3}')
    paths=$(grep "^paths_total" "$stats" | awk '{print $3}')
    found=$(grep "^paths_found" "$stats" | awk '{print $3}')
    pfav=$(grep "^pending_favs" "$stats" | awk '{print $3}')
    pending=$(grep "^pending_total" "$stats" | awk '{print $3}')
    crashes=$(grep "^unique_crashes" "$stats" | awk '{print $3}')
    hangs=$(grep "^unique_hangs" "$stats" | awk '{print $3}')
    bitmap=$(grep "^bitmap_cvg" "$stats" | awk '{print $3}')
    stability=$(grep "^stability" "$stats" | awk '{print $3}')
    cycles=$(grep "^cycles_done" "$stats" | awk '{print $3}')
    depth=$(grep "^max_depth" "$stats" | awk '{print $3}')
    slowest=$(grep "^slowest_exec_ms" "$stats" | awk '{print $3}')

    is_alive="no"
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        is_alive="yes"
        ALIVE=$((ALIVE + 1))
    else
        DEAD=$((DEAD + 1))
    fi

    runtime=$((CUR_TIME - start))
    days=$((runtime / 86400))
    hrs=$(((runtime % 86400) / 3600))
    mins=$(((runtime % 3600) / 60))

    printf ">>> %-15s  alive=%-3s  %dd %dh %dm  cycles=%s  execs=%s  eps=%s  cov=%s  stab=%s  crashes=%s  hangs=%s  depth=%s  found=%s  slowest=%sms\n" \
        "$instance" "$is_alive" "$days" "$hrs" "$mins" \
        "${cycles:-0}" "${execs:-0}" "${eps:-0}" "${bitmap:-?}" "${stability:-?}" \
        "${crashes:-0}" "${hangs:-0}" "${depth:-0}" "${found:-0}" "${slowest:-?}"

    TOTAL_EXECS=$((TOTAL_EXECS + ${execs:-0}))
    TOTAL_CRASHES=$((TOTAL_CRASHES + ${crashes:-0}))
    TOTAL_HANGS=$((TOTAL_HANGS + ${hangs:-0}))
    TOTAL_PFAV=$((TOTAL_PFAV + ${pfav:-0}))
    TOTAL_PENDING=$((TOTAL_PENDING + ${pending:-0}))
    TOTAL_PATHS=$((TOTAL_PATHS + ${found:-0}))

    # Track max coverage
    cov_num=$(echo "$bitmap" | tr -d '%')
    if [ -n "$cov_num" ] && command -v bc >/dev/null 2>&1; then
        if [ "$(echo "$cov_num > $MAX_COVERAGE" | bc 2>/dev/null)" = "1" ]; then
            MAX_COVERAGE="$cov_num"
        fi
    fi
done

echo ""
echo "Summary"
echo "======="
echo "  Fuzzers alive  : $ALIVE"
echo "  Fuzzers dead   : $DEAD"
echo "  Total execs    : $TOTAL_EXECS"
echo "  Total crashes  : $TOTAL_CRASHES"
echo "  Total hangs    : $TOTAL_HANGS"
echo "  Pending favs   : $TOTAL_PFAV"
echo "  Pending total  : $TOTAL_PENDING"
echo "  New paths      : $TOTAL_PATHS"
echo "  Max coverage   : ${MAX_COVERAGE}%"
