#!/bin/bash
# Quick diagnostic for AFL fuzzer state

OUTPUT_DIR="${1:-/tmp/sqlright_test}"

echo "=== AFL Fuzzer Diagnostic ==="
echo "Output: $OUTPUT_DIR"
echo ""

total_execs=0
total_crashes=0
total_pending=0
total_instances=0

for stats in "$OUTPUT_DIR"/*/fuzzer_stats; do
    [ -f "$stats" ] || continue
    instance=$(basename "$(dirname "$stats")")

    execs=$(grep "execs_done" "$stats" | awk '{print $3}')
    crashes=$(grep "unique_crashes" "$stats" | awk '{print $3}')
    pending=$(grep "pending_total" "$stats" | awk '{print $3}')
    execs_since_crash=$(grep "execs_since_crash" "$stats" | awk '{print $3}')
    cycle=$(grep "cycles_done" "$stats" | awk '{print $3}')
    bitmap=$(grep "bitmap_cvg" "$stats" | awk '{print $3}')
    eps=$(grep "execs_per_sec" "$stats" | awk '{print $3}')

    total_execs=$((total_execs + execs))
    total_crashes=$((total_crashes + crashes))
    total_pending=$((total_pending + pending))
    total_instances=$((total_instances + 1))

    printf "%-12s execs=%6d eps=%6s crashes=%d pending=%5d cycle=%d bitmap=%s\n" \
        "$instance:" "$execs" "$eps" "$crashes" "$pending" "$cycle" "$bitmap"

    if [ "$execs_since_crash" = "0" ]; then
        echo "  ⚠️  execs_since_crash=0 (every execution crashes!)"
    fi
done

echo ""
if [ "$total_instances" -gt 0 ]; then
    echo "TOTAL: instances=$total_instances execs=$total_execs crashes=$total_crashes avg_pending=$((total_pending / total_instances))"
else
    echo "No fuzzer_stats files found yet - AFL instances are still initializing/calibrating..."
    echo "AFL processes running: $(ps aux | grep afl-fuzz | grep -v grep | wc -l)"

    # Show calibration progress indicators
    if [ -f "$OUTPUT_DIR/primary/.cur_output" ]; then
        cur_output_age=$(stat -c %Y "$OUTPUT_DIR/primary/.cur_output" 2>/dev/null)
        now=$(date +%s)
        age_sec=$((now - cur_output_age))
        echo "Last test execution: ${age_sec}s ago (actively running: $([ $age_sec -lt 10 ] && echo "YES" || echo "NO"))"
    fi

    # Check queue processing
    total_seeds=$(find "$OUTPUT_DIR/primary/queue" -type f -name "id:*" 2>/dev/null | wc -l)
    state_files=$(find "$OUTPUT_DIR/primary/queue/.state" -type f 2>/dev/null | wc -l)
    echo "Seeds in queue: $total_seeds, State files created: $state_files"

    # CPU usage
    afl_cpu=$(ps aux | grep afl-fuzz | grep -v grep | awk '{sum+=$3} END {print sum}')
    echo "Total AFL CPU usage: ${afl_cpu}%"
fi
echo ""

# Check if there are crashing inputs
crash_count=$(find "$OUTPUT_DIR" -path "*/crashes/*" -type f ! -name "README.txt" | wc -l)
echo "Total crash files: $crash_count"

if [ "$crash_count" -gt 0 ]; then
    echo ""
    echo "=== Crash Distribution ==="
    for crash_dir in "$OUTPUT_DIR"/*/crashes/; do
        [ -d "$crash_dir" ] || continue
        instance=$(basename "$(dirname "$crash_dir")")
        count=$(find "$crash_dir" -type f ! -name "README.txt" | wc -l)
        [ "$count" -gt 0 ] && echo "$instance: $count crashes"
    done

    echo ""
    echo "=== Analyzing crash inputs ==="
    first_crash=$(find "$OUTPUT_DIR"/primary/crashes/ -type f ! -name "README.txt" | head -1)
    if [ -n "$first_crash" ]; then
        echo "First crash: $(basename "$first_crash")"
        echo "All crashes from same source seed?"
        find "$OUTPUT_DIR" -path "*/crashes/*" -type f ! -name "README.txt" -exec basename {} \; | \
            grep -o "src:[0-9]*" | sort | uniq -c
    fi
fi
