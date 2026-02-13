#!/bin/bash
# Quick diagnostic for AFL fuzzer state

OUTPUT_DIR="${1:-/tmp/sqlright_test}"
VERBOSE="${2:-}"

echo "=== SQLRight Fuzzer Monitor ==="
echo "Output: $OUTPUT_DIR"
echo ""

total_execs=0
total_crashes=0
total_pending=0
total_instances=0
max_coverage="0.00"
crash_warning_count=0
slow_instances=()
total_eps=0

# Collect stats
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

    # Track max coverage (remove % sign for comparison)
    cov_num=$(echo "$bitmap" | tr -d '%')
    if [ -n "$cov_num" ] && (( $(echo "$cov_num > $max_coverage" | bc -l 2>/dev/null || echo 0) )); then
        max_coverage="$cov_num"
    fi

    # Sum EPS (convert to integer for addition)
    eps_int=$(echo "$eps" | cut -d. -f1)
    total_eps=$((total_eps + eps_int))

    if [ "$execs_since_crash" = "0" ]; then
        crash_warning_count=$((crash_warning_count + 1))
    fi

    # Flag truly stuck instances (< 5 execs and eps=0)
    if (( execs < 5 )) && (( $(echo "$eps == 0" | bc -l 2>/dev/null || echo 0) )); then
        slow_instances+=("$instance: execs=$execs eps=$eps crashes=$crashes")
    fi
done

# Display summary
if [ "$total_instances" -gt 0 ]; then
    avg_pending=$((total_pending / total_instances))
    echo "SUMMARY: $total_instances instances | $total_execs execs | $total_crashes crashes | ${total_eps} eps | ${max_coverage}% cov"
    echo ""

    # Show warnings
    if [ "$crash_warning_count" -gt 0 ]; then
        echo "⚠️  $crash_warning_count instances with execs_since_crash=0 (crashing on every exec)"
    fi

    # Show slow instances (limit to 5 if there are many)
    if [ "${#slow_instances[@]}" -gt 0 ]; then
        echo ""
        if [ "${#slow_instances[@]}" -gt 10 ]; then
            echo "Stuck instances: ${#slow_instances[@]} (showing first 5)"
            for i in {0..4}; do
                [ -n "${slow_instances[$i]}" ] && echo "  ${slow_instances[$i]}"
            done
        else
            echo "Stuck instances (${#slow_instances[@]}):"
            for instance_info in "${slow_instances[@]}"; do
                echo "  $instance_info"
            done
        fi
    fi

    # Crash file count
    crash_count=$(find "$OUTPUT_DIR" -path "*/crashes/*" -type f ! -name "README.txt" 2>/dev/null | wc -l)
    echo ""
    echo "Total crash files: $crash_count"

    # Verbose mode: show detailed crash analysis
    if [ "$VERBOSE" = "--verbose" ] || [ "$VERBOSE" = "-v" ]; then
        if [ "$crash_count" -gt 0 ]; then
            echo ""
            echo "=== Crash Distribution ==="
            for crash_dir in "$OUTPUT_DIR"/*/crashes/; do
                [ -d "$crash_dir" ] || continue
                instance=$(basename "$(dirname "$crash_dir")")
                count=$(find "$crash_dir" -type f ! -name "README.txt" 2>/dev/null | wc -l)
                [ "$count" -gt 0 ] && echo "$instance: $count crashes"
            done

            echo ""
            echo "=== Crash Source Analysis ==="
            first_crash=$(find "$OUTPUT_DIR"/primary/crashes/ -type f ! -name "README.txt" 2>/dev/null | head -1)
            if [ -n "$first_crash" ]; then
                echo "First crash: $(basename "$first_crash")"
                echo "Crashes by source seed:"
                find "$OUTPUT_DIR" -path "*/crashes/*" -type f ! -name "README.txt" -exec basename {} \; 2>/dev/null | \
                    grep -o "src:[0-9]*" | sort | uniq -c | head -10
            fi
        fi
    else
        echo ""
        echo "Run with --verbose for detailed crash analysis"
    fi
else
    echo "No fuzzer_stats files found yet - AFL instances are still initializing/calibrating..."
    echo ""
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
