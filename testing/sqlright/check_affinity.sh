#!/bin/bash
# Check AFL CPU affinity and detect issues

OUTPUT_DIR="${1:-/tmp/sqlright_test}"

echo "=== AFL CPU Affinity Check ==="
echo ""

# Check if any AFL processes exist
if ! pgrep -f afl-fuzz > /dev/null; then
    echo "No AFL instances running."
    exit 1
fi

# Count instances per CPU
echo "Instances per CPU:"
ps -eLo psr,comm | grep afl-fuzz | awk '{print $1}' | sort -n | uniq -c | while read count cpu; do
    if [ "$count" -gt 1 ]; then
        echo "  ⚠️  CPU $cpu: $count instances (CONFLICT - multiple instances on same core)"
    else
        echo "  ✓  CPU $cpu: $count instance"
    fi
done
echo ""

# Check CPU 0 usage
cpu0_count=$(ps -eLo psr,comm | grep afl-fuzz | awk '$1 == 0' | wc -l)
if [ "$cpu0_count" -gt 0 ]; then
    echo "⚠️  WARNING: $cpu0_count AFL instances on CPU 0 (should be reserved for system)"
else
    echo "✓  CPU 0 is reserved (no AFL instances)"
fi
echo ""

# Show AFL CPU affinity
echo "AFL instance CPU bindings:"
ps aux | grep "afl-fuzz" | grep -v grep | while read -r line; do
    pid=$(echo "$line" | awk '{print $2}')
    instance=$(echo "$line" | grep -oP '(-M|-S) \K\S+' || echo "unknown")
    affinity=$(taskset -cp "$pid" 2>/dev/null | awk '{print $NF}')
    current_cpu=$(ps -o psr= -p "$pid" 2>/dev/null | tr -d ' ')
    echo "  $instance (PID $pid): bound to CPU $affinity, currently on CPU $current_cpu"
done
echo ""

# Check fuzzer stats
if [ -d "$OUTPUT_DIR/primary" ]; then
    echo "Fuzzer throughput (execs/sec):"
    for stats in "$OUTPUT_DIR"/*/fuzzer_stats; do
        [ -f "$stats" ] || continue
        instance=$(basename "$(dirname "$stats")")
        execs_sec=$(grep "execs_per_sec" "$stats" | awk '{print $3}')
        printf "  %-15s %s execs/sec\n" "$instance:" "$execs_sec"
    done | head -10
    echo "  ..."
fi
