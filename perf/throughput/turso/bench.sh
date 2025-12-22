#!/bin/bash

RUNS=${1:-10}
CMD="cargo run --release --bin write-throughput -- -t 6 --mode legacy -i 1000"

echo "Running benchmark $RUNS times..."
echo "Command: $CMD"
echo

sum=0
count=0
results=()

for i in $(seq 1 $RUNS); do
    # Clean up db files
    rm -f write_throughput_test.db write_throughput_test.db-wal 2>/dev/null

    # Run command with timeout
    timeout 10s bash -c "$CMD" > /tmp/bench_output_$$ 2>&1
    exit_code=$?
    
    if [[ $exit_code -eq 124 ]]; then
        # Timeout occurred (exit code 124 from timeout command)
        printf "Run %2d: HUNG (exceeded 10 seconds), continuing...\n" "$i"
        continue
    fi
    
    output=$(cat /tmp/bench_output_$$)
    rm -f /tmp/bench_output_$$
    
    throughput=$(echo "$output" | grep -E '^Turso,' | cut -d',' -f5)

    if [[ -n "$throughput" ]]; then
        results+=("$throughput")
        sum=$(echo "$sum + $throughput" | bc)
        count=$((count + 1))
        printf "Run %2d: %.2f ops/sec\n" "$i" "$throughput"
    else
        printf "Run %2d: FAILED\n" "$i"
        echo "$output" | tail -5
    fi
done

echo
if [[ $count -gt 0 ]]; then
    avg=$(echo "scale=2; $sum / $count" | bc)
    echo "================================"
    echo "Successful runs: $count / $RUNS"
    echo "Average: $avg ops/sec"

    # Calculate min/max
    min=${results[0]}
    max=${results[0]}
    for r in "${results[@]}"; do
        if (( $(echo "$r < $min" | bc -l) )); then min=$r; fi
        if (( $(echo "$r > $max" | bc -l) )); then max=$r; fi
    done
    echo "Min: $min ops/sec"
    echo "Max: $max ops/sec"
else
    echo "All runs failed!"
    exit 1
fi
