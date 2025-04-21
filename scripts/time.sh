#!/bin/bash

VFS="$1"
SQL="$2"
N="$3"
CMD=(./target/release/limbo --vfs "$VFS" test.db "$SQL")

total=0
times=()

echo "Running: ${CMD[*]}"
echo "Iterations: $N"
echo "-------------------------------------"

for i in $(seq 1 $N); do
    t=$(/usr/bin/time -f "%e" "${CMD[@]}" 2>&1 >/dev/null)
    times+=($t)
    total=$(awk "BEGIN {print $total + $t}")
    echo "Run $i: ${t}s"
done

echo "-------------------------------------"
echo "All run times: ${times[*]}"
avg=$(awk "BEGIN {print $total / $N}")
echo "Average time: ${avg}s"
