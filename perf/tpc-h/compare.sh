#!/bin/bash
# Compare TPC-H query performance between main branch and current branch
# Usage: ./compare.sh [main_binary] [current_binary]
# This is for manual testing convenience.

if [ -z "$1" ]; then
    echo "Error: main binary path required as first argument" >&2
    exit 1
fi
MAIN_BIN=$1
CURR_BIN=${2:-./target/release/tursodb}
DB=perf/tpc-h/TPC-H.db
if [ ! -f "$DB" ]; then
    echo "Error: Database file '$DB' not found" >&2
    exit 1
fi
QUERIES_DIR=perf/tpc-h/queries

printf "%-8s %12s %12s %10s\n" "Query" "Main (ms)" "Current (ms)" "Delta"
printf "%-8s %12s %12s %10s\n" "-----" "---------" "------------" "-----"

for q in $(ls $QUERIES_DIR/*.sql | sort -V); do
    # Skip if first line contains LIMBO_SKIP
    if head -1 "$q" | grep -q "LIMBO_SKIP"; then
        continue
    fi

    qname=$(basename "$q" .sql)
    sql=$(cat "$q")

    # Run main branch twice, take best
    main_best=999999
    for i in 1 2; do
        t=$( { time $MAIN_BIN $DB "$sql" > /dev/null 2>&1; } 2>&1 | grep real | awk -F'[ms]' '{print ($1*60000)+($2*1000)}' )
        if (( $(echo "$t < $main_best" | bc -l) )); then
            main_best=$t
        fi
    done

    # Run current branch twice, take best
    curr_best=999999
    for i in 1 2; do
        t=$( { time $CURR_BIN $DB "$sql" > /dev/null 2>&1; } 2>&1 | grep real | awk -F'[ms]' '{print ($1*60000)+($2*1000)}' )
        if (( $(echo "$t < $curr_best" | bc -l) )); then
            curr_best=$t
        fi
    done

    # Calculate delta
    if (( $(echo "$main_best > 0" | bc -l) )); then
        delta=$(echo "scale=1; (($curr_best - $main_best) / $main_best) * 100" | bc)
        delta_str="${delta}%"
    else
        delta_str="N/A"
    fi

    printf "Q%-7s %12.0f %12.0f %10s\n" "$qname" "$main_best" "$curr_best" "$delta_str"
done
