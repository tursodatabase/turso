#!/bin/bash
# Compare TPC-H query performance between main branch and current branch
# Usage: ./compare.sh [main_binary] [current_binary]
# This is for manual testing convenience.
#
# Environment knobs:
#   TPCH_QUERY_GLOB  glob selecting queries under perf/tpc-h/queries (default: *.sql)
#   TPCH_REPS        timed repetitions per binary, best one wins (default: 2)
#   TPCH_PRERUN      command run before every timed run, outside the timing
#                    (e.g. dropping the OS page cache to measure cold reads)
#   TPCH_RAW_LOG     file to append every individual repetition time to

if [ -z "$1" ]; then
    echo "Error: main binary path required as first argument" >&2
    exit 1
fi
MAIN_BIN=$1
CURR_BIN=${2:-$(./scripts/cargo-target-dir)/release/tursodb}
DB=perf/tpc-h/TPC-H.db
QUERY_GLOB=${TPCH_QUERY_GLOB:-*.sql}
REPS=${TPCH_REPS:-2}
if [ ! -f "$DB" ]; then
    echo "Error: Database file '$DB' not found" >&2
    exit 1
fi
if [ ! -f "$MAIN_BIN" ]; then
    echo "Error: Main binary '$MAIN_BIN' not found" >&2
    exit 1
fi
if [ ! -f "$CURR_BIN" ]; then
    echo "Error: Current binary '$CURR_BIN' not found" >&2
    exit 1
fi
QUERIES_DIR=perf/tpc-h/queries
QUERY_PATHS=("$QUERIES_DIR"/$QUERY_GLOB)

if [ ! -e "${QUERY_PATHS[0]}" ]; then
    echo "Error: No queries matched '$QUERY_GLOB' in '$QUERIES_DIR'" >&2
    exit 1
fi

# %R prints the elapsed wall time as plain seconds, so a run over a minute long
# stays a single number instead of the "1m2.345s" that needs unpacking.
TIMEFORMAT=%R

# Times one run of $1 and prints the milliseconds it took.
time_run() {
    local bin=$1 sql=$2 secs
    [ -n "$TPCH_PRERUN" ] && eval "$TPCH_PRERUN"
    secs=$( { time $bin $DB "$sql" > /dev/null 2>&1; } 2>&1 | tail -1 )
    awk -v s="$secs" 'BEGIN { printf "%.0f", s * 1000 }'
}

printf "%-8s %12s %12s %10s\n" "Query" "Main (ms)" "Current (ms)" "Delta"
printf "%-8s %12s %12s %10s\n" "-----" "---------" "------------" "-----"

for q in $(printf '%s\n' "${QUERY_PATHS[@]}" | sort -V); do
    # Skip if first line contains LIMBO_SKIP
    if head -1 "$q" | grep -q "LIMBO_SKIP"; then
        continue
    fi

    qname=$(basename "$q" .sql)
    sql=$(cat "$q")

    # Run main branch, take best
    main_best=999999999
    for ((i = 0; i < REPS; i++)); do
        t=$(time_run "$MAIN_BIN" "$sql")
        [ -n "$TPCH_RAW_LOG" ] && echo "Q$qname main rep$i $t" >> "$TPCH_RAW_LOG"
        if (( $(echo "$t < $main_best" | bc -l) )); then
            main_best=$t
        fi
    done

    # Run current branch, take best
    curr_best=999999999
    for ((i = 0; i < REPS; i++)); do
        t=$(time_run "$CURR_BIN" "$sql")
        [ -n "$TPCH_RAW_LOG" ] && echo "Q$qname curr rep$i $t" >> "$TPCH_RAW_LOG"
        if (( $(echo "$t < $curr_best" | bc -l) )); then
            curr_best=$t
        fi
    done

    # Calculate delta. Divide before scaling and bc rounds the ratio to a tenth,
    # which lands every delta on a multiple of 10%, so do the percentage in awk.
    if (( $(echo "$main_best > 0" | bc -l) )); then
        delta_str=$(awk -v m="$main_best" -v c="$curr_best" \
            'BEGIN { printf "%.1f%%", ((c - m) / m) * 100 }')
    else
        delta_str="N/A"
    fi

    printf "Q%-7s %12.0f %12.0f %10s\n" "$qname" "$main_best" "$curr_best" "$delta_str"
done
