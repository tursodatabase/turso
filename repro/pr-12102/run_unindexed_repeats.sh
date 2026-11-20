#!/usr/bin/env bash
# Runs the two unindexed cases (plain, generated) of the pull benchmark 3 times
# for each variant, in alternating order, and prints the median pull times.
# Use it to see if a small difference between the variants is noise.
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_binaries pull
D=$OUT/pull_bench_unindexed
mkdir -p "$D"
SIZES=${SIZES:-1000,2000,4000,10000,20000,40000}

for r in 1 2 3; do
  for case in plain generated; do
    if [ $(( (r + ${#case}) % 2 )) -eq 0 ]; then order="base pr"; else order="pr base"; fi
    for v in $order; do
      echo "$(date +%T) $case, $v, run $r"
      PULL_REPRO_TOTAL_ROWS=50000 PULL_REPRO_CASES=$case PULL_REPRO_SIZES=$SIZES PULL_REPRO_REPEATS=1 \
        "$BIN/pull_$v" large_update_pull_scaling --ignored --nocapture --test-threads=1 \
        > "$D/$case.$v.r$r.out" 2> "$D/$case.$v.r$r.err" || true
      if ! grep -q '^test result: ok' "$D/$case.$v.r$r.out"; then
        echo "The benchmark failed. See $D/$case.$v.r$r.out." >&2
        exit 1
      fi
    done
  done
done
python3 "$KIT/summarize_pull_bench.py" "$D" | tee "$D/comparison.csv"
