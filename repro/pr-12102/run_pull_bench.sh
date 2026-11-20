#!/usr/bin/env bash
# Runs the pull benchmark of the PR (large_update_pull_scaling) for both variants.
# Each (table case, variant) runs in its own process. The variant that runs
# first alternates between cases, as in the PR description.
#
# Do not build or run other work during the benchmark. In my runs, a parallel
# build made one 10,000-row base pull 45% slower (122 s against 83 s).
#
# Set SIZES to run fewer sizes, for example SIZES=1000,2000 for a short check.
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_binaries pull
D=$OUT/pull_bench
mkdir -p "$D"
SIZES=${SIZES:-1000,2000,4000,10000,20000,40000}

run() {
  local case=$1 v=$2
  echo "$(date +%T) $case, $v"
  PULL_REPRO_TOTAL_ROWS=50000 PULL_REPRO_CASES=$case PULL_REPRO_SIZES=$SIZES PULL_REPRO_REPEATS=1 \
    "$BIN/pull_$v" large_update_pull_scaling --ignored --nocapture --test-threads=1 \
    > "$D/$case.$v.out" 2> "$D/$case.$v.err" || true
  # The test compares the schema and the contents of both replicas with the remote.
  if ! grep -q '^test result: ok' "$D/$case.$v.out"; then
    echo "The benchmark failed for $case, $v. See $D/$case.$v.out and $D/$case.$v.err." >&2
    exit 1
  fi
}

run indexed base
run indexed pr
run generated_indexed pr
run generated_indexed base
run plain base
run plain pr
run generated pr
run generated base
python3 "$KIT/summarize_pull_bench.py" "$D" | tee "$D/comparison.csv"
